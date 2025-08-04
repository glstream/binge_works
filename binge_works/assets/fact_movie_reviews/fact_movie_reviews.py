import time
import requests
from datetime import datetime, timedelta
from dagster import (
    asset,
    Output,
    MetadataValue,
    DailyPartitionsDefinition,
    AssetKey,
    AssetExecutionContext,
    StaticPartitionsDefinition, # Included for reference, but Daily is used
)
import dagster as dg

from binge_works.resources.TMDBResource import TMDBResource
# Assuming your postgres resource is correctly defined and imported
# from dagster_postgres import PostgresResource

# Define the daily partitions - Use the same definition as for shows
from ...constants import daily_partitions_def
# Example uses 2023-01-01, adjust as necessary.daily_partitions_def

# --- Partitioned Assets ---
@asset(
    name="get_movies_for_reviews",
    tags={"layer": "silver"},
    kinds={"sql", "postgres", "silver"}, # Removed 'api' kind as TMDB resource not needed here
    required_resource_keys={"postgres"}, # Removed 'tmdb' resource key
    partitions_def=daily_partitions_def # Apply the partitions definition
)
def get_movies_for_reviews(context: AssetExecutionContext):
    """
    Asset to get movies that need review updates for the given partition date.
    Checks movies whose last review capture was before the lookback period relative
    to the partition date, or new movies released within the years_lookback period.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    days_lookback = 7
    years_lookback = 7

    partition_date_str = context.partition_key
    context.log.info(f"Running for partition date: {partition_date_str}")

    movies_to_update = []
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

        with postgres.get_connection() as conn:
            # Assuming connection yields dict-like rows or adjust cursor factory
            with conn.cursor() as cursor:
                # Use partition_date_str and correct date comparison logic
                query = """
                    SELECT m.id
                    FROM tmdb_data.dim_popular_movies m -- Use alias 'm' for movies table
                    LEFT JOIN (
                        SELECT movie_id, MAX(created_date) as last_captured
                        FROM tmdb_data.fact_movie_reviews
                        GROUP BY movie_id
                    ) r ON m.id = r.movie_id
                    WHERE
                        -- Condition 1: Movies never captured before (check release date relative to partition date)
                        (r.last_captured IS NULL
                        -- Ensure newly added movies without reviews aren't infinitely old
                        AND m.release_date >= (%s::DATE - INTERVAL '%s years') -- Use alias 'm'
                        )
                        -- Condition 2: Movies captured, but older than 'days_lookback' from the partition date
                        -- AND the movie's release date is within 'years_lookback' from the partition date
                        OR (
                            r.last_captured < (%s::DATE - INTERVAL '%s days')
                            -- Corrected date comparison below
                            AND m.release_date >= (%s::DATE - INTERVAL '%s years') -- Use alias 'm'
                        )
                    ORDER BY m.popularity DESC; -- Use alias 'm'
                """
                params = (
                    partition_date_str, years_lookback, # For Condition 1
                    partition_date_str, days_lookback,  # For Condition 2 (part 1)
                    partition_date_str, years_lookback  # For Condition 2 (part 2)
                )
                cursor.execute(query, params)

                result = cursor.fetchall()
                # Access by column name 'id' - assuming dict cursor based on prior errors
                movies_to_update = [row['id'] for row in result] if result else []

                context.log.info(f"Found {len(movies_to_update)} movies needing review updates for partition {partition_date_str}.")

    except KeyError as ke:
        context.log.error(f"KeyError accessing fetched results in get_movies_for_reviews. Expected key 'id'. Error: {ke}")
        context.log.error(f"Sample row causing error (if available): {result[0] if result else 'N/A'}")
        raise ke
    except Exception as e:
        context.log.error(f"Error getting movies for partition {partition_date_str}: {e}")
        raise e

    end_time = time.time()
    duration = end_time - start_time
    return Output(
        value=movies_to_update,
        metadata={
            "partition_key": MetadataValue.text(partition_date_str),
            "execution_time": MetadataValue.float(duration),
            "movies_count": MetadataValue.int(len(movies_to_update)), # Renamed from shows_count
            "lookback_days": MetadataValue.int(days_lookback),
            "lookback_years": MetadataValue.int(years_lookback),
            "operation": MetadataValue.text("fetch_ids")
        }
    )

@asset(
    name="fetch_movie_reviews",
    tags={"layer": "silver"},
    kinds={"api", "tmdb", "silver"},
    required_resource_keys={"tmdb"}, # TMDB resource is needed here
    partitions_def=daily_partitions_def,
    # Explicit dependency on the corresponding partition of the upstream asset
    deps=[AssetKey("get_movies_for_reviews")]
)
def fetch_movie_reviews(context: AssetExecutionContext, get_movies_for_reviews: list):
    """
    Asset to fetch movie reviews from TMDB API for the movies identified
    for the given partition date. Applies rate limiting and error handling.
    """
    start_time = time.time()
    partition_date_str = context.partition_key
    # Get TMDB resource from context
    tmdb: TMDBResource = context.resources.tmdb
    context.log.info(f"Fetching movie reviews for partition date: {partition_date_str}")

    movies_to_update = get_movies_for_reviews # Output from the upstream partition
    all_reviews = []
    skipped_movies = [] # Track skipped movies

    if not movies_to_update:
        context.log.info("No movies identified for review fetching in this partition.")
    else:
        context.log.info(f"Fetching reviews for {len(movies_to_update)} movies.")

        for movie_id in movies_to_update:
            # Outer try-except for errors related to a specific movie_id
            try:
                page = 1
                total_pages = 1

                # Inner while loop for pagination with its own error handling
                while page <= total_pages and page <= 5: # Limit to 5 pages per movie
                    try:
                        # Apply rate limiting delay
                        time.sleep(0.2) # Adjust delay as needed

                        context.log.debug(f"Fetching reviews for movie {movie_id}, page {page}")

                        # Use the TMDBResource to get movie reviews
                        reviews_data = tmdb.get_movie_reviews(movie_id=movie_id, page=page)

                        if reviews_data:
                            total_pages = reviews_data.get('total_pages', 1)
                            results = reviews_data.get('results', [])

                            if not results and page == 1:
                                context.log.debug(f"Movie {movie_id} has 0 reviews on page 1.")
                                break # No more pages needed if page 1 is empty

                            for review in results:
                                # Extract and parse data safely
                                created_at_str = review.get('created_at')
                                updated_at_str = review.get('updated_at')
                                try:
                                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')) if created_at_str else None
                                    updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00')) if updated_at_str else None
                                    review_date_id = int(created_at.strftime("%Y%m%d")) if created_at else None
                                except Exception as date_err:
                                    context.log.warning(f"Error parsing dates for movie {movie_id}, review {review.get('id')}: {date_err}")
                                    created_at, updated_at, review_date_id = None, None, None

                                content = review.get('content', '')
                                content_length = len(content)
                                author = review.get('author_details', {})
                                author_name = review.get('author', '')
                                author_username = author.get('username', '')
                                rating = author.get('rating')

                                all_reviews.append({
                                    'tmdb_review_id': review.get('id'),
                                    'movie_id': movie_id, # Correct field name
                                    'author_name': author_name,
                                    'author_username': author_username,
                                    'date_id': review_date_id,
                                    'rating': rating,
                                    'content_length': content_length,
                                    'review_created_at': created_at,
                                    'review_updated_at': updated_at,
                                    'loaded_partition_date': partition_date_str # Add partition date
                                })

                            page += 1
                        else:
                            # API call succeeded but returned no data dict
                            context.log.warning(f"No review data dict returned for movie {movie_id}, page {page}. Moving to next page/movie.")
                            context.log.debug(f"Received data: {reviews_data}")
                            break # Exit while loop for this movie

                    # Handle errors specific to fetching a page (network, parsing etc.)
                    except requests.exceptions.RequestException as req_err:
                        context.log.error(f"HTTP Error fetching reviews for movie {movie_id}, page {page}: {req_err}")
                        break # Stop fetching pages for this movie
                    except Exception as page_err:
                        context.log.warning(f"Error fetching/processing page {page} for movie {movie_id}: {page_err}")
                        break # Stop trying to fetch more pages for this movie

            # Handle errors making it impossible to process the movie_id at all
            except Exception as e:
                context.log.error(f"Failed to process movie {movie_id} due to error: {str(e)}")
                skipped_movies.append(movie_id)
                continue # Skip to the next movie_id

    end_time = time.time()
    duration = end_time - start_time

    # Include skipped movie info in metadata
    return Output(
        value=all_reviews,
        metadata={
            "partition_key": MetadataValue.text(partition_date_str),
            "execution_time": MetadataValue.float(duration),
            "reviews_fetched_count": MetadataValue.int(len(all_reviews)),
            "movies_processed_count": MetadataValue.int(len(movies_to_update)), # Renamed from shows_processed_count
            "movies_skipped_count": MetadataValue.int(len(skipped_movies)), # Renamed
            "skipped_movies": MetadataValue.json(skipped_movies) if skipped_movies else None, # Renamed
            "operation": MetadataValue.text("fetch_data")
        }
    )

@asset(
    name="load_movie_reviews",
    tags={"layer": "gold"},
    kinds={"sql", "postgres", "gold"},
    required_resource_keys={"postgres"},
    partitions_def=daily_partitions_def,
    deps=[AssetKey("fetch_movie_reviews")] # Explicit deps
)
def load_movie_reviews(context: AssetExecutionContext, fetch_movie_reviews: list):
    """
    Asset to load movie reviews data into fact_movie_reviews table for the given partition date.
    Uses ON CONFLICT to update existing reviews based on tmdb_review_id.
    """
    start_time = time.time()
    partition_date_str = context.partition_key
    postgres = context.resources.postgres
    reviews = fetch_movie_reviews # Output from the upstream partition

    context.log.info(f"Loading movie reviews for partition date: {partition_date_str}")

    if not reviews:
        context.log.info("No movie reviews fetched to load for this partition.")
        # Return 0 correctly, matching expected output type (int)
        return Output(
            value=0,
            metadata={
                "partition_key": MetadataValue.text(partition_date_str),
                "reviews_loaded": MetadataValue.int(0),
                "operation": MetadataValue.text("load_data_noop")
            }
        )

    batch_size = 500
    total_inserted_updated = 0

    # Get the partition date object for the new column
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
    except ValueError:
        context.log.error(f"Invalid partition key format: {partition_date_str}. Expected YYYY-MM-DD.")
        # Fail the load if the partition key is bad
        raise ValueError(f"Invalid partition key format: {partition_date_str}")


    try:
        with postgres.get_connection() as conn:
            for i in range(0, len(reviews), batch_size):
                batch = reviews[i:i+batch_size]

                values_template = []
                values_data = []

                for review in batch:
                    # Handle potentially NULL date_id based on fetch logic
                    if review.get('date_id') is None:
                        context.log.debug(f"Review {review.get('tmdb_review_id')} for movie {review.get('movie_id')} has NULL date_id.")

                    # 10 placeholders for 10 columns being inserted
                    values_template.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    values_data.extend([
                        review['tmdb_review_id'],
                        review['movie_id'], # Correct field name
                        review['author_name'],
                        review['author_username'],
                        review.get('date_id'), # Use .get() for safety, handles None
                        review['rating'],
                        review['content_length'],
                        review['review_created_at'], # Should be datetime or None
                        review['review_updated_at'], # Should be datetime or None
                        partition_date # Add the partition date
                    ])

                # Build and execute the query
                insert_query = f"""
                    INSERT INTO tmdb_data.fact_movie_reviews (
                        tmdb_review_id, movie_id, author_name, author_username,
                        date_id, rating, content_length, review_created_at,
                        review_updated_at, loaded_partition_date -- Added column
                        -- created_date uses DEFAULT CURRENT_TIMESTAMP
                    )
                    VALUES {', '.join(values_template)}
                    ON CONFLICT (tmdb_review_id)
                    DO UPDATE SET
                        movie_id = EXCLUDED.movie_id, -- Update movie_id if needed (unlikely)
                        author_name = EXCLUDED.author_name,
                        author_username = EXCLUDED.author_username,
                        date_id = EXCLUDED.date_id,
                        rating = EXCLUDED.rating,
                        content_length = EXCLUDED.content_length,
                        review_updated_at = EXCLUDED.review_updated_at, --# Definitely update this
                        loaded_partition_date = EXCLUDED.loaded_partition_date, --# Update with the current partition date
                        created_date = CURRENT_TIMESTAMP -- Update the DB timestamp on modification
                """

                with conn.cursor() as cursor:
                    cursor.execute(insert_query, values_data)
                    # Commit after each batch
                    conn.commit()

                total_inserted_updated += len(batch)
                context.log.info(f"Inserted/updated {len(batch)} movie reviews (total this partition: {total_inserted_updated})")

    except Exception as e:
        context.log.error(f"Error loading movie reviews for partition {partition_date_str}: {e}")
        # Consider rolling back the transaction if conn was used outside batch loop
        raise e # Re-raise to fail the asset run

    end_time = time.time()
    duration = end_time - start_time
    return Output(
        value=total_inserted_updated, # Return count of loaded reviews
        metadata={
            "partition_key": MetadataValue.text(partition_date_str),
            "execution_time": MetadataValue.float(duration),
            "reviews_loaded": MetadataValue.int(total_inserted_updated),
            "operation": MetadataValue.text("load_data")
        }
    )