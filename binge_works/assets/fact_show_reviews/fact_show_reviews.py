import time
import requests
from datetime import datetime, timedelta
from dagster import (
    asset,
    Output,
    MetadataValue,
    DailyPartitionsDefinition,
    AssetKey,
    AssetExecutionContext, # Use AssetExecutionContext for better type hinting
    StaticPartitionsDefinition, # Added for completeness, but Daily is used
)
import dagster as dg
from binge_works.resources.TMDBResource import TMDBResource


# Define the daily partitions starting from a specific date
# Choose a start date relevant to your data history or when you want processing to begin
from ...constants import daily_partitions_def
daily_partitions_def = daily_partitions_def # Adjust start_date as needed



# --- Partitioned Assets ---
@asset(
    name="get_shows_for_reviews",
    tags={"layer": "silver"},
    kinds={"api", "postgres", "silver"},
    required_resource_keys={"postgres"},
    partitions_def=daily_partitions_def # Apply the partitions definition
)
def get_shows_for_reviews(context: AssetExecutionContext):
    """
    Asset to get shows that need review updates for the given partition date.
    Checks shows whose last review capture was before the lookback period relative 
    to the partition date, or new shows.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    days_lookback = 7  # Can also be configurable via resource or config
    years_lookback = 7 # Can also be configurable via resource or config

    # Get the partition date string (e.g., '2024-03-27')
    partition_date_str = context.partition_key
    context.log.info(f"Running for partition date: {partition_date_str}")
    
    shows_to_update = []
    try:
        # Convert partition date string to date object for comparison if needed, 
        # but casting in SQL is often easier.
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                # Use the partition_date_str in the query instead of NOW() or CURRENT_DATE
                # Cast the partition string to DATE or TIMESTAMP in SQL
                # Note: Using %s for intervals requires careful handling in psycopg2;
                # it's safer to pass interval values directly if possible or construct
                # the interval string safely. Here we pass days/years as params.
                query = """
                    SELECT s.id
                    FROM tmdb_data.dim_popular_shows s
                    LEFT JOIN (
                        SELECT series_id, MAX(created_date) as last_captured
                        FROM tmdb_data.fact_show_reviews
                        GROUP BY series_id
                    ) r ON s.id = r.series_id
                    WHERE 
                        -- Condition 1: Shows never captured before
                        r.last_captured IS NULL 
                        -- Condition 2: Shows captured, but older than 'days_lookback' from the partition date
                        -- AND the show's first air date is within 'years_lookback' from the partition date
                        OR (
                            r.last_captured < (%s::DATE - INTERVAL '%s days')
                            AND s.first_air_date >= (%s::DATE - INTERVAL '%s years') 
                        )
                    ORDER BY s.popularity DESC;
                """
                cursor.execute(query, (partition_date_str, days_lookback, partition_date_str, years_lookback))
                
                result = cursor.fetchall()
                # Note: cursor.fetchall() in psycopg2 by default returns tuples. 
                # If using DictCursor or similar, access would be by column name.
                # Assuming default cursor, access by index.
                shows_to_update = [row['id'] for row in result] if result else []
                context.log.info(f"Found {len(shows_to_update)} shows needing review updates for partition {partition_date_str}.")

    except Exception as e:
        context.log.error(f"Error getting shows for partition {partition_date_str}: {e}")
        # Re-raise or handle appropriately depending on desired failure behavior
        raise e

    end_time = time.time()
    duration = end_time - start_time
    return Output(
        value=shows_to_update,
        metadata={
            "partition_key": MetadataValue.text(partition_date_str),
            "execution_time": MetadataValue.float(duration),
            "shows_count": MetadataValue.int(len(shows_to_update)),
            "lookback_days": MetadataValue.int(days_lookback),
            "lookback_years": MetadataValue.int(years_lookback),
            "operation": MetadataValue.text("fetch_ids")
        }
    )

@asset(
    name="fetch_show_reviews",
    tags={"layer": "silver"},
    kinds={"api", "tmdb", "silver"},
    required_resource_keys={"tmdb"},
    partitions_def=daily_partitions_def,
    deps=[AssetKey("get_shows_for_reviews")]
)
def fetch_show_reviews(context: AssetExecutionContext, get_shows_for_reviews: list):
    """
    Asset to fetch show reviews from TMDB API for the shows identified
    for the given partition date.
    """
    start_time = time.time()
    partition_date_str = context.partition_key
    tmdb = context.resources.tmdb
    context.log.info(f"Fetching reviews for partition date: {partition_date_str}")

    shows_to_update = get_shows_for_reviews # This is the output from the upstream partition
    all_reviews = []
    skipped_shows = []
    
    if not shows_to_update:
        context.log.info("No shows identified for review fetching in this partition.")
    else:
        context.log.info(f"Fetching reviews for {len(shows_to_update)} shows.")
        
        for series_id in shows_to_update:
            try:
                page = 1
                total_pages = 1
                
                # TMDB reviews are paginated
                while page <= total_pages and page <= 5:  # Limit to 5 pages per show
                    try:
                        context.log.debug(f"Fetching reviews for show {series_id}, page {page}")
                        
                        # Use the TMDBResource to get reviews
                        reviews_data = tmdb.get_show_reviews(series_id=series_id, page=page)
                        
                        if reviews_data:
                            total_pages = reviews_data.get('total_pages', 1)
                            
                            for review in reviews_data.get('results', []):
                                # Extract the review date
                                created_at_str = review.get('created_at')
                                updated_at_str = review.get('updated_at')
                                
                                try:
                                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')) if created_at_str else None
                                    updated_at = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00')) if updated_at_str else None
                                    
                                    # Calculate review date_id (YYYYMMDD format) from review's creation time
                                    review_date_id = int(created_at.strftime("%Y%m%d")) if created_at else None
                                except Exception as date_err:
                                    context.log.warning(f"Error parsing dates for show {series_id}, review {review.get('id')}: {date_err}")
                                    created_at = None
                                    updated_at = None
                                    review_date_id = None
                                
                                # Calculate content length
                                content = review.get('content', '')
                                content_length = len(content)
                                
                                # Extract author details
                                author = review.get('author_details', {})
                                author_name = review.get('author', '')
                                author_username = author.get('username', '')
                                rating = author.get('rating')
                                
                                all_reviews.append({
                                    'tmdb_review_id': review.get('id'),
                                    'series_id': series_id,
                                    'author_name': author_name,
                                    'author_username': author_username,
                                    'date_id': review_date_id,
                                    'rating': rating,
                                    'content_length': content_length,
                                    'review_created_at': created_at,
                                    'review_updated_at': updated_at,
                                    'loaded_partition_date': partition_date_str
                                })
                            
                            page += 1
                        else:
                            context.log.warning(f"No review data returned for show {series_id}, page {page}. Moving to next show.")
                            break
                    except Exception as page_err:
                        context.log.warning(f"Error fetching page {page} for show {series_id}: {page_err}")
                        break  # Stop trying to fetch more pages for this show
            
            except requests.exceptions.HTTPError as http_err:
                # Handle 404 errors by logging and skipping the show
                context.log.error(f"HTTP Error fetching reviews for show {series_id}: {http_err}")
                skipped_shows.append(series_id)
                continue  # Skip to the next show
                
            except Exception as e:
                # Handle all other errors by logging and skipping the show
                context.log.error(f"Unexpected error fetching reviews for show {series_id}: {str(e)}")
                skipped_shows.append(series_id)
                continue  # Skip to the next show

    end_time = time.time()
    duration = end_time - start_time
    
    # Include skipped shows in metadata
    return Output(
        value=all_reviews,
        metadata={
            "partition_key": MetadataValue.text(partition_date_str),
            "execution_time": MetadataValue.float(duration),
            "reviews_fetched_count": MetadataValue.int(len(all_reviews)),
            "shows_processed_count": MetadataValue.int(len(shows_to_update)),
            "shows_skipped_count": MetadataValue.int(len(skipped_shows)),
            "skipped_shows": MetadataValue.json(skipped_shows) if skipped_shows else None,
            "operation": MetadataValue.text("fetch_data")
        }
    )
    
@asset(
    name="load_show_reviews",
    tags={"layer": "gold"},
    kinds={"sql", "postgres", "gold"},
    required_resource_keys={"postgres"},
    partitions_def=daily_partitions_def, # Apply the partitions definition
)
def load_show_reviews(context: AssetExecutionContext, fetch_show_reviews: list):
    """
    Asset to load reviews data into fact_show_reviews table for the given partition date.
    Uses ON CONFLICT to update existing reviews based on tmdb_review_id.
    """
    start_time = time.time()
    partition_date_str = context.partition_key
    postgres = context.resources.postgres
    reviews = fetch_show_reviews # Output from the upstream partition
    
    context.log.info(f"Loading reviews for partition date: {partition_date_str}")

    if not reviews:
        context.log.info("No reviews fetched to load for this partition.")
        end_time = time.time()
        duration = end_time - start_time
        return Output(
            value=0, # Return count of loaded reviews
            metadata={
                "partition_key": MetadataValue.text(partition_date_str),
                "execution_time": MetadataValue.float(duration),
                "reviews_loaded": MetadataValue.int(0),
                "operation": MetadataValue.text("load_data")
            }
        )
    
    batch_size = 500 # Keep batching for efficiency
    total_inserted_updated = 0
    
    # Get the partition date object for the new column
    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d").date()

    try:
        with postgres.get_connection() as conn:
            for i in range(0, len(reviews), batch_size):
                batch = reviews[i:i+batch_size]
                
                # Prepare the query with multiple value sets
                values_template = []
                values_data = []
                
                for review in batch:
                    # Ensure date_id is not None before appending, or handle it in SQL
                    if review.get('date_id') is None:
                        context.log.warning(f"Review {review.get('tmdb_review_id')} has no creation date, skipping date_id.")
                        # Decide how to handle: skip review, use partition date, use NULL?
                        # Let's use NULL for date_id if review_created_at was null.
                        # The DB column must allow NULLs for this.
                        # ALTER TABLE tmdb_data.fact_show_reviews ALTER COLUMN date_id DROP NOT NULL;
                    
                    # Add partition_date to the data being inserted
                    values_template.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)") # 10 placeholders
                    values_data.extend([
                        review['tmdb_review_id'],
                        review['series_id'],
                        review['author_name'],
                        review['author_username'],
                        review.get('date_id'), # Use .get() for safety if it might be missing or None
                        review['rating'],
                        review['content_length'],
                        review['review_created_at'],
                        review['review_updated_at'],
                        partition_date # Add the partition date for the new column
                    ])
                
                # Build and execute the query
                # Added loaded_partition_date column
                # Updated ON CONFLICT clause
                insert_query = f"""
                    INSERT INTO tmdb_data.fact_show_reviews (
                        tmdb_review_id, series_id, author_name, author_username, 
                        date_id, rating, content_length, review_created_at, 
                        review_updated_at, loaded_partition_date -- Added column
                        -- created_date uses DEFAULT CURRENT_TIMESTAMP
                    )
                    VALUES {', '.join(values_template)}
                    ON CONFLICT (tmdb_review_id) 
                    DO UPDATE SET
                        series_id = EXCLUDED.series_id, -- Ensure series_id is updated if it changes (unlikely)
                        author_name = EXCLUDED.author_name,
                        author_username = EXCLUDED.author_username,
                        date_id = EXCLUDED.date_id,
                        rating = EXCLUDED.rating,
                        content_length = EXCLUDED.content_length,
                        review_created_at = EXCLUDED.review_created_at, -- Update original create time? Maybe not.
                        review_updated_at = EXCLUDED.review_updated_at, -- Definitely update this
                        loaded_partition_date = EXCLUDED.loaded_partition_date, -- Update with the current partition date
                        created_date = CURRENT_TIMESTAMP -- Update the DB timestamp on modification
                """
                
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, values_data)
                    conn.commit() # Commit after each batch
                
                total_inserted_updated += len(batch)
                context.log.info(f"Inserted/updated {len(batch)} reviews (total this partition: {total_inserted_updated})")

    except Exception as e:
        context.log.error(f"Error loading reviews for partition {partition_date_str}: {e}")
        # Depending on requirements, you might want to roll back or handle partial loads
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
