import dagster as dg
import time
from datetime import datetime
from typing import List, Set
from ...constants import movie_aug_partitions_def


@dg.asset(
    name="get_new_movies",
    required_resource_keys={"tmdb"},
    kinds={"python", "api", "task"},
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def
)
def get_new_movies(context: dg.AssetExecutionContext):
    """
    Fetch movie augmentation data from TMDB API.
    
    Args:
        context: AssetExecutionContext
    """
    start_time = time.time()
    tmdb = context.resources.tmdb
    
    partition_date_str = context.partition_key
    
    context.log.info(f"Fetching movie augmentation data for partition: {partition_date_str}")
    page = 1 
    total_pages = 1
    movies = []
    
    while page <= total_pages:
        context.log.info(f"Fetching page {page} of movie augmentation data.")
        try:
            # Fetch movie data using the discover_movies method from your resource
            movie_data = tmdb.discover_movies(
                release_date=partition_date_str,
                page=page
            )
            
            if movie_data and "results" in movie_data:
                total_pages = movie_data.get("total_pages", 1)
                results = movie_data.get('results', [])
                context.log.info(f"Page: {page} out of total pages: {total_pages}")
                context.log.info(f"Fetched {len(results)} records from TMDB API.")
                
                if not results:
                    context.log.info("No results found.")
                    break
                
                # Process movie data
                for movie in results:
                    release_date = movie.get("release_date")
                    partition_date = partition_date_str
                    load_date = datetime.now().strftime("%Y-%m-%d")
                    movie_id = movie.get("id")
                    cleaned_release_date = release_date if release_date else None
                    movies.append((movie_id, cleaned_release_date, partition_date, load_date))
                page += 1
            else:
                context.log.info("No results found in API response.")
                break
        except Exception as e:
            context.log.error(f"Error fetching movie data: {str(e)}")
            break
            
    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration} seconds")
    
    return dg.Output(
        value=movies,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.movie_aug",
            "operation": "fetch",
            "records_fetched": len(movies)
        }
    )
@dg.asset(
    name="insert_new_movies",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "task"},
    deps=[dg.AssetKey("get_new_movies")],
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def
)
def insert_new_movies(context: dg.AssetExecutionContext, get_new_movies):
    """
    Insert movie augmentation data into the database.
    
    Args:
        context: AssetExecutionContext
        get_new_movies: List of tuples containing movie data (movie_id, release_date, load_date)
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movies = get_new_movies
    
    # Example snippet for insert_new_movies
    cleaned_movies = []
    for movie_tuple in get_new_movies:
        # Assuming movie_tuple is (movie_id, release_date, partition_date, load_date)
        movie_id, release_date, partition_date, load_date = movie_tuple
        # Convert empty string or potentially invalid dates to None for release_date
        cleaned_release_date = release_date if release_date else None # Simple check for empty string
        # Add more robust date validation/parsing if needed
        cleaned_movies.append((movie_id, cleaned_release_date, partition_date, load_date))
    
    context.log.info(f"Received {len(movies)} movies to insert")
    
    if cleaned_movies:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                # Insert data with conflict handling on both movie_id and load_date
                insert_query = """
                INSERT INTO tmdb_data.movie_aug (movie_id, release_date, partition_date,load_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (movie_id) 
                DO UPDATE SET
                    release_date = EXCLUDED.release_date,
                    partition_date = EXCLUDED.partition_date,
                    load_date = EXCLUDED.load_date
                """
                cursor.executemany(insert_query, cleaned_movies)
                conn.commit()
                context.log.info(f"Successfully inserted {len(movies)} movies")
    else:
        context.log.info("No movies to insert")
    
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=len(movies),
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.movie_aug",
            "operation": "insert",
            "records_inserted": len(movies)
        }
    )
    
@dg.asset(
    name="get_new_movie_ids_for_details",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "task"},
    deps=[dg.AssetKey("insert_new_movies")],
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def
)
def get_new_movie_ids_for_details(context: dg.AssetExecutionContext, insert_new_movies):
    """
    Query the movie_aug table for the current partition and return movie IDs 
    that need detailed information fetched from the API.
    
    Args:
        context: AssetExecutionContext
        insert_new_movies: Number of movies inserted in previous step
    """
    start_time = time.time()
    postgres = context.resources.postgres
    partition_date_str = context.partition_key
    movie_ids = []
    
    # Skip the process if no movies were inserted
    if insert_new_movies == 0:
        context.log.info(f"No movies were inserted for partition {partition_date_str}, skipping detail fetching")
        return dg.Output(
            value=[],
            metadata={
                "execution_time": 0,
                "operation": "query",
                "movie_count": 0,
                "partition": partition_date_str
            }
        )
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Query movie IDs from the augmentation table for the current partition
            query = """
            SELECT movie_id 
            FROM tmdb_data.movie_aug
            WHERE partition_date = %s
            """
            cursor.execute(query, (partition_date_str,))
            results = cursor.fetchall()
            
            if results:
                # Extract movie IDs from query results
                movie_ids = [row["movie_id"] for row in results]
                context.log.info(f"Found {len(movie_ids)} movies to fetch details for partition {partition_date_str}")
            else:
                context.log.info(f"No movies found for partition {partition_date_str}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=movie_ids,
        metadata={
            "execution_time": duration,
            "operation": "query",
            "movie_count": len(movie_ids),
            "partition": partition_date_str
        }
    )

@dg.asset(
    name="get_new_movie_details",
    required_resource_keys={"tmdb"},
    kinds={"python", "api", "task"},
    deps=[dg.AssetKey("get_new_movie_ids_for_details")],    
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def,
)
def get_new_movie_details(
    context: dg.AssetExecutionContext, 
    get_new_movie_ids_for_details: List[int]):
    """
    Fetch detailed movie details and store and prepare for loading into the movie_performance_fact table.

    Args:
        context (dg.AssetExecutionContext): _description_
        get_new_movie_ids_for_details (_type_): _description_
    """
    movie_ids = get_new_movie_ids_for_details
    if movie_ids is None:
        context.log.warning("Input 'get_new_movie_ids_for_details' is None. Skipping processing.")
        unique_movie_ids: Set[int] = set()
        input_count = 0
    elif not isinstance(movie_ids, (list, tuple, set)):
        # Handle unexpected input types gracefully
        context.log.error(f"Input 'get_new_movie_ids_for_details' is not a list/tuple/set (type: {type(movie_ids)}). Skipping processing.")
        unique_movie_ids: Set[int] = set()
        input_count = 0 # Or len(movie_ids_input) if trying to count elements anyway? Safer as 0.
    else:
        unique_movie_ids: Set[int] = set(movie_ids)
        input_count = len(movie_ids)

    context.log.info(f"Received {input_count} movie IDs, processing {len(unique_movie_ids)} unique IDs.")
    context.log.info(f"Movie IDs: {movie_ids}")
    start_time = time.time()
    tmdb = context.resources.tmdb
    
    movie_data = []    
    
    for movie_id in unique_movie_ids:
        try:
            # Fetch movie details
            movie_details = tmdb.get_movie_details(movie_id, append_to_response="credits,production_companies")
            
            if not movie_details or not movie_details.get("id"):
                context.log.warning(f"Skipping Movie ID {movie_id}: Invalid or empty response from API.")
                continue

            # context.log.info(f"Movie Details: {json.dumps(movie_details, indent=2)}")
            budget = movie_details.get("budget", 0)
            profit = movie_details.get("revenue", 0) - budget
            roi = (profit / budget) * 100 if budget > 0 else 0
            is_profitable = profit > 0
            
            genre_list = movie_details.get("genres", [])
            cast_list = movie_details.get("credits", {}).get("cast", [])
            crew_list = movie_details.get("credits", {}).get("crew", [])

            
            # Prepare data for loading
            movie_data.append({
                "movie_id": movie_details.get("id"),
                "relese_date:": movie_details.get("release_date"),
                "title": movie_details.get("title"),
                "original_title": movie_details.get("original_title"),
                "original_language": movie_details.get("original_language"),
                "overview": movie_details.get("overview"),
                "tagline": movie_details.get("tagline"),
                "status": movie_details.get("status"),
                "runtime": movie_details.get("runtime"),
                "is_adult": movie_details.get("adult", False),
                "is_video": movie_details.get("video", False),
                "poster_path": movie_details.get("poster_path"),
                "backdrop_path": movie_details.get("backdrop_path"),
                "homepage": movie_details.get("homepage"),
                "imdb_id": movie_details.get("imdb_id"),
                "tmdb_popularity": movie_details.get("popularity"),
                "vote_count": movie_details.get("vote_count"),
                "vote_average": movie_details.get("vote_average"),
                "revenue": movie_details.get("revenue"),
                "profit": profit,
                "budget": budget,
                "roi": roi,
                "is_profitable": is_profitable,
                "genres": genre_list,
                "cast": cast_list, 
                "crew": crew_list,
                "domestic_revenue": movie_details.get("domestic_revenue"),
                "international_revenue": movie_details.get("international_revenue"),
                "opening_weekend_revenue": movie_details.get("opening_weekend_revenue"),
                "box_office_rank": movie_details.get("box_office_rank") 
            })
            
            # context.log.info(f"Prepared data for Movie ID {movie_id}: {movie_details}")
            
        except Exception as e:
            context.log.error(f"Error fetching details for Movie ID {movie_id}: {e}")
            
    end_time = time.time()
    duration = end_time - start_time    
    
    return dg.Output(
        value=movie_data,
        metadata={
            "execution_time": duration,
            "movies_prepared": len(movie_ids),
            "api": "movie/movie_id",
            "operation": "request"
        }
    )