import dagster as dg
import time
from datetime import datetime

from ...constants import movie_aug_partitions_def


@dg.asset(
    name="get_movie_augmentation",
    required_resource_keys={"tmdb"},
    kinds={"python", "api", "task"},
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def
)
def get_movie_augmentation(context: dg.AssetExecutionContext):
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
    name="insert_movie_augmentation",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "task"},
    deps=[dg.AssetKey("get_movie_augmentation")],
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def
)
def insert_movie_augmentation(context: dg.AssetExecutionContext, get_movie_augmentation):
    """
    Insert movie augmentation data into the database.
    
    Args:
        context: AssetExecutionContext
        get_movie_augmentation: List of tuples containing movie data (movie_id, release_date, load_date)
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movies = get_movie_augmentation
    
    # Example snippet for insert_movie_augmentation
    cleaned_movies = []
    for movie_tuple in get_movie_augmentation:
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
    name="get_movie_ids_for_details",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "task"},
    deps=[dg.AssetKey("insert_movie_augmentation")],
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def
)
def get_movie_ids_for_details(context: dg.AssetExecutionContext, insert_movie_augmentation):
    """
    Query the movie_aug table for the current partition and return movie IDs 
    that need detailed information fetched from the API.
    
    Args:
        context: AssetExecutionContext
        insert_movie_augmentation: Number of movies inserted in previous step
    """
    start_time = time.time()
    postgres = context.resources.postgres
    partition_date_str = context.partition_key
    movie_ids = []
    
    # Skip the process if no movies were inserted
    if insert_movie_augmentation == 0:
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