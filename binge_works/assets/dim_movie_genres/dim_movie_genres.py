import dagster as dg
from dagster import Output, AssetKey, MetadataValue
import time
import json



@dg.asset(
    name="extract_movie_genres",
    required_resource_keys={"tmdb"},
    kinds={"python", "bronze"},
    tags={'layer': 'extract'}
)
def extract_movie_genres(context):
    """Asset to extract movie genres data from the TMDB API"""
    start_time = time.time()
    tmdb = context.resources.tmdb
    
    # Fetch movie genres from TMDB API
    genre_response = tmdb.get_genres_list(medium="movie")
    genres = genre_response.get("genres", [])
    
    duration = time.time() - start_time
    
    return Output(
        value=genres,
        metadata={
            "execution_time": MetadataValue.float(duration),
            "genre_count": MetadataValue.int(len(genres)),
            "operation": "extract"
        }
    )

@dg.asset(
    name="load_dim_movie_genres",
    deps=[AssetKey("extract_movie_genres")],
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "gold"},
    tags={'layer': 'dimensional'}
)
def load_dim_movie_genres(context, extract_movie_genres):
    """Asset to load movie genre data into the dimension table"""
    start_time = time.time()
    postgres = context.resources.postgres
    genres = extract_movie_genres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Get count before the merge
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_movie_genres")
            before_count = cursor.fetchone()["count"]
            
            # Prepare all data for batch insertion
            genre_data_list = []
            
            # Transform data into the desired format
            for genre in genres:
                genre_tuple = (
                    genre.get('id'),
                    genre.get('name')
                )
                genre_data_list.append(genre_tuple)
            
            # Execute batch insert with execute_batch
            context.log.info(f"Batch inserting {len(genre_data_list)} movie genres with execute_batch...")
            
            insert_query = """
            INSERT INTO tmdb_data.dim_movie_genres (
                genre_id, genre_name
            ) VALUES (%s, %s)
            ON CONFLICT (genre_id) 
            DO UPDATE SET
                genre_name = EXCLUDED.genre_name,
                last_updated = CURRENT_TIMESTAMP
            """
            
            postgres.execute_batch(cursor, insert_query, genre_data_list, page_size=100)
            
            conn.commit()
            
            # Get count after the merge
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_movie_genres")
            after_count = cursor.fetchone()["count"]
    
    duration = time.time() - start_time
    
    # Calculate new vs updated records
    new_records = after_count - before_count
    updated_records = len(genre_data_list) - new_records
    
    return Output(
        value={
            "records_processed": len(genre_data_list),
            "new_records": new_records,
            "updated_records": updated_records,
            "total_records": after_count,
            "table": "tmdb_data.dim_movie_genres"
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "records_processed": MetadataValue.int(len(genre_data_list)),
            "new_records": MetadataValue.int(new_records),
            "updated_records": MetadataValue.int(updated_records),
            "total_records": MetadataValue.int(after_count)
        }
    )