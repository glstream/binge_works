import dagster as dg
import time
import json
from binge_works.resources.TMDBResource import TMDBResource


@dg.asset(name="truncate_popular_movies_table", 
        description="Truncate the popular movies table",
        required_resource_keys={"postgres"},
        kinds={"python", "postgres", "bronze"},
        tags={'layer':'staging'},)
def truncate_popular_movies_table(context):
    """Asset to truncate the popular movies table before reload"""
    context.log.info("Truncating popular movies staging table")
    postgres = context.resources.postgres
    start_time = time.time()
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
            
            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS staging.popular_movies (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    original_title TEXT,
                    popularity FLOAT,
                    release_date DATE,
                    video BOOLEAN,
                    vote_average FLOAT,
                    vote_count INTEGER,
                    adult BOOLEAN,
                    genre_ids JSONB,
                    original_language TEXT,
                    overview TEXT,
                    backdrop_path TEXT,
                    poster_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Truncate the STAGING table, not the production table
            cursor.execute("TRUNCATE TABLE staging.popular_movies")
            
            # Also create the target table if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tmdb_data.dim_popular_movies (
                    id INTEGER PRIMARY KEY,
                    title TEXT,
                    original_title TEXT,
                    popularity FLOAT,
                    release_date DATE,
                    video BOOLEAN,
                    vote_average FLOAT,
                    vote_count INTEGER,
                    adult BOOLEAN,
                    genre_ids JSONB,
                    original_language TEXT,
                    overview TEXT,
                    backdrop_path TEXT,
                    poster_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "table": "staging.popular_movies",
            "operation": "truncate"
        })

@dg.asset(name="extract_popular_movies_metadata",
        description="Extract popular movies metadata from TMDB",
        deps=[dg.AssetKey("truncate_popular_movies_table")],
        kinds={"python", "bronze"},
        tags={'layer':'source'},)
def extract_popular_movies_metadata(context, tmdb: TMDBResource):
    """Asset to extract popular movies metadata from TMDB"""
    start_time = time.time()
    
    popular_movies_response = tmdb.get_popular_movies(page=1)
    
    metadata = {
        "total_pages": popular_movies_response.get("total_pages", 0),
        "total_results": popular_movies_response.get("total_results", 0),
        "page_size": len(popular_movies_response.get("results", [])),
    }
    
    duration = time.time() - start_time
    
    return dg.Output(
        value=metadata,
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "total_pages": dg.MetadataValue.int(metadata["total_pages"]),
            "total_results": dg.MetadataValue.int(metadata["total_results"]),
            "page_size": dg.MetadataValue.int(metadata["page_size"]),
        }
    )    
    
@dg.asset(name="extract_popular_movies_data",
        description="Extract popular movies data from TMDB",
        deps=[dg.AssetKey("extract_popular_movies_metadata")],
        kinds={"python", "bronze"},
        tags={'layer':'source'})
def extract_popular_movies_data(context, tmdb: TMDBResource, extract_popular_movies_metadata):
    """Asset to extract all popular movies from the API"""
    start_time = time.time()
    
    # Get total pages from metadata, but limit to a reasonable number if needed
    total_pages = min(extract_popular_movies_metadata["total_pages"], 500)
    
    # Use a dictionary to ensure uniqueness by ID
    unique_movies = {}
    
    # Process all pages sequentially
    for page in range(1, total_pages + 1):
        try:
            response = tmdb.get_popular_movies(page=page)
            movies = response.get("results", [])
            
            # Add to the unique movies dictionary, using ID as key
            for movie in movies:
                if 'id' in movie:
                    unique_movies[movie['id']] = movie
            
            # Log progress
            if page % 50 == 0 or page == 1 or page == total_pages:
                context.log.info(f"Processed page {page} with {len(movies)} movies, {len(unique_movies)} unique movies so far")
        except Exception as e:
            context.log.error(f"Error processing page {page}: {str(e)}")
            break
    
    # Convert back to a list
    all_movies = list(unique_movies.values())
    
    duration = time.time() - start_time
    
    # Extract some statistics for metadata
    languages = set(movie.get("original_language") for movie in all_movies)
    en_movies = [movie for movie in all_movies if movie.get("original_language") == "en"]
    
    return dg.Output(
        value=all_movies,
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "total_movies": dg.MetadataValue.int(len(all_movies)),
            "pages_processed": dg.MetadataValue.int(total_pages),
            "languages_count": dg.MetadataValue.int(len(languages)),
            "english_movies_count": dg.MetadataValue.int(len(en_movies)),
            "english_percentage": dg.MetadataValue.float(
                round(len(en_movies) / len(all_movies) * 100, 2) if all_movies else 0
            ),
        }
    )

@dg.asset(
    name="load_movies_to_staging",
    deps=[dg.AssetKey("extract_popular_movies_data")],
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "silver"},
    tags={'layer':'staging'},
)
def load_movies_to_staging(context, extract_popular_movies_data):
    """Asset to load all movies into a staging table"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Prepare all data for batch insertion
            movie_data_list = []
            
            # Prepare all data
            for movie in extract_popular_movies_data:
                # Handle JSON fields properly
                movie_data = (
                    movie.get('id'),
                    movie.get('title'),
                    movie.get('original_title'),
                    movie.get('popularity'),
                    movie.get('release_date') or None,
                    movie.get('video', False),
                    movie.get('vote_average'),
                    movie.get('vote_count'),
                    movie.get('adult', False),
                    json.dumps(movie.get('genre_ids', [])),
                    movie.get('original_language'),
                    movie.get('overview'),
                    movie.get('backdrop_path'),
                    movie.get('poster_path')
                )
                movie_data_list.append(movie_data)
            
            # Execute batch insert
            context.log.info(f"Batch inserting {len(movie_data_list)} records with execute_batch...")
            
            insert_query = """
            INSERT INTO staging.popular_movies (
                id, title, original_title, popularity, release_date, 
                video, vote_average, vote_count, adult, 
                genre_ids, original_language, overview, backdrop_path, poster_path
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """
            
            postgres.execute_batch(cursor, insert_query, movie_data_list, page_size=1000)
            
            conn.commit()
            
            # Get statistics about the staging table
            cursor.execute("SELECT COUNT(*) FROM staging.popular_movies")
            total_count = cursor.fetchone()["count"]
            
            cursor.execute("SELECT COUNT(*) FROM staging.popular_movies WHERE original_language = 'en'")
            english_count = cursor.fetchone()["count"]
    
    duration = time.time() - start_time
    
    return dg.Output(
        value={
            "records_inserted": total_count,
            "table": "staging.popular_movies",
            "total_count": total_count,
            "english_count": english_count
        },
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "records_inserted": dg.MetadataValue.int(total_count),
            "total_count": dg.MetadataValue.int(total_count),
            "english_count": dg.MetadataValue.int(english_count),
            "english_percentage": dg.MetadataValue.float(
                round(english_count / total_count * 100, 2) if total_count else 0
            ),
        }
    )

@dg.asset(
    name="load_movies_to_production",
    deps=[dg.AssetKey("load_movies_to_staging")],
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "gold"},
    tags={'layer':'prod'},
)
def load_movies_to_production(context, load_movies_to_staging):
    """Asset to load and filter English movies directly into tmdb_data schema"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Get count before the merge
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_popular_movies")
            before_count = cursor.fetchone()["count"]
            
            # Use SQL to filter and merge in a single step
            # Only English movies are selected from staging and merged to production
            merge_query = """
            INSERT INTO tmdb_data.dim_popular_movies AS m (
                id, title, original_title, popularity, release_date, 
                video, vote_average, vote_count, adult, 
                genre_ids, original_language, overview, backdrop_path, poster_path,
                created_at, last_updated
            )
            SELECT 
                id, title, original_title, popularity, release_date, 
                video, vote_average, vote_count, adult, 
                genre_ids, original_language, overview, backdrop_path, poster_path,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM staging.popular_movies
            WHERE original_language = 'en'
            ON CONFLICT (id) 
            DO UPDATE SET
                title           = EXCLUDED.title,
                original_title  = EXCLUDED.original_title,
                popularity      = EXCLUDED.popularity,
                release_date    = EXCLUDED.release_date,
                video           = EXCLUDED.video,
                vote_average    = EXCLUDED.vote_average,
                vote_count      = EXCLUDED.vote_count,
                adult           = EXCLUDED.adult,
                genre_ids       = EXCLUDED.genre_ids,
                original_language = EXCLUDED.original_language,
                overview        = EXCLUDED.overview,
                backdrop_path   = EXCLUDED.backdrop_path,
                poster_path     = EXCLUDED.poster_path,
                last_updated    = CURRENT_TIMESTAMP
            WHERE 
                m.title           IS DISTINCT FROM EXCLUDED.title OR
                m.original_title  IS DISTINCT FROM EXCLUDED.original_title OR
                m.popularity      IS DISTINCT FROM EXCLUDED.popularity OR
                m.release_date    IS DISTINCT FROM EXCLUDED.release_date OR
                m.video           IS DISTINCT FROM EXCLUDED.video OR
                m.vote_average    IS DISTINCT FROM EXCLUDED.vote_average OR
                m.vote_count      IS DISTINCT FROM EXCLUDED.vote_count OR
                m.adult           IS DISTINCT FROM EXCLUDED.adult OR
                m.genre_ids       IS DISTINCT FROM EXCLUDED.genre_ids OR
                m.original_language IS DISTINCT FROM EXCLUDED.original_language OR
                m.overview        IS DISTINCT FROM EXCLUDED.overview OR
                m.backdrop_path   IS DISTINCT FROM EXCLUDED.backdrop_path OR
                m.poster_path     IS DISTINCT FROM EXCLUDED.poster_path;
            """
            cursor.execute(merge_query)
            affected_rows = cursor.rowcount
            
            conn.commit()
            
            # Get count after the merge
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_popular_movies")
            after_count = cursor.fetchone()["count"]
            
            # Get stats about production data
            cursor.execute("""
                SELECT 
                    MIN(popularity) as min_pop,
                    MAX(popularity) as max_pop,
                    AVG(popularity) as avg_pop,
                    COUNT(*) as total
                FROM tmdb_data.dim_popular_movies
            """)
            stats = cursor.fetchone()
    
    duration = time.time() - start_time
    
    # Calculate new vs updated records
    new_records = after_count - before_count
    updated_records = affected_rows - new_records
    
    return dg.Output(
        value={
            "affected_rows": affected_rows,
            "new_records": new_records,
            "updated_records": updated_records,
            "total_records": after_count,
            "table": "tmdb_data.dim_popular_movies"
        },
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "affected_rows": dg.MetadataValue.int(affected_rows),
            "new_records": dg.MetadataValue.int(new_records),
            "updated_records": dg.MetadataValue.int(updated_records),
            "total_records": dg.MetadataValue.int(after_count),
            "min_popularity": dg.MetadataValue.float(stats["min_pop"] if stats else 0),
            "max_popularity": dg.MetadataValue.float(stats["max_pop"] if stats else 0),
            "avg_popularity": dg.MetadataValue.float(stats["avg_pop"] if stats else 0),
        }
    )

@dg.asset(
    name="cleanup_movies_staging",
    deps=[dg.AssetKey("load_movies_to_production")],
    required_resource_keys={"postgres"},
    kinds={"python", "task"},
    tags={'layer':'cleanup'},
)
def cleanup_movies_staging(context, load_movies_to_production):
    """Asset to clean up staging tables"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Truncate the staging table
            cursor.execute("TRUNCATE TABLE staging.popular_movies")
            conn.commit()
            
            # Verify it's empty
            cursor.execute("SELECT COUNT(*) FROM staging.popular_movies")
            staging_count = cursor.fetchone()["count"]
    
    duration = time.time() - start_time
    cleanup_successful = staging_count == 0
    
    return dg.Output(
        value={
            "cleanup_successful": cleanup_successful,
            "staging_count": staging_count
        },
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "cleanup_successful": dg.MetadataValue.bool(cleanup_successful),
            "production_records": dg.MetadataValue.int(load_movies_to_production["total_records"]),
        }
    )