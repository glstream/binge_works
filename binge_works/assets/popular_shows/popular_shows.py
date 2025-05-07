import dagster as dg
from dagster import AssetKey, Output, MetadataValue
import json
import time

from binge_works.resources.TMDBResource import TMDBResource


@dg.asset(name="extract_popular_shows_metadata",
            kinds={"python", "bronze"},
            tags={'layer':'source'},
            )
def extract_popular_shows_metadata(context, tmdb: TMDBResource):
    """Asset to extract metadata about the API results"""
    start_time = time.time()
    
    # Get the first page to understand the structure and total pages
    response = tmdb.get_popular_shows(page=1)
    
    metadata = {
        "total_pages": response.get("total_pages", 0),
        "total_results": response.get("total_results", 0),
        "page_size": len(response.get("results", [])),
    }
    
    duration = time.time() - start_time
    
    return Output(
        value=metadata,
        metadata={
            "execution_time": MetadataValue.float(duration),
            "total_pages": MetadataValue.int(metadata["total_pages"]),
            "total_results": MetadataValue.int(metadata["total_results"]),
            "page_size": MetadataValue.int(metadata["page_size"]),
        }
    )

@dg.asset(
    name="extract_all_popular_shows",
    deps=[AssetKey("extract_popular_shows_metadata")],
    kinds={"python", "bronze"},
    tags={'layer':'source'},
)
def extract_all_popular_shows(context, tmdb: TMDBResource, extract_popular_shows_metadata):
    """Asset to extract all popular shows from the API"""
    start_time = time.time()
    
    total_pages = min(extract_popular_shows_metadata["total_pages"], 500)
    
    # Use a dictionary to ensure uniqueness by ID
    unique_shows = {}
    
    # Simply process all pages sequentially
    for page in range(1, total_pages + 1):
        try:
            response = tmdb.get_popular_shows(page=page)
            shows = response.get("results", [])
            
            # Add to the unique shows dictionary, using ID as key
            for show in shows:
                if 'id' in show:
                    unique_shows[show['id']] = show
            
            # Only log every 50 pages, plus log the first and last page
            if page % 50 == 0 or page == 1 or page == total_pages:
                context.log.info(f"Processed page {page} with {len(shows)} shows, {len(unique_shows)} unique shows so far")
        except Exception as e:
            context.log.error(f"Error processing page {page}: {str(e)}")
            break
    
    # Convert back to a list
    all_shows = list(unique_shows.values())
    
    duration = time.time() - start_time
    
    # Extract some statistics for metadata
    languages = set(show.get("original_language") for show in all_shows)
    en_shows = [show for show in all_shows if show.get("original_language") == "en"]
    
    return Output(
        value=all_shows,
        metadata={
            "execution_time": MetadataValue.float(duration),
            "total_shows": MetadataValue.int(len(all_shows)),
            "pages_processed": MetadataValue.int(total_pages),
            "languages_count": MetadataValue.int(len(languages)),
            "english_shows_count": MetadataValue.int(len(en_shows)),
            "english_percentage": MetadataValue.float(
                round(len(en_shows) / len(all_shows) * 100, 2) if all_shows else 0
            ),
        }
    )



@dg.asset(
    name="load_to_staging_table",
    deps=[AssetKey("extract_all_popular_shows")],
    required_resource_keys={"postgres"},
    kinds={"python","postgres", "silver"},
    tags={'layer':'staging'},
)
def load_to_staging_table(context, extract_all_popular_shows):
    """Asset to load all shows into a staging table"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
            
            # Create staging table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS staging.shows (
                id INTEGER PRIMARY KEY,
                name TEXT,
                original_name TEXT,
                overview TEXT,
                first_air_date DATE,
                popularity FLOAT,
                vote_average FLOAT,
                vote_count INTEGER,
                poster_path TEXT,
                backdrop_path TEXT,
                original_language TEXT,
                genre_ids JSONB,
                origin_country JSONB,
                adult BOOLEAN,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            
            # Clear the staging table
            cursor.execute("TRUNCATE TABLE staging.shows")
            
            # Prepare all data for batch insertion
            show_data_list = []
            
            # Prepare all data
            for show in extract_all_popular_shows:
                # Handle JSON fields properly
                show_data = (
                    show.get('id'),
                    show.get('name'),
                    show.get('original_name'),
                    show.get('overview'),
                    show.get('first_air_date') or None,
                    show.get('popularity'),
                    show.get('vote_average'),
                    show.get('vote_count'),
                    show.get('poster_path'),
                    show.get('backdrop_path'),
                    show.get('original_language'),
                    json.dumps(show.get('genre_ids', [])),
                    json.dumps(show.get('origin_country', [])),
                    show.get('adult', False)
                )
                show_data_list.append(show_data)
            
            # Execute batch insert with execute_batch
            context.log.info(f"Batch inserting {len(show_data_list)} records with execute_batch...")
            
            insert_query = """
            INSERT INTO staging.shows (
                id, name, original_name, overview, first_air_date, 
                popularity, vote_average, vote_count, poster_path, 
                backdrop_path, original_language, genre_ids, origin_country,
                adult
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """
            
            postgres.execute_batch(cursor, insert_query, show_data_list, page_size=1000)

            
            conn.commit()
            
            # Get statistics about the staging table
            cursor.execute("SELECT COUNT(*) FROM staging.shows")
            total_count = cursor.fetchone()["count"]
            
            cursor.execute("SELECT COUNT(*) FROM staging.shows WHERE original_language = 'en'")
            english_count = cursor.fetchone()["count"]
    
    duration = time.time() - start_time
    
    return Output(
        value={
            "records_inserted": total_count,  # execute_batch doesn't return count, so use table count
            "table": "staging.shows",
            "total_count": total_count,
            "english_count": english_count
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "records_inserted": MetadataValue.int(total_count),
            "total_count": MetadataValue.int(total_count),
            "english_count": MetadataValue.int(english_count),
            "english_percentage": MetadataValue.float(
                round(english_count / total_count * 100, 2) if total_count else 0
            ),
        }
    )

@dg.asset(
        name="create_shows_history_table" ,
        kinds={"python", "postgres", "bronze"},
        deps=[AssetKey("load_to_staging_table")],
        required_resource_keys={"postgres"},
        )
def create_shows_history_table(context,load_to_staging_table):
    staging_table = load_to_staging_table.get("table")
    context.log.info(f"Creating shows_history table based on {staging_table}")
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create tmdb_data schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create shows table in tmdb_data schema if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.dim_shows_history (
                id_sk INTEGER PRIMARY KEY,
                id INTEGER NOT NULL,
                name TEXT,
                original_name TEXT,
                overview TEXT,
                first_air_date DATE,
                popularity FLOAT,
                vote_average FLOAT,
                vote_count INTEGER,
                poster_path TEXT,
                backdrop_path TEXT,
                original_language TEXT,
                genre_ids JSONB,
                origin_country JSONB,
                adult BOOLEAN,
                effective_date DATE NOT NULL,        -- When this version became effective
                expiration_date DATE,                -- When this version expired (NULL = current)
                is_current BOOLEAN NOT NULL,         -- Flag for current version
                update_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- When this record was created
            )
            """
            cursor.execute(create_table_query)
    return Output(value={"table": "tmdb_data.dim_shows_history"})

@dg.asset(
    name="load_to_shows_history_table",
    deps=[AssetKey("create_shows_history_table")],
    required_resource_keys={"postgres"},
    kinds={"python","postgres", "silver"},
)
def load_to_shows_history_table(context,load_to_staging_table):
    """Asset to load all shows into a type II SCD table"""
    postgres = context.resources.postgres
    staging_table = load_to_staging_table.get("table")
    context.log.info(f"Creating shows_history table based on {staging_table}")
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            upsert_shows_history = """
            BEGIN;

            -- Create temp table with records that need to be inserted or updated
            CREATE TEMP TABLE temp_shows AS
            SELECT 
                ss.id, 
                ss.name, 
                ss.original_name, 
                ss.overview, 
                ss.first_air_date, 
                ss.popularity, 
                ss.vote_average, 
                ss.vote_count, 
                ss.poster_path, 
                ss.backdrop_path, 
                ss.original_language, 
                ss.genre_ids, 
                ss.origin_country,
                ss.adult
            FROM staging.shows ss
            LEFT JOIN tmdb_data.dim_shows_history sh
                ON ss.id = sh.id AND sh.is_current = true
            WHERE 
                -- New records that don't exist in history table
                (sh.id IS NULL AND ss.id IS NOT NULL AND ss.original_language = 'en')
                OR 
                -- Existing records that have changed
                (sh.is_current = true AND (
                    ss.name != sh.name OR
                    ss.original_name != sh.original_name OR
                    ss.overview != sh.overview OR
                    ss.first_air_date != sh.first_air_date OR
                    ss.popularity != sh.popularity OR
                    ss.vote_average != sh.vote_average OR
                    ss.vote_count != sh.vote_count OR
                    ss.poster_path != sh.poster_path OR
                    ss.backdrop_path != sh.backdrop_path OR
                    ss.original_language != sh.original_language OR
                    ss.genre_ids != sh.genre_ids OR
                    ss.origin_country != sh.origin_country OR
                    ss.adult != sh.adult
                ));

            -- Update existing records to mark them as expired
            UPDATE tmdb_data.dim_shows_history sh
            SET expiration_date = CURRENT_DATE, is_current = false
            WHERE id IN (SELECT id FROM temp_shows)
            AND is_current = true;

            -- Insert new versions with a generated surrogate key
            INSERT INTO tmdb_data.dim_shows_history (
                id_sk, id, name, original_name, overview, first_air_date, 
                popularity, vote_average, vote_count, poster_path, 
                backdrop_path, original_language, genre_ids, origin_country,
                adult, effective_date, expiration_date, is_current
            )
            SELECT
                -- Generate a new unique surrogate key based on the maximum value in the table
                COALESCE((SELECT MAX(id_sk) FROM tmdb_data.dim_shows_history), 1000000) + ROW_NUMBER() OVER (ORDER BY id),
                id, name, original_name, overview, first_air_date, 
                popularity, vote_average, vote_count, poster_path, 
                backdrop_path, original_language, genre_ids, origin_country,
                adult, CURRENT_DATE, NULL, true
            FROM temp_shows;

            DROP TABLE temp_shows;
            COMMIT;
            """
            cursor.execute(upsert_shows_history)
    return

@dg.asset(
    name="load_to_production",
    deps=[AssetKey("load_to_staging_table")],
    required_resource_keys={"postgres"},
    kinds={"python","postgres", "gold"},
    tags={'layer':'prod'},
)
def load_to_production(context, load_to_staging_table):
    """Asset to load and filter English shows directly into tmdb_data schema"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create tmdb_data schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create shows table in tmdb_data schema if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.dim_popular_shows (
                id INTEGER PRIMARY KEY,
                name TEXT,
                original_name TEXT,
                overview TEXT,
                first_air_date DATE,
                popularity FLOAT,
                vote_average FLOAT,
                vote_count INTEGER,
                poster_path TEXT,
                backdrop_path TEXT,
                original_language TEXT,
                genre_ids JSONB,
                origin_country JSONB,
                adult BOOLEAN,
                insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
            
            # Get count before the merge
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_popular_shows")
            before_count = cursor.fetchone()["count"]
            
            # Use SQL to filter and merge in a single step
            # Only English shows are selected from staging and merged to production
            merge_query = """
            INSERT INTO tmdb_data.dim_popular_shows AS s (
                id, name, original_name, overview, first_air_date, 
                popularity, vote_average, vote_count, poster_path, 
                backdrop_path, original_language, genre_ids, origin_country,
                adult, insert_timestamp, last_updated
            )
            SELECT 
                id, name, original_name, overview, first_air_date, 
                popularity, vote_average, vote_count, poster_path, 
                backdrop_path, original_language, genre_ids, origin_country,
                adult, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM staging.shows
            WHERE original_language = 'en'
            ON CONFLICT (id) 
            DO UPDATE SET
                name            = EXCLUDED.name,
                original_name   = EXCLUDED.original_name,
                overview        = EXCLUDED.overview,
                first_air_date  = EXCLUDED.first_air_date,
                popularity      = EXCLUDED.popularity,
                vote_average    = EXCLUDED.vote_average,
                vote_count      = EXCLUDED.vote_count,
                poster_path     = EXCLUDED.poster_path,
                backdrop_path   = EXCLUDED.backdrop_path,
                original_language = EXCLUDED.original_language,
                genre_ids       = EXCLUDED.genre_ids,
                origin_country  = EXCLUDED.origin_country,
                adult           = EXCLUDED.adult,
                last_updated    = CURRENT_TIMESTAMP
            WHERE 
                s.name            IS DISTINCT FROM EXCLUDED.name OR
                s.original_name   IS DISTINCT FROM EXCLUDED.original_name OR
                s.overview        IS DISTINCT FROM EXCLUDED.overview OR
                s.first_air_date  IS DISTINCT FROM EXCLUDED.first_air_date OR
                s.popularity      IS DISTINCT FROM EXCLUDED.popularity OR
                s.vote_average    IS DISTINCT FROM EXCLUDED.vote_average OR
                s.vote_count      IS DISTINCT FROM EXCLUDED.vote_count OR
                s.poster_path     IS DISTINCT FROM EXCLUDED.poster_path OR
                s.backdrop_path   IS DISTINCT FROM EXCLUDED.backdrop_path OR
                s.original_language IS DISTINCT FROM EXCLUDED.original_language OR
                s.genre_ids       IS DISTINCT FROM EXCLUDED.genre_ids OR
                s.origin_country  IS DISTINCT FROM EXCLUDED.origin_country OR
                s.adult           IS DISTINCT FROM EXCLUDED.adult;
            """
            cursor.execute(merge_query)
            affected_rows = cursor.rowcount
            
            conn.commit()
            
            # Get count after the merge
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_popular_shows")
            after_count = cursor.fetchone()["count"]
            
            # Get stats about production data
            cursor.execute("""
                SELECT 
                    MIN(popularity) as min_pop,
                    MAX(popularity) as max_pop,
                    AVG(popularity) as avg_pop,
                    COUNT(*) as total
                FROM tmdb_data.dim_popular_shows
            """)
            stats = cursor.fetchone()
    
    duration = time.time() - start_time
    
    # Calculate new vs updated records
    new_records = after_count - before_count
    updated_records = affected_rows - new_records
    
    return Output(
        value={
            "affected_rows": affected_rows,
            "new_records": new_records,
            "updated_records": updated_records,
            "total_records": after_count,
            "table": "tmdb_data.dim_popular_shows"
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "affected_rows": MetadataValue.int(affected_rows),
            "new_records": MetadataValue.int(new_records),
            "updated_records": MetadataValue.int(updated_records),
            "total_records": MetadataValue.int(after_count),
            "min_popularity": MetadataValue.float(stats["min_pop"] if stats else 0),
            "max_popularity": MetadataValue.float(stats["max_pop"] if stats else 0),
            "avg_popularity": MetadataValue.float(stats["avg_pop"] if stats else 0),
        }
    )

@dg.asset(
    name="cleanup_staging",
    deps=[AssetKey("load_to_production"), AssetKey("load_to_shows_history_table")],
    required_resource_keys={"postgres"},
    kinds={"python","task"},
    tags={'layer':'cleanup'},
)
def cleanup_staging(context, load_to_production):
    """Asset to clean up staging tables"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Truncate the staging table
            cursor.execute("TRUNCATE TABLE staging.shows")
            conn.commit()
            
            # Verify it's empty
            cursor.execute("SELECT COUNT(*) FROM staging.shows")
            staging_count = cursor.fetchone()["count"]
    
    duration = time.time() - start_time
    cleanup_successful = staging_count == 0
    
    return Output(
        value={
            "cleanup_successful": cleanup_successful,
            "staging_count": staging_count
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "cleanup_successful": MetadataValue.bool(cleanup_successful),
            "production_records": MetadataValue.int(load_to_production["total_records"]),
        }
    )  