import dagster as dg
from ...constants import movie_aug_partitions_def
from typing import List, Dict, Any
import time


@dg.asset(
    name="load_movie_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[dg.AssetKey("get_new_movie_details"), dg.AssetKey("updated_movie_details")],
    partitions_def=movie_aug_partitions_def,
)
def load_movie_dim(
    context: dg.AssetExecutionContext, 
    get_new_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]]
    ):       
    """
    Load movie details into the movie_dim table.

    Args:
        context (dg.AssetExecutionContext): _description_
        get_new_movie_details (_type_): _description_
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movie_data_combined = get_new_movie_details + updated_movie_details
    
    unique_movie_dict = {}
    for movie in movie_data_combined:
        movie_id = movie.get("movie_id")
        if movie_id is not None:
            unique_movie_dict[movie_id] = movie # Keeps the last seen version for each ID

    # Now use the deduplicated data
    movie_data = list(unique_movie_dict.values())
    context.log.info(f"Processing {len(movie_data)} unique movie records for staging.")
        
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # create staging table if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
            create_table_query = """
                CREATE TABLE IF NOT EXISTS staging.movie_dim_staging (
                    movie_id INTEGER PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    original_title VARCHAR(255),
                    original_language VARCHAR(10),
                    overview TEXT,
                    tagline TEXT,
                    status VARCHAR(50),
                    runtime INTEGER,
                    budget DECIMAL(15, 2),
                    is_adult BOOLEAN,
                    is_video BOOLEAN,
                    poster_path VARCHAR(255),
                    backdrop_path VARCHAR(255),
                    homepage VARCHAR(255),
                    imdb_id VARCHAR(20),
                    tmdb_popularity DECIMAL(10, 3),
                    vote_count INTEGER,
                    vote_average DECIMAL(3, 1)
                )"""
                
            cursor.execute(create_table_query)
            conn.commit()
            context.log.info("movie_dim_staging table created successfully.")
            
            # Truncate the table
            cursor.execute("TRUNCATE TABLE staging.movie_dim_staging")
            conn.commit()
            
            # Prepare batch data
            insert_query = """
                INSERT INTO staging.movie_dim_staging (
                    movie_id, title, original_title, original_language, overview,
                    tagline, status, runtime, budget, is_adult, is_video,
                    poster_path, backdrop_path, homepage, imdb_id,
                    tmdb_popularity, vote_count, vote_average
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            #create batch data
            batch_data = []
            # context.log.info(f"Movie Data: {json.dumps(movie_data, indent=2)}")
            
            MAX_LEN = 254

            for movie in movie_data:
                title = movie.get("title")
                original_title = movie.get("original_title")
                trimmed_title = title[:MAX_LEN] if title else None
                trimmed_original_title = original_title[:MAX_LEN] if original_title else None
                trimmed_poster_path = movie.get("poster_path")[:MAX_LEN] if movie.get("poster_path") else None
                trimmed_backdrop_path = movie.get("backdrop_path")[:MAX_LEN] if movie.get("backdrop_path") else None
                trimmed_homepage = movie.get("homepage")[:MAX_LEN] if movie.get("homepage") else None
                trimmed_imdb_id = movie.get("imdb_id")[:MAX_LEN] if movie.get("imdb_id") else None
                
                
                batch_data.append((
                    movie.get("movie_id"),
                    trimmed_title,
                    trimmed_original_title,
                    movie.get("original_language"),
                    movie.get("overview"),
                    movie.get("tagline"),
                    movie.get("status"),
                    movie.get("runtime"),
                    movie.get("budget"),
                    movie.get("is_adult", False),
                    movie.get("is_video", False),
                    trimmed_poster_path,
                    trimmed_backdrop_path,
                    trimmed_homepage,
                    trimmed_imdb_id,
                    movie.get("tmdb_popularity"),
                    movie.get("vote_count"),
                    movie.get("vote_average")   
                ))
                
            postgres.execute_batch(cursor, insert_query, batch_data, page_size=1000)
            conn.commit()
            
            # Get statistics about the staging table
            cursor.execute("SELECT COUNT(*) FROM staging.movie_dim_staging")
            movie_staging_count = cursor.fetchone()["count"]
                
            upsert_movie_query = """
                BEGIN;
                CREATE TEMP TABLE temp_movie_dim AS 
                
                SELECT 
                    mds.*
                FROM staging.movie_dim_staging mds
                    LEFT JOIN tmdb_data.movie_dim md ON mds.movie_id = md.movie_id and md.is_current = true
                WHERE -- NEW RECORDS
                    (md.movie_id IS NULL AND mds.movie_id IS NOT NULL)
                    OR -- UPDATED RECORDS
                    (md.is_current = true AND (
                        mds.title != md.title OR
                        mds.original_title != md.original_title OR
                        mds.original_language != md.original_language OR
                        mds.overview != md.overview OR
                        mds.tagline != md.tagline OR
                        mds.status != md.status OR
                        mds.runtime != md.runtime OR
                        mds.budget != md.budget OR
                        mds.is_adult != md.is_adult OR
                        mds.is_video != md.is_video OR
                        mds.poster_path != md.poster_path OR
                        mds.backdrop_path != md.backdrop_path OR
                        mds.homepage != md.homepage OR
                        mds.imdb_id != md.imdb_id OR
                        mds.tmdb_popularity != md.tmdb_popularity OR
                        mds.vote_count != md.vote_count OR
                        mds.vote_average != md.vote_average
                    ));
                --update existing records
                UPDATE tmdb_data.movie_dim md
                SET expiration_date = CURRENT_DATE,
                    is_current = false
                WHERE md.movie_id IN (SELECT movie_id FROM temp_movie_dim)
                AND md.is_current = true;
                --insert new records
                INSERT INTO tmdb_data.movie_dim (
                    movie_id,
                    title,
                    original_title,         
                    original_language,
                    overview,
                    tagline,
                    status,
                    runtime,
                    budget,
                    is_adult,
                    is_video,
                    poster_path,            
                    backdrop_path,
                    homepage,
                    imdb_id,
                    tmdb_popularity,
                    vote_count,
                    vote_average,
                    effective_date,
                    expiration_date,
                    is_current
                )
                SELECT
                    mds.movie_id,
                    mds.title,
                    mds.original_title,
                    mds.original_language,
                    mds.overview,
                    mds.tagline,
                    mds.status,
                    mds.runtime,
                    mds.budget,
                    mds.is_adult,
                    mds.is_video,
                    mds.poster_path,
                    mds.backdrop_path,
                    mds.homepage,
                    mds.imdb_id,
                    mds.tmdb_popularity,
                    mds.vote_count,
                    mds.vote_average,
                    CURRENT_DATE,
                    NULL,
                    true
                FROM temp_movie_dim mds;
                DROP TABLE temp_movie_dim;
                COMMIT;"""
                
            cursor.execute(upsert_movie_query)
            conn.commit()
            context.log.info("Data loaded into movie_dim table successfully.")
            
    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration} seconds")
    context.log.info(f"Table created: tmdb_data.movie_dim")
    context.log.info(f"Operation: create")
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "movies_loaded": len(movie_data),
            "staging_count": movie_staging_count,
            "staging_table": "staging.movie_dim_staging",  
            "table": "tmdb_data.movie_dim",
            "operation": "load"
        }
    )

@dg.asset(
    name="update_movie_dim_review_counts",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table_update"},
    # This asset depends on the movie dimension table being populated/updated first
    deps=[dg.AssetKey("load_movie_dim")],
    tags={'layer': 'silver', 'dimension': 'movie', 'enhancement': 'review_count'},
    # Keeping partitioning consistent with the dependent asset
    partitions_def=movie_aug_partitions_def,
)
def update_movie_dim_review_counts(context: dg.AssetExecutionContext):
    """
    Calculates the total review count for each movie from fact_movie_reviews
    and updates the review_count column in the currently active movie_dim records.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    context.log.info("Starting update of review counts in movie_dim.")

    # SQL query to aggregate counts and update the movie_dim table
    update_query = """
    WITH review_counts AS (
        -- Aggregate counts from the review fact table
        SELECT
            movie_id,
            COUNT(*) AS num_reviews
        FROM
            tmdb_data.fact_movie_reviews
        GROUP BY
            movie_id
    )
    -- Update the main dimension table
    UPDATE tmdb_data.movie_dim md
    SET review_count = COALESCE(rc.num_reviews, 0) 
    FROM review_counts rc
    WHERE
        md.movie_id = rc.movie_id  
        AND md.is_current = true;  -- IMPORTANT: Only update the currently active record

    -- Second pass: Ensure movies present in dim but *never* reviewed are set to 0
    -- This catches movies missed by the JOIN if they have absolutely no reviews.
    UPDATE tmdb_data.movie_dim md
    SET review_count = 0
    WHERE
        md.is_current = true
        AND md.review_count IS NULL -- Only update if not set by the previous step (or default wasn't 0)
        AND NOT EXISTS ( -- Check explicitly if no reviews exist for this movie_id
            SELECT 1
            FROM tmdb_data.fact_movie_reviews fmr
            WHERE fmr.movie_id = md.movie_id
        );
    """

    rows_affected = 0
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info("Executing review count aggregation and update query...")
                cursor.execute(update_query)
                # Fetch the number of rows affected by the update
                rows_affected = cursor.rowcount
                conn.commit()
                context.log.info(f"Successfully updated review counts in movie_dim. Rows affected (approx): {rows_affected}")

    except Exception as e:
        if 'conn' in locals() and conn:
            try:
                conn.rollback()
                context.log.info("Transaction rolled back due to error.")
            except Exception as rb_e:
                context.log.error(f"Error during rollback: {rb_e}")
        context.log.error(f"Error updating movie_dim with review counts: {e}", exc_info=True)
        raise # Fail the asset run

    duration = time.time() - start_time
    context.log.info(f"Review count update finished in {duration:.2f} seconds.")

    return dg.Output(
        value=True, # Indicate success
        metadata={
            "execution_time": duration,
            "rows_affected_approx": rows_affected,
            "updated_table": "tmdb_data.movie_dim",
            "updated_column": "review_count",
            "source_table": "tmdb_data.fact_movie_reviews",
            "operation": "update_aggregate"
        }
    )