# In movie_performance_fact.py

import dagster as dg
import time
import json
from datetime import datetime
from typing import List, Dict, Any, Set, Tuple
from ...constants import movie_aug_partitions_def

@dg.asset(
    name="get_movie_details",
    required_resource_keys={"tmdb"},
    kinds={"python", "api", "task"},
    deps=[dg.AssetKey("get_movie_ids_for_details")],
    tags={'layer': 'staging'},
    partitions_def=movie_aug_partitions_def,
)
def get_movie_details(
    context: dg.AssetExecutionContext, 
    get_movie_ids_for_details: List[int]):
    """
    Fetch detailed movie details and store and prepare for loading into the movie_performance_fact table.

    Args:
        context (dg.AssetExecutionContext): _description_
        get_movie_ids_for_details (_type_): _description_
    """
    movie_ids = get_movie_ids_for_details
    if movie_ids is None:
        context.log.warning("Input 'get_movie_ids_for_details' is None. Skipping processing.")
        unique_movie_ids: Set[int] = set()
        input_count = 0
    elif not isinstance(movie_ids, (list, tuple, set)):
        # Handle unexpected input types gracefully
        context.log.error(f"Input 'get_movie_ids_for_details' is not a list/tuple/set (type: {type(movie_ids)}). Skipping processing.")
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

# @dg.asset(
#     name="load_movie_dim",
#     required_resource_keys={"postgres"},
#     kinds={"python", "postgres", "table"},
#     deps=[dg.AssetKey("get_movie_details"), dg.AssetKey("updated_movie_details")],
#     partitions_def=movie_aug_partitions_def,
# )
# def load_movie_dim(
#     context: dg.AssetExecutionContext, 
#     get_movie_details: List[Dict[str, Any]],
#     updated_movie_details: List[Dict[str, Any]]
#     ):       
#     """
#     Load movie details into the movie_dim table.

#     Args:
#         context (dg.AssetExecutionContext): _description_
#         get_movie_details (_type_): _description_
#     """
#     start_time = time.time()
#     postgres = context.resources.postgres
#     movie_data_combined = get_movie_details + updated_movie_details
    
#     unique_movie_dict = {}
#     for movie in movie_data_combined:
#         movie_id = movie.get("movie_id")
#         if movie_id is not None:
#             unique_movie_dict[movie_id] = movie # Keeps the last seen version for each ID

#     # Now use the deduplicated data
#     movie_data = list(unique_movie_dict.values())
#     context.log.info(f"Processing {len(movie_data)} unique movie records for staging.")
        
#     with postgres.get_connection() as conn:
#         with conn.cursor() as cursor:
#             # create staging table if it doesn't exist
#             cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
#             create_table_query = """
#                 CREATE TABLE IF NOT EXISTS staging.movie_dim_staging (
#                     movie_id INTEGER PRIMARY KEY,
#                     title VARCHAR(255) NOT NULL,
#                     original_title VARCHAR(255),
#                     original_language VARCHAR(10),
#                     overview TEXT,
#                     tagline TEXT,
#                     status VARCHAR(50),
#                     runtime INTEGER,
#                     budget DECIMAL(15, 2),
#                     is_adult BOOLEAN,
#                     is_video BOOLEAN,
#                     poster_path VARCHAR(255),
#                     backdrop_path VARCHAR(255),
#                     homepage VARCHAR(255),
#                     imdb_id VARCHAR(20),
#                     tmdb_popularity DECIMAL(10, 3),
#                     vote_count INTEGER,
#                     vote_average DECIMAL(3, 1)
#                 )"""
                
#             cursor.execute(create_table_query)
#             conn.commit()
#             context.log.info("movie_dim_staging table created successfully.")
            
#             # Truncate the table
#             cursor.execute("TRUNCATE TABLE staging.movie_dim_staging")
#             conn.commit()
            
#             # Prepare batch data
#             insert_query = """
#                 INSERT INTO staging.movie_dim_staging (
#                     movie_id, title, original_title, original_language, overview,
#                     tagline, status, runtime, budget, is_adult, is_video,
#                     poster_path, backdrop_path, homepage, imdb_id,
#                     tmdb_popularity, vote_count, vote_average
#                 ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             """
#             #create batch data
#             batch_data = []
#             # context.log.info(f"Movie Data: {json.dumps(movie_data, indent=2)}")
            
#             MAX_LEN = 254

#             for movie in movie_data:
#                 title = movie.get("title")
#                 original_title = movie.get("original_title")
#                 trimmed_title = title[:MAX_LEN] if title else None
#                 trimmed_original_title = original_title[:MAX_LEN] if original_title else None
#                 trimmed_poster_path = movie.get("poster_path")[:MAX_LEN] if movie.get("poster_path") else None
#                 trimmed_backdrop_path = movie.get("backdrop_path")[:MAX_LEN] if movie.get("backdrop_path") else None
#                 trimmed_homepage = movie.get("homepage")[:MAX_LEN] if movie.get("homepage") else None
#                 trimmed_imdb_id = movie.get("imdb_id")[:MAX_LEN] if movie.get("imdb_id") else None
                
                
#                 batch_data.append((
#                     movie.get("movie_id"),
#                     trimmed_title,
#                     trimmed_original_title,
#                     movie.get("original_language"),
#                     movie.get("overview"),
#                     movie.get("tagline"),
#                     movie.get("status"),
#                     movie.get("runtime"),
#                     movie.get("budget"),
#                     movie.get("is_adult", False),
#                     movie.get("is_video", False),
#                     trimmed_poster_path,
#                     trimmed_backdrop_path,
#                     trimmed_homepage,
#                     trimmed_imdb_id,
#                     movie.get("tmdb_popularity"),
#                     movie.get("vote_count"),
#                     movie.get("vote_average")   
#                 ))
                
#             postgres.execute_batch(cursor, insert_query, batch_data, page_size=1000)
#             conn.commit()
            
#             # Get statistics about the staging table
#             cursor.execute("SELECT COUNT(*) FROM staging.movie_dim_staging")
#             movie_staging_count = cursor.fetchone()["count"]
                
#             upsert_movie_query = """
#                 BEGIN;
#                 CREATE TEMP TABLE temp_movie_dim AS 
                
#                 SELECT 
#                     mds.*
#                 FROM staging.movie_dim_staging mds
#                     LEFT JOIN tmdb_data.movie_dim md ON mds.movie_id = md.movie_id and md.is_current = true
#                 WHERE -- NEW RECORDS
#                     (md.movie_id IS NULL AND mds.movie_id IS NOT NULL)
#                     OR -- UPDATED RECORDS
#                     (md.is_current = true AND (
#                         mds.title != md.title OR
#                         mds.original_title != md.original_title OR
#                         mds.original_language != md.original_language OR
#                         mds.overview != md.overview OR
#                         mds.tagline != md.tagline OR
#                         mds.status != md.status OR
#                         mds.runtime != md.runtime OR
#                         mds.budget != md.budget OR
#                         mds.is_adult != md.is_adult OR
#                         mds.is_video != md.is_video OR
#                         mds.poster_path != md.poster_path OR
#                         mds.backdrop_path != md.backdrop_path OR
#                         mds.homepage != md.homepage OR
#                         mds.imdb_id != md.imdb_id OR
#                         mds.tmdb_popularity != md.tmdb_popularity OR
#                         mds.vote_count != md.vote_count OR
#                         mds.vote_average != md.vote_average
#                     ));
#                 --update existing records
#                 UPDATE tmdb_data.movie_dim md
#                 SET expiration_date = CURRENT_DATE,
#                     is_current = false
#                 WHERE md.movie_id IN (SELECT movie_id FROM temp_movie_dim)
#                 AND md.is_current = true;
#                 --insert new records
#                 INSERT INTO tmdb_data.movie_dim (
#                     movie_id,
#                     title,
#                     original_title,         
#                     original_language,
#                     overview,
#                     tagline,
#                     status,
#                     runtime,
#                     budget,
#                     is_adult,
#                     is_video,
#                     poster_path,            
#                     backdrop_path,
#                     homepage,
#                     imdb_id,
#                     tmdb_popularity,
#                     vote_count,
#                     vote_average,
#                     effective_date,
#                     expiration_date,
#                     is_current
#                 )
#                 SELECT
#                     mds.movie_id,
#                     mds.title,
#                     mds.original_title,
#                     mds.original_language,
#                     mds.overview,
#                     mds.tagline,
#                     mds.status,
#                     mds.runtime,
#                     mds.budget,
#                     mds.is_adult,
#                     mds.is_video,
#                     mds.poster_path,
#                     mds.backdrop_path,
#                     mds.homepage,
#                     mds.imdb_id,
#                     mds.tmdb_popularity,
#                     mds.vote_count,
#                     mds.vote_average,
#                     CURRENT_DATE,
#                     NULL,
#                     true
#                 FROM temp_movie_dim mds;
#                 DROP TABLE temp_movie_dim;
#                 COMMIT;"""
                
#             cursor.execute(upsert_movie_query)
#             conn.commit()
#             context.log.info("Data loaded into movie_dim table successfully.")
            
#     end_time = time.time()
#     duration = end_time - start_time
#     context.log.info(f"Execution time: {duration} seconds")
#     context.log.info(f"Table created: tmdb_data.movie_dim")
#     context.log.info(f"Operation: create")
#     return dg.Output(
#         value=True,
#         metadata={
#             "execution_time": duration,
#             "movies_loaded": len(movie_data),
#             "staging_count": movie_staging_count,
#             "staging_table": "staging.movie_dim_staging",  
#             "table": "tmdb_data.movie_dim",
#             "operation": "load"
#         }
#     )

# @dg.asset(
#     name="update_movie_dim_review_counts",
#     required_resource_keys={"postgres"},
#     kinds={"python", "postgres", "table_update"},
#     # This asset depends on the movie dimension table being populated/updated first
#     deps=[dg.AssetKey("load_movie_dim")],
#     tags={'layer': 'silver', 'dimension': 'movie', 'enhancement': 'review_count'},
#     # Keeping partitioning consistent with the dependent asset
#     partitions_def=movie_aug_partitions_def,
# )
# def update_movie_dim_review_counts(context: dg.AssetExecutionContext):
#     """
#     Calculates the total review count for each movie from fact_movie_reviews
#     and updates the review_count column in the currently active movie_dim records.
#     """
#     start_time = time.time()
#     postgres = context.resources.postgres
#     context.log.info("Starting update of review counts in movie_dim.")

#     # SQL query to aggregate counts and update the movie_dim table
#     update_query = """
#     WITH review_counts AS (
#         -- Aggregate counts from the review fact table
#         SELECT
#             movie_id,
#             COUNT(*) AS num_reviews
#         FROM
#             tmdb_data.fact_movie_reviews
#         GROUP BY
#             movie_id
#     )
#     -- Update the main dimension table
#     UPDATE tmdb_data.movie_dim md
#     SET review_count = COALESCE(rc.num_reviews, 0) 
#     FROM review_counts rc
#     WHERE
#         md.movie_id = rc.movie_id  
#         AND md.is_current = true;  -- IMPORTANT: Only update the currently active record

#     -- Second pass: Ensure movies present in dim but *never* reviewed are set to 0
#     -- This catches movies missed by the JOIN if they have absolutely no reviews.
#     UPDATE tmdb_data.movie_dim md
#     SET review_count = 0
#     WHERE
#         md.is_current = true
#         AND md.review_count IS NULL -- Only update if not set by the previous step (or default wasn't 0)
#         AND NOT EXISTS ( -- Check explicitly if no reviews exist for this movie_id
#             SELECT 1
#             FROM tmdb_data.fact_movie_reviews fmr
#             WHERE fmr.movie_id = md.movie_id
#         );
#     """

#     rows_affected = 0
#     try:
#         with postgres.get_connection() as conn:
#             with conn.cursor() as cursor:
#                 context.log.info("Executing review count aggregation and update query...")
#                 cursor.execute(update_query)
#                 # Fetch the number of rows affected by the update
#                 rows_affected = cursor.rowcount
#                 conn.commit()
#                 context.log.info(f"Successfully updated review counts in movie_dim. Rows affected (approx): {rows_affected}")

#     except Exception as e:
#         if 'conn' in locals() and conn:
#             try:
#                 conn.rollback()
#                 context.log.info("Transaction rolled back due to error.")
#             except Exception as rb_e:
#                 context.log.error(f"Error during rollback: {rb_e}")
#         context.log.error(f"Error updating movie_dim with review counts: {e}", exc_info=True)
#         raise # Fail the asset run

#     duration = time.time() - start_time
#     context.log.info(f"Review count update finished in {duration:.2f} seconds.")

#     return dg.Output(
#         value=True, # Indicate success
#         metadata={
#             "execution_time": duration,
#             "rows_affected_approx": rows_affected,
#             "updated_table": "tmdb_data.movie_dim",
#             "updated_column": "review_count",
#             "source_table": "tmdb_data.fact_movie_reviews",
#             "operation": "update_aggregate"
#         }
#     )

@dg.asset(
    name="load_genres_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[
            dg.AssetKey("get_movie_details"), 
            dg.AssetKey("updated_movie_details")],
    partitions_def=movie_aug_partitions_def,
)
def load_genres_dim(
    context: dg.AssetExecutionContext, 
    get_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]],
    ):
    """
    Extracts unique genre combinations and loads/updates them into genres_dim (SCD Type 1)
    using genre_ids_key (e.g., '28-12-35') as the primary key.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movie_details = get_movie_details + updated_movie_details

    # Use a dictionary to store unique combinations, keyed by the NEW primary key (composite_ids)
    # Value will store the other details needed for insertion
    unique_genre_data: Dict[str, Dict[str, Any]] = {} 

    if not movie_details:
        # Handle no input data
        return dg.Output(value=True, metadata={"combinations_found": 0, "rows_affected": 0, "operation": "skipped"})

    # --- 1. Extract Unique Combinations ---
    for movie in movie_details:
        genres = movie.get("genres", [])
        if not genres or not isinstance(genres, list):
            # Handle movies with no genres consistently
            sorted_ids = ()
            sorted_names = ("None",)
            composite_name = "None"
            genre_composite_id = "None" # Use "None" as the key for no genres
        else:
            sorted_genres = sorted(genres, key=lambda g: g.get("name", ""))
            sorted_ids = tuple(g.get("id") for g in sorted_genres if g.get("id") is not None)
            sorted_names = tuple(g.get("name", "") for g in sorted_genres)
            # Create the composite name (pipe-separated names)
            composite_name = "|".join(sorted_names) if sorted_names else "None"
            # Create the composite IDs key (hyphen-separated IDs) - THIS IS THE NEW PK
            genre_composite_id = "-".join(map(str, sorted_ids)) if sorted_ids else "None"
            # Ensure consistency if names exist but IDs don't (or vice versa)
            if not sorted_ids and composite_name != "None": genre_composite_id = "None-NoIDs" # Or handle error
            if not sorted_names and genre_composite_id != "None": composite_name = "None-NoNames" # Or handle error


        # Store the details keyed by the genre_composite_id
        unique_genre_data[genre_composite_id] = {
            "composite_name": composite_name,
            "ids": list(sorted_ids),   # Convert tuple to list for DB array
            "names": list(sorted_names) # Convert tuple to list for DB array
        }

    context.log.info(f"Found {len(unique_genre_data)} unique genre combinations based on ID keys.")

    # --- 2. Insert or Update (Upsert) into DB (SCD Type 1) ---
    rows_affected = 0
    if unique_genre_data:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                # Query needs 4 placeholders for the 4 columns being explicitly set
                upsert_query = """
                    INSERT INTO tmdb_data.genres_dim (
                        genre_composite_id, 
                        genre_composite_name, 
                        genre_ids, 
                        genre_names, 
                        load_date, 
                        update_timestamp 
                    ) VALUES (%s, %s, %s, %s, CURRENT_DATE, CURRENT_TIMESTAMP)
                    ON CONFLICT (genre_composite_id) DO UPDATE SET 
                        genre_composite_name = EXCLUDED.genre_composite_name,
                        genre_ids = EXCLUDED.genre_ids,
                        genre_names = EXCLUDED.genre_names,
                        update_timestamp = CURRENT_TIMESTAMP; 
                """
                # Prepare data: list of tuples (key, name, ids_list, names_list)
                batch_data = [
                    (ids_key, details["composite_name"], details["ids"], details["names"])
                    for ids_key, details in unique_genre_data.items()
                ]

                # Log a sample
                context.log.info(f"Sample batch data for genres_dim upsert: {batch_data[:3]}")

                try:
                    cursor.executemany(upsert_query, batch_data)
                    rows_affected = cursor.rowcount
                    conn.commit()
                    context.log.info(f"Upserted {len(batch_data)} combinations into genres_dim (SCD Type 1). Approx rows affected: {rows_affected}")
                except Exception as e:
                    conn.rollback()
                    context.log.error(f"Error upserting genre combinations: {e}", exc_info=True)
                    raise

    duration = time.time() - start_time
    
    return dg.Output(value=True,
                metadata={ 
                    "execution_time": duration,
                    "combinations_processed": len(unique_genre_data),
                    "rows_affected": rows_affected,
                    "table": "tmdb_data.genres_dim",
                    "operation": "upsert_scd1"}
                )
    
@dg.asset(
    name="load_cast_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[
        dg.AssetKey("get_movie_details"),
        dg.AssetKey("updated_movie_details") 
    ],
    tags={'layer': 'silver', 'dimension': 'cast'},
    partitions_def=movie_aug_partitions_def,
)
def load_cast_dim(
    context: dg.AssetExecutionContext,
    get_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]]
    ):
    """
    Loads the top 10 cast members (by order) for each movie into the cast_dim table.
    Assumes a simple load strategy (e.g., truncate and insert or insert with ON CONFLICT DO NOTHING
    if duplicates across partitions/runs are possible and acceptable).
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movie_data_combined = get_movie_details + updated_movie_details

    # Deduplicate movies based on movie_id, keeping the latest version
    unique_movie_dict = {movie.get("movie_id"): movie for movie in movie_data_combined if movie.get("movie_id") is not None}
    movie_data = list(unique_movie_dict.values())
    context.log.info(f"Processing cast data for {len(movie_data)} unique movies.")

    cast_to_load: List[Tuple] = []
    records_processed = 0
    cast_members_extracted = 0

    MAX_LEN = 254 # Reusing MAX_LEN from load_movie_dim for consistency

    for movie in movie_data:
        records_processed += 1
        movie_id = movie.get("movie_id")
        cast_list = movie.get("cast", [])

        if not movie_id:
            context.log.warning(f"Skipping movie with missing movie_id in cast processing.")
            continue

        if not cast_list or not isinstance(cast_list, list):
            context.log.debug(f"No cast list found or invalid format for movie_id: {movie_id}")
            continue

        # Filter and sort cast members by order, then take top 10
        valid_cast = [c for c in cast_list if isinstance(c, dict) and c.get("order") is not None]
        sorted_cast = sorted(valid_cast, key=lambda c: c.get("order", float('inf')))
        top_cast = [c for c in sorted_cast if c.get("order", float('inf')) < 10]

        for cast_member in top_cast:
            cast_members_extracted += 1
            # Trim string fields to avoid potential DB errors
            name = cast_member.get("name")
            original_name = cast_member.get("original_name")
            known_for = cast_member.get("known_for_department")
            profile_path = cast_member.get("profile_path")
            character = cast_member.get("character")
            credit_id = cast_member.get("credit_id")

            cast_to_load.append((
                movie_id,
                cast_member.get("adult", False),
                cast_member.get("gender"),
                cast_member.get("id"), # This is the person ID
                known_for[:MAX_LEN] if known_for else None,
                name[:MAX_LEN] if name else None,
                original_name[:MAX_LEN] if original_name else None,
                profile_path[:MAX_LEN] if profile_path else None,
                cast_member.get("cast_id"),
                character[:MAX_LEN] if character else None,
                credit_id[:MAX_LEN] if credit_id else None,
                cast_member.get("order")
            ))

    rows_inserted = 0
    if cast_to_load:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:

                # Simple Insert Strategy (consider adding ON CONFLICT if needed)
                # If running partitioned, you might want to delete existing data for the partition first
                # Or use ON CONFLICT (movie_id, credit_id) DO NOTHING/UPDATE
                insert_query = """
                    INSERT INTO tmdb_data.cast_dim (
                        movie_id, adult, gender, person_id, known_for_department, name,
                        original_name, profile_path, cast_id, character, credit_id, cast_order
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (movie_id, credit_id) DO NOTHING; -- Example: Ignore duplicates
                """
                try:
                    postgres.execute_batch(cursor, insert_query, cast_to_load, page_size=1000)
                    rows_inserted = cursor.rowcount # Note: execute_batch might not give exact rowcount easily depending on implementation/DB
                    conn.commit()
                    context.log.info(f"Attempted to insert {len(cast_to_load)} cast records. Rows affected (approx): {rows_inserted}")
                except Exception as e:
                    conn.rollback()
                    context.log.error(f"Error loading cast_dim: {e}", exc_info=True)
                    raise

    duration = time.time() - start_time
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "movies_processed": records_processed,
            "cast_members_extracted": cast_members_extracted,
            "cast_records_loaded": rows_inserted, # This might be an approximation
            "table": "tmdb_data.cast_dim",
            "operation": "load"
        }
    )

# --- New Asset: load_crew_dim ---
@dg.asset(
    name="load_crew_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[
        dg.AssetKey("get_movie_details"),
        dg.AssetKey("updated_movie_details") # Assuming updated details also contain credits
    ],
    tags={'layer': 'silver', 'dimension': 'crew'},
    partitions_def=movie_aug_partitions_def,
)
def load_crew_dim(
    context: dg.AssetExecutionContext,
    get_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]]
    ):
    """
    Loads important crew members (Director, Screenplay, Producer, etc.) for each movie
    into the crew_dim table.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    
    movie_data_combined = get_movie_details + updated_movie_details

    # Deduplicate movies
    unique_movie_dict = {movie.get("movie_id"): movie for movie in movie_data_combined if movie.get("movie_id") is not None}
    movie_data = list(unique_movie_dict.values())
    context.log.info(f"Processing crew data for {len(movie_data)} unique movies.")

    crew_to_load: List[Tuple] = []
    records_processed = 0
    crew_members_extracted = 0

    # Define "important" jobs - adjust this list as needed
    IMPORTANT_JOBS = {
        "Director", "Screenplay", "Producer", "Director of Photography",
        "Production Design", "Costume Design", "Editor", "Original Music Composer"
    }

    MAX_LEN = 254 # Reusing MAX_LEN for consistency

    for movie in movie_data:
        records_processed += 1
        movie_id = movie.get("movie_id")
        crew_list = movie.get("crew", []) 

        if not movie_id:
            context.log.warning(f"Skipping movie with missing movie_id in crew processing.")
            continue

        if not crew_list or not isinstance(crew_list, list):
            context.log.debug(f"No crew list found or invalid format for movie_id: {movie_id}")
            continue

        for crew_member in crew_list:
            if isinstance(crew_member, dict) and crew_member.get("job") in IMPORTANT_JOBS:
                crew_members_extracted += 1
                # Trim string fields
                name = crew_member.get("name")
                original_name = crew_member.get("original_name")
                known_for = crew_member.get("known_for_department")
                profile_path = crew_member.get("profile_path")
                department = crew_member.get("department")
                job = crew_member.get("job")
                credit_id = crew_member.get("credit_id")

                crew_to_load.append((
                    movie_id,
                    crew_member.get("adult", False),
                    crew_member.get("gender"),
                    crew_member.get("id"), # Person ID
                    known_for[:MAX_LEN] if known_for else None,
                    name[:MAX_LEN] if name else None,
                    original_name[:MAX_LEN] if original_name else None,
                    profile_path[:MAX_LEN] if profile_path else None,
                    credit_id[:MAX_LEN] if credit_id else None,
                    department[:MAX_LEN] if department else None,
                    job[:MAX_LEN] if job else None,
                ))

    context.log.info(f"Extracted {crew_members_extracted} important crew members from {records_processed} movies.")

    rows_inserted = 0
    if crew_to_load:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:

                # Simple Insert Strategy
                insert_query = """
                    INSERT INTO tmdb_data.crew_dim (
                        movie_id, adult, gender, person_id, known_for_department, name,
                        original_name, profile_path, credit_id, department, job
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (movie_id, credit_id) DO NOTHING; -- Example: Ignore duplicates
                """
                try:
                    postgres.execute_batch(cursor, insert_query, crew_to_load, page_size=1000)
                    rows_inserted = cursor.rowcount # Approximation
                    conn.commit()
                    context.log.info(f"Attempted to insert {len(crew_to_load)} crew records. Rows affected (approx): {rows_inserted}")
                except Exception as e:
                    conn.rollback()
                    context.log.error(f"Error loading crew_dim: {e}", exc_info=True)
                    raise

    duration = time.time() - start_time
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "movies_processed": records_processed,
            "crew_members_extracted": crew_members_extracted,
            "crew_records_loaded": rows_inserted, # Approximation
            "table": "tmdb_data.crew_dim",
            "operation": "load"
        }
    )
    
@dg.asset(name="load_movie_performance_fact",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[dg.AssetKey("get_movie_details"),
            dg.AssetKey("update_movie_dim_review_counts"), 
            dg.AssetKey("load_genres_dim"),
            dg.AssetKey("load_cast_dim"),
            dg.AssetKey("load_crew_dim"),
            dg.AssetKey("updated_movie_details")],
    tags={'layer': 'silver'},
    partitions_def=movie_aug_partitions_def,
)
def load_movie_performance_fact(
    context: dg.AssetExecutionContext, 
    get_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]],
    ):
    """
    Load movie performance data into the movie_performance_fact table.

    Args:
        context (dg.AssetExecutionContext): _description_
        get_movie_details (_type_): _description_
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movie_data = get_movie_details + updated_movie_details
    
    if not movie_data:
        context.log.info("No movie data received. Skipping performance fact load.")
        return dg.Output(value=True, metadata={"records_processed": 0, "records_inserted": 0, "operation": "skipped"})
    
    records_processed = 0
    records_inserted = 0
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            
            
            # Insert data into the movie_performance_fact table
            partition_date_str = context.partition_key
            load_date = datetime.now().strftime("%Y-%m-%d")
            update_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            

            batch_data_to_insert = []
            for data in movie_data:
                records_processed += 1
                movie_id = data.get("movie_id")

                if not movie_id:
                    context.log.warning(f"Skipping record due to missing movie_id.")
                    continue
                
                genres = data.get("genres", [])
                release_date = data.get("release_date") if data.get("release_date") != '' else None
                if not genres or not isinstance(genres, list): genre_composite_id = "None"
                else:
                    sorted_genres = sorted(genres, key=lambda g: g.get("name", ""))
                    sorted_ids = tuple(g.get("id") for g in sorted_genres if g.get("id") is not None)
                    genre_composite_id = "-".join(map(str, sorted_ids)) if sorted_ids else "None"
                    
                # Prepare tuple in the correct order for the INSERT statement
                batch_data_to_insert.append((
                    data.get("movie_id"), # Use the looked-up key
                    release_date,
                    genre_composite_id,
                    data.get("revenue"),
                    data.get("budget"),
                    data.get("profit"),
                    data.get("roi"),
                    data.get("domestic_revenue"),
                    data.get("international_revenue"),
                    data.get("opening_weekend_revenue"),
                    data.get("box_office_rank"),
                    data.get("is_profitable"),
                    partition_date_str,
                    load_date,
                    update_date,
                    
                ))
            if batch_data_to_insert:
                insert_query = """
                INSERT INTO tmdb_data.movie_performance_fact (
                    movie_id, release_date, genre_composite_id, revenue, budget, profit, roi,
                    domestic_revenue, international_revenue, opening_weekend_revenue,
                    box_office_rank, is_profitable, partition_date, load_date, update_timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (movie_id) 
                DO NOTHING
                """
                try:
                    # Using execute_batch for potential performance gains over executemany
                    postgres.execute_batch(cursor, insert_query, batch_data_to_insert, page_size=1000) # Adjust page_size as needed
                    conn.commit()
                    movies_loaded_count = len(batch_data_to_insert)
                    context.log.info(f"Successfully inserted {movies_loaded_count} records using execute_batch.")
                
                except Exception as e:
                    conn.rollback() # Rollback on error during batch insert
                    context.log.error(f"Error during batch insert: {e}")
                    context.log.info("Rolling back transaction due to error.")
                    raise
            else:
                context.log.info("No valid data prepared for insertion after lookup and validation.")
    
    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration} seconds")
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "movies_loaded": len(movie_data),
            "table": "tmdb_data.movie_performance_fact",
            "operation": "load"
        }
    )