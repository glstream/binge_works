# In movie_performance_fact.py

import dagster as dg
import time
import json
from datetime import datetime
from typing import List, Dict, Any, Set, Tuple
from ...constants import movie_aug_partitions_def

# @dg.asset(
#     name="get_new_movie_details",
#     required_resource_keys={"tmdb"},
#     kinds={"python", "api", "task"},
#     deps=[dg.AssetKey("get_new_movie_ids_for_details")],    
#     tags={'layer': 'staging'},
#     partitions_def=movie_aug_partitions_def,
# )
# def get_new_movie_details(
#     context: dg.AssetExecutionContext, 
#     get_new_movie_ids_for_details: List[int]):
#     """
#     Fetch detailed movie details and store and prepare for loading into the movie_performance_fact table.

#     Args:
#         context (dg.AssetExecutionContext): _description_
#         get_new_movie_ids_for_details (_type_): _description_
#     """
#     movie_ids = get_new_movie_ids_for_details
#     if movie_ids is None:
#         context.log.warning("Input 'get_new_movie_ids_for_details' is None. Skipping processing.")
#         unique_movie_ids: Set[int] = set()
#         input_count = 0
#     elif not isinstance(movie_ids, (list, tuple, set)):
#         # Handle unexpected input types gracefully
#         context.log.error(f"Input 'get_new_movie_ids_for_details' is not a list/tuple/set (type: {type(movie_ids)}). Skipping processing.")
#         unique_movie_ids: Set[int] = set()
#         input_count = 0 # Or len(movie_ids_input) if trying to count elements anyway? Safer as 0.
#     else:
#         unique_movie_ids: Set[int] = set(movie_ids)
#         input_count = len(movie_ids)

#     context.log.info(f"Received {input_count} movie IDs, processing {len(unique_movie_ids)} unique IDs.")
#     context.log.info(f"Movie IDs: {movie_ids}")
#     start_time = time.time()
#     tmdb = context.resources.tmdb
    
#     movie_data = []    
    
#     for movie_id in unique_movie_ids:
#         try:
#             # Fetch movie details
#             movie_details = tmdb.get_new_movie_details(movie_id, append_to_response="credits,production_companies")
            
#             if not movie_details or not movie_details.get("id"):
#                 context.log.warning(f"Skipping Movie ID {movie_id}: Invalid or empty response from API.")
#                 continue

#             # context.log.info(f"Movie Details: {json.dumps(movie_details, indent=2)}")
#             budget = movie_details.get("budget", 0)
#             profit = movie_details.get("revenue", 0) - budget
#             roi = (profit / budget) * 100 if budget > 0 else 0
#             is_profitable = profit > 0
            
#             genre_list = movie_details.get("genres", [])
#             cast_list = movie_details.get("credits", {}).get("cast", [])
#             crew_list = movie_details.get("credits", {}).get("crew", [])

            
#             # Prepare data for loading
#             movie_data.append({
#                 "movie_id": movie_details.get("id"),
#                 "relese_date:": movie_details.get("release_date"),
#                 "title": movie_details.get("title"),
#                 "original_title": movie_details.get("original_title"),
#                 "original_language": movie_details.get("original_language"),
#                 "overview": movie_details.get("overview"),
#                 "tagline": movie_details.get("tagline"),
#                 "status": movie_details.get("status"),
#                 "runtime": movie_details.get("runtime"),
#                 "is_adult": movie_details.get("adult", False),
#                 "is_video": movie_details.get("video", False),
#                 "poster_path": movie_details.get("poster_path"),
#                 "backdrop_path": movie_details.get("backdrop_path"),
#                 "homepage": movie_details.get("homepage"),
#                 "imdb_id": movie_details.get("imdb_id"),
#                 "tmdb_popularity": movie_details.get("popularity"),
#                 "vote_count": movie_details.get("vote_count"),
#                 "vote_average": movie_details.get("vote_average"),
#                 "revenue": movie_details.get("revenue"),
#                 "profit": profit,
#                 "budget": budget,
#                 "roi": roi,
#                 "is_profitable": is_profitable,
#                 "genres": genre_list,
#                 "cast": cast_list, 
#                 "crew": crew_list,
#                 "domestic_revenue": movie_details.get("domestic_revenue"),
#                 "international_revenue": movie_details.get("international_revenue"),
#                 "opening_weekend_revenue": movie_details.get("opening_weekend_revenue"),
#                 "box_office_rank": movie_details.get("box_office_rank") 
#             })
            
#             # context.log.info(f"Prepared data for Movie ID {movie_id}: {movie_details}")
            
#         except Exception as e:
#             context.log.error(f"Error fetching details for Movie ID {movie_id}: {e}")
            
#     end_time = time.time()
#     duration = end_time - start_time    
    
#     return dg.Output(
#         value=movie_data,
#         metadata={
#             "execution_time": duration,
#             "movies_prepared": len(movie_ids),
#             "api": "movie/movie_id",
#             "operation": "request"
#         }
#     )

@dg.asset(name="load_movie_performance_fact",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[dg.AssetKey("get_new_movie_details"),
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
    get_new_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]],
    ):
    """
    Load movie performance data into the movie_performance_fact table.

    Args:
        context (dg.AssetExecutionContext): _description_
        get_new_movie_details (_type_): _description_
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movie_data = get_new_movie_details + updated_movie_details
    
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