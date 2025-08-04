import dagster as dg
from ...constants import movie_aug_partitions_def
from typing import List, Dict, Any
import time


@dg.asset(
    name="load_genres_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[
            dg.AssetKey("get_new_movie_details"), 
            dg.AssetKey("updated_movie_details")],
    partitions_def=movie_aug_partitions_def,
)
def load_genres_dim(
    context: dg.AssetExecutionContext, 
    get_new_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]],
    ):
    """
    Extracts unique genre combinations and loads/updates them into genres_dim (SCD Type 1)
    using genre_ids_key (e.g., '28-12-35') as the primary key.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movie_details = get_new_movie_details + updated_movie_details

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