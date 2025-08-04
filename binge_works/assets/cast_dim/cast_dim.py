import dagster as dg
from ...constants import movie_aug_partitions_def
from typing import List, Dict, Any, Tuple
import time


@dg.asset(
    name="load_cast_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[
        dg.AssetKey("get_new_movie_details"),
        dg.AssetKey("updated_movie_details") 
    ],
    tags={'layer': 'silver', 'dimension': 'cast'},
    partitions_def=movie_aug_partitions_def,
)
def load_cast_dim(
    context: dg.AssetExecutionContext,
    get_new_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]]
    ):
    """
    Loads the top 10 cast members (by order) for each movie into the cast_dim table.
    Assumes a simple load strategy (e.g., truncate and insert or insert with ON CONFLICT DO NOTHING
    if duplicates across partitions/runs are possible and acceptable).
    """
    start_time = time.time()
    postgres = context.resources.postgres
    movie_data_combined = get_new_movie_details + updated_movie_details

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
