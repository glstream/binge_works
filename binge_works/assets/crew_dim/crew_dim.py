import dagster as dg
from ...constants import movie_aug_partitions_def
from typing import List, Dict, Any, Tuple
import time


@dg.asset(
    name="load_crew_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[
        dg.AssetKey("get_new_movie_details"),
        dg.AssetKey("updated_movie_details") # Assuming updated details also contain credits
    ],
    tags={'layer': 'silver', 'dimension': 'crew'},
    partitions_def=movie_aug_partitions_def,
)
def load_crew_dim(
    context: dg.AssetExecutionContext,
    get_new_movie_details: List[Dict[str, Any]],
    updated_movie_details: List[Dict[str, Any]]
    ):
    """
    Loads important crew members (Director, Screenplay, Producer, etc.) for each movie
    into the crew_dim table.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    
    movie_data_combined = get_new_movie_details + updated_movie_details

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
    