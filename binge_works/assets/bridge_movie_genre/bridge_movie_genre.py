import dagster as dg
from dagster import Output, AssetKey, MetadataValue
import time
import logging # Import logging

# Get a logger instance
log = logging.getLogger(__name__)

# --- Optional: Define Asset Key for the source if known ---
# If tmdb_data.movie_performance_fact is represented by a specific asset,
# you might want to define its key here for clarity and lineage.
# Example: MOVIE_PERFORMANCE_FACT = AssetKey("movie_performance_fact")

@dg.asset(
    name="update_bridge_movie_genre",
    deps=[AssetKey("load_movie_performance_fact")],
    required_resource_keys={"postgres"},
    compute_kind="sql", # Changed from 'kinds' to 'compute_kind' for modern Dagster
    tags={'layer': 'dimensional'} # You can keep tags if preferred
)
def update_bridge_movie_genre(context):
    """
    Asset to populate the bridge_movie_genre table by unpacking
    hyphen-separated genre IDs (genre_composite_id) from the
    movie_performance_fact table. Uses ON CONFLICT DO NOTHING to handle
    existing movie-genre pairs efficiently.
    """
    start_time = time.time()
    postgres = context.resources.postgres
    inserted_count = 0
    total_count = 0
    stats = {"movie_count": 0, "genre_count": 0}

    # Your SQL query, slightly simplified to rely solely on ON CONFLICT for idempotency
    # (assuming a UNIQUE or PRIMARY KEY constraint exists on (movie_id, genre_id) in bridge_movie_genre)
    update_query = """
    WITH MovieGenrePairs AS (
        -- Step 1: Unpack the genre IDs for each movie
        SELECT
            f.movie_id,
            -- Split the string by '-', unnest the resulting array into separate rows,
            -- and cast the genre_id (which is text after splitting) to an integer.
            (unnest(string_to_array(f.packed_genre_ids_string, '-')))::INTEGER AS genre_id
        FROM (
            -- Select distinct movie IDs and their packed genre strings from the source table
            SELECT DISTINCT
                movie_id AS movie_id,                     -- Source movie ID column
                genre_composite_id AS packed_genre_ids_string -- Source packed genre ID column
            FROM
                tmdb_data.movie_performance_fact          -- Source table
            WHERE
                genre_composite_id IS NOT NULL
                AND genre_composite_id != ''
                AND genre_composite_id != 'None'          -- Ensure the genre string is valid
        ) AS f
    )
    -- Step 2: Insert the unpacked pairs into the bridge table
    INSERT INTO tmdb_data.bridge_movie_genre (movie_id, genre_id)
    SELECT
        mgp.movie_id,
        mgp.genre_id
    FROM
        MovieGenrePairs mgp
    -- Use ON CONFLICT assuming a UNIQUE or PRIMARY KEY constraint exists on (movie_id, genre_id)
    -- This handles both duplicates within the source data (e.g., "35-28-35") and pre-existing rows.
    ON CONFLICT (movie_id, genre_id) DO NOTHING;
    """

    try:
        with postgres.get_connection() as conn:
            # It's good practice to run DML (INSERT/UPDATE/DELETE) within a transaction
            with conn.cursor() as cursor:
                context.log.info("Populating bridge_movie_genre from movie_performance_fact...")
                cursor.execute(update_query)
                # cursor.rowcount for INSERT ON CONFLICT DO NOTHING in PostgreSQL
                # typically reports the number of rows actually inserted.
                inserted_count = cursor.rowcount
                context.log.info(f"Successfully inserted {inserted_count} new rows.")

                # Get post-update statistics
                context.log.info("Fetching updated table statistics...")
                cursor.execute("SELECT COUNT(*) as total_count FROM tmdb_data.bridge_movie_genre")
                total_count_result = cursor.fetchone()
                total_count = total_count_result['total_count'] if total_count_result else 0 # Fetch by name if cursor returns dicts

                cursor.execute("""
                    SELECT
                        COUNT(DISTINCT movie_id) as movie_count,
                        COUNT(DISTINCT genre_id) as genre_count
                    FROM tmdb_data.bridge_movie_genre
                """)
                stats_result = cursor.fetchone()
                if stats_result:
                    stats = {"movie_count": stats_result['movie_count'] if stats_result['movie_count'] is not None else 0,
                            "genre_count": stats_result['genre_count'] if stats_result['genre_count'] is not None else 0}
                else:
                    stats = {"movie_count": 0, "genre_count": 0} # Ensure stats are zeroed if table is empty

        duration = time.time() - start_time
        context.log.info(f"bridge_movie_genre update complete in {duration:.2f} seconds. Total rows: {total_count}")

        return Output(
            value={
                "inserted_count": inserted_count, # Rows actually inserted by this run
                "total_count": total_count,
                "table": "tmdb_data.bridge_movie_genre",
                "movie_count": stats["movie_count"],
                "genre_count": stats["genre_count"]
            },
            metadata={
                "execution_time": MetadataValue.float(duration),
                "inserted_count": MetadataValue.int(inserted_count),
                "total_count": MetadataValue.int(total_count),
                "movie_count": MetadataValue.int(stats["movie_count"]),
                "genre_count": MetadataValue.int(stats["genre_count"]),
                "preview": MetadataValue.md(f"Inserted **{inserted_count}** new rows. Total rows now **{total_count}**.") # Example enhanced metadata
            }
        )
    except Exception as e:
        context.log.error(f"Error updating bridge_movie_genre: {e}")
        # Re-raise the exception to fail the Dagster run and surface the error
        raise