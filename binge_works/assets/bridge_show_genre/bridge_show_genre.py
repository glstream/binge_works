import dagster as dg
from dagster import Output, AssetKey, MetadataValue
import time


@dg.asset(
    name="update_bridge_show_genre",
    deps=[dg.AssetKey("load_to_production"),  # Depends on dim_popular_shows 
        dg.AssetKey("load_dim_tv_genres")],  # Depends on dim_tv_genres])
    required_resource_keys={"postgres"},
    kinds={"sql", "postgres", "gold"},
    tags={'layer': 'dimensional'}
)
def update_bridge_show_genre(context):
    """Asset to incrementally update the bridge table between shows and genres"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create a temporary table with the current show-genre relationships from dimension table
            cursor.execute("""
            CREATE TEMP TABLE temp_show_genre AS
            SELECT
                s.id AS show_id,
                (jsonb_array_elements_text(s.genre_ids))::INTEGER AS genre_id
            FROM
                tmdb_data.dim_popular_shows s
            WHERE
                jsonb_array_length(s.genre_ids) > 0;
            """)
            
            # Find records to delete (exist in bridge but not in current data)
            delete_query = """
            DELETE FROM tmdb_data.bridge_show_genre b
            WHERE NOT EXISTS (
                SELECT 1 FROM temp_show_genre t
                WHERE t.show_id = b.show_id AND t.genre_id = b.genre_id
            )
            AND b.show_id IN (SELECT DISTINCT show_id FROM temp_show_genre);
            """
            cursor.execute(delete_query)
            deleted_count = cursor.rowcount
            
            # Find records to add (exist in current data but not in bridge)
            insert_query = """
            INSERT INTO tmdb_data.bridge_show_genre (show_id, genre_id)
            SELECT t.show_id, t.genre_id
            FROM temp_show_genre t
            WHERE NOT EXISTS (
                SELECT 1 FROM tmdb_data.bridge_show_genre b
                WHERE b.show_id = t.show_id AND b.genre_id = t.genre_id
            )
            ON CONFLICT DO NOTHING;
            """
            cursor.execute(insert_query)
            inserted_count = cursor.rowcount
            
            # Get statistics
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.bridge_show_genre")
            total_count = cursor.fetchone()["count"]
            
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT show_id) as show_count,
                    COUNT(DISTINCT genre_id) as genre_count
                FROM tmdb_data.bridge_show_genre
            """)
            stats = cursor.fetchone()
            
            # Drop the temp table
            cursor.execute("DROP TABLE temp_show_genre")
    
    duration = time.time() - start_time
    
    return Output(
        value={
            "deleted_count": deleted_count,
            "inserted_count": inserted_count,
            "total_count": total_count,
            "table": "tmdb_data.bridge_show_genre",
            "show_count": stats["show_count"],
            "genre_count": stats["genre_count"]
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "deleted_count": MetadataValue.int(deleted_count),
            "inserted_count": MetadataValue.int(inserted_count),
            "total_count": MetadataValue.int(total_count),
            "show_count": MetadataValue.int(stats["show_count"]),
            "genre_count": MetadataValue.int(stats["genre_count"])
        }
    )