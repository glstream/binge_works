import dagster as dg
from dagster import Output, AssetKey, MetadataValue
import time

@dg.asset(name="update_person_project_bridge",
        kinds={"sql", "postgres", "silver"},
        deps=[dg.AssetKey("deduplicate_popular_people_by_name"), dg.AssetKey("insert_dim_popular_people") ],
        required_resource_keys={"postgres"})
def update_person_project_bridge(context):
    """Asset to incrementally update the bridge table from the JSONB data"""
    context.log.info("Updating person-project bridge table")
    postgres = context.resources.postgres
    start_time = time.time()
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create a temporary table with the current person-project relationships
            cursor.execute("""
            CREATE TEMP TABLE temp_person_project AS
            Select person_id, 
			project_id, 
			media_type, 
			coalesce(title, name) as project_title
			from (SELECT 
                pp.id AS person_id,
                (jsonb_array_elements(known_for)->>'id')::integer AS project_id,
                jsonb_array_elements(known_for)->>'media_type' AS media_type,
                jsonb_array_elements(known_for)->>'title' as title,
				jsonb_array_elements(known_for)->>'name' as name
            FROM staging.popular_people pp) t1
            """)
            
            # Find records to delete (exist in bridge but not in current data)
            # Only for person IDs that exist in the current dataset
            delete_query = """
            DELETE FROM tmdb_data.bridge_person_project b
            WHERE NOT EXISTS (
                SELECT 1 FROM temp_person_project t
                WHERE t.person_id = b.person_id AND t.project_id = b.project_id
            )
            AND b.person_id IN (SELECT DISTINCT person_id FROM temp_person_project);
            """
            cursor.execute(delete_query)
            deleted_count = cursor.rowcount
            
            # Find records to add or update
            upsert_query = """
            INSERT INTO tmdb_data.bridge_person_project (
                person_id, 
                project_id, 
                media_type, 
                project_title,
                last_updated
            )
            SELECT 
                t.person_id,
                t.project_id,
                t.media_type,
                t.project_title,
                CURRENT_TIMESTAMP
            FROM temp_person_project t
            ON CONFLICT (person_id, project_id) 
            DO UPDATE SET
                media_type = EXCLUDED.media_type,
                project_title = EXCLUDED.project_title,
                last_updated = CURRENT_TIMESTAMP
            WHERE 
                bridge_person_project.media_type IS DISTINCT FROM EXCLUDED.media_type OR
                bridge_person_project.project_title IS DISTINCT FROM EXCLUDED.project_title;
            """
            cursor.execute(upsert_query)
            upserted_count = cursor.rowcount
            
            # Get statistics
            cursor.execute("SELECT COUNT(*) FROM tmdb_data.bridge_person_project")
            total_count = cursor.fetchone()["count"]
            
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT person_id) as person_count,
                    COUNT(DISTINCT project_id) as project_count,
                    COUNT(DISTINCT media_type) as media_type_count
                FROM tmdb_data.bridge_person_project
            """)
            stats = cursor.fetchone()
            
            # Drop the temp table
            cursor.execute("DROP TABLE temp_person_project")
    
    end_time = time.time()
    duration = end_time - start_time
    
    return Output(
        value={
            "deleted_count": deleted_count,
            "upserted_count": upserted_count,
            "total_count": total_count,
            "table": "tmdb_data.bridge_person_project"
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "deleted_count": MetadataValue.int(deleted_count),
            "upserted_count": MetadataValue.int(upserted_count),
            "total_count": MetadataValue.int(total_count),
            "person_count": MetadataValue.int(stats["person_count"]),
            "project_count": MetadataValue.int(stats["project_count"]),
            "media_type_count": MetadataValue.int(stats["media_type_count"])
        }
    )