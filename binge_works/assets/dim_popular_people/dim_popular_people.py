import dagster as dg
from dagster import AssetKey, Output, MetadataValue
import json
import time

# "adult": false,
#             "gender": 2,
#             "id": 108588,
#             "known_for_department": "Acting",
#             "name": "Darri Ingolfsson",
#             "original_name": "Darri Ingolfsson",
#             "popularity": 3.112,
#             "profile_path": "/eBavqmvod9lKuyNw9BxL78UnKkX.jpg",

@dg.asset(name="insert_dim_popular_people",
        kinds={"sql", "postgres", "silver"},
        deps=[dg.AssetKey("deduplicate_popular_people_by_name")],
        tags={'layer':'silver'},
        required_resource_keys={"postgres"}
        )
def insert_dim_popular_people(context):
        """Asset to insert popular people data into the dim_popular_people table"""
        start_time = time.time()
        postgres = context.resources.postgres
        
        with postgres.get_connection() as conn:
                with conn.cursor() as cursor:
                        # Get count before the merge
                        cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_popular_people")
                        before_count = cursor.fetchone()["count"]
                        insert_query = """
                                INSERT INTO tmdb_data.dim_popular_people as dp (
                                        id, gender, known_for_department, name, popularity, profile_path, insert_timestamp, last_update
                                        )
                                SELECT
                                        id, gender, known_for_department, name, popularity, profile_path, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                                FROM
                                        staging.popular_people
                                ON CONFLICT (id) 
                                DO UPDATE SET 
                                        gender = EXCLUDED.gender,
                                        known_for_department = EXCLUDED.known_for_department,
                                        name = EXCLUDED.name,
                                        popularity = EXCLUDED.popularity,
                                        profile_path = EXCLUDED.profile_path,
                                        last_update = CURRENT_TIMESTAMP
                                WHERE 
                                        dp.gender IS DISTINCT FROM EXCLUDED.gender OR
                                        dp.known_for_department IS DISTINCT FROM EXCLUDED.known_for_department OR
                                        dp.name IS DISTINCT FROM EXCLUDED.name OR
                                        dp.popularity IS DISTINCT FROM EXCLUDED.popularity OR
                                        dp.profile_path IS DISTINCT FROM EXCLUDED.profile_path  
                                """
                        cursor.execute(insert_query)
                        affected_rows = cursor.rowcount
                        
                        conn.commit()
                        
                        # Get count after the merge
                        cursor.execute("SELECT COUNT(*) FROM tmdb_data.dim_popular_people")
                        after_count = cursor.fetchone()["count"]
                        
                        # Get stats about production data
                        cursor.execute("""
                                SELECT 
                                MIN(popularity) as min_pop,
                                MAX(popularity) as max_pop,
                                AVG(popularity) as avg_pop,
                                COUNT(*) as total
                                FROM tmdb_data.dim_popular_people
                        """)
                        stats = cursor.fetchone()
        duration = time.time() - start_time
        
        # Calculate new vs updated records
        new_records = after_count - before_count
        updated_records = affected_rows - new_records
        
        return Output(
                value={
                        "total_records": after_count,
                        "new_records": new_records,
                        "affected_records": affected_rows,
                        "updated_records": updated_records,
                        "stats": stats
                },
                metadata={
                        "execution_time": MetadataValue.float(duration),
                        "table": MetadataValue.text("tmdb_data.dim_popular_people"),
                        "operation": MetadataValue.text("insert")
                }
        )
