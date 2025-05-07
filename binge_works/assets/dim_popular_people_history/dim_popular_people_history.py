import dagster as dg
from dagster import Output, AssetKey, MetadataValue
import time


@dg.asset(name="insert_people_history",
        kinds={"sql", "postgres", "silver"},
        required_resource_keys={"postgres"})
def insert_people_history(context):
    """Asset to insert popular people data into the dim_people_history table"""
    postgres = context.resources.postgres
    start_time = time.time()
    new_records = 0
    updated_records = 0
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # First, count how many records will be new vs updated
            count_query = """
            select count(*) from (
            SELECT 
                COUNT(CASE WHEN ph.id IS NULL THEN 1 END) as new_records
            FROM staging.popular_people pp
            LEFT JOIN tmdb_data.dim_people_history ph
                ON pp.id = ph.id AND ph.is_current = true
            WHERE 
                -- New records that don't exist in history table
                (ph.id IS NULL AND pp.id IS NOT NULL)
                OR 
                -- Existing records that have changed
                (ph.is_current = true AND (
                    pp.gender != ph.gender OR
                    pp.known_for_department != ph.known_for_department OR
                    pp.name != ph.name OR
                    pp.popularity != ph.popularity OR
                    pp.profile_path != ph.profile_path))
                ) counts
            """
            cursor.execute(count_query)
            new_records  = cursor.fetchone()["count"]
            
            # Then execute the actual upsert
            upsert_shows_history = """
            BEGIN;

            -- Create temp table with records that need to be inserted or updated
            CREATE TEMP TABLE temp_people AS
            SELECT 
                pp.id, 
                pp.gender, 
                pp.known_for_department, 
                pp.name, 
                pp.popularity, 
                pp.profile_path
            FROM staging.popular_people pp
            LEFT JOIN tmdb_data.dim_people_history ph
                ON pp.id = ph.id AND ph.is_current = true
            WHERE 
                -- New records that don't exist in history table
                (ph.id IS NULL AND pp.id IS NOT NULL)
                OR 
                -- Existing records that have changed
                (ph.is_current = true AND (
                    pp.gender != ph.gender OR
                    pp.known_for_department != ph.known_for_department OR
                    pp.name != ph.name OR
                    pp.popularity != ph.popularity OR
                    pp.profile_path != ph.profile_path
                ));

            -- Update existing records to mark them as expired
            UPDATE tmdb_data.dim_people_history ph
            SET expiration_date = CURRENT_DATE, is_current = false
            WHERE id IN (SELECT id FROM temp_people)
            AND is_current = true;

            -- Insert new versions with a generated surrogate key
            INSERT INTO tmdb_data.dim_people_history (
                id_sk, id, gender, known_for_department, name, popularity, 
                profile_path, effective_date, expiration_date, is_current
            )
            SELECT
                -- Generate a new unique surrogate key based on the maximum value in the table
                COALESCE((SELECT MAX(id_sk) FROM tmdb_data.dim_people_history), 1000000) + ROW_NUMBER() OVER (ORDER BY id),
                id, gender, known_for_department, name, popularity, 
                profile_path, CURRENT_DATE, NULL, true
            FROM temp_people;

            DROP TABLE temp_people;
            COMMIT;
            """
            cursor.execute(upsert_shows_history)
            
    end_time = time.time()
    duration = end_time - start_time
    
    return Output(
        value=True,
        metadata={
            "operation": "insert",
            "execution_time": MetadataValue.float(duration),
            "table": "tmdb_data.dim_people_history",
            "new_records": MetadataValue.int(new_records),
        }
    )