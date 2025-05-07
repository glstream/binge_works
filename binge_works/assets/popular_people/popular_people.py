import dagster as dg
from dagster import AssetKey, Output, MetadataValue
import json
import time

from binge_works.resources.TMDBResource import TMDBResource

@dg.asset(
    name="truncate_popular_people_table",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "bronze"},
    tags={'layer':'staging'},
)
def truncate_popular_people_table(context):
    """Asset to truncate the popular_people staging table before loading new data"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
            
            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS staging.popular_people (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    adult BOOLEAN,
                    gender INTEGER,
                    known_for_department TEXT,
                    popularity FLOAT,
                    profile_path TEXT,
                    known_for JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Truncate the table
            cursor.execute("TRUNCATE TABLE staging.popular_people")
            conn.commit()
    
    duration = time.time() - start_time
    
    return Output(
        value=True,
        metadata={
            "execution_time": MetadataValue.float(duration),
            "table": MetadataValue.text("staging.popular_people"),
            "operation": MetadataValue.text("truncate")
        }
    )
    
@dg.asset(name="extract_popular_people_metadata",
            kinds={"python", "bronze"},
            deps=[AssetKey("truncate_popular_people_table")],
            tags={'layer':'source'},
            )
def extract_popular_people_metadata(context, tmdb: TMDBResource):
    """Asset to extract metadata about Popeular people the API results"""
    start_time = time.time()
    
    # Get the first page to understand the structure and total pages
    response = tmdb.get_popular_people(page=1)
    
    metadata = {
        "total_pages": response.get("total_pages", 0),
        "total_results": response.get("total_results", 0),
        "page_size": len(response.get("results", [])),
    }
    
    duration = time.time() - start_time
    
    return Output(
        value=metadata,
        metadata={
            "execution_time": MetadataValue.float(duration),
            "total_pages": MetadataValue.int(metadata["total_pages"]),
            "total_results": MetadataValue.int(metadata["total_results"]),
            "page_size": MetadataValue.int(metadata["page_size"]),
        }
    )
    
@dg.asset(
    name="extract_all_popular_people",
    deps=[AssetKey("extract_popular_people_metadata")],
    kinds={"python", "bronze"},
    tags={'layer':'source'},
)
def extract_all_popular_people(context, tmdb: TMDBResource, extract_popular_people_metadata):
    """Asset to extract all popular people from the API"""
    start_time = time.time()
    
    total_pages = min(extract_popular_people_metadata["total_pages"], 500)
    
    # Use a dictionary to ensure uniqueness by ID
    unique_people = {}
    
    # Process all pages sequentially
    for page in range(1, total_pages + 1):
        try:
            response = tmdb.get_popular_people(page=page)
            people = response.get("results", [])
            
            # Add to the unique people dictionary, using ID as key
            for person in people:
                if 'id' in person:
                    unique_people[person['id']] = person
            
            # Log progress at regular intervals
            if page % 50 == 0 or page == 1 or page == total_pages:
                context.log.info(f"Processed page {page} with {len(people)} people, {len(unique_people)} unique people so far")
        except Exception as e:
            context.log.error(f"Error processing page {page}: {str(e)}")
            break
    
    # Convert back to a list
    all_people = list(unique_people.values())
    
    duration = time.time() - start_time
    
    # Extract some statistics for metadata
    gender_counts = {
        0: len([p for p in all_people if p.get("gender") == 0]),  # Not specified
        1: len([p for p in all_people if p.get("gender") == 1]),  # Female
        2: len([p for p in all_people if p.get("gender") == 2]),  # Male
        3: len([p for p in all_people if p.get("gender") == 3])   # Non-binary
    }
    
    # Calculate average popularity
    popularity_values = [p.get("popularity", 0) for p in all_people]
    avg_popularity = sum(popularity_values) / len(popularity_values) if popularity_values else 0
    
    # Count people with profile images
    with_images = len([p for p in all_people if p.get("profile_path")])
    
    return Output(
        value=all_people,
        metadata={
            "execution_time": MetadataValue.float(duration),
            "total_people": MetadataValue.int(len(all_people)),
            "pages_processed": MetadataValue.int(total_pages),
            "gender_distribution": {
                "not_specified": MetadataValue.int(gender_counts[0]),
                "female": MetadataValue.int(gender_counts[1]),
                "male": MetadataValue.int(gender_counts[2]),
                "non_binary": MetadataValue.int(gender_counts[3])
            },
            "avg_popularity": MetadataValue.float(round(avg_popularity, 2)),
            "with_profile_images": MetadataValue.int(with_images),
            "with_images_percentage": MetadataValue.float(
                round(with_images / len(all_people) * 100, 2) if all_people else 0
            ),
        }
    )
    
@dg.asset(
    name="load_popular_people_to_postgres",
    deps=[
        AssetKey("truncate_popular_people_table"),
        AssetKey("extract_all_popular_people")
    ],
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "bronze"},
    tags={'layer':'staging'},
)
def load_popular_people_to_postgres(context, extract_all_popular_people):
    """Asset to load popular people data into PostgreSQL staging table"""
    start_time = time.time()
    postgres = context.resources.postgres
    people_data = extract_all_popular_people
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Prepare data for insertion
            people_data_list = []
            
            for person in people_data:
                # Convert known_for list to JSON string for JSONB field
                known_for_json = json.dumps(person.get("known_for", []))
                
                person_data = (
                    person.get("id"),
                    person.get("name"),
                    person.get("adult", False),
                    person.get("gender"),
                    person.get("known_for_department"),
                    person.get("popularity"),
                    person.get("profile_path"),
                    known_for_json
                )
                people_data_list.append(person_data)
            
            # Execute batch insert
            context.log.info(f"Batch inserting {len(people_data_list)} records...")
            
            insert_query = """
            INSERT INTO staging.popular_people 
            (id, name, adult, gender, known_for_department, popularity, profile_path, known_for)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """
            
            # Assuming postgres.execute_batch exists similar to the shows example
            postgres.execute_batch(cursor, insert_query, people_data_list, page_size=1000)
            conn.commit()
            
            # Get statistics about the staging table
            cursor.execute("SELECT COUNT(*) FROM staging.popular_people")
            total_count = cursor.fetchone()["count"]
            
            # Get gender distribution stats
            cursor.execute("""
                SELECT 
                    gender,
                    COUNT(*) as count
                FROM staging.popular_people
                GROUP BY gender
            """)
            gender_stats = {row["gender"]: row["count"] for row in cursor.fetchall()}
    
    duration = time.time() - start_time
    
    return Output(
        value={
            "records_inserted": len(people_data_list),
            "table": "staging.popular_people",
            "total_count": total_count
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "records_inserted": MetadataValue.int(len(people_data_list)),
            "total_count": MetadataValue.int(total_count),
            "gender_distribution": {
                "not_specified": MetadataValue.int(gender_stats.get(0, 0)),
                "female": MetadataValue.int(gender_stats.get(1, 0)),
                "male": MetadataValue.int(gender_stats.get(2, 0)),
                "non_binary": MetadataValue.int(gender_stats.get(3, 0))
            },
        }
    )    

import dagster as dg
from dagster import AssetKey, Output, MetadataValue
import time

@dg.asset(
    name="deduplicate_popular_people_by_name",
    deps=[AssetKey("load_popular_people_to_postgres")],
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "bronze"},
    tags={'layer':'staging'},
)
def deduplicate_popular_people_by_name(context):
    """Asset to deduplicate people in the staging.popular_people table based on name"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Count records before deduplication
            cursor.execute("SELECT COUNT(*) FROM staging.popular_people")
            count_before = cursor.fetchone()["count"]
            
            # Delete duplicate entries keeping only the one with highest popularity for each name
            cursor.execute("""
                DELETE FROM staging.popular_people
                WHERE id IN (
                    SELECT id
                    FROM (
                        SELECT 
                            id,
                            ROW_NUMBER() OVER (PARTITION BY name ORDER BY popularity DESC) as row_num
                        FROM staging.popular_people
                    ) ranked
                    WHERE row_num > 1
                )
            """)
            
            # Count records after deduplication
            cursor.execute("SELECT COUNT(*) FROM staging.popular_people")
            count_after = cursor.fetchone()["count"]
            
            # Calculate duplicates removed
            duplicates_removed = count_before - count_after
                        
            # Commit the transaction
            conn.commit()
    
    duration = time.time() - start_time
    
    return Output(
        value={
            "records_before": count_before,
            "records_after": count_after,
            "duplicates_removed": duplicates_removed,
            "table": "staging.popular_people"
        },
        metadata={
            "execution_time": MetadataValue.float(duration),
            "records_before": MetadataValue.int(count_before),
            "records_after": MetadataValue.int(count_after),
            "duplicates_removed": MetadataValue.int(duplicates_removed),
            "duplicate_percentage": MetadataValue.float(
                round(duplicates_removed / count_before * 100, 2) if count_before > 0 else 0
            ),
        }
    )