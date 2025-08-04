import dagster as dg
import time


@dg.asset(
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    tags={'layer': 'staging'}
)
def create_movie_aug_table(context: dg.AssetExecutionContext):
    """
    Create a table for movie_dim augmentation.
    
    Args:
        context: AssetExecutionContext
    """
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create tmdb_data schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.movie_aug (
                movie_id INTEGER PRIMARY KEY,
                release_date DATE,
                partition_date DATE NOT NULL,
                load_date DATE NOT NULL
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            context.log.info("movie_aug table created successfully.")
    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration} seconds")
    context.log.info(f"Table created: tmdb_data.movie_aug")
    context.log.info(f"Operation: create")
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.movie_aug",
            "operation": "create"
        }
    )
    
@dg.asset(
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    tags={'type': 'create'},
    deps=[]
)
def create_movie_dim(context: dg.AssetExecutionContext):
    """
    Create a table for movie_dim augmentation.
    
    Args:
        context: AssetExecutionContext
    """
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create tmdb_data schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.movie_dim (
                movie_key SERIAL PRIMARY KEY,
                movie_id INTEGER NOT NULL,  -- TMDB movie_id
                title VARCHAR(255) NOT NULL,
                original_title VARCHAR(255),
                original_language VARCHAR(10),
                overview TEXT,
                tagline TEXT,
                status VARCHAR(50),
                runtime INTEGER,
                budget DECIMAL(15, 2),
                is_adult BOOLEAN,
                is_video BOOLEAN,
                poster_path VARCHAR(255),
                backdrop_path VARCHAR(255),
                homepage VARCHAR(255),
                imdb_id VARCHAR(20),
                tmdb_popularity DECIMAL(10, 3),
                vote_count INTEGER,
                vote_average DECIMAL(3, 1),
                effective_date DATE NOT NULL,        -- When this version became effective
                expiration_date DATE,                -- When this version expired (NULL = current)
                is_current BOOLEAN NOT NULL,         -- Flag for current version
                update_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- When this record was created
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            context.log.info("movie_aug table created successfully.")
    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration} seconds")
    context.log.info(f"Table created: tmdb_data.movie_dim")
    context.log.info(f"Operation: create")
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.movie_dim",
            "operation": "create"
        }
    )
    
@dg.asset(
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    tags={'type': 'create'},
    deps=[],
)
def create_genres_dim(context: dg.AssetExecutionContext):
    """
    Create the genres_dim table to store unique genre combinations.
    
    Args:
        context: AssetExecutionContext
    """
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.genres_dim (
                genre_composite_id TEXT PRIMARY KEY,
                genre_composite_name TEXT UNIQUE NOT NULL, 
                genre_ids INTEGER[],          
                genre_names TEXT[],           
                load_date DATE NOT NULL DEFAULT CURRENT_DATE,
                update_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP 
                );
            """
            cursor.execute(create_table_query)
            conn.commit()
            context.log.info("genres_dim table created or already exists.")
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.genres_dim",
            "operation": "create"
        }
    )


@dg.asset(
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    deps=[dg.AssetKey("create_genres_dim")],
    tags={'type': 'create'}
)
def create_movie_performance_fact(context: dg.AssetExecutionContext):
    """
    Create a table for movie_dim augmentation.
    
    Args:
        context: AssetExecutionContext
    """
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create tmdb_data schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.movie_performance_fact (
                performance_key SERIAL PRIMARY KEY,
                movie_id INTEGER,
                release_date DATE,
                genre_composite_id TEXT REFERENCES tmdb_data.genres_dim(genre_composite_id),
                revenue DECIMAL(15, 2),
                budget DECIMAL(15, 2),
                profit DECIMAL(15, 2),
                roi DECIMAL(10, 4),  -- Return on Investment
                domestic_revenue DECIMAL(15, 2),
                international_revenue DECIMAL(15, 2),
                opening_weekend_revenue DECIMAL(15, 2),
                box_office_rank INTEGER,
                is_profitable BOOLEAN,
                partition_date DATE NOT NULL,
                load_date DATE NOT NULL DEFAULT CURRENT_DATE,
                update_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_movies UNIQUE (movie_id) 
            );
            """
            cursor.execute(create_table_query)
            conn.commit()
            context.log.info("movie_aug table created successfully.")
    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration} seconds")
    context.log.info(f"Table created: tmdb_data.movie_performance_fact")
    context.log.info(f"Operation: create")
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.movie_performance_fact",
            "operation": "create"
        }
    )
    
@dg.asset(name="create_movie_genre_bridge_table",
        kinds={"sql", "postgres", "bronze"},
        required_resource_keys={"postgres"}
        )
def create_movie_genre_bridge_table(context):
    """Asset to create the movie-genre bridge table structure if it doesn't exist"""
    context.log.info("Creating movie-genre bridge table")
    postgres = context.resources.postgres
    start_time = time.time()
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if needed
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create bridge table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.bridge_movie_genre (
                movie_id INTEGER NOT NULL,
                genre_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (movie_id, genre_id)
            )
            """
            cursor.execute(create_table_query)
    
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.bridge_movie_genre",
            "operation": "create_structure"
        })
    
@dg.asset(name="create_person_project_bridge_table",
        kinds={"sql", "postgres", "bronze"},
        required_resource_keys={"postgres"})
def create_person_project_bridge_table(context):
    """Asset to create the bridge table structure if it doesn't exist"""
    context.log.info("Creating person-project bridge table")
    postgres = context.resources.postgres
    start_time = time.time()
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if needed
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create bridge table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.bridge_person_project (
                person_id INTEGER NOT NULL,
                project_id INTEGER NOT NULL,
                media_type TEXT,
                project_title TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (person_id, project_id)
            )
            """
            cursor.execute(create_table_query)
    
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "table": "tmdb_data.bridge_person_project",
            "operation": "create_structure"
        })
    
    
@dg.asset(name="create_show_genre_bridge_table",
        kinds={"sql", "postgres", "bronze"},
        required_resource_keys={"postgres"},)
def create_show_genre_bridge_table(context):
    """Asset to create the show-genre bridge table structure if it doesn't exist"""
    context.log.info("Creating show-genre bridge table")
    postgres = context.resources.postgres
    start_time = time.time()
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if needed
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create bridge table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.bridge_show_genre (
                show_id INTEGER NOT NULL,
                genre_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (show_id, genre_id)
            )
            """
            cursor.execute(create_table_query)
    
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.bridge_show_genre",
            "operation": "create_structure"
        })

@dg.asset(
    name="create_dim_movie_genres_table",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "bronze"},
    tags={'layer': 'dimensional'}
)
def create_dim_movie_genres_table(context):
    """Asset to create the movie genres dimension table if it doesn't exist"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create dimension table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.dim_movie_genres (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
    
    duration = time.time() - start_time
    
    return dg.Output(
        value={"table": "tmdb_data.dim_movie_genres"},
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "operation": "create_table"
        }
    )

@dg.asset(name="create_popular_dim_popular_people", 
        tags={'layer':'bronze'},
        kinds={"sql", "postgres", "bronze"},
        required_resource_keys={"postgres"}
        )
def create_popular_dim_popular_people(context):
        """Asset to create the dim_popular_people table"""
        start_time = time.time()
        postgres = context.resources.postgres
        
        with postgres.get_connection() as conn:
                with conn.cursor() as cursor:
                        create_table_query = """
                                CREATE TABLE IF NOT EXISTS tmdb_data.dim_popular_people (
                                id INT PRIMARY KEY,
                                gender INT,
                                known_for_department TEXT,
                                name TEXT,
                                popularity FLOAT,
                                profile_path TEXT,
                                insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                )
                        """
                        cursor.execute(create_table_query)
        end_time = time.time()
        duration = end_time - start_time
        return dg.Output(
                value=True,
                metadata={
                        "execution_time": dg.MetadataValue.float(duration),
                        "table": dg.MetadataValue.text("tmdb_data.dim_popular_people"),
                        "operation": dg.MetadataValue.text("create")
                }
        )

@dg.asset(name="create_people_history_table" ,
        kinds={"sql", "postgres", "bronze"},
        required_resource_keys={"postgres"})
def create_people_history_table(context):
    staging_table = "staging.popular_people"
    context.log.info(f"Creating shows_history table based on {staging_table}")
    postgres = context.resources.postgres
    start_time = time.time()
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create tmdb_data schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create shows table in tmdb_data schema if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.dim_people_history (
                id_sk INTEGER PRIMARY KEY,
                id INTEGER NOT NULL,
                gender INT,
                known_for_department TEXT,
                name TEXT,
                popularity FLOAT,
                profile_path TEXT,
                effective_date DATE NOT NULL,        -- When this version became effective
                expiration_date DATE,                -- When this version expired (NULL = current)
                is_current BOOLEAN NOT NULL,         -- Flag for current version
                update_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- When this record was created
            )
            """
            cursor.execute(create_table_query)
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=True,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.dim_people_history",
            "operation": "create"
        })


@dg.asset(
    name="create_dim_tv_genres_table",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "bronze"},
    tags={'layer': 'dimensional'}
)
def create_dim_tv_genres_table(context):
    """Asset to create the TV genres dimension table if it doesn't exist"""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")
            
            # Create dimension table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.dim_tv_genres (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_query)
    
    duration = time.time() - start_time
    
    return dg.Output(
        value={"table": "tmdb_data.dim_tv_genres"},
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "operation": "create_table"
        }
    )

@dg.asset(
    name="create_fact_movie_reviews",
    tags={"layer": "bronze"},
    kinds={"sql", "postgres", "bronze"},
    required_resource_keys={"postgres"}
)
def create_fact_movie_reviews(context: dg.AssetExecutionContext):
    """Asset to create the fact_movie_reviews table. This is unpartitioned."""
    start_time = time.time()
    postgres = context.resources.postgres

    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Add loaded_partition_date column
            create_table_query = """
                CREATE TABLE IF NOT EXISTS tmdb_data.fact_movie_reviews (
                    review_id SERIAL PRIMARY KEY,
                    tmdb_review_id VARCHAR(255) UNIQUE,
                    movie_id INTEGER NOT NULL,
                    author_name VARCHAR(255),
                    author_username VARCHAR(255),
                    date_id INTEGER, -- Allow NULL if review date is unknown
                    rating DECIMAL(3,1),
                    content_length INTEGER,
                    review_created_at TIMESTAMP,
                    review_updated_at TIMESTAMP,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When the record was inserted/updated in DB
                    loaded_partition_date DATE, -- Add column to track partition date
                    FOREIGN KEY (movie_id) REFERENCES tmdb_data.dim_popular_movies(id) -- Ensure dim_popular_movies exists
                );
                -- Optional: Add indexes
                CREATE INDEX IF NOT EXISTS idx_fact_movie_reviews_created_date ON tmdb_data.fact_movie_reviews (created_date);
                CREATE INDEX IF NOT EXISTS idx_fact_movie_reviews_movie_id ON tmdb_data.fact_movie_reviews (movie_id);
            """
            cursor.execute(create_table_query)

            # Add the new column idempotently
            add_column_query = """
                ALTER TABLE tmdb_data.fact_movie_reviews
                ADD COLUMN IF NOT EXISTS loaded_partition_date DATE;
            """
            cursor.execute(add_column_query)

            # Ensure date_id allows NULLs as planned in fetch/load
            alter_date_id_query = """
                ALTER TABLE tmdb_data.fact_movie_reviews ALTER COLUMN date_id DROP NOT NULL;
            """
            try:
                cursor.execute(alter_date_id_query)
            except Exception as alter_err:
                # Handle cases where it might already allow NULLs or other constraints prevent dropping NOT NULL easily
                context.log.warning(f"Could not ensure date_id allows NULLs (may already be set): {alter_err}")
                conn.rollback() # Rollback the failed ALTER attempt
            else:
                conn.commit() # Commit successful ALTER or schema creation

    end_time = time.time()
    duration = end_time - start_time
    context.log.info("Ensured fact_movie_reviews table exists and has loaded_partition_date column.")
    return dg.Output(
        value=True,
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "table": dg.MetadataValue.text("tmdb_data.fact_movie_reviews"),
            "operation": dg.MetadataValue.text("ensure_schema")
        }
    )

@dg.asset(
    name="create_fact_show_reviews",
    tags={"layer": "bronze"},
    kinds={"sql", "postgres", "bronze"},
    required_resource_keys={"postgres"}
)
def create_fact_show_reviews(context: dg.AssetExecutionContext):
    """Asset to create the fact_show_reviews table. This is unpartitioned."""
    start_time = time.time()
    postgres = context.resources.postgres
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # This schema definition is idempotent, safe to run multiple times
            create_table_query = """
                CREATE TABLE IF NOT EXISTS tmdb_data.fact_show_reviews (
                    review_id SERIAL PRIMARY KEY,
                    tmdb_review_id VARCHAR(255) UNIQUE,
                    series_id INTEGER NOT NULL,
                    author_name VARCHAR(255),
                    author_username VARCHAR(255),
                    date_id INTEGER, -- Allow NULL if review date is unknown
                    rating DECIMAL(3,1),
                    content_length INTEGER,
                    review_created_at TIMESTAMP,
                    review_updated_at TIMESTAMP,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    loaded_partition_date DATE,
                    FOREIGN KEY (series_id) REFERENCES tmdb_data.dim_popular_shows(id)
                );
                -- Add indexes
                CREATE INDEX IF NOT EXISTS idx_fact_show_reviews_created_date ON tmdb_data.fact_show_reviews (created_date);
                CREATE INDEX IF NOT EXISTS idx_fact_show_reviews_series_id ON tmdb_data.fact_show_reviews (series_id);
            """
            cursor.execute(create_table_query)
            
            # Add the new column if it doesn't exist (idempotent addition)
            add_column_query = """
                ALTER TABLE tmdb_data.fact_show_reviews
                ADD COLUMN IF NOT EXISTS loaded_partition_date DATE;
            """
            cursor.execute(add_column_query)
            
            # Ensure date_id allows NULLs
            try:
                alter_date_id_query = """
                    ALTER TABLE tmdb_data.fact_show_reviews ALTER COLUMN date_id DROP NOT NULL;
                """
                cursor.execute(alter_date_id_query)
            except Exception as alter_err:
                context.log.warning(f"Could not ensure date_id allows NULLs (may already be set): {alter_err}")
                conn.rollback()
            else:
                conn.commit()

    end_time = time.time()
    duration = end_time - start_time
    context.log.info("Ensured fact_show_reviews table exists and has loaded_partition_date column.")
    return dg.Output(
        value=True, # Output doesn't change much for schema creation
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "table": dg.MetadataValue.text("tmdb_data.fact_show_reviews"),
            "operation": dg.MetadataValue.text("ensure_schema")
        }
    )


# --- New Asset: create_cast_dim ---
@dg.asset(
    name="create_cast_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    tags={'type': 'create', 'layer': 'silver'}, # Added layer tag for consistency
    deps=[], # Typically create assets don't have upstream deps within the group
)
def create_cast_dim(context: dg.AssetExecutionContext):
    """
    Create the cast_dim table to store cast members for movies.

    Args:
        context: AssetExecutionContext
    """
    start_time = time.time()
    postgres = context.resources.postgres
    table_name = "tmdb_data.cast_dim"
    context.log.info(f"Attempting to create table: {table_name}")

    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Ensure tmdb_data schema exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")

            # Define the table structure based on load_cast_dim
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.cast_dim (
                movie_id INTEGER NOT NULL,
                adult BOOLEAN,
                gender INTEGER,
                person_id INTEGER NOT NULL, -- TMDB person ID
                known_for_department VARCHAR(255),
                name VARCHAR(255),
                original_name VARCHAR(255),
                profile_path VARCHAR(255),
                cast_id INTEGER, -- Specific ID for this casting role
                character VARCHAR(512), -- Increased size for potentially long character names
                credit_id VARCHAR(255) NOT NULL, -- Unique credit identifier from TMDB
                cast_order INTEGER,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- Define composite primary key
                PRIMARY KEY (movie_id, credit_id)
            );
            """
            cursor.execute(create_table_query)
            conn.commit()
            context.log.info(f"{table_name} table created or already exists.")

    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration:.2f} seconds")
    context.log.info(f"Table created: {table_name}")
    context.log.info(f"Operation: create")

    return dg.Output(
        value=True,
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "table": dg.MetadataValue.text(table_name),
            "operation": dg.MetadataValue.text("create")
        }
    )

# --- New Asset: create_crew_dim ---
@dg.asset(
    name="create_crew_dim",
    required_resource_keys={"postgres"},
    kinds={"python", "postgres", "table"},
    tags={'type': 'create', 'layer': 'silver'}, # Added layer tag for consistency
    deps=[], # Typically create assets don't have upstream deps within the group
)
def create_crew_dim(context: dg.AssetExecutionContext):
    """
    Create the crew_dim table to store important crew members for movies.

    Args:
        context: AssetExecutionContext
    """
    start_time = time.time()
    postgres = context.resources.postgres
    table_name = "tmdb_data.crew_dim"
    context.log.info(f"Attempting to create table: {table_name}")

    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Ensure tmdb_data schema exists
            cursor.execute("CREATE SCHEMA IF NOT EXISTS tmdb_data")

            # Define the table structure based on load_crew_dim
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tmdb_data.crew_dim (
                movie_id INTEGER NOT NULL,
                adult BOOLEAN,
                gender INTEGER,
                person_id INTEGER NOT NULL, -- TMDB person ID
                known_for_department VARCHAR(255),
                name VARCHAR(255),
                original_name VARCHAR(255),
                profile_path VARCHAR(255),
                credit_id VARCHAR(255) NOT NULL, -- Unique credit identifier from TMDB
                department VARCHAR(255),
                job VARCHAR(255),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- Define composite primary key
                PRIMARY KEY (movie_id, credit_id)
            );
            """
            cursor.execute(create_table_query)
            conn.commit()
            context.log.info(f"{table_name} table created or already exists.")

    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration:.2f} seconds")
    context.log.info(f"Table created: {table_name}")
    context.log.info(f"Operation: create")

    return dg.Output(
        value=True,
        metadata={
            "execution_time": dg.MetadataValue.float(duration),
            "table": dg.MetadataValue.text(table_name),
            "operation": dg.MetadataValue.text("create")
        }
    )
