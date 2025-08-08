import dagster as dg
import pandas as pd
from scipy.stats import hmean
from datetime import datetime
import uuid
import time



@dg.asset(name="create_sf_ranks_history",
        description="Creates the sf_ranks_history table if it doesn't exist.",
        required_resource_keys={"postgres"}
        )
def create_sf_ranks_history(context):
    postgres = context.resources.postgres
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
        
            create_table_query = """
            CREATE TABLE IF NOT EXISTS dynastr.sf_player_ranks_hist (
            hist_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            player_full_name               TEXT,
            ktc_player_id                  VARCHAR,
            team                           TEXT,
            _position                      TEXT,
            ktc_sf_value                   INTEGER,
            ktc_sf_rank                    INTEGER,
            ktc_one_qb_value               INTEGER,
            ktc_one_qb_rank                INTEGER,
            fc_sf_value                    INTEGER,
            fc_sf_rank                     INTEGER,
            fc_one_qb_value                INTEGER,
            fc_one_qb_rank                 INTEGER,
            dp_sf_value                    INTEGER,
            dp_sf_rank                     INTEGER,
            dp_one_qb_value                INTEGER,
            dp_one_qb_rank                 INTEGER,
            ktc_sf_normalized_value        REAL,
            fc_sf_normalized_value         REAL,
            dp_sf_normalized_value         REAL,
            ktc_one_qb_normalized_value    REAL,
            fc_one_qb_normalized_value     REAL,
            dp_one_qb_normalized_value     REAL,
            average_normalized_sf_value    REAL,
            average_normalized_one_qb_value REAL,
            superflex_sf_value             INTEGER,
            superflex_one_qb_value         INTEGER,
            superflex_sf_rank              INTEGER,
            superflex_one_qb_rank          INTEGER,
            superflex_sf_pos_rank          INTEGER,
            superflex_one_qb_pos_rank      INTEGER,
            insert_date                    TIMESTAMP WITH TIME ZONE,
            rank_type                      VARCHAR(50),
            display_player_full_name       TEXT,
            dd_sf_value                    INTEGER,
            dd_sf_rank                     INTEGER,
            dd_one_qb_value                INTEGER,
            dd_one_qb_rank                 INTEGER,
            dd_sf_normalized_value         REAL,
            dd_one_qb_normalized_value     REAL,
            player_rank_sk                 INTEGER,
            valid_from                     TIMESTAMP WITH TIME ZONE,
            valid_to                       TIMESTAMP WITH TIME ZONE,
            is_current                     BOOLEAN DEFAULT TRUE
            );
            """ 
        
            cursor.execute(create_table_query)
            context.log.info("sf_ranks_history table created.")
    
    return dg.Output(value=True,metadata={
        "table": dg.MetadataValue.text("sf_ranks_history"),}
    )

# Truncate fantasy navigator rankings staging table

@dg.asset(name="truncate_sf_player_ranks_staging",
        required_resource_keys={"postgres"},
        kinds={"sql", "postgres", "bronze"},
        tags={"layer": "bronze"},)
def truncate_sf_player_ranks_staging(context):
    """Asset to truncate the sf_player_ranks table"""
    start_time = time.time()
    postgres = context.resources.postgres
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create schema if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
        
            create_table_query = """CREATE TABLE IF NOT EXISTS staging.sf_player_ranks (
                player_full_name VARCHAR,
                ktc_player_id VARCHAR,
                team VARCHAR,
                _position VARCHAR,
                ktc_sf_value INTEGER,
                ktc_sf_rank INTEGER,
                ktc_one_qb_value INTEGER,
                ktc_one_qb_rank INTEGER,
                fc_sf_value INTEGER,
                fc_sf_rank INTEGER,
                fc_one_qb_value INTEGER,
                fc_one_qb_rank INTEGER,
                dp_sf_value INTEGER,
                dp_sf_rank INTEGER,
                dp_one_qb_value INTEGER,
                dp_one_qb_rank INTEGER,
                ktc_sf_normalized_value FLOAT,
                fc_sf_normalized_value FLOAT,
                dp_sf_normalized_value FLOAT,
                ktc_one_qb_normalized_value FLOAT,
                fc_one_qb_normalized_value FLOAT,
                dp_one_qb_normalized_value FLOAT,
                average_normalized_sf_value FLOAT,
                average_normalized_one_qb_value FLOAT,
                superflex_sf_value INTEGER,
                superflex_one_qb_value INTEGER,
                superflex_sf_rank INTEGER,
                superflex_one_qb_rank INTEGER,
                superflex_sf_pos_rank INTEGER,
                superflex_one_qb_pos_rank INTEGER,
                insert_date TIMESTAMP,
                rank_type VARCHAR,
                display_player_full_name VARCHAR,
                dd_sf_value INTEGER,
                dd_sf_rank INTEGER,
                dd_one_qb_value INTEGER,
                dd_one_qb_rank INTEGER,
                dd_sf_normalized_value FLOAT,
                dd_one_qb_normalized_value FLOAT,
                PRIMARY KEY (ktc_player_id, rank_type)
            );
            """ 
        
            cursor.execute(create_table_query)
            context.log.info("sf_player_ranks staging table created.")
            
            context.log.info("Truncating sf_player_ranks table...")
            
            cursor.execute("TRUNCATE TABLE staging.sf_player_ranks")
            
            conn.commit()
            context.log.info("sf_player_ranks table truncated.")
    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(value=True,
            metadata={
                "execution_time": dg.MetadataValue.float(duration),
                "table": dg.MetadataValue.text("staging.sf_player_ranks"),
                "operation": dg.MetadataValue.text("truncate")}
            )

@dg.asset(
    name="raw_player_asset_values_hist",
    description="Fetches combined player and draft pick data from various source tables.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    tags={"layer": "staging", "source": "postgres"},
    deps=[
        dg.AssetKey("load_sf_ktc_rookie_picks"), 
        dg.AssetKey("load_one_qb_ktc_rookie_picks"),
        dg.AssetKey("ktc_player_ranks_formatted"),  
        dg.AssetKey("fc_player_ranks_formatted"),
        dg.AssetKey("dp_player_ranks_formatted"),
        dg.AssetKey("dd_player_ranks_formatted"),
        ],
)
def raw_player_asset_values_hist(context: dg.OpExecutionContext) -> dg.Output[pd.DataFrame]:
    """
    Executes a SQL query to join and fetch player and draft pick data
    from ktc, fc, dp, and dd rank tables.
    """
    postgres = context.resources.postgres
    
    query = """
    with ktc_picks as (
    select *
    from dynastr.ktc_player_ranks ktc 
    where 1=1
    and (ktc.player_full_name like '%2025 Round%' or ktc.position = 'RDP') 
    ),
    dp_picks as (
        select 
            CASE 
                WHEN (player_full_name like '%Mid%' or player_first_name = '2026') 
                THEN (CASE WHEN player_first_name = '2026' 
                        THEN CONCAT(player_first_name, ' Mid ', player_last_name) else player_full_name end)
            ELSE player_full_name END as player_full_name
        ,sf_value
        ,one_qb_value
    FROM dynastr.dp_player_ranks 
    WHERE 1=1
    and player_position = 'PICK'
    ),

    fc_picks as (
        SELECT 
            CASE 
                WHEN lower(player_full_name) NOT LIKE '%pick%' THEN 
                    player_first_name || ' Mid ' || player_last_name || 
                    CASE
                        WHEN RIGHT(regexp_replace(player_last_name, '\D', '', 'g'), 1) = '1' THEN 'st'
                        WHEN RIGHT(regexp_replace(player_last_name, '\D', '', 'g'), 1) = '2' THEN 'nd'
                        WHEN RIGHT(regexp_replace(player_last_name, '\D', '', 'g'), 1) = '3' THEN 'rd'
                        ELSE 'th'
                    END
                ELSE player_full_name 
            END as player_full_name,
            sf_value,
            one_qb_value		  
        FROM dynastr.fc_player_ranks
        WHERE 1 = 1
        AND player_position = 'PICK'
        AND rank_type = 'dynasty'
    ),
    dd_picks as ( 
        SELECT 
SUBSTRING(name_id FROM 1 FOR 4) || ' ' || INITCAP(SUBSTRING(name_id FROM 5 FOR LENGTH(name_id) - 9)) || ' ' || SUBSTRING(name_id FROM LENGTH(name_id) - 4 FOR 3) AS player_full_name
,sf_trade_value as sf_value
,trade_value as one_qb_value
FROM dynastr.dd_player_ranks 
WHERE 1=1
AND (name_id like '%202%')
AND rank_type = 'dynasty'
    ),

    asset_values as (
    select p.full_name as player_full_name
    ,ktc.ktc_player_id as player_id
    , case when ktc.team = 'KCC' then 'KC' else ktc.team end as team
    ,ktc.sf_value as ktc_sf_value
    ,ktc.one_qb_value as ktc_one_qb_value
    ,fc.sf_value as fc_sf_value
    ,fc.one_qb_value as fc_one_qb_value
    ,dp.sf_value as dp_sf_value
    ,dp.one_qb_value as dp_one_qb_value
    ,dd.sf_trade_value as dd_sf_value
    ,dd.trade_value as dd_one_qb_value
    , p.player_position as _position

    from dynastr.players p
    inner join (select player_first_name, player_last_name, team, ktc_player_id, sf_value, one_qb_value from dynastr.ktc_player_ranks where rank_type = 'dynasty') ktc on lower(concat(p.first_name, p.last_name)) = lower(concat(ktc.player_first_name, ktc.player_last_name))
    inner join (select sleeper_player_id, sf_value, one_qb_value from dynastr.fc_player_ranks where rank_type = 'dynasty') fc on fc.sleeper_player_id = p.player_id 
    inner join dynastr.dp_player_ranks dp on lower(concat(p.first_name, p.last_name)) = lower(concat(dp.player_first_name, dp.player_last_name))
    inner join (select name_id, sf_trade_value, trade_value from dynastr.dd_player_ranks where rank_type = 'dynasty') dd on lower(concat(p.first_name,p.last_name, p.player_position)) = dd.name_id

    UNION ALL 

    select ktc.player_full_name as player_full_name
    ,ktc.ktc_player_id as player_id
    , null as team
    ,ktc.sf_value as ktc_sf_value
    ,ktc.one_qb_value as ktc_one_qb_value
    ,coalesce(fc.sf_value, ktc.sf_value) as fc_sf_value
    ,coalesce(fc.one_qb_value, ktc.one_qb_value) as fc_one_qb_value
    ,coalesce(dp.sf_value, ktc.sf_value) as dp_sf_value
    ,coalesce(dp.one_qb_value, ktc.one_qb_value) as dp_one_qb_value
    ,coalesce(dd.sf_value, ktc.sf_value) as dd_sf_value -- Use value from dd_picks
    ,coalesce(dd.one_qb_value, ktc.one_qb_value) as dd_one_qb_value -- Use value from dd_picks
    , CASE WHEN substring(lower(ktc.player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
        WHEN position = 'RDP' THEN 'Pick'
        ELSE position END as _position
    from ktc_picks ktc
    left join fc_picks fc on lower(ktc.player_full_name) = lower(fc.player_full_name)
    left join dp_picks dp on lower(ktc.player_full_name) = lower(dp.player_full_name)
    left join dd_picks dd on lower(ktc.player_full_name) = lower(dd.player_full_name)
    where 1=1
    and (ktc.player_full_name like '%2025 Round%' or ktc.position = 'RDP')
        )

    select 
    player_full_name
    , player_full_name as display_player_full_name
    , player_id
    , team
    , _position
    , ktc_sf_value
    , ROW_NUMBER() over (order by ktc_sf_value desc) as ktc_sf_rank
    , ktc_one_qb_value
    , ROW_NUMBER() over (order by ktc_one_qb_value desc) as ktc_one_qb_rank
    , fc_sf_value
    , ROW_NUMBER() over (order by fc_sf_value desc) as fc_sf_rank
    , fc_one_qb_value
    , ROW_NUMBER() over (order by fc_one_qb_value desc) as fc_one_qb_rank
    , dp_sf_value
    , ROW_NUMBER() over (order by dp_sf_value desc) as dp_sf_rank
    , dp_one_qb_value
    , ROW_NUMBER() over (order by dp_one_qb_value desc) as dp_one_qb_rank
    , dd_sf_value
    , ROW_NUMBER() over (order by dd_sf_value desc) as dd_sf_rank
    , dd_one_qb_value
    , ROW_NUMBER() over (order by dd_one_qb_value desc) as dd_one_qb_rank

    from asset_values
    """
    conn = None # Ensure conn is defined for finally block
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info("Fetching raw player and pick data...")
                cursor.execute(query)
                data = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                context.log.info(f"Fetched {len(data)} rows.")

        values_df = pd.DataFrame(data, columns=column_names)
        # Convert relevant columns to numeric, coercing errors
        for col in ['ktc_sf_value', 'ktc_one_qb_value', 'fc_sf_value', 'fc_one_qb_value', 'dp_sf_value', 'dp_one_qb_value', 'dd_sf_value', 'dd_one_qb_value']:
            values_df[col] = pd.to_numeric(values_df[col], errors='coerce')


        return dg.Output(
            value=values_df,
            metadata={
                "num_rows": len(values_df),
                "columns": list(values_df.columns),
                "preview": dg.MetadataValue.md(values_df.head().to_markdown()),
            }
        )
    except Exception as e:
        context.log.error(f"Error fetching raw player data: {e}")
        if conn:
            conn.rollback() # Rollback if error occurs during fetch (less likely but good practice)
        raise
    finally:
        # Connection is managed by the context manager, no manual close needed here
        pass

# --- Asset 2: Process and Normalize Data ---

@dg.asset(
    name="processed_player_ranks_hist",
    description="Normalizes player values, calculates harmonic means, and ranks players.",
    compute_kind="pandas",
    tags={"layer": "processing"},
)
def processed_player_ranks_hist(context: dg.OpExecutionContext, raw_player_asset_values_hist: pd.DataFrame) -> dg.Output[pd.DataFrame]:
    """
    Takes the raw player/pick data, normalizes values, calculates harmonic mean based
    normalized values, and generates overall/positional ranks. Also includes average normalized values.
    """
    values_df = raw_player_asset_values_hist.copy()
    context.log.info(f"Processing {len(values_df)} rows.")

    # --- Step 1: Rename player_id ---
    if 'player_id' in values_df.columns:
        context.log.info("Renaming 'player_id' column to 'ktc_player_id'.")
        values_df.rename(columns={'player_id': 'ktc_player_id'}, inplace=True)
    else:
        context.log.warning("'player_id' column not found in input DataFrame. Skipping rename.")
        # If ktc_player_id might already exist from the source query (unlikely based on your query), handle that case if necessary.


    # Define value columns and normalization targets
    sf_source_cols = ['ktc_sf_value', 'fc_sf_value', 'dp_sf_value', 'dd_sf_value']
    sf_norm_cols = ['ktc_sf_normalized_value', 'fc_sf_normalized_value', 'dp_sf_normalized_value', 'dd_sf_normalized_value']
    one_qb_source_cols = ['ktc_one_qb_value', 'fc_one_qb_value', 'dp_one_qb_value', 'dd_one_qb_value']
    one_qb_norm_cols = ['ktc_one_qb_normalized_value', 'fc_one_qb_normalized_value', 'dp_one_qb_normalized_value', 'dd_one_qb_normalized_value']

    # Normalize Superflex values
    for source, norm_col in zip(sf_source_cols, sf_norm_cols):
        min_val = values_df[source].min()
        max_val = values_df[source].max()
        if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
            context.log.warning(f"Cannot normalize {source}, min={min_val}, max={max_val}. Setting normalized to 0.")
            values_df[norm_col] = 0.0
        else:
            # Fill NA with 0 *before* normalization as per original logic
            values_df[norm_col] = ((values_df[source].fillna(0) - min_val) / (max_val - min_val)) * 9999

    # Normalize 1-QB values
    for source, norm_col in zip(one_qb_source_cols, one_qb_norm_cols):
        min_val = values_df[source].min()
        max_val = values_df[source].max()
        if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
            context.log.warning(f"Cannot normalize {source}, min={min_val}, max={max_val}. Setting normalized to 0.")
            values_df[norm_col] = 0.0
        else:
             # Fill NA with 0 *before* normalization
            values_df[norm_col] = ((values_df[source].fillna(0) - min_val) / (max_val - min_val)) * 9999

    # --- Step 2: Add Average Calculation ---
    context.log.info("Calculating average normalized values.")
    values_df['average_normalized_sf_value'] = values_df[sf_norm_cols].mean(axis=1)
    values_df['average_normalized_one_qb_value'] = values_df[one_qb_norm_cols].mean(axis=1)
    # Optional: Fill NaN in average columns if any rows had all NaN inputs to mean()
    values_df['average_normalized_sf_value'] = values_df['average_normalized_sf_value'].fillna(0)
    values_df['average_normalized_one_qb_value'] = values_df['average_normalized_one_qb_value'].fillna(0)


    # Calculate harmonic mean of *normalized* values
    values_df['superflex_sf_value'] = values_df[sf_norm_cols].apply(
        lambda row: hmean(row[row > 0]) if any(row > 0) else 0.0, axis=1
    )
    values_df['superflex_one_qb_value'] = values_df[one_qb_norm_cols].apply(
        lambda row: hmean(row[row > 0]) if any(row > 0) else 0.0, axis=1
    )

    # Rank based on harmonic mean values
    values_df['superflex_sf_rank'] = values_df['superflex_sf_value'].rank(ascending=False, method='min').fillna(0).astype(int)
    values_df['superflex_one_qb_rank'] = values_df['superflex_one_qb_value'].rank(ascending=False, method='min').fillna(0).astype(int)

    # Calculate positional ranks
    values_df['superflex_sf_pos_rank'] = values_df.groupby('_position')['superflex_sf_value'].rank(method='min', ascending=False).fillna(0).astype(int)
    values_df['superflex_one_qb_pos_rank'] = values_df.groupby('_position')['superflex_one_qb_value'].rank(method='min', ascending=False).fillna(0).astype(int)

    # Add insert date and type
    enrty_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z") # Use UTC
    values_df['insert_date'] = enrty_time
    values_df['rank_type'] = 'dynasty' # Hardcoded as per original logic

    # Ensure final value/rank columns are integer type (includes harmonic mean based ones)
    final_value_rank_cols = [
        'superflex_sf_value', 'superflex_sf_rank',
        'superflex_one_qb_value', 'superflex_one_qb_rank',
        'superflex_sf_pos_rank', 'superflex_one_qb_pos_rank'
        # Note: average_normalized values are typically floats, keep them as such unless integer conversion is desired
    ]
    for col in final_value_rank_cols:
        values_df[col] = values_df[col].fillna(0).astype(int)

    # Define the final list of columns in the desired order for the output DataFrame
    # Now includes the renamed ktc_player_id and the calculated average columns
    final_cols = [
        'player_full_name', 'display_player_full_name', 'ktc_player_id', # Corrected name
        'team', '_position',
        'ktc_sf_value', 'ktc_sf_rank', 'ktc_one_qb_value', 'ktc_one_qb_rank',
        'fc_sf_value', 'fc_sf_rank', 'fc_one_qb_value', 'fc_one_qb_rank',
        'dp_sf_value', 'dp_sf_rank', 'dp_one_qb_value', 'dp_one_qb_rank',
        'dd_sf_value', 'dd_sf_rank', 'dd_one_qb_value', 'dd_one_qb_rank',
        'ktc_sf_normalized_value', 'fc_sf_normalized_value', 'dp_sf_normalized_value', 'dd_sf_normalized_value',
        'ktc_one_qb_normalized_value', 'fc_one_qb_normalized_value', 'dp_one_qb_normalized_value', 'dd_one_qb_normalized_value',
        'average_normalized_sf_value', # Now calculated
        'average_normalized_one_qb_value', # Now calculated
        'superflex_sf_value', 'superflex_one_qb_value', 'superflex_sf_rank',
        'superflex_one_qb_rank', 'superflex_sf_pos_rank', 'superflex_one_qb_pos_rank',
        'insert_date', 'rank_type'
    ]

    # Fill NaNs in source value/rank columns before final selection, if needed for DB constraints
    # These are columns coming from the initial query or normalization steps.
    source_value_rank_cols = [
        'ktc_sf_value', 'ktc_sf_rank', 'ktc_one_qb_value', 'ktc_one_qb_rank',
        'fc_sf_value', 'fc_sf_rank', 'fc_one_qb_value', 'fc_one_qb_rank',
        'dp_sf_value', 'dp_sf_rank', 'dp_one_qb_value', 'dp_one_qb_rank',
        'dd_sf_value', 'dd_sf_rank', 'dd_one_qb_value', 'dd_one_qb_rank',
        'ktc_sf_normalized_value', 'fc_sf_normalized_value', 'dp_sf_normalized_value', 'dd_sf_normalized_value',
        'ktc_one_qb_normalized_value', 'fc_one_qb_normalized_value', 'dp_one_qb_normalized_value', 'dd_one_qb_normalized_value'
    ]
    for col in source_value_rank_cols:
        # Check if column exists before trying to fillna, belt-and-suspenders approach
        if col in values_df.columns:
            # Fill with 0 as per apparent original logic for these source/norm cols
            values_df[col] = values_df[col].fillna(0)

    # Reorder and select final columns using the corrected list
    # This line should now work without a KeyError
    processed_df = values_df[final_cols]


    context.log.info("Finished processing player ranks.")
    return dg.Output(
        value=processed_df,
        metadata={
            "num_rows": len(processed_df),
            "columns": list(processed_df.columns),
            "preview": dg.MetadataValue.md(processed_df.head().to_markdown()),
        }
    )

@dg.asset(name="load_sf_player_ranks_staging",
        description="Loads the processed player/pick data into the staging.sf_player_ranks table.",
        compute_kind="postgres",
        required_resource_keys={"postgres"},
        deps=[dg.AssetKey("truncate_sf_player_ranks_staging"), dg.AssetKey("processed_player_ranks_hist")],
        tags={"layer": "staging", "sink": "postgres"},
)
def load_sf_player_ranks_staging(context: dg.OpExecutionContext, processed_player_ranks_hist: pd.DataFrame) -> dg.Output:
    """
    Inserts or updates player/pick data in the staging.sf_player_ranks table
    using an ON CONFLICT clause.
    """
    start_time = time.time()
    df_to_load = processed_player_ranks_hist
    
    postgres = context.resources.postgres
    insert_count = 0

    # Define the INSERT query with ON CONFLICT UPDATE
    # Column names match the DataFrame columns selected in the previous asset
    insert_query = """
    INSERT INTO staging.sf_player_ranks (
        player_full_name, display_player_full_name, ktc_player_id, team, _position, ktc_sf_value,
        ktc_sf_rank, ktc_one_qb_value, ktc_one_qb_rank, fc_sf_value,
        fc_sf_rank, fc_one_qb_value, fc_one_qb_rank, dp_sf_value,
        dp_sf_rank, dp_one_qb_value, dp_one_qb_rank,
        dd_sf_value, dd_sf_rank, dd_one_qb_value, dd_one_qb_rank,
        ktc_sf_normalized_value, fc_sf_normalized_value,
        dp_sf_normalized_value, dd_sf_normalized_value,
        ktc_one_qb_normalized_value,
        fc_one_qb_normalized_value, dp_one_qb_normalized_value, dd_one_qb_normalized_value,
        average_normalized_sf_value, average_normalized_one_qb_value,
        superflex_sf_value, superflex_one_qb_value, superflex_sf_rank,
        superflex_one_qb_rank, superflex_sf_pos_rank, superflex_one_qb_pos_rank,
        insert_date, rank_type
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    ON CONFLICT (ktc_player_id, rank_type) DO UPDATE SET
        player_full_name = EXCLUDED.player_full_name,
        display_player_full_name = EXCLUDED.display_player_full_name,
        team = EXCLUDED.team,
        _position = EXCLUDED._position,
        ktc_sf_value = EXCLUDED.ktc_sf_value,
        ktc_sf_rank = EXCLUDED.ktc_sf_rank,
        ktc_one_qb_value = EXCLUDED.ktc_one_qb_value,
        ktc_one_qb_rank = EXCLUDED.ktc_one_qb_rank,
        fc_sf_value = EXCLUDED.fc_sf_value,
        fc_sf_rank = EXCLUDED.fc_sf_rank,
        fc_one_qb_value = EXCLUDED.fc_one_qb_value,
        fc_one_qb_rank = EXCLUDED.fc_one_qb_rank,
        dp_sf_value = EXCLUDED.dp_sf_value,
        dp_sf_rank = EXCLUDED.dp_sf_rank,
        dp_one_qb_value = EXCLUDED.dp_one_qb_value,
        dp_one_qb_rank = EXCLUDED.dp_one_qb_rank,
        dd_sf_value = EXCLUDED.dd_sf_value,
        dd_sf_rank = EXCLUDED.dd_sf_rank,
        dd_one_qb_value = EXCLUDED.dd_one_qb_value,
        dd_one_qb_rank = EXCLUDED.dd_one_qb_rank,
        ktc_sf_normalized_value = EXCLUDED.ktc_sf_normalized_value,
        fc_sf_normalized_value = EXCLUDED.fc_sf_normalized_value,
        dp_sf_normalized_value = EXCLUDED.dp_sf_normalized_value,
        dd_sf_normalized_value = EXCLUDED.dd_sf_normalized_value,
        ktc_one_qb_normalized_value = EXCLUDED.ktc_one_qb_normalized_value,
        fc_one_qb_normalized_value = EXCLUDED.fc_one_qb_normalized_value,
        dp_one_qb_normalized_value = EXCLUDED.dp_one_qb_normalized_value,
        dd_one_qb_normalized_value = EXCLUDED.dd_one_qb_normalized_value,
        average_normalized_sf_value = EXCLUDED.average_normalized_sf_value,
        average_normalized_one_qb_value = EXCLUDED.average_normalized_one_qb_value,
        superflex_sf_value = EXCLUDED.superflex_sf_value,
        superflex_one_qb_value = EXCLUDED.superflex_one_qb_value,
        superflex_sf_rank = EXCLUDED.superflex_sf_rank,
        superflex_one_qb_rank = EXCLUDED.superflex_one_qb_rank,
        superflex_sf_pos_rank = EXCLUDED.superflex_sf_pos_rank,
        superflex_one_qb_pos_rank = EXCLUDED.superflex_one_qb_pos_rank,
        insert_date = EXCLUDED.insert_date;
    """
    data_to_insert = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in df_to_load.itertuples(index=False, name=None)
    ]
    insert_count = len(data_to_insert)
    context.log.info(f"Attempting to insert/update {insert_count} rows...")
    
    conn = None
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                # Using execute_batch for efficiency
                postgres.execute_batch(cursor, insert_query, data_to_insert, page_size=100) # Adjust page_size as needed
            conn.commit()
            context.log.info(f"Successfully processed {insert_count} rows for insert/update.")

    except Exception as e:
        context.log.error(f"Error loading processed player data into staging: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        # Connection managed by context manager
        pass

    duration = time.time() - start_time
    return dg.Output(
        value={
            "records_processed": insert_count,
            "table_name": "staging.sf_player_ranks",
        },
        metadata={
            "execution_time_seconds": dg.MetadataValue.float(duration),
            "records_processed": dg.MetadataValue.int(insert_count),
            "target_table": "staging.sf_player_ranks",
        },
    )

@dg.asset(
    name="load_sf_player_ranks_hist",
    description="Loads the processed player/pick data into the sf_player_ranks table.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    tags={"layer": "gold", "sink": "postgres"},
    deps=[dg.AssetKey("load_sf_player_ranks_staging")],
)
def load_sf_player_ranks_hist(context: dg.OpExecutionContext) -> dg.Output:

    """Loading historical player data into the sf_player_ranks table"""
    
    postgres = context.resources.postgres
    
    start_time = time.time()
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Define precision for rounding float comparisons
            float_precision = 2 
            upsert_query = f"""
            --Create temp table with records that need to be inserted or updated
            CREATE TEMP TABLE temp_sf_player_ranks AS
            SELECT sf.* 
            FROM staging.sf_player_ranks sf
            LEFT JOIN dynastr.sf_player_ranks_hist sfh ON sf.ktc_player_id = sfh.ktc_player_id AND sfh.is_current = true AND sf.rank_type = sfh.rank_type
            WHERE sfh.hist_sk IS NULL
            OR (
                sfh.is_current = true AND (
                    COALESCE(sf.player_full_name, '') != COALESCE(sfh.player_full_name, '') OR
                    COALESCE(sf.display_player_full_name, '') != COALESCE(sfh.display_player_full_name, '') OR
                    COALESCE(sf.team, '') != COALESCE(sfh.team, '') OR
                    COALESCE(sf._position, '') != COALESCE(sfh._position, '') OR
                    COALESCE(sf.ktc_sf_value, -1) != COALESCE(sfh.ktc_sf_value, -1) OR
                    COALESCE(sf.ktc_sf_rank, -1) != COALESCE(sfh.ktc_sf_rank, -1) OR
                    COALESCE(sf.ktc_one_qb_value, -1) != COALESCE(sfh.ktc_one_qb_value, -1) OR
                    COALESCE(sf.ktc_one_qb_rank, -1) != COALESCE(sfh.ktc_one_qb_rank, -1) OR
                    COALESCE(sf.fc_sf_value, -1) != COALESCE(sfh.fc_sf_value, -1) OR
                    COALESCE(sf.fc_sf_rank, -1) != COALESCE(sfh.fc_sf_rank, -1) OR
                    COALESCE(sf.fc_one_qb_value, -1) != COALESCE(sfh.fc_one_qb_value, -1) OR
                    COALESCE(sf.fc_one_qb_rank, -1) != COALESCE(sfh.fc_one_qb_rank, -1) OR
                    COALESCE(sf.dp_sf_value, -1) != COALESCE(sfh.dp_sf_value, -1) OR
                    COALESCE(sf.dp_sf_rank, -1) != COALESCE(sfh.dp_sf_rank, -1) OR
                    COALESCE(sf.dp_one_qb_value, -1) != COALESCE(sfh.dp_one_qb_value, -1) OR
                    COALESCE(sf.dp_one_qb_rank, -1) != COALESCE(sfh.dp_one_qb_rank, -1) OR
                    COALESCE(sf.dd_sf_value, -1) != COALESCE(sfh.dd_sf_value, -1) OR
                    COALESCE(sf.dd_sf_rank, -1) != COALESCE(sfh.dd_sf_rank, -1) OR
                    COALESCE(sf.dd_one_qb_value, -1) != COALESCE(sfh.dd_one_qb_value, -1) OR
                    COALESCE(sf.dd_one_qb_rank, -1) != COALESCE(sfh.dd_one_qb_rank, -1) OR
                    -- Rounded float comparisons
                    ROUND(COALESCE(sf.ktc_sf_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.ktc_sf_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.fc_sf_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.fc_sf_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.dp_sf_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.dp_sf_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.dd_sf_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.dd_sf_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.ktc_one_qb_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.ktc_one_qb_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.fc_one_qb_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.fc_one_qb_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.dp_one_qb_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.dp_one_qb_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.dd_one_qb_normalized_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.dd_one_qb_normalized_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.average_normalized_sf_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.average_normalized_sf_value, -1.0)::numeric, {float_precision}) OR
                    ROUND(COALESCE(sf.average_normalized_one_qb_value, -1.0)::numeric, {float_precision}) != ROUND(COALESCE(sfh.average_normalized_one_qb_value, -1.0)::numeric, {float_precision}) OR
                    -- End rounded float comparisons
                    COALESCE(sf.superflex_sf_value, -1) != COALESCE(sfh.superflex_sf_value, -1) OR
                    COALESCE(sf.superflex_one_qb_value, -1) != COALESCE(sfh.superflex_one_qb_value, -1) OR
                    COALESCE(sf.superflex_sf_rank, -1) != COALESCE(sfh.superflex_sf_rank, -1) OR
                    COALESCE(sf.superflex_one_qb_rank, -1) != COALESCE(sfh.superflex_one_qb_rank, -1) OR
                    COALESCE(sf.superflex_sf_pos_rank, -1) != COALESCE(sfh.superflex_sf_pos_rank, -1) OR
                    COALESCE(sf.superflex_one_qb_pos_rank, -1) != COALESCE(sfh.superflex_one_qb_pos_rank, -1)
                )
            );

            -- Update existing records: Set valid_to and is_current = false for rows that are changing
            UPDATE dynastr.sf_player_ranks_hist sfh
            SET valid_to = CURRENT_TIMESTAMP, is_current = false
            WHERE sfh.is_current = true
            AND EXISTS (
                SELECT 1 FROM temp_sf_player_ranks tsf
                WHERE tsf.ktc_player_id = sfh.ktc_player_id AND tsf.rank_type = sfh.rank_type
            );
            
            -- Insert new/updated records as current
            INSERT INTO dynastr.sf_player_ranks_hist (
                player_full_name, display_player_full_name, ktc_player_id, team, _position, ktc_sf_value,
                ktc_sf_rank, ktc_one_qb_value, ktc_one_qb_rank, fc_sf_value,
                fc_sf_rank, fc_one_qb_value, fc_one_qb_rank, dp_sf_value,
                dp_sf_rank, dp_one_qb_value, dp_one_qb_rank,
                dd_sf_value, dd_sf_rank, dd_one_qb_value, dd_one_qb_rank,
                ktc_sf_normalized_value, fc_sf_normalized_value,
                dp_sf_normalized_value, dd_sf_normalized_value,
                ktc_one_qb_normalized_value,
                fc_one_qb_normalized_value, dp_one_qb_normalized_value, dd_one_qb_normalized_value,
                average_normalized_sf_value, average_normalized_one_qb_value,
                superflex_sf_value, superflex_one_qb_value, superflex_sf_rank,
                superflex_one_qb_rank, superflex_sf_pos_rank, superflex_one_qb_pos_rank,
                insert_date, rank_type,
                valid_from, is_current
            )
            SELECT
                player_full_name, display_player_full_name, ktc_player_id, team, _position, ktc_sf_value,
                ktc_sf_rank, ktc_one_qb_value, ktc_one_qb_rank, fc_sf_value,
                fc_sf_rank, fc_one_qb_value, fc_one_qb_rank, dp_sf_value,
                dp_sf_rank, dp_one_qb_value, dp_one_qb_rank,
                dd_sf_value, dd_sf_rank, dd_one_qb_value, dd_one_qb_rank,
                ktc_sf_normalized_value, fc_sf_normalized_value,
                dp_sf_normalized_value, dd_sf_normalized_value,
                ktc_one_qb_normalized_value,
                fc_one_qb_normalized_value, dp_one_qb_normalized_value, dd_one_qb_normalized_value,
                average_normalized_sf_value, average_normalized_one_qb_value,
                superflex_sf_value, superflex_one_qb_value, superflex_sf_rank,
                superflex_one_qb_rank, superflex_sf_pos_rank, superflex_one_qb_pos_rank,
                insert_date, rank_type,
                CURRENT_TIMESTAMP, true
            FROM temp_sf_player_ranks sf;
            
            DROP TABLE temp_sf_player_ranks;
            """
            context.log.info("Executing SCD Type 2 logic for sf_player_ranks_hist...")
            # Explicitly cast to numeric for ROUND function if needed, depending on exact PG types (FLOAT vs REAL)
            cursor.execute(upsert_query) 
            context.log.info(f"Upsert operation affected {cursor.rowcount} rows.")

        # Commit happens automatically when 'with conn:' block exits without error
        context.log.info("Transaction committed.")

    end_time = time.time()
    duration = end_time - start_time
    
    return dg.Output(
        value=True,
        metadata={
            "duration_seconds": dg.MetadataValue.float(duration),
            "target_table": dg.MetadataValue.text("dynastr.sf_player_ranks_hist"),
            "operation": dg.MetadataValue.text("SCD Type 2 Upsert"),
        }
    )
