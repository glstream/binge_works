# requirements.txt (ensure these are installed)
# dagster
# dagster-postgres # Assuming you have this for the resource definition elsewhere
# requests
# psycopg2-binary # Or psycopg2

import dagster as dg
import requests
import csv
from io import StringIO
from psycopg2.extras import execute_batch
from datetime import datetime
import os

# --- Configuration ---
POSTGRES_RESOURCE_KEY = "postgres" # Matching your setup
TARGET_TABLE = "dynastr.dp_player_ranks"
SCHEMA_NAME = "dynastr" # Assuming schema exists
DP_CSV_URL = "https://raw.githubusercontent.com/dynastyprocess/data/master/files/values.csv"

# --- Assets ---

@dg.asset(
    name="dp_raw_player_data",
    description="Fetches and parses player value data from the DynastyProcess CSV file.",
    compute_kind="python",
    metadata={"source": DP_CSV_URL}
)
def dp_raw_player_data(context: dg.AssetExecutionContext) -> list[list]:
    """
    Fetches CSV data from DynastyProcess GitHub, parses it, formats draft picks,
    adds timestamps, and structures it for loading.
    """
    context.log.info(f"Fetching DynastyProcess CSV data from {DP_CSV_URL}")
    try:
        res = requests.get(DP_CSV_URL, timeout=30)
        res.raise_for_status()
        data = res.text
    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to fetch CSV data: {e}")
        raise dg.Failure(f"Failed to fetch CSV from {DP_CSV_URL}. Error: {e}")

    try:
        f = StringIO(data)
        # Use csv.reader correctly, reading the whole line as one field initially is wrong.
        # Read directly with comma delimiter.
        reader = csv.reader(f, delimiter=",")
        header = next(reader) # Skip header row
        context.log.info(f"CSV Header: {header}")

        dp_players_parsed = list(reader) # Read all rows
        context.log.info(f"Read {len(dp_players_parsed)} rows from CSV.")

        dp_players_prepped = []
        entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z") # Generate timestamp here

        # Process rows and structure data
        for i, row in enumerate(dp_players_parsed):
            # Basic check for expected number of columns (based on Airflow indexing up to 11)
            if len(row) < 12:
                context.log.warning(f"Skipping row {i+1} due to insufficient columns ({len(row)}): {row}")
                continue

            try:
                player_full_name = row[0].replace("'", "").replace('"', "").replace(' III', "").replace(' II', "").replace(' Jr.', "")
                # Draft Pick Formatting Logic (from Airflow task)
                if '.' in player_full_name and any(year in player_full_name for year in ['2024', '2025', '2026', '2027']): # More robust year check
                    try:
                        parts = player_full_name.split(' ')
                        year = parts[0]
                        pick_details = parts[-1]
                        round_num = pick_details.split('.')[0]
                        pick_num_str = pick_details.split('.')[-1]
                        pick_num = int(pick_num_str)
                        player_full_name = f"{year} Round {round_num} Pick {pick_num}"
                        
                        # For draft picks, first/last name might not make sense, use pick info or None
                        player_first_name = f"{year} R{round_num}"
                        player_last_name = f"P{pick_num}"
                        
                    except (IndexError, ValueError) as fmt_e:
                        context.log.warning(f"Could not format draft pick '{row[0]}': {fmt_e}. Using original name.")
                        player_first_name = player_full_name.split(" ")[0] # Fallback name parsing
                        player_last_name = player_full_name.split(" ")[-1] if " " in player_full_name else player_first_name
                else:
                    # Regular player name parsing
                    player_first_name = player_full_name.split(" ")[0]
                    player_last_name = player_full_name.split(" ")[-1] if " " in player_full_name else player_first_name

                # Safely convert numeric values, defaulting to None if conversion fails
                def safe_int_convert(value_str):
                    if value_str is None or value_str == '': return None
                    try: return int(value_str)
                    except (ValueError, TypeError): return None

                # Safely get values, defaulting '' to None
                def safe_get(value_str):
                    return None if value_str == '' else value_str

                # Structure matches the VALUES clause in dp_player_load
                player_record = [
                    player_first_name,
                    player_last_name,
                    player_full_name,
                    safe_get(row[11]), # fp_player_id (column index 11)
                    safe_get(row[1]),  # player_position
                    safe_get(row[2]),  # team
                    safe_get(row[3]),  # age
                    safe_get(row[5]),  # one_qb_rank_ecr
                    safe_get(row[6]),  # sf_rank_ecr
                    safe_get(row[7]),  # ecr_pos
                    safe_int_convert(row[8]), # one_qb_value
                    safe_int_convert(row[9]), # sf_value
                    entry_time, # insert_date
                ]
                dp_players_prepped.append(player_record)

            except IndexError as e:
                context.log.warning(f"Skipping row {i+1} due to IndexError: {e}. Row data: {row}")
            except Exception as e:
                context.log.error(f"Error processing row {i+1}: {row}. Error: {e}")

        context.log.info(f"Successfully processed {len(dp_players_prepped)} player/pick records.")
        return dp_players_prepped

    except Exception as e:
        context.log.error(f"Failed to parse CSV data: {e}")
        raise dg.Failure(f"Failed to parse CSV data. Error: {e}")


@dg.asset(
    name="dp_player_ranks_loaded",
    description=f"Loads the parsed DynastyProcess player data into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    metadata={"target_table": TARGET_TABLE}
    # Dependency inferred from input argument dp_raw_player_data
)
def dp_player_ranks_loaded(context: dg.AssetExecutionContext, dp_raw_player_data: list[list]) -> dg.Output:
    """
    Validates and loads DynastyProcess data.
    Uses INSERT ON CONFLICT (player_full_name) to add or update player ranks.
    """
    postgres = context.resources.postgres

    # Validation (from data_validation task)
    if not dp_raw_player_data:
        context.log.warning("No DP player data received from parsing step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No data parsed"})

    context.log.info(f"Received {len(dp_raw_player_data)} DP player records to load.")

    # SQL matches dp_player_load Airflow task
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            player_first_name, player_last_name, player_full_name, fp_player_id,
            player_position, team, age, one_qb_rank_ecr, sf_rank_ecr,
            ecr_pos, one_qb_value, sf_value, insert_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (player_full_name) -- Conflict target from Airflow task
        DO UPDATE SET
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            fp_player_id = EXCLUDED.fp_player_id, -- Make sure to update ID on conflict too
            player_position = EXCLUDED.player_position,
            team = EXCLUDED.team,
            age = EXCLUDED.age,
            one_qb_rank_ecr = EXCLUDED.one_qb_rank_ecr,
            sf_rank_ecr = EXCLUDED.sf_rank_ecr,
            ecr_pos = EXCLUDED.ecr_pos,
            one_qb_value = EXCLUDED.one_qb_value,
            sf_value = EXCLUDED.sf_value,
            insert_date = EXCLUDED.insert_date;
    """

    try:
        # Adapt based on your actual PostgresResource implementation
        with postgres.get_connection() as conn: # Example assuming get_connection method
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(dp_raw_player_data)} DP rows...")
                # Ensure data types are compatible with DB (e.g., None for missing ints)
                execute_batch(cursor, insert_sql, dp_raw_player_data, page_size=1000)
                row_count = cursor.rowcount # Approx affected rows
                context.log.info(f"DP Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(dp_raw_player_data)} DP players into {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_processed": len(dp_raw_player_data)},
            metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(dp_raw_player_data)),
            }
        )
    except AttributeError:
        # Fallback if the resource has execute_batch directly
        try:
            context.log.info(f"Executing batch insert/update for {len(dp_raw_player_data)} DP rows using resource method...")
            postgres.execute_batch(insert_sql, dp_raw_player_data, page_size=1000)
            context.log.info(f"Successfully inserted/updated {len(dp_raw_player_data)} DP players into {TARGET_TABLE}.")
            return dg.Output(
                value={"rows_processed": len(dp_raw_player_data)},
                metadata={
                    "table": TARGET_TABLE,
                    "rows_processed": dg.MetadataValue.int(len(dp_raw_player_data)),
                }
            )
        except Exception as e:
            context.log.error(f"Database error during DP batch load (using resource method): {e}")
            raise dg.Failure(f"Failed to load DP data into {TARGET_TABLE}. Error: {e}")
    except Exception as e:
        context.log.error(f"Database error during DP batch load (using connection): {e}")
        # You might want to inspect dp_raw_player_data here for problematic records if possible
        # context.log.debug(f"Data sample causing error: {dp_raw_player_data[:5]}")
        raise dg.Failure(f"Failed to load DP data into {TARGET_TABLE}. Error: {e}")


@dg.asset(
    name="dp_player_ranks_formatted",
    description=f"Formats the player_first_name column in the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    # IMPORTANT: Depends on the DP load being finished
    deps=[dp_player_ranks_loaded],
    metadata={"target_table": TARGET_TABLE}
)
def dp_player_ranks_formatted(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Applies formatting rules to the player_first_name column in the DP ranks table.
    Runs after the data loading step for dp_player_ranks is complete.
    """
    postgres = context.resources.postgres
    context.log.info(f"Starting formatting for player_first_name in {TARGET_TABLE}")

    # SQL formatting logic from surrogate_key_formatting Airflow task
    # This uses the exact same replacements as the KTC formatting task.
    replacements = [
        ('.', ''), (' Jr', ''), (' III', ''), ('Jeffery', 'Jeff'), ('Joshua', 'Josh'),
        ('William', 'Will'), (' II', ''), ("''", ''), ('Kenneth', 'Ken'),
        ('Mitchell', 'Mitch'), ('DWayne', 'Dee')
        # Removed ('Gabe','Gabriel') as it wasn't in this specific Airflow task's version
    ]

    update_expr = "player_first_name"
    for find, replace_with in replacements:
        update_expr = f"replace({update_expr}, %s, %s)"

    update_sql = f"UPDATE {TARGET_TABLE} SET player_first_name = {update_expr};"
    params = tuple(item for pair in replacements for item in pair)

    try:
        # Adapt based on your actual PostgresResource implementation
        with postgres.get_connection() as conn: # Example assuming get_connection method
            with conn.cursor() as cursor:
                context.log.info("Established connection. Executing formatting update...")
                cursor.execute(update_sql, params)
                updated_rows = cursor.rowcount
                context.log.info(f"Formatting update complete. Rows affected: {updated_rows}")
        # Commit handled by context manager

        context.log.info(f"Successfully formatted player_first_name in {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_affected": updated_rows},
            metadata={
                "table": TARGET_TABLE,
                "rows_affected": dg.MetadataValue.int(updated_rows),
                "column_formatted": "player_first_name"
            }
        )
    except AttributeError:
        # Fallback if the resource has execute directly
        try:
            context.log.info("Executing formatting update using resource execute...")
            result = postgres.execute(update_sql, parameters=params) # Adapt based on actual method
            updated_rows = result.rowcount if hasattr(result, 'rowcount') else -1 # Or get from return value
            context.log.info(f"Formatting update complete. Rows affected: {updated_rows}")
            return dg.Output(
                value={"rows_affected": updated_rows},
                metadata={
                    "table": TARGET_TABLE,
                    "rows_affected": dg.MetadataValue.int(updated_rows),
                    "column_formatted": "player_first_name"
                }
            )
        except Exception as e:
            context.log.error(f"Database error during formatting (using resource method): {e}")
            raise dg.Failure(f"Failed to format {TARGET_TABLE}. Error: {e}")
    except Exception as e:
        context.log.error(f"Database error during formatting (using connection): {e}")
        raise dg.Failure(f"Failed to format {TARGET_TABLE}. Error: {e}")
