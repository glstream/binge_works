import dagster as dg
import requests
from datetime import datetime
import json

# --- Configuration ---
POSTGRES_RESOURCE_KEY = "postgres" # Matching your example asset
TARGET_TABLE = "dynastr.fc_player_ranks"
SCHEMA_NAME = "dynastr" # Assuming schema exists

FC_SF_API_URLS = {
    "dynasty": "https://api.fantasycalc.com/values/current?isDynasty=true&numQbs=2&_source=dynastysuperflex",
    "redraft": "https://api.fantasycalc.com/values/current?isDynasty=false&numQbs=2&_source=dynastysuperflex",
}

FC_ONE_QB_API_URLS = {
    "dynasty": "https://api.fantasycalc.com/values/current?isDynasty=true&numQbs=1",
    "redraft": "https://api.fantasycalc.com/values/current?isDynasty=false&numQbs=1",
}

# --- Helper Function for Name Parsing (from Airflow task) ---
def parse_player_name(name_str: str) -> tuple[str, str]:
    """Parses player name into first and last name components."""
    p = (
        name_str.replace("'", "")
        .replace(" III", "")
        .replace(" II", "")
        .replace(" Jr.", "")
        .split(" ")
    )
    first_name = p[0]
    last_name = p[-1] if len(p) > 1 else first_name # Basic handling for single names
    return first_name, last_name

# --- Assets ---

@dg.asset(
    name="fc_sf_raw_player_data",
    description="Fetches Superflex player data (dynasty and redraft) from FantasyCalc API.",
    compute_kind="python",
    metadata={"source": "fantasycalc.com"}
)
def fc_sf_raw_player_data(context: dg.AssetExecutionContext) ->  dg.Output:
    """Fetches SF player data from FantasyCalc API and structures it as a list of lists."""
    fc_sf_players = []
    entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z") # Generate timestamp here

    context.log.info(f"Fetching FantasyCalc SF data from: {FC_SF_API_URLS.keys()}")
    for rank_type, api_call in FC_SF_API_URLS.items():
        try:
            context.log.info(f"Fetching {rank_type} SF data from {api_call}")
            fc_sf_req = requests.get(api_call, timeout=30)
            fc_sf_req.raise_for_status()
            players_data = fc_sf_req.json()
            context.log.info(f"Received {len(players_data)} players for {rank_type} SF.")

            for i in players_data:
                try:
                    first_name, last_name = parse_player_name(i["player"]["name"])
                    # Structure matches the VALUES clause in sf_fc_player_load
                    player_record = [
                        first_name,
                        last_name,
                        i["player"]["name"],
                        i["player"]["id"],
                        i["player"].get("mflId"), # Use .get() for robustness
                        i["player"].get("sleeperId"),
                        i["player"]["position"],
                        rank_type,
                        None,  # one_qb_overall_rank placeholder
                        None,  # one_qb_position_rank placeholder
                        None,  # one_qb_value placeholder
                        None,  # one_qb_trend_30_day placeholder
                        i.get("overallRank"), # Use .get() for robustness
                        i.get("positionRank"),
                        i.get("value"),
                        i.get("trend30Day"),
                        entry_time,
                    ]
                    fc_sf_players.append(player_record)
                except KeyError as e:
                    context.log.warning(f"Missing key {e} in player record: {i}. Skipping record.")
                except Exception as e:
                    context.log.error(f"Error processing player record {i}: {e}. Skipping record.")

        except requests.exceptions.RequestException as e:
            context.log.error(f"API request failed for {rank_type} SF ({api_call}): {e}")
            # Decide if failure is critical - here we continue to try other URLs
            continue
        except json.JSONDecodeError as e:
            context.log.error(f"Failed to decode JSON response for {rank_type} SF ({api_call}): {e}")
            continue

    context.log.info(f"Total SF players fetched: {len(fc_sf_players)}")
    return dg.Output(
        value=fc_sf_players,    
        metadata={
            "num_rows": len(fc_sf_players),
            "columns": [
                "player_first_name", "player_last_name", "player_full_name", "fc_player_id",
                "mfl_player_id", "sleeper_player_id", "player_position", "rank_type",
                "one_qb_overall_rank", "one_qb_position_rank", "one_qb_value", "one_qb_trend_30_day",
                "sf_overall_rank", "sf_position_rank", "sf_value", "sf_trend_30_day", "insert_date"
            ],
        },
    )

@dg.asset(
    name="fc_player_ranks_sf_loaded",
    description=f"Loads the scraped FantasyCalc SF player data into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    metadata={"target_table": TARGET_TABLE, "load_type": "superflex"}
)
def fc_player_ranks_sf_loaded(context: dg.AssetExecutionContext, fc_sf_raw_player_data) -> dg.Output:
    """
    Validates and loads FantasyCalc Superflex data.
    Uses INSERT ON CONFLICT to add new players or update existing ones with SF specific values.
    """
    postgres = context.resources.postgres # Access resource as per user's example

    # Validation (from sf_data_validation task)
    if not fc_sf_raw_player_data:
        context.log.warning("No SF player data received from API step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No SF data fetched"})

    context.log.info(f"Received {len(fc_sf_raw_player_data)} SF player records to load.")

    # SQL matches sf_fc_player_load Airflow task
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            player_first_name, player_last_name, player_full_name, fc_player_id,
            mfl_player_id, sleeper_player_id, player_position, rank_type,
            one_qb_overall_rank, one_qb_position_rank, one_qb_value, one_qb_trend_30_day,
            sf_overall_rank, sf_position_rank, sf_value, sf_trend_30_day,
            insert_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (player_full_name, rank_type)
        DO UPDATE SET
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            player_position = EXCLUDED.player_position,
            sf_overall_rank = EXCLUDED.sf_overall_rank,
            sf_position_rank = EXCLUDED.sf_position_rank,
            sf_value = EXCLUDED.sf_value,
            sf_trend_30_day = EXCLUDED.sf_trend_30_day,
            insert_date = EXCLUDED.insert_date,
            -- Update IDs only if the new value is not null and the old one might be? Or just always update?
            -- Choosing to update always here, adjust if needed.
            fc_player_id = EXCLUDED.fc_player_id,
            mfl_player_id = EXCLUDED.mfl_player_id,
            sleeper_player_id = EXCLUDED.sleeper_player_id;
    """

    try:
        # Assuming context.resources.postgres has a method like get_connection or similar
        # Adapt this based on your actual PostgresResource implementation
        # If it directly provides execute_batch, use that. If it gives a connection, use psycopg2.extras
        with postgres.get_connection() as conn: # Example assuming get_connection method
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(fc_sf_raw_player_data)} SF rows...")
                postgres.execute_batch(cursor, insert_sql, fc_sf_raw_player_data, page_size=1000)
                row_count = cursor.rowcount # Approx affected rows
                context.log.info(f"SF Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(fc_sf_raw_player_data)} SF players into {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_processed": len(fc_sf_raw_player_data)},
            metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(fc_sf_raw_player_data)),
                "load_type": "superflex"
            }
        )
    except AttributeError:
        # Fallback if the resource has execute_batch directly (less common for generic resource)
        try:
            context.log.info(f"Executing batch insert/update for {len(fc_sf_raw_player_data)} SF rows using resource method...")
            # This assumes your custom resource or the one you're using has this method signature
            postgres.execute_batch(insert_sql, fc_sf_raw_player_data, page_size=1000)
            context.log.info(f"Successfully inserted/updated {len(fc_sf_raw_player_data)} SF players into {TARGET_TABLE}.")
            # Note: Cannot get row_count easily this way unless the method returns it.
            return dg.Output(
                value={"rows_processed": len(fc_sf_raw_player_data)},
                metadata={
                    "table": TARGET_TABLE,
                    "rows_processed": dg.MetadataValue.int(len(fc_sf_raw_player_data)),
                    "load_type": "superflex"
                }
            )
        except Exception as e:
            context.log.error(f"Database error during SF batch load (using resource method): {e}")
            raise dg.Failure(f"Failed to load SF data into {TARGET_TABLE}. Error: {e}")
    except Exception as e:
        context.log.error(f"Database error during SF batch load (using connection): {e}")
        raise dg.Failure(f"Failed to load SF data into {TARGET_TABLE}. Error: {e}")


@dg.asset(
    name="fc_one_qb_raw_player_data",
    description="Fetches 1-QB player data (dynasty and redraft) from FantasyCalc API.",
    compute_kind="python",
    metadata={"source": "fantasycalc.com"},
    # IMPORTANT: Ensure SF data is loaded before fetching 1-QB data
    deps=[fc_player_ranks_sf_loaded]
)
def fc_one_qb_raw_player_data(context: dg.AssetExecutionContext) -> list[list]:
    """Fetches 1-QB player data from FantasyCalc API, depends on SF load completion."""
    fc_one_qb_players = []
    entry_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z") # Generate timestamp here

    context.log.info(f"Fetching FantasyCalc 1-QB data from: {FC_ONE_QB_API_URLS.keys()}")
    for rank_type, api_call in FC_ONE_QB_API_URLS.items():
        try:
            context.log.info(f"Fetching {rank_type} 1-QB data from {api_call}")
            fc_one_qb_req = requests.get(api_call, timeout=30)
            fc_one_qb_req.raise_for_status()
            players_data = fc_one_qb_req.json()
            context.log.info(f"Received {len(players_data)} players for {rank_type} 1-QB.")

            for i in players_data:
                try:
                    first_name, last_name = parse_player_name(i["player"]["name"])
                    # Structure matches the VALUES clause in one_qb_fc_player_load
                    player_record = [
                        first_name,
                        last_name,
                        i["player"]["name"],
                        i["player"]["id"],
                        i["player"].get("mflId"),
                        i["player"].get("sleeperId"),
                        i["player"]["position"],
                        rank_type,
                        i.get("overallRank"), # 1-QB values
                        i.get("positionRank"),
                        i.get("value"),
                        i.get("trend30Day"),
                        None,  # sf_overall_rank placeholder
                        None,  # sf_position_rank placeholder
                        None,  # sf_value placeholder
                        None,  # sf_trend_30_day placeholder
                        entry_time,
                    ]
                    fc_one_qb_players.append(player_record)
                except KeyError as e:
                    context.log.warning(f"Missing key {e} in player record: {i}. Skipping record.")
                except Exception as e:
                    context.log.error(f"Error processing player record {i}: {e}. Skipping record.")

        except requests.exceptions.RequestException as e:
            context.log.error(f"API request failed for {rank_type} 1-QB ({api_call}): {e}")
            continue
        except json.JSONDecodeError as e:
            context.log.error(f"Failed to decode JSON response for {rank_type} 1-QB ({api_call}): {e}")
            continue

    context.log.info(f"Total 1-QB players fetched: {len(fc_one_qb_players)}")
    return fc_one_qb_players

@dg.asset(
    name="fc_player_ranks_one_qb_loaded",
    description=f"Loads the scraped FantasyCalc 1-QB player data into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    metadata={"target_table": TARGET_TABLE, "load_type": "one_qb"}
    # Dependency inferred from input argument fc_one_qb_raw_player_data
)
def fc_player_ranks_one_qb_loaded(context: dg.AssetExecutionContext, fc_one_qb_raw_player_data: list[list]) -> dg.Output:
    """
    Validates and loads FantasyCalc 1-QB data.
    Uses INSERT ON CONFLICT to add new players or update existing ones with 1-QB specific values.
    """
    postgres = context.resources.postgres

    # Validation (from one_qb_data_validation task)
    if not fc_one_qb_raw_player_data:
        context.log.warning("No 1-QB player data received from API step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No 1-QB data fetched"})

    context.log.info(f"Received {len(fc_one_qb_raw_player_data)} 1-QB player records to load.")

    # SQL matches one_qb_fc_player_load Airflow task
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            player_first_name, player_last_name, player_full_name, fc_player_id,
            mfl_player_id, sleeper_player_id, player_position, rank_type,
            one_qb_overall_rank, one_qb_position_rank, one_qb_value, one_qb_trend_30_day,
            sf_overall_rank, sf_position_rank, sf_value, sf_trend_30_day,
            insert_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (player_full_name, rank_type)
        DO UPDATE SET
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            player_position = EXCLUDED.player_position,
            one_qb_overall_rank = EXCLUDED.one_qb_overall_rank,
            one_qb_position_rank = EXCLUDED.one_qb_position_rank,
            one_qb_value = EXCLUDED.one_qb_value,
            one_qb_trend_30_day = EXCLUDED.one_qb_trend_30_day,
            insert_date = EXCLUDED.insert_date,
            -- Update IDs only if the new value is not null and the old one might be? Or just always update?
            -- Choosing to update always here, adjust if needed.
            fc_player_id = EXCLUDED.fc_player_id,
            mfl_player_id = EXCLUDED.mfl_player_id,
            sleeper_player_id = EXCLUDED.sleeper_player_id;
    """

    try:
        # Adapt based on your actual PostgresResource implementation
        with postgres.get_connection() as conn: # Example assuming get_connection method
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(fc_one_qb_raw_player_data)} 1-QB rows...")
                postgres.execute_batch(cursor, insert_sql, fc_one_qb_raw_player_data, page_size=1000)
                row_count = cursor.rowcount # Approx affected rows
                context.log.info(f"1-QB Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(fc_one_qb_raw_player_data)} 1-QB players into {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_processed": len(fc_one_qb_raw_player_data)},
            metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(fc_one_qb_raw_player_data)),
                "load_type": "one_qb"
            }
        )
    except AttributeError:
        # Fallback if the resource has execute_batch directly
        try:
            context.log.info(f"Executing batch insert/update for {len(fc_one_qb_raw_player_data)} 1-QB rows using resource method...")
            postgres.execute_batch(insert_sql, fc_one_qb_raw_player_data, page_size=1000)
            context.log.info(f"Successfully inserted/updated {len(fc_one_qb_raw_player_data)} 1-QB players into {TARGET_TABLE}.")
            return dg.Output(
                value={"rows_processed": len(fc_one_qb_raw_player_data)},
                metadata={
                    "table": TARGET_TABLE,
                    "rows_processed": dg.MetadataValue.int(len(fc_one_qb_raw_player_data)),
                    "load_type": "one_qb"
            }
            )
        except Exception as e:
            context.log.error(f"Database error during 1-QB batch load (using resource method): {e}")
            raise dg.Failure(f"Failed to load 1-QB data into {TARGET_TABLE}. Error: {e}")
    except Exception as e:
        context.log.error(f"Database error during 1-QB batch load (using connection): {e}")
        raise dg.Failure(f"Failed to load 1-QB data into {TARGET_TABLE}. Error: {e}")


@dg.asset(
    name="fc_player_ranks_formatted",
    description=f"Formats the player_first_name column in the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    # IMPORTANT: Depends on the 1-QB load being finished
    deps=[fc_player_ranks_one_qb_loaded],
    metadata={"target_table": TARGET_TABLE}
)
def fc_player_ranks_formatted(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Applies formatting rules to the player_first_name column in the target table.
    Runs after all data loading steps for fc_player_ranks are complete.
    """
    postgres = context.resources.postgres
    context.log.info(f"Starting formatting for player_first_name in {TARGET_TABLE}")

    # SQL formatting logic from surrogate_key_formatting Airflow task
    # Using parameterized query for safety and readability
    replacements = [
        ('.', ''), (' Jr', ''), (' III', ''), ('Jeffery', 'Jeff'), ('Joshua', 'Josh'),
        ('William', 'Will'), (' II', ''), ("''", ''), ('Kenneth', 'Ken'),
        ('Mitchell', 'Mitch'), ('DWayne', 'Dee'), ('Gabe', 'Gabriel') 
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
            # Assumes a method like execute(sql, parameters) exists on your resource
            result = postgres.execute(update_sql, parameters=params) # Adapt based on actual method
            updated_rows = result.rowcount if hasattr(result, 'rowcount') else -1 # Or get from return value if applicable
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
