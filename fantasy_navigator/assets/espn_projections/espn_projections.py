import dagster as dg
import requests
import json
from datetime import datetime, timedelta # Keep timedelta if needed elsewhere
import os # Keep os if needed elsewhere
# from psycopg2.extras import execute_batch # Import if needed by resource/fallback
from typing import List, Dict, Any, Optional, Tuple

# --- Configuration ---
POSTGRES_RESOURCE_KEY = "postgres" # Matching your setup
TARGET_TABLE = "dynastr.espn_player_projections"
SCHEMA_NAME = "dynastr" # Assuming schema exists

# Determine Season Year Dynamically
# Use context execution time if available, otherwise use current time
# ESPN filters seem to use current/previous season, so use current year.
current_year = datetime.now().year
SEASON_YEAR = str(current_year)
context_date = datetime.now() # Fallback if no context available
context_date_str = context_date.strftime("%Y-%m-%d") # For logging

ESPN_API_URL_TEMPLATE = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{season}/segments/0/leaguedefaults/3?scoringPeriodId=0&view=kona_player_info"

# Define column names based on the INSERT statement in the Airflow task
ESPN_TABLE_COLUMNS = [
    "player_first_name", "player_last_name", "player_full_name", "espn_player_id",
    "ppr_rank", "ppr_auction_value", "total_projection", "recs", "rec_yards",
    "rec_tds", "carries", "rush_yards", "rush_tds", "pass_attempts",
    "pass_completions", "pass_yards", "pass_tds", "pass_ints", "insert_date"
]

# --- Helper Function for ESPN Stat Extraction ---
# Mapping based on common ESPN stat IDs (verify if possible)
# https://github.com/cwendt94/espn-api/blob/master/espn_api/football/constant.py can be a reference
STAT_MAP = {
    '0': 'pass_attempts', '1': 'pass_completions', '3': 'pass_yards', '4': 'pass_tds',
    '20': 'pass_ints', '23': 'carries', '24': 'rush_yards', '25': 'rush_tds',
    '53': 'recs', '42': 'rec_yards', '43': 'rec_tds'
    # Add other relevant stat IDs if needed
}

def get_espn_stat(player_stats_list: list, stat_id: str, default=0):
    """Safely extracts a stat from the ESPN player stats list."""
    if not player_stats_list:
        return default
    # Find the projection stat block (often id '10YYYY' or the last one)
    # The Airflow code used stats[-1], let's assume that's the projection block.
    # A more robust method might loop through stats looking for statSourceId == 1 (projection)
    # or scoringPeriodId == 0 (season total projection)
    projection_stats = player_stats_list[-1]
    if 'stats' in projection_stats:
        # Use .get() for safety, round the result
        value = projection_stats['stats'].get(stat_id, default)
        try:
            # Ensure value is numeric before rounding
            return round(float(value)) if value is not None else default
        except (ValueError, TypeError):
            return default
    return default

# --- Assets ---

@dg.asset(
    name="espn_raw_projections_data",
    description=f"Fetches {SEASON_YEAR} player projections from the ESPN API.",
    compute_kind="python",
    metadata={"source": "fantasy.espn.com"}
)
def espn_raw_projections_data(context: dg.AssetExecutionContext) -> Optional[List[Dict[str, Any]]]:
    """Fetches the raw player list from the ESPN API using configured filters."""
    # Try to get season year from context if scheduled
    effective_season_year = SEASON_YEAR

    # Construct filters dynamically
    filters = {
        "players": {
            "filterStatsForExternalIds": {"value": [int(effective_season_year)-1, int(effective_season_year)]},
            "filterSlotIds": {"value": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,23,24]}, # Common player slots
            "filterStatsForSourceIds": {"value": [0, 1]}, # Actual (0) and Projected (1)
            "useFullProjectionTable": {"value": True},
            # Sort by projected points for the target season
            "sortAppliedStatTotal": {"sortAsc": False, "sortPriority": 3, "value": f"10{effective_season_year}"},
            "sortDraftRanks": {"sortPriority": 2, "sortAsc": True, "value": "PPR"}, # Secondary sort by PPR draft rank
            "sortPercOwned": {"sortPriority": 4, "sortAsc": False},
            "limit": 1075, # Fetch a large batch
            "offset": 0,
            "filterRanksForScoringPeriodIds": {"value": [1]}, # Typically season outlook rank
            "filterRanksForRankTypes": {"value": ["PPR"]},
            "filterRanksForSlotIds": {"value": [0, 2, 4, 6, 17, 16]}, # QB, RB, WR, TE, Flex types
            "filterStatsForTopScoringPeriodIds": {
                "value": 2, # Number of periods? Seems related to external IDs maybe?
                # Projections (10), Actuals (00), Season Type (0), Scoring Period Type (2?) - This needs ESPN API knowledge
                "additionalValue": [f"00{effective_season_year}", f"10{effective_season_year}", f"00{effective_season_year}", f"02{effective_season_year}"]
            },
        }
    }
    headers = {"x-fantasy-filter": json.dumps(filters)}
    url = ESPN_API_URL_TEMPLATE.format(season=effective_season_year)

    context.log.info(f"Requesting ESPN data from: {url}")
    # context.log.debug(f"Using headers: {headers}") # Be careful logging sensitive info if headers contain tokens

    try:
        req = requests.get(url, headers=headers, timeout=60) # Increased timeout
        context.log.info(f"API Response Status Code: {req.status_code}")
        req.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        res = req.json()

        if "players" in res and isinstance(res["players"], list):
            context.log.info(f"Successfully fetched {len(res['players'])} player records from ESPN API.")
            # context.log.debug(f"First player record sample: {res['players'][0] if res['players'] else 'None'}")
            return res["players"]
        else:
            context.log.warning("ESPN API response did not contain a 'players' list.")
            return None

    except requests.exceptions.RequestException as e:
        context.log.error(f"ESPN API request failed: {e}")
        return None
    except json.JSONDecodeError as e:
        context.log.error(f"Failed to decode JSON response from ESPN API: {e}. Response text: {req.text[:500]}") # Log partial response
        return None
    except Exception as e:
        context.log.error(f"An unexpected error occurred during ESPN API fetch: {e}")
        return None


@dg.asset(
    name="espn_player_projections_loaded",
    description=f"Loads parsed {SEASON_YEAR} ESPN player projections into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    group_name="espn_projections",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    metadata={"target_table": TARGET_TABLE}
    # Dependency inferred from input argument espn_raw_projections_data
)
def espn_player_projections_loaded(context: dg.AssetExecutionContext, espn_raw_projections_data: Optional[List[Dict[str, Any]]]) -> dg.Output:
    """
    Validates, parses, and loads ESPN projections data using execute_batch.
    Uses INSERT ON CONFLICT (espn_player_id) to add or update projections.
    """
    postgres = context.resources.postgres

    # Validation (Combined from Airflow validation + load start)
    if not espn_raw_projections_data:
        context.log.warning("No ESPN projection data received from API step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No data from API"})

    context.log.info(f"Received {len(espn_raw_projections_data)} ESPN raw player records to process.")

    # Generate timestamp ONCE before the loop
    entry_timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    espn_players_to_load = []
    skipped_count = 0

    # Parse data (from projections_load Airflow task)
    for player in espn_raw_projections_data:
        try:
            player_info = player.get("player")
            if not player_info:
                context.log.warning(f"Skipping record due to missing 'player' key: {player.get('id')}")
                skipped_count += 1
                continue

            player_id = player.get("id")
            full_name_raw = player_info.get("fullName")
            if not player_id or not full_name_raw:
                context.log.warning(f"Skipping record due to missing 'id' or 'fullName': ID={player_id}, Name={full_name_raw}")
                skipped_count += 1
                continue

            # Name Cleaning
            full_name = full_name_raw.replace("'", "").replace('"', "").replace(' III', "").replace(' II', "").replace(' Jr.', "")
            first_name = full_name.split(" ")[0]
            last_name = full_name.split(" ")[-1] if " " in full_name else first_name

            # Ranks and Auction Values (handle missing data)
            ranks = player_info.get("draftRanksByRankType", {}).get("PPR", {})
            ppr_rank = ranks.get("rank", -1) # Defaulting to -1 like Airflow task
            ppr_auction = ranks.get("auctionValue", -1)

            # Stats Extraction (using helper for safety)
            stats_list = player_info.get("stats", [])
            total_projection = round(stats_list[-1].get("appliedTotal", 0)) if stats_list else 0 # Check list exists

            # Build tuple in the order of ESPN_TABLE_COLUMNS
            player_tuple = (
                first_name,
                last_name,
                full_name, # Use cleaned full name
                player_id,
                ppr_rank,
                ppr_auction,
                total_projection,
                get_espn_stat(stats_list, '53'), # recs
                get_espn_stat(stats_list, '42'), # rec_yards
                get_espn_stat(stats_list, '43'), # rec_tds
                get_espn_stat(stats_list, '23'), # carries
                get_espn_stat(stats_list, '24'), # rush_yards
                get_espn_stat(stats_list, '25'), # rush_tds
                get_espn_stat(stats_list, '0'), # pass_attempts
                get_espn_stat(stats_list, '1'), # pass_completions
                get_espn_stat(stats_list, '3'), # pass_yards
                get_espn_stat(stats_list, '4'), # pass_tds
                get_espn_stat(stats_list, '20'), # pass_ints
                entry_timestamp_str,
            )

            if len(player_tuple) != len(ESPN_TABLE_COLUMNS):
                context.log.error(f"CRITICAL: Tuple length mismatch for player {player_id}. Expected {len(ESPN_TABLE_COLUMNS)}, got {len(player_tuple)}. Fix parsing logic.")
                skipped_count += 1
                continue # Skip this malformed record

            espn_players_to_load.append(player_tuple)

        # Catch specific errors during parsing if possible
        except (KeyError, IndexError, TypeError, AttributeError) as e:
            context.log.warning(f"Skipping player record due to parsing error: {e}. Player ID: {player.get('id')}. Data sample: {str(player)[:200]}")
            skipped_count += 1
        except Exception as e: # Catch broader errors
            context.log.error(f"Unexpected error processing player record: {e}. Player ID: {player.get('id')}")
            skipped_count += 1

    if not espn_players_to_load:
        context.log.warning("No valid ESPN records remain after parsing/filtering. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No valid data after parsing"})

    context.log.info(f"Attempting to load {len(espn_players_to_load)} valid ESPN projection records. Skipped {skipped_count} invalid records.")

    # Prepare SQL
    column_names_str = ", ".join(ESPN_TABLE_COLUMNS)
    value_placeholders = ", ".join(["%s"] * len(ESPN_TABLE_COLUMNS))
    # SQL matches projections_load Airflow task, ON CONFLICT uses espn_player_id
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({column_names_str})
        VALUES ({value_placeholders})
        ON CONFLICT (espn_player_id) DO UPDATE SET
            player_first_name=excluded.player_first_name, -- Also update names if ID conflicts
            player_last_name=excluded.player_last_name,
            player_full_name=excluded.player_full_name,
            ppr_rank=excluded.ppr_rank,
            ppr_auction_value=excluded.ppr_auction_value,
            total_projection=excluded.total_projection,
            recs=excluded.recs,
            rec_yards=excluded.rec_yards,
            rec_tds=excluded.rec_tds,
            carries=excluded.carries,
            rush_yards=excluded.rush_yards,
            rush_tds=excluded.rush_tds,
            pass_attempts=excluded.pass_attempts,
            pass_completions=excluded.pass_completions,
            pass_yards=excluded.pass_yards,
            pass_tds=excluded.pass_tds,
            pass_ints=excluded.pass_ints,
            insert_date=excluded.insert_date;
        """

    try:
        # Adapt based on your actual PostgresResource implementation
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(espn_players_to_load)} ESPN rows...")
                from psycopg2.extras import execute_batch # Ensure import
                execute_batch(cursor, insert_sql, espn_players_to_load, page_size=1000)
                row_count = cursor.rowcount
                context.log.info(f"ESPN Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(espn_players_to_load)} ESPN projections into {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_processed": len(espn_players_to_load)},
            metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(espn_players_to_load)),
                "rows_skipped_parsing": dg.MetadataValue.int(skipped_count)
            }
        )
    except AttributeError:
        # Fallback for resource.execute_batch
        try:
            context.log.warning("Falling back to execute_batch on resource directly.")
            postgres.execute_batch(insert_sql, espn_players_to_load, page_size=1000)
            context.log.info(f"Successfully inserted/updated {len(espn_players_to_load)} ESPN projections into {TARGET_TABLE} (via resource.execute_batch).")
            return dg.Output(
                value={"rows_processed": len(espn_players_to_load)},
                metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(espn_players_to_load)),
                "rows_skipped_parsing": dg.MetadataValue.int(skipped_count)
                }
            )
        except Exception as e_fallback:
            context.log.error(f"Database error during ESPN batch load (resource method fallback): {e_fallback}")
            raise dg.Failure(f"Failed to load ESPN data into {TARGET_TABLE}. Error: {e_fallback}")
    except Exception as e_main:
        context.log.error(f"Database error during ESPN batch load (connection method): {e_main}")
        raise dg.Failure(f"Failed to load ESPN data into {TARGET_TABLE}. Error: {e_main}")


@dg.asset(
    name="espn_player_projections_formatted",
    description=f"Formats the player_first_name column in the {TARGET_TABLE} table.",
    compute_kind="postgres",
    group_name="espn_projections",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    # Depends on the load step finishing
    deps=[espn_player_projections_loaded],
    metadata={"target_table": TARGET_TABLE}
)
def espn_player_projections_formatted(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Applies generic formatting rules to the player_first_name column in the ESPN projections table.
    Replicates the logic from previous surrogate_key_formatting tasks.
    """
    postgres = context.resources.postgres
    context.log.info(f"Starting formatting for player_first_name in {TARGET_TABLE}")

    # Replicating standard formatting logic from previous DAGs
    replacements = [
        ('.', ''), (' Jr', ''), (' III', ''), ('Jeffery', 'Jeff'), ('Joshua', 'Josh'),
        ('William', 'Will'), (' II', ''), ("''", ''), ('Kenneth', 'Ken'),
        ('Mitchell', 'Mitch'), ('DWayne', 'Dee')
    ]

    update_expr = "player_first_name"
    params = []
    for find, replace_with in replacements:
        update_expr = f"replace({update_expr}, %s, %s)"
        params.extend([find, replace_with])

    update_sql = f"UPDATE {TARGET_TABLE} SET player_first_name = {update_expr};"

    try:
        # Adapt based on your actual PostgresResource implementation
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info("Established connection. Executing formatting update for player_first_name...")
                cursor.execute(update_sql, tuple(params)) # Pass params as a tuple
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
            context.log.warning("Falling back to execute on resource directly.")
            result = postgres.execute(update_sql, parameters=tuple(params)) # Adapt based on actual method
            updated_rows = result.rowcount if hasattr(result, 'rowcount') else -1
            context.log.info(f"Formatting update complete. Rows affected: {updated_rows} (via resource.execute).")
            return dg.Output(
                value={"rows_affected": updated_rows},
                metadata={
                    "table": TARGET_TABLE,
                    "rows_affected": dg.MetadataValue.int(updated_rows),
                    "column_formatted": "player_first_name"
                }
            )
        except Exception as e_fallback:
            context.log.error(f"Database error during formatting (resource method fallback): {e_fallback}")
            raise dg.Failure(f"Failed to format {TARGET_TABLE}. Error: {e_fallback}")
    except Exception as e_main:
        context.log.error(f"Database error during formatting (connection method): {e_main}")
        raise dg.Failure(f"Failed to format {TARGET_TABLE}. Error: {e_main}")

# --- Schedule Definition (Example - Based on Airflow @weekly) ---

espn_projections_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job(
        name="espn_projections_job",
        selection=dg.AssetSelection.assets(espn_player_projections_formatted),
        config={ "resources": { POSTGRES_RESOURCE_KEY: { "config": { } } } } # Add config if needed
    ),
    cron_schedule="0 0 * * 0",  # Example: Run Midnight Sunday morning (start of week)
    description="Weekly ESPN player projection pull and load",
)

# --- Definitions ---
# Add the assets (espn_raw_projections_data, espn_player_projections_loaded,
# espn_player_projections_formatted) and espn_projections_schedule
# to your main Definitions object.