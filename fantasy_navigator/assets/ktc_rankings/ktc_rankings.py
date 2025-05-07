import dagster as dg
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

TARGET_TABLE = "dynastr.ktc_player_ranks"
SCHEMA_NAME = "dynastr"

@dg.asset(
    name="ktc_raw_player_data",
    description="Scrapes dynasty and redraft player data from KeepTradeCut.",
    compute_kind="python",
    metadata={"source": "keeptradecut.com"}
)
def ktc_raw_player_data(context: dg.AssetExecutionContext) -> list[dict]:
    """
    Fetches player data from KeepTradeCut website for both dynasty and redraft formats.
    """
    page = "0"
    league_format = {"single_qb": "1", "sf": "2"}

    # Dynasty rankings URL
    dynasty_url = f"https://keeptradecut.com/dynasty-rankings?page={page}&filters=QB|WR|RB|TE|RDP&format={league_format['sf']}"
    context.log.info(f"Fetching Dynasty data from: {dynasty_url}")
    
    try:
        dynasty_res = requests.get(dynasty_url, timeout=30)
        dynasty_res.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to fetch dynasty data: {e}")
        raise

    # Redraft rankings URL
    redraft_url = "https://keeptradecut.com/fantasy-rankings"
    context.log.info(f"Fetching Redraft data from: {redraft_url}")
    try:
        redraft_res = requests.get(redraft_url, timeout=30)
        redraft_res.raise_for_status()
    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to fetch redraft data: {e}")
        raise

    # Function to parse the script containing player data
    def parse_players_script(response, rank_type, log) -> list[dict]:
        soup = BeautifulSoup(response.text, "html.parser")
        scripts = soup.find_all("script")
        players_json = None

        for script in scripts:
            if 'var playersArray =' in script.text:
                players_script = script.string
                # Use regex to extract the JSON string
                pattern = r"var playersArray = (\[.*?\]);"
                match = re.search(pattern, players_script, re.DOTALL)
                if match:
                    players_array_str = match.group(1)
                    try:
                        players_json = json.loads(players_array_str)
                    except json.JSONDecodeError as e:
                        log.error(f"JSON decode error for {rank_type}: {e}")
                        players_json = []
                    break # Found the array, stop searching scripts
                else:
                    log.warning(f"'var playersArray =' found but regex failed for {rank_type}.")

        if players_json is None:
            log.warning(f"playersArray variable not found in scripts for {rank_type} URL.")
            return []
        else:
            # Add rank_type to each player record
            for player in players_json:
                player['rank_type'] = rank_type
            context.log.info(f"Successfully parsed {len(players_json)} players for {rank_type}.")
            return players_json

    # Parse both dynasty and redraft rankings
    dynasty_players = parse_players_script(dynasty_res, 'dynasty', context.log)
    redraft_players = parse_players_script(redraft_res, 'redraft', context.log)

    # Combine both rankings into one list
    ktc_players_json = dynasty_players + redraft_players
    context.log.info(f"Total players combined: {len(ktc_players_json)}")

    return ktc_players_json

@dg.asset(
    name="ktc_player_ranks_loaded",
    description=f"Loads the scraped KTC player data into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    metadata={"target_table": TARGET_TABLE}
)
def ktc_player_ranks_loaded(context: dg.AssetExecutionContext, ktc_raw_player_data: list[dict]) -> dg.Output:
    """
    Validates the scraped data and loads it into the dynastr.ktc_player_ranks table
    using an INSERT ... ON CONFLICT DO UPDATE statement.
    """
    postgres = context.resources.postgres

    if not ktc_raw_player_data:
        context.log.warning("No player data received from scraping step. Skipping database load.")
        # Optionally raise an error or handle as needed:
        # raise dg.Failure("No player data scraped.")
        # Or return metadata indicating skipped rows
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No data scraped"})

    context.log.info(f"Received {len(ktc_raw_player_data)} player records to process.")
    entry_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z") # Use consistent UTC time
    ktc_players_to_load = []
    
    
    def replace_special_characters(name: str) -> str:
        """
        Replace special characters in player names.
        """
        replacements = {
            " III": "",
            " Jr.": "",
            " II": "",
            "'": "",
            "''": ""
        }
        for old, new in replacements.items():
            name = name.replace(old, new)
        return name
    

    # Data Transformation and preparation for batch execution
    for ktc_player in ktc_raw_player_data:
        try:
            # Basic cleaning similar to Airflow task
            player_name = ktc_player["playerName"]
            first_name = player_name.split(" ")[0].replace("Cam", "Cameron")
            # More robust last name extraction needed if names are complex
            # Handle suffixes like Jr., III, II, etc. and remove apostrophes
            last_name_parts = replace_special_characters(player_name).split(" ")
            last_name = last_name_parts[-1] if len(last_name_parts) > 1 else first_name

            # Handle potential missing keys gracefully using .get()
            ktc_players_to_load.append(
                (
                    first_name,
                    last_name,
                    player_name,
                    ktc_player.get("playerID"),
                    ktc_player.get("slug"),
                    ktc_player.get("position"),
                    ktc_player.get("positionID"),
                    ktc_player.get("team"),
                    ktc_player.get("rookie"),
                    ktc_player.get("college"),
                    ktc_player.get("age"),
                    ktc_player.get("heightFeet"),
                    ktc_player.get("heightInches"),
                    ktc_player.get("weight"),
                    ktc_player.get("seasonsExperience"),
                    ktc_player.get("pickRound"),
                    ktc_player.get("pickNum"), # Note: Typo 'pink_num' in Airflow SQL fixed to 'pick_num'
                    ktc_player.get("oneQBValues", {}).get("value"),
                    ktc_player.get("oneQBValues", {}).get("startSitValue"),
                    ktc_player.get("oneQBValues", {}).get("rank"),
                    ktc_player.get("oneQBValues", {}).get("overallTrend"),
                    ktc_player.get("oneQBValues", {}).get("positionalTrend"),
                    ktc_player.get("oneQBValues", {}).get("positionalRank"),
                    ktc_player.get("oneQBValues", {}).get("rookieRank"),
                    ktc_player.get("oneQBValues", {}).get("rookiePositionalRank"),
                    ktc_player.get("oneQBValues", {}).get("kept"),
                    ktc_player.get("oneQBValues", {}).get("traded"),
                    ktc_player.get("oneQBValues", {}).get("cut"),
                    ktc_player.get("oneQBValues", {}).get("overallTier"),
                    ktc_player.get("oneQBValues", {}).get("positionalTier"),
                    ktc_player.get("oneQBValues", {}).get("rookieTier"),
                    ktc_player.get("oneQBValues", {}).get('rookiePositionalTier'),
                    ktc_player.get("oneQBValues", {}).get('startSitOverallRank'),
                    ktc_player.get("oneQBValues", {}).get('startSitPositionalRank'),
                    ktc_player.get("oneQBValues", {}).get('startSitOverallTier'),
                    ktc_player.get("oneQBValues", {}).get('startSitPositionalTier'),
                    ktc_player.get("oneQBValues", {}).get('startSitOneQBFlexTier'),
                    ktc_player.get("oneQBValues", {}).get('startSitSuperflexFlexTier'),
                    ktc_player.get("superflexValues", {}).get("value"),
                    ktc_player.get("superflexValues", {}).get("startSitValue"),
                    ktc_player.get("superflexValues", {}).get("rank"),
                    ktc_player.get("superflexValues", {}).get("overallTrend"),
                    ktc_player.get("superflexValues", {}).get('positionalTrend'),
                    ktc_player.get("superflexValues", {}).get('positionalRank'),
                    ktc_player.get("superflexValues", {}).get('rookieRank'),
                    ktc_player.get("superflexValues", {}).get("rookiePositionalRank"),
                    ktc_player.get("superflexValues", {}).get("kept"),
                    ktc_player.get("superflexValues", {}).get("traded"),
                    ktc_player.get("superflexValues", {}).get("cut"),
                    ktc_player.get("superflexValues", {}).get("overallTier"),
                    ktc_player.get("superflexValues", {}).get('positionalTier'),
                    ktc_player.get("superflexValues", {}).get("rookieTier"),
                    ktc_player.get("superflexValues", {}).get("rookiePositionalTier"),
                    ktc_player.get("superflexValues", {}).get("startSitOverallRank"),
                    ktc_player.get("superflexValues", {}).get("startSitPositionalRank"),
                    ktc_player.get("superflexValues", {}).get("startSitOverallTier"),
                    ktc_player.get("superflexValues", {}).get("startSitPositionalTier"),
                    entry_time,
                    ktc_player.get("rank_type") # Already added in the scraping asset
                )
            )
        except Exception as e:
            context.log.error(f"Error processing player data: {ktc_player}. Error: {e}")
            # Decide whether to skip the record or fail the asset run
            continue # Skip this record

    if not ktc_players_to_load:
        context.log.warning("No players left to load after processing/error handling.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No valid data after processing"})


    # Corrected column list based on Airflow task (fixed 'pink_num')
    # Ensure this matches your actual table schema!
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            player_first_name, player_last_name, player_full_name, ktc_player_id, slug,
            position, position_id, team, rookie, college, age, height_feet, height_inches,
            weight, season_experience, pick_round, pick_num, one_qb_value, start_sit_value,
            rank, overall_trend, positional_trend, positional_rank, rookie_rank,
            rookie_positional_rank, kept, traded, cut, overall_tier, positional_tier,
            rookie_tier, rookie_positional_tier, start_sit_overall_rank, start_sit_positional_rank,
            start_sit_overall_tier, start_sit_positional_tier, start_sit_oneQB_flex_tier,
            start_sit_superflex_flex_tier, sf_value, sf_start_sit_value, sf_rank,
            sf_overall_trend, sf_positional_trend, sf_positional_rank, sf_rookie_rank,
            sf_rookie_positional_rank, sf_kept, sf_traded, sf_cut, sf_overall_tier,
            sf_positional_tier, sf_rookie_tier, sf_rookie_positional_tier,
            sf_start_sit_overall_rank, sf_start_sit_positional_rank, sf_start_sit_overall_tier,
            sf_start_sit_positional_tier, insert_date, rank_type
        )
        VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (ktc_player_id, rank_type)
        DO UPDATE SET
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            player_full_name = EXCLUDED.player_full_name,
            slug = EXCLUDED.slug,
            position = EXCLUDED.position,
            position_id = EXCLUDED.position_id,
            team = EXCLUDED.team,
            rookie = EXCLUDED.rookie,
            college = EXCLUDED.college,
            age = EXCLUDED.age,
            height_feet = EXCLUDED.height_feet,
            height_inches = EXCLUDED.height_inches,
            weight = EXCLUDED.weight,
            season_experience = EXCLUDED.season_experience,
            pick_round = EXCLUDED.pick_round,
            pick_num = EXCLUDED.pick_num,         -- Fixed typo here
            one_qb_value = EXCLUDED.one_qb_value, -- Moved updates for conflicting keys here
            start_sit_value = EXCLUDED.start_sit_value,
            rank = EXCLUDED.rank,
            overall_trend = EXCLUDED.overall_trend,
            positional_trend = EXCLUDED.positional_trend,
            positional_rank = EXCLUDED.positional_rank,
            rookie_rank = EXCLUDED.rookie_rank,
            rookie_positional_rank = EXCLUDED.rookie_positional_rank,
            kept = EXCLUDED.kept,
            traded = EXCLUDED.traded,
            cut = EXCLUDED.cut,
            overall_tier = EXCLUDED.overall_tier,
            positional_tier = EXCLUDED.positional_tier,
            rookie_tier = EXCLUDED.rookie_tier,
            rookie_positional_tier = EXCLUDED.rookie_positional_tier,
            start_sit_overall_rank = EXCLUDED.start_sit_overall_rank,
            start_sit_positional_rank = EXCLUDED.start_sit_positional_rank,
            start_sit_overall_tier = EXCLUDED.start_sit_overall_tier,
            start_sit_positional_tier = EXCLUDED.start_sit_positional_tier,
            start_sit_oneQB_flex_tier = EXCLUDED.start_sit_oneQB_flex_tier,
            start_sit_superflex_flex_tier = EXCLUDED.start_sit_superflex_flex_tier,
            sf_value = EXCLUDED.sf_value,         -- Moved updates for conflicting keys here
            sf_start_sit_value = EXCLUDED.sf_start_sit_value,
            sf_rank = EXCLUDED.sf_rank,           -- Moved updates for conflicting keys here
            sf_overall_trend = EXCLUDED.sf_overall_trend,
            sf_positional_trend = EXCLUDED.sf_positional_trend,
            sf_positional_rank = EXCLUDED.sf_positional_rank,
            sf_rookie_rank = EXCLUDED.sf_rookie_rank,
            sf_rookie_positional_rank = EXCLUDED.sf_rookie_positional_rank,
            sf_kept = EXCLUDED.sf_kept,
            sf_traded = EXCLUDED.sf_traded,
            sf_cut = EXCLUDED.sf_cut,
            sf_overall_tier = EXCLUDED.sf_overall_tier,
            sf_positional_tier = EXCLUDED.sf_positional_tier,
            sf_rookie_tier = EXCLUDED.sf_rookie_tier,
            sf_rookie_positional_tier = EXCLUDED.sf_rookie_positional_tier,
            sf_start_sit_overall_rank = EXCLUDED.sf_start_sit_overall_rank,
            sf_start_sit_positional_rank = EXCLUDED.sf_start_sit_positional_rank,
            sf_start_sit_overall_tier = EXCLUDED.sf_start_sit_overall_tier,
            sf_start_sit_positional_tier = EXCLUDED.sf_start_sit_positional_tier,
            insert_date = EXCLUDED.insert_date; -- Update insert_date on conflict
    """

    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(ktc_players_to_load)} rows...")
                # Using psycopg2's execute_batch for efficiency
                postgres.execute_batch(cursor, insert_sql, ktc_players_to_load, page_size=1000)
                row_count = cursor.rowcount # Gives affected rows for the last batch execution in execute_batch
                context.log.info(f"Batch execution complete. Affected rows (approximate for UPSERT): {row_count if row_count >= 0 else 'Unknown'}")
            # conn.commit() happens automatically when exiting the `with conn:` block if no exceptions occurred
        context.log.info(f"Successfully inserted/updated {len(ktc_players_to_load)} KTC players into {TARGET_TABLE}.")

        return dg.Output(
            value={"rows_processed": len(ktc_players_to_load)},
            metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(ktc_players_to_load)),
                "batch_size": 1000,
                "operation": "INSERT ON CONFLICT UPDATE"
            }
        )

    except Exception as e:
        context.log.error(f"Database error during batch load: {e}")
        # conn.rollback() happens automatically if an exception occurs within `with conn:`
        raise dg.Failure(f"Failed to load data into {TARGET_TABLE}. Error: {e}")


@dg.asset(
    name="ktc_player_ranks_formatted",
    description=f"Formats the player_first_name column in the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    # Depends on the previous asset to ensure data is loaded first
    deps=[ktc_player_ranks_loaded],
    metadata={"target_table": TARGET_TABLE}
)
def ktc_player_ranks_formatted(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Applies formatting rules to the player_first_name column in the target table.
    Runs after the data loading step.
    """
    postgres = context.resources.postgres
    context.log.info(f"Starting formatting for player_first_name in {TARGET_TABLE}")

    # It's safer and often clearer to apply replacements individually or chain them in SQL
    # The deeply nested REPLACE can be hard to read and maintain.
    # Using multiple UPDATE statements or a single UPDATE with chained REPLACE
    # Note: This could be less performant than the nested version on some DBs,
    # but is more readable. Test performance if it becomes an issue.

    # Define replacements as a list of tuples (find, replace_with)
    replacements = [
        ('.', ''), (' Jr', ''), (' III', ''), (' Jeffery', 'Jeff'), (' Joshua', 'Josh'),
        (' William', 'Will'), (' II', ''), ("''", ''), (' Kenneth', 'Ken'),
        (' Mitchell', 'Mitch'), (' DWayne', 'Dee'), (" De'Von", 'Devon'), # Handle quote carefully
        ("'", '')
    ]

    # Build the chained replace statement dynamically (careful with SQL injection if inputs were external)
    # Start with the column name
    update_expr = "player_first_name"
    for find, replace_with in replacements:
        # Use psycopg2 parameterization for the strings being replaced
        update_expr = f"replace({update_expr}, %s, %s)"

    update_sql = f"UPDATE {TARGET_TABLE} SET player_first_name = {update_expr};"

    # Create the parameters tuple for execute
    params = tuple(item for pair in replacements for item in pair)

    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info("Established connection. Executing formatting update...")
                cursor.execute(update_sql, params)
                updated_rows = cursor.rowcount
                context.log.info(f"Formatting update complete. Rows affected: {updated_rows}")
            # Commit happens automatically
        context.log.info(f"Successfully formatted player_first_name in {TARGET_TABLE}.")

        return dg.Output(
            value = {"rows_affected": updated_rows},
            metadata={
                "table": TARGET_TABLE,
                "rows_affected": dg.MetadataValue.int(updated_rows),
                "column_formatted": "player_first_name"
            }
        )

    except Exception as e:
        context.log.error(f"Database error during formatting: {e}")
        # Rollback happens automatically
        raise dg.Failure(f"Failed to format {TARGET_TABLE}. Error: {e}")
