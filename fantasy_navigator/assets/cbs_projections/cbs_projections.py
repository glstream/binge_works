import dagster as dg
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from typing import List, Tuple, Optional # For type hinting

# --- Configuration ---
POSTGRES_RESOURCE_KEY = "postgres" # Matching your setup
TARGET_TABLE = "dynastr.cbs_player_projections"
SCHEMA_NAME = "dynastr" # Assuming schema exists
CBS_POSITIONS = ['QB', 'RB', 'WR', 'TE']
CBS_SCORING = 'ppr'

current_year = datetime.now().year # Fallback if context time isn't available
PROJECTION_YEAR = current_year # Adjust logic if needed (e.g., based on month)
CBS_BASE_URL_TEMPLATE = "https://www.cbssports.com/fantasy/football/stats/{position}/{year}/season/projections/{scoring}/"


# --- Assets ---

@dg.asset(
    name="cbs_raw_projections_data",
    description=f"Scrapes {PROJECTION_YEAR} player projections from CBS Sports for {CBS_SCORING} scoring.",
    compute_kind="python", 
    metadata={"source": "cbssports.com"}
)
def cbs_raw_projections_data(context: dg.AssetExecutionContext) -> List[Tuple[str, str, str, Optional[float], str]]:
    """
    Scrapes CBS Sports website for player projections across multiple positions.
    Performs initial name cleaning and structuring.
    Returns a list of tuples: (first_name, last_name, full_name, projection, timestamp).
    """
    # Try to get year from context if scheduled, otherwise use calculated default
    effective_projection_year = PROJECTION_YEAR

    cbs_projections_players = []
    # Generate timestamp ONCE for all records scraped in this run
    entry_timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    context.log.info(f"Starting CBS scrape for positions: {CBS_POSITIONS}, year: {effective_projection_year}, scoring: {CBS_SCORING}")

    for position in CBS_POSITIONS:
        projection_url = CBS_BASE_URL_TEMPLATE.format(
            position=position,
            year=effective_projection_year,
            scoring=CBS_SCORING
        )
        context.log.info(f"Scraping URL: {projection_url}")

        try:
            res = requests.get(projection_url, timeout=30)
            res.raise_for_status() # Check for HTTP errors
            soup = BeautifulSoup(res.text, "html.parser")

            # Find projection rows - adjust selector if CBS changes structure
            # Using a more specific selector might be safer if possible
            projections_rows = soup.find_all("tr", class_="TableBase-bodyTr")
            context.log.info(f"Found {len(projections_rows)} potential player rows for {position}.")

            if not projections_rows:
                context.log.warning(f"No projection rows found for {position} at {projection_url}. Structure might have changed.")
                continue

            for k, row in enumerate(projections_rows):
                player_cells = row.find_all('td')
                if not player_cells or len(player_cells) < 3: # Need at least name and projection columns
                    context.log.warning(f"Skipping row {k+1} for {position}: Not enough cells found.")
                    continue

                # Extract Player Name (handle potential errors)
                try:
                    player_link_tag = player_cells[0].find('a', href=True) # Find the link specifically
                    if not player_link_tag:
                        context.log.warning(f"Skipping row {k+1} for {position}: Player link not found in first cell.")
                        continue
                    # Initial cleaning based on Airflow task
                    player_full_name_raw = player_link_tag.text
                    player_full_name = player_full_name_raw.replace("'", "").replace('"', "").replace(" III", "").replace(" II", "").replace("Gabe", "Gabriel").replace(" Jr.", "").strip()
                    if not player_full_name:
                        context.log.warning(f"Skipping row {k+1} for {position}: Empty player name after cleaning '{player_full_name_raw}'.")
                        continue
                    player_first_name = player_full_name.split()[0]
                    player_last_name = player_full_name.split()[-1] if ' ' in player_full_name else player_first_name
                except Exception as e:
                    context.log.warning(f"Skipping row {k+1} for {position}: Error parsing player name. Details: {e}")
                    continue

                # Extract Projection (handle potential errors)
                try:
                    # Airflow used player[-2], assuming projection is second to last column
                    # Verify this index is correct based on actual CBS table structure
                    projection_text = player_cells[-2].text.strip()
                    total_projection = float(projection_text) if projection_text else None
                except (IndexError, ValueError, TypeError) as e:
                    context.log.warning(f"Skipping row {k+1} for {position} ({player_full_name}): Error parsing projection '{player_cells[-2].text if len(player_cells)>1 else 'N/A'}'. Details: {e}")
                    total_projection = None # Set to None if parsing fails

                # Append structured data
                cbs_projections_players.append(
                    (player_first_name, player_last_name, player_full_name, total_projection, entry_timestamp_str)
                )

        except requests.exceptions.RequestException as e:
            context.log.error(f"Failed to fetch data for {position}: {e}")
            # Continue to next position even if one fails
        except Exception as e:
            context.log.error(f"An unexpected error occurred while processing {position}: {e}")
            # Continue to next position

    context.log.info(f"Scraping finished. Total records extracted: {len(cbs_projections_players)}")
    return cbs_projections_players


@dg.asset(
    name="cbs_player_projections_loaded",
    description=f"Loads the scraped CBS player projections into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    metadata={"target_table": TARGET_TABLE}
)
def cbs_player_projections_loaded(context: dg.AssetExecutionContext, cbs_raw_projections_data: List[Tuple[str, str, str, Optional[float], str]]) -> dg.Output:
    """
    Validates and loads CBS projections data using execute_batch.
    Uses INSERT ON CONFLICT (player_full_name) to add or update projections.
    """
    postgres = context.resources.postgres

    # Validation (from data_validation task)
    if not cbs_raw_projections_data:
        context.log.warning("No CBS projection data received from scraping step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No data scraped"})

    # Filter out records where essential data might be missing (e.g., name or projection after parsing)
    data_to_load = [row for row in cbs_raw_projections_data if row[2] and row[3] is not None] # Ensure full_name and projection exist
    skipped_count = len(cbs_raw_projections_data) - len(data_to_load)
    if skipped_count > 0:
        context.log.warning(f"Skipped {skipped_count} records due to missing name or projection.")

    if not data_to_load:
        context.log.warning("No valid records remain after filtering. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No valid data after filtering"})

    context.log.info(f"Attempting to load {len(data_to_load)} valid CBS projection records.")

    # SQL matches cbs_player_load Airflow task
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            player_first_name,
            player_last_name,
            player_full_name,
            total_projection,
            insert_date
        )
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (player_full_name) -- Conflict target from Airflow task
        DO UPDATE SET
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            -- player_full_name = EXCLUDED.player_full_name, -- No need to update the conflict key itself
            total_projection = EXCLUDED.total_projection,
            insert_date = EXCLUDED.insert_date;
    """

    try:
        # Adapt based on your actual PostgresResource implementation
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(data_to_load)} CBS rows...")
                from psycopg2.extras import execute_batch # Ensure import
                execute_batch(cursor, insert_sql, data_to_load, page_size=1000)
                row_count = cursor.rowcount
                context.log.info(f"CBS Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(data_to_load)} CBS projections into {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_processed": len(data_to_load)},
            metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(data_to_load)),
                "rows_skipped_validation": dg.MetadataValue.int(skipped_count)
            }
        )
    except AttributeError:
        # Fallback for resource.execute_batch
        try:
            context.log.warning("Falling back to execute_batch on resource directly.")
            postgres.execute_batch(insert_sql, data_to_load, page_size=1000)
            context.log.info(f"Successfully inserted/updated {len(data_to_load)} CBS projections into {TARGET_TABLE} (via resource.execute_batch).")
            return dg.Output(
                value={"rows_processed": len(data_to_load)},
                metadata={
                    "table": TARGET_TABLE,
                    "rows_processed": dg.MetadataValue.int(len(data_to_load)),
                    "rows_skipped_validation": dg.MetadataValue.int(skipped_count)
                }
            )
        except Exception as e_fallback:
            context.log.error(f"Database error during CBS batch load (resource method fallback): {e_fallback}")
            raise dg.Failure(f"Failed to load CBS data into {TARGET_TABLE}. Error: {e_fallback}")
    except Exception as e_main:
        context.log.error(f"Database error during CBS batch load (connection method): {e_main}")
        raise dg.Failure(f"Failed to load CBS data into {TARGET_TABLE}. Error: {e_main}")


@dg.asset(
    name="cbs_player_projections_formatted",
    description=f"Formats the player_first_name column in the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    # Depends on the load step finishing
    deps=[cbs_player_projections_loaded],
    metadata={"target_table": TARGET_TABLE}
)
def cbs_player_projections_formatted(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Applies generic formatting rules to the player_first_name column in the CBS projections table.
    """
    postgres = context.resources.postgres
    context.log.info(f"Starting formatting for player_first_name in {TARGET_TABLE}")

    # SQL formatting logic from surrogate_key_formatting Airflow task
    replacements = [
        ('.', ''), (' Jr', ''), (' III', ''), ('Jeffery', 'Jeff'), ('Joshua', 'Josh'),
        ('William', 'Will'), (' II', ''), ("''", ''), ('Kenneth', 'Ken'),
        ('Mitchell', 'Mitch'), ('DWayne', 'Dee')
        # Note: 'Gabe' -> 'Gabriel' was handled during scraping in this DAG
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
