import dagster as dg
import requests
from bs4 import BeautifulSoup
import re
# from psycopg2.extras import execute_batch # Import if needed by resource/fallback
from datetime import datetime
import os
from typing import List, Tuple, Optional, Any # For type hinting
import time # For potential sleep between requests

# --- Configuration ---
POSTGRES_RESOURCE_KEY = "postgres" # Matching your setup
TARGET_TABLE = "dynastr.nfl_player_projections"
SCHEMA_NAME = "dynastr" # Assuming schema exists

# Determine Season Year Dynamically - Use context time if available, otherwise current time
# Context date is May 4, 2025, so use 2025
DEFAULT_SEASON_YEAR = "2025"
NFL_URL_TEMPLATE = "https://fantasy.nfl.com/research/projections?offset={offset}&position=O&sort=projectedPts&statCategory=projectedStats&statSeason={year}&statType=seasonProjectedStats"
# Pagination range from Airflow task
NFL_PAGINATION_START = 1
NFL_PAGINATION_END = 846 # Exclusive end for range, so use 846 to get up to 845
NFL_PAGINATION_STEP = 25

# --- Assets ---

@dg.asset(
    name="nfl_raw_projections_data",
    description=f"Scrapes paginated {DEFAULT_SEASON_YEAR} player projections from NFL.com.",
    compute_kind="python",
    group_name="nfl_projections", # Added group_name
    metadata={"source": "fantasy.nfl.com"}
)
def nfl_raw_projections_data(context: dg.AssetExecutionContext) -> List[Tuple[str, str, str, str, str, Optional[int], str]]:
    """
    Scrapes paginated NFL.com fantasy projections, parses HTML, extracts data,
    cleans names, converts projection to int, adds timestamp, and structures the output.
    Returns list of tuples: (first_name, last_name, full_name, nfl_id, slug, projection, timestamp).
    """
    effective_season_year = DEFAULT_SEASON_YEAR

    nfl_projections_players_raw = [] # Temp list to hold [name, id, href, projection_str]
    # Generate timestamp ONCE for all records scraped in this run
    entry_timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    context.log.info(f"Starting NFL scrape for season {effective_season_year}, pages {NFL_PAGINATION_START} to {NFL_PAGINATION_END-1}...")

    for i in range(NFL_PAGINATION_START, NFL_PAGINATION_END, NFL_PAGINATION_STEP):
        offset = str(i)
        scrape_url = NFL_URL_TEMPLATE.format(offset=offset, year=effective_season_year)
        context.log.info(f"Scraping URL: {scrape_url}")

        try:
            res = requests.get(scrape_url, timeout=30)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            # Find projection values and names separately based on Airflow task
            projections_tds = soup.find_all("td", class_="stat projected numeric sorted last")
            names_tds = soup.find_all("td", class_="playerNameAndInfo first")

            page_player_count = len(names_tds)
            context.log.info(f"Found {page_player_count} player names and {len(projections_tds)} projection cells on page with offset {offset}.")

            if page_player_count != len(projections_tds):
                context.log.warning(f"Mismatch between player count ({page_player_count}) and projection count ({len(projections_tds)}) for offset {offset}. Skipping page.")
                continue # Skip this page if counts don't align

            if page_player_count == 0:
                context.log.info(f"No players found for offset {offset}. Stopping pagination.")
                break # Stop if a page has no players (likely end of results)

            for idx in range(page_player_count):
                name_td = names_tds[idx]
                projection_td = projections_tds[idx]

                # --- Extract Name, ID, Href ---
                player_name = None
                nfl_player_id = None
                slug_href = None
                try:
                    link_tag = name_td.find("a")
                    if link_tag and link_tag.has_attr('href'):
                        # Initial cleaning based on Airflow task
                        player_name_raw = link_tag.get_text()
                        player_name = player_name_raw.replace("'", "").replace('"', "").replace(" III", "").replace(" II", "").replace("Gabe", "Gabriel").replace(" Jr.", "").strip()
                        slug_href = link_tag["href"]
                        # Extract ID using regex (safer than split)
                        match = re.search(r'playerId=(\d+)', slug_href)
                        if match:
                            nfl_player_id = match.group(1)
                        else:
                            context.log.warning(f"Could not extract playerId from href: {slug_href}")
                    else:
                        context.log.warning(f"No valid player link found in name cell for index {idx}, offset {offset}.")
                        continue # Skip if essential info missing
                except Exception as e:
                    context.log.warning(f"Error parsing name/ID/href for index {idx}, offset {offset}: {e}")
                    continue # Skip this player on error

                if not player_name or not nfl_player_id or not slug_href:
                    context.log.warning(f"Skipping player at index {idx}, offset {offset} due to missing name/id/href after parsing.")
                    continue

                # --- Extract Projection (IMPROVED) ---
                projection_val_str = None
                projection_val_int = None
                try:
                    projection_val_str = projection_td.get_text(strip=True)
                    if projection_val_str:
                        # Convert to float first for decimals, then to int
                        projection_val_int = int(float(projection_val_str))
                    else:
                        projection_val_int = 0 # Default if empty
                        context.log.debug(f"Empty projection string for {player_name}. Defaulting to 0.")
                except (ValueError, TypeError) as e:
                    context.log.warning(f"Could not convert projection '{projection_val_str}' to int for {player_name} (ID: {nfl_player_id}): {e}. Using 0.")
                    projection_val_int = 0 # Default on conversion error
                except Exception as e:
                    context.log.warning(f"Unexpected error parsing projection for {player_name} (ID: {nfl_player_id}): {e}. Using 0.")
                    projection_val_int = 0


                nfl_projections_players_raw.append(
                    [player_name, nfl_player_id, slug_href, projection_val_int]
                )

            # Optional: Add a small delay between page requests to be polite
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            context.log.error(f"Failed to fetch data for offset {offset}: {e}. Stopping pagination.")
            break # Stop pagination if a request fails
        except Exception as e:
            context.log.error(f"An unexpected error occurred processing page with offset {offset}: {e}. Stopping pagination.")
            break # Stop pagination on unexpected errors

    context.log.info(f"Scraping finished. Extracted {len(nfl_projections_players_raw)} raw records.")

    # --- Final Structuring (from Airflow task) ---
    nfl_players_prepped = []
    skipped_structuring = 0
    for i in nfl_projections_players_raw:
        try:
            # i[0]=fullname, i[1]=nfl_player_id, i[2]=slug, i[3]=projection_int
            full_name = i[0]
            first_name = full_name.split(" ")[0]
            last_name = full_name.split(" ")[-1] if " " in full_name else first_name
            nfl_id = i[1] # Already extracted
            slug = i[2] # Already extracted
            projection = i[3] # Already converted to int (or defaulted to 0)

            # Check if projection is None (shouldn't happen with defaulting logic, but as safety)
            if projection is None:
                context.log.warning(f"Skipping record during structuring due to None projection: {full_name}")
                skipped_structuring += 1
                continue

            nfl_players_prepped.append(
                ( # Use tuple for consistency
                    first_name,
                    last_name,
                    full_name,
                    nfl_id,
                    slug,
                    projection, # Already int
                    entry_timestamp_str
                )
            )
        except Exception as e:
            context.log.error(f"Error during final structuring for raw record {i}: {e}")
            skipped_structuring += 1

    context.log.info(f"Successfully structured {len(nfl_players_prepped)} records. Skipped {skipped_structuring} during structuring.")
    return nfl_players_prepped


@dg.asset(
    name="nfl_player_projections_loaded",
    description=f"Loads the scraped NFL player projections into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    group_name="nfl_projections",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    metadata={"target_table": TARGET_TABLE}
)
def nfl_player_projections_loaded(context: dg.AssetExecutionContext, nfl_raw_projections_data: List[Tuple[str, str, str, str, str, Optional[int], str]]) -> dg.Output:
    """
    Validates and loads NFL projections data using execute_batch.
    Uses INSERT ON CONFLICT (nfl_player_id) to add or update projections.
    """
    postgres = context.resources.postgres

    # Validation (from data_validation task)
    if not nfl_raw_projections_data:
        context.log.warning("No NFL projection data received from scraping step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No data scraped/structured"})

    # Filter out any records where projection might still be None (safety check)
    data_to_load = [row for row in nfl_raw_projections_data if row[5] is not None]
    skipped_count = len(nfl_raw_projections_data) - len(data_to_load)
    if skipped_count > 0:
        context.log.warning(f"Filtered out {skipped_count} records with None projection before loading.")

    if not data_to_load:
        context.log.warning("No valid NFL records remain after filtering. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": "No valid data after filtering"})

    context.log.info(f"Attempting to load {len(data_to_load)} valid NFL projection records.")

    # SQL matches nfl_player_load Airflow task
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} (
            player_first_name,
            player_last_name,
            player_full_name,
            nfl_player_id,
            slug,
            total_projection,
            insert_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (nfl_player_id) -- Conflict target from Airflow task
        DO UPDATE SET
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            player_full_name = EXCLUDED.player_full_name,
            slug = EXCLUDED.slug,
            total_projection = EXCLUDED.total_projection,
            insert_date = EXCLUDED.insert_date;
    """

    try:
        # Adapt based on your actual PostgresResource implementation
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(data_to_load)} NFL rows...")
                from psycopg2.extras import execute_batch # Ensure import
                execute_batch(cursor, insert_sql, data_to_load, page_size=1000)
                row_count = cursor.rowcount
                context.log.info(f"NFL Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(data_to_load)} NFL projections into {TARGET_TABLE}.")
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
            context.log.info(f"Successfully inserted/updated {len(data_to_load)} NFL projections into {TARGET_TABLE} (via resource.execute_batch).")
            return dg.Output(
                value={"rows_processed": len(data_to_load)},
                metadata={
                    "table": TARGET_TABLE,
                    "rows_processed": dg.MetadataValue.int(len(data_to_load)),
                    "rows_skipped_validation": dg.MetadataValue.int(skipped_count)
                }
            )
        except Exception as e_fallback:
            context.log.error(f"Database error during NFL batch load (resource method fallback): {e_fallback}")
            raise dg.Failure(f"Failed to load NFL data into {TARGET_TABLE}. Error: {e_fallback}")
    except Exception as e_main:
        context.log.error(f"Database error during NFL batch load (connection method): {e_main}")
        raise dg.Failure(f"Failed to load NFL data into {TARGET_TABLE}. Error: {e_main}")


@dg.asset(
    name="nfl_player_projections_formatted",
    description=f"Formats the player_first_name column in the {TARGET_TABLE} table.",
    compute_kind="postgres",
    group_name="nfl_projections",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    # Depends on the load step finishing
    deps=[nfl_player_projections_loaded],
    metadata={"target_table": TARGET_TABLE}
)
def nfl_player_projections_formatted(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Applies generic formatting rules to the player_first_name column in the NFL projections table.
    Replicates the logic from previous surrogate_key_formatting tasks.
    """
    postgres = context.resources.postgres
    context.log.info(f"Starting formatting for player_first_name in {TARGET_TABLE}")

    # Replicating standard formatting logic from Airflow task
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
