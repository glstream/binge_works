import dagster as dg
import requests
from datetime import datetime # Make sure datetime is imported
from typing import Optional, List, Dict, Any # For type hinting

# --- Configuration ---
POSTGRES_RESOURCE_KEY = "postgres" # Matching your setup
TARGET_TABLE = "dynastr.dd_player_ranks"
SCHEMA_NAME = "dynastr" # Assuming schema exists
DD_REDRAFT_URL = "https://dynasty-daddy.com/api/v1/values/adp/redraft"
DD_DYNASTY_URL = "https://dynasty-daddy.com/api/v1/values/adp/dynasty"

# Define the expected columns for the INSERT statement - ADD 'insert_date'
# IMPORTANT: Make sure this order matches your table schema if using INSERT without explicit columns,
# but it's best practice to list columns explicitly in the INSERT statement anyway.
DD_TABLE_COLUMNS = [
    "name_id", "rank_type", "trade_value", "sf_trade_value",
    "sf_position_rank", "position_rank", "all_time_high_sf",
    "all_time_low_sf", "all_time_high", "all_time_low",
    "three_month_high_sf", "three_month_high", "three_month_low_sf",
    "three_month_low", "last_month_value", "last_month_value_sf",
    "all_time_best_rank_sf", "all_time_best_rank", "all_time_worst_rank_sf",
    "all_time_worst_rank", "three_month_best_rank_sf", "three_month_best_rank",
    "three_month_worst_rank_sf", "three_month_worst_rank", "last_month_rank",
    "last_month_rank_sf", "sf_overall_rank", "overall_rank",
    "insert_date" 
]

# --- Assets ---

@dg.asset(
    name="dd_raw_redraft_data",
    description="Fetches redraft player data from the Dynasty Daddy API.",
    compute_kind="python",
    metadata={"source": "dynasty-daddy.com"}
)
def dd_raw_redraft_data(context: dg.AssetExecutionContext) -> Optional[List[Dict[str, Any]]]:
    """Fetches redraft data, returns list of dicts or None on failure."""
    context.log.info(f"Fetching Dynasty Daddy redraft data from {DD_REDRAFT_URL}")
    try:
        response = requests.get(DD_REDRAFT_URL, timeout=30)
        if response.status_code == 200:
            data = response.json()
            context.log.info(f"Successfully fetched {len(data)} redraft records.")
            return data
        else:
            context.log.warning(f"API request failed with status {response.status_code}. Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        context.log.error(f"API request failed: {e}")
        # Optionally raise dg.Failure here if this data is critical
        return None
    except Exception as e:
        context.log.error(f"An unexpected error occurred during API fetch: {e}")
        return None

@dg.asset(
    name="dd_player_ranks_redraft_loaded",
    description=f"Loads the fetched Dynasty Daddy redraft data into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    deps=[dg.AssetKey("dd_raw_redraft_data")],
    metadata={"target_table": TARGET_TABLE, "load_type": "redraft"}
)
def dd_player_ranks_redraft_loaded(context: dg.AssetExecutionContext, dd_raw_redraft_data: Optional[List[Dict[str, Any]]]) -> dg.Output:
    """
    Validates and loads Dynasty Daddy redraft data using execute_batch.
    Includes an insert_date timestamp.
    Uses INSERT ON CONFLICT (name_id, rank_type) to add or update player ranks.
    """
    postgres = context.resources.postgres
    rank_type = "redraft"

    if not dd_raw_redraft_data:
        context.log.warning(f"No {rank_type} player data received from API step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": f"No {rank_type} data fetched"})

    context.log.info(f"Received {len(dd_raw_redraft_data)} {rank_type} player records to process for loading.")

    # Generate timestamp ONCE before the loop
    # Using UTC is generally recommended for server times
    insert_timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    # Alternative: datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z") for local time if needed

    # Prepare data for execute_batch (list of tuples)
    data_to_load = []
    for item in dd_raw_redraft_data:
        try:
            # Create tuple in the order of DD_TABLE_COLUMNS (excluding insert_date initially)
            player_tuple_values = tuple(item.get(col) for col in DD_TABLE_COLUMNS[2:-1]) # Get values for cols after name_id/rank_type, before insert_date
            # Prepend name_id/rank_type, append insert_date
            full_tuple = (
                item.get('name_id'),
                rank_type
            ) + player_tuple_values + (
                insert_timestamp_str, # Add the timestamp string
            )

            if len(full_tuple) != len(DD_TABLE_COLUMNS):
                context.log.warning(f"Tuple length mismatch for item {item.get('name_id')}. Expected {len(DD_TABLE_COLUMNS)}, got {len(full_tuple)}. Skipping.")
                continue
            if item.get('name_id') is None: # Skip if primary key part is missing
                context.log.warning(f"Missing 'name_id' in item: {item}. Skipping.")
                continue
            data_to_load.append(full_tuple)
        except Exception as e:
            context.log.error(f"Error processing item {item.get('name_id', 'MISSING_ID')} for batch load: {e}. Skipping item.")

    if not data_to_load:
        context.log.warning(f"No valid {rank_type} records to load after processing. Skipping database operation.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": f"No valid {rank_type} data after processing"})

    # Explicitly list columns in INSERT, including insert_date
    column_names_str = ", ".join(DD_TABLE_COLUMNS)
    # Increment placeholder count
    value_placeholders = ", ".join(["%s"] * len(DD_TABLE_COLUMNS))

    # SQL uses ON CONFLICT (name_id, rank_type) DO UPDATE
    # Add insert_date to the SET clause
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({column_names_str})
        VALUES ({value_placeholders})
        ON CONFLICT (name_id, rank_type) DO UPDATE SET
            trade_value = EXCLUDED.trade_value,
            sf_trade_value = EXCLUDED.sf_trade_value,
            sf_position_rank = EXCLUDED.sf_position_rank,
            position_rank = EXCLUDED.position_rank,
            all_time_high_sf = EXCLUDED.all_time_high_sf,
            all_time_low_sf = EXCLUDED.all_time_low_sf,
            all_time_high = EXCLUDED.all_time_high,
            all_time_low = EXCLUDED.all_time_low,
            three_month_high_sf = EXCLUDED.three_month_high_sf,
            three_month_high = EXCLUDED.three_month_high,
            three_month_low_sf = EXCLUDED.three_month_low_sf,
            three_month_low = EXCLUDED.three_month_low,
            last_month_value = EXCLUDED.last_month_value,
            last_month_value_sf = EXCLUDED.last_month_value_sf,
            all_time_best_rank_sf = EXCLUDED.all_time_best_rank_sf,
            all_time_best_rank = EXCLUDED.all_time_best_rank,
            all_time_worst_rank_sf = EXCLUDED.all_time_worst_rank_sf,
            all_time_worst_rank = EXCLUDED.all_time_worst_rank,
            three_month_best_rank_sf = EXCLUDED.three_month_best_rank_sf,
            three_month_best_rank = EXCLUDED.three_month_best_rank,
            three_month_worst_rank_sf = EXCLUDED.three_month_worst_rank_sf,
            three_month_worst_rank = EXCLUDED.three_month_worst_rank,
            last_month_rank = EXCLUDED.last_month_rank,
            last_month_rank_sf = EXCLUDED.last_month_rank_sf,
            sf_overall_rank = EXCLUDED.sf_overall_rank,
            overall_rank = EXCLUDED.overall_rank,
            insert_date = EXCLUDED.insert_date; -- Added update for insert_date
        """

    try:
        # Use context manager for connection and cursor
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(data_to_load)} {rank_type} rows...")

                postgres.execute_batch(cursor, insert_sql, data_to_load, page_size=500)
                row_count = cursor.rowcount
                context.log.info(f"{rank_type} Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(data_to_load)} {rank_type} players into {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_processed": len(data_to_load)},
            metadata={
                "table": TARGET_TABLE,
                "rows_processed": dg.MetadataValue.int(len(data_to_load)),
                "load_type": rank_type
            }
        )
    # Keep Fallback for AttributeError in case get_connection doesn't exist
    except AttributeError:
        try:
            context.log.warning("Falling back to execute_batch on resource directly.")
            postgres.execute_batch(insert_sql, data_to_load, page_size=500) # Adapt call signature if needed
            context.log.info(f"Successfully inserted/updated {len(data_to_load)} {rank_type} players into {TARGET_TABLE} (via resource.execute_batch).")
            # Note: row_count might not be available here
            return dg.Output(
                value={"rows_processed": len(data_to_load)},
                metadata={ "table": TARGET_TABLE, "rows_processed": dg.MetadataValue.int(len(data_to_load)), "load_type": rank_type }
            )
        except Exception as e_fallback:
            context.log.error(f"Database error during {rank_type} batch load (resource method fallback): {e_fallback}")
            raise dg.Failure(f"Failed to load {rank_type} data into {TARGET_TABLE}. Error: {e_fallback}")
    except Exception as e_main:
        context.log.error(f"Database error during {rank_type} batch load (connection method): {e_main}")
        raise dg.Failure(f"Failed to load {rank_type} data into {TARGET_TABLE}. Error: {e_main}")


@dg.asset(
    name="dd_raw_dynasty_data",
    description="Fetches dynasty player data from the Dynasty Daddy API.",
    compute_kind="python",
    metadata={"source": "dynasty-daddy.com"}
)
def dd_raw_dynasty_data(context: dg.AssetExecutionContext) -> Optional[List[Dict[str, Any]]]:
    """Fetches dynasty data, returns list of dicts or None on failure."""
    context.log.info(f"Fetching Dynasty Daddy dynasty data from {DD_DYNASTY_URL}")
    try:
        response = requests.get(DD_DYNASTY_URL, timeout=30)
        if response.status_code == 200:
            data = response.json()
            context.log.info(f"Successfully fetched {len(data)} dynasty records.")
            return data
        else:
            context.log.warning(f"API request failed with status {response.status_code}. Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        context.log.error(f"API request failed: {e}")
        return None
    except Exception as e:
        context.log.error(f"An unexpected error occurred during API fetch: {e}")
        return None

@dg.asset(
    name="dd_player_ranks_dynasty_loaded",
    description=f"Loads the fetched Dynasty Daddy dynasty data into the {TARGET_TABLE} table.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    metadata={"target_table": TARGET_TABLE, "load_type": "dynasty"},
)
def dd_player_ranks_dynasty_loaded(context: dg.AssetExecutionContext, dd_raw_dynasty_data: Optional[List[Dict[str, Any]]]) -> dg.Output:
    """
    Validates and loads Dynasty Daddy dynasty data using execute_batch.
    Includes an insert_date timestamp.
    Uses INSERT ON CONFLICT (name_id, rank_type) to add or update player ranks.
    """
    postgres = context.resources.postgres
    rank_type = "dynasty" # Set rank_type for this load

    if not dd_raw_dynasty_data:
        context.log.warning(f"No {rank_type} player data received from API step. Skipping database load.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": f"No {rank_type} data fetched"})

    context.log.info(f"Received {len(dd_raw_dynasty_data)} {rank_type} player records to process for loading.")

    # Generate timestamp ONCE before the loop
    insert_timestamp_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    # Prepare data for execute_batch (list of tuples) - same logic as redraft load asset
    data_to_load = []
    for item in dd_raw_dynasty_data:
        try:
            # Create tuple in the order of DD_TABLE_COLUMNS (excluding insert_date initially)
            player_tuple_values = tuple(item.get(col) for col in DD_TABLE_COLUMNS[2:-1])
            # Prepend name_id/rank_type, append insert_date
            full_tuple = (
                item.get('name_id'),
                rank_type
            ) + player_tuple_values + (
                insert_timestamp_str, # Add the timestamp string
            )

            if len(full_tuple) != len(DD_TABLE_COLUMNS):
                context.log.warning(f"Tuple length mismatch for item {item.get('name_id')}. Expected {len(DD_TABLE_COLUMNS)}, got {len(full_tuple)}. Skipping.")
                continue
            if item.get('name_id') is None:
                context.log.warning(f"Missing 'name_id' in item: {item}. Skipping.")
                continue
            data_to_load.append(full_tuple)
        except Exception as e:
            context.log.error(f"Error processing item {item.get('name_id', 'MISSING_ID')} for batch load: {e}. Skipping item.")

    if not data_to_load:
        context.log.warning(f"No valid {rank_type} records to load after processing. Skipping database operation.")
        return dg.Output(value={"rows_processed": 0}, metadata={"skipped": True, "reason": f"No valid {rank_type} data after processing"})

    # Re-use the same SQL template logic as the redraft load asset
    column_names_str = ", ".join(DD_TABLE_COLUMNS)
    value_placeholders = ", ".join(["%s"] * len(DD_TABLE_COLUMNS))
    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({column_names_str})
        VALUES ({value_placeholders})
        ON CONFLICT (name_id, rank_type) DO UPDATE SET
            trade_value = EXCLUDED.trade_value,
            sf_trade_value = EXCLUDED.sf_trade_value,
            sf_position_rank = EXCLUDED.sf_position_rank,
            position_rank = EXCLUDED.position_rank,
            all_time_high_sf = EXCLUDED.all_time_high_sf,
            all_time_low_sf = EXCLUDED.all_time_low_sf,
            all_time_high = EXCLUDED.all_time_high,
            all_time_low = EXCLUDED.all_time_low,
            three_month_high_sf = EXCLUDED.three_month_high_sf,
            three_month_high = EXCLUDED.three_month_high,
            three_month_low_sf = EXCLUDED.three_month_low_sf,
            three_month_low = EXCLUDED.three_month_low,
            last_month_value = EXCLUDED.last_month_value,
            last_month_value_sf = EXCLUDED.last_month_value_sf,
            all_time_best_rank_sf = EXCLUDED.all_time_best_rank_sf,
            all_time_best_rank = EXCLUDED.all_time_best_rank,
            all_time_worst_rank_sf = EXCLUDED.all_time_worst_rank_sf,
            all_time_worst_rank = EXCLUDED.all_time_worst_rank,
            three_month_best_rank_sf = EXCLUDED.three_month_best_rank_sf,
            three_month_best_rank = EXCLUDED.three_month_best_rank,
            three_month_worst_rank_sf = EXCLUDED.three_month_worst_rank_sf,
            three_month_worst_rank = EXCLUDED.three_month_worst_rank,
            last_month_rank = EXCLUDED.last_month_rank,
            last_month_rank_sf = EXCLUDED.last_month_rank_sf,
            sf_overall_rank = EXCLUDED.sf_overall_rank,
            overall_rank = EXCLUDED.overall_rank,
            insert_date = EXCLUDED.insert_date; -- Added update for insert_date
    """

    try:
        # Use context manager for connection and cursor
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Established connection. Executing batch insert/update for {len(data_to_load)} {rank_type} rows...")
                postgres.execute_batch(cursor, insert_sql, data_to_load, page_size=500)
                row_count = cursor.rowcount
                context.log.info(f"{rank_type} Batch execution complete. Affected rows: {row_count if row_count >= 0 else 'Unknown'}")
        # Commit handled by context manager

        context.log.info(f"Successfully inserted/updated {len(data_to_load)} {rank_type} players into {TARGET_TABLE}.")
        return dg.Output(
            value={"rows_processed": len(data_to_load)},
            metadata={ "table": TARGET_TABLE, "rows_processed": dg.MetadataValue.int(len(data_to_load)), "load_type": rank_type }
        )
    # Keep Fallback for AttributeError
    except AttributeError:
        try:
            context.log.warning("Falling back to execute_batch on resource directly.")
            postgres.execute_batch(insert_sql, data_to_load, page_size=500) # Adapt call signature if needed
            context.log.info(f"Successfully inserted/updated {len(data_to_load)} {rank_type} players into {TARGET_TABLE} (via resource.execute_batch).")
            return dg.Output(
                value={"rows_processed": len(data_to_load)},
                metadata={ "table": TARGET_TABLE, "rows_processed": dg.MetadataValue.int(len(data_to_load)), "load_type": rank_type }
            )
        except Exception as e_fallback:
            context.log.error(f"Database error during {rank_type} batch load (resource method fallback): {e_fallback}")
            raise dg.Failure(f"Failed to load {rank_type} data into {TARGET_TABLE}. Error: {e_fallback}")
    except Exception as e_main:
        context.log.error(f"Database error during {rank_type} batch load (connection method): {e_main}")
        raise dg.Failure(f"Failed to load {rank_type} data into {TARGET_TABLE}. Error: {e_main}")


@dg.asset(
    name="dd_player_ranks_formatted",
    description=f"Formats the name_id column in the {TARGET_TABLE} table by deleting duplicates and renaming.",
    compute_kind="postgres",
    required_resource_keys={POSTGRES_RESOURCE_KEY},
    deps=[dd_player_ranks_dynasty_loaded, dd_player_ranks_redraft_loaded],
    metadata={"target_table": TARGET_TABLE}
)
def dd_player_ranks_formatted(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Applies specific formatting rules to the name_id column in the DD ranks table.
    For each rule (find_val, replace_val):
    1. Deletes rows with name_id = find_val if a row with name_id = replace_val 
       and the same rank_type already exists.
    2. Updates remaining rows with name_id = find_val to name_id = replace_val.
    Runs after all data loading steps for dd_player_ranks are complete.
    """
    postgres = context.resources.postgres
    context.log.info(f"Starting formatting for name_id in {TARGET_TABLE} using delete-then-update strategy.")
    
    replacements = [
        ('camwardqb', 'cameronwardqb'), 
        ('amonrastbrownwr', 'amon-rabrownwr'), 
        ('jaxonsmithnjigbawr', 'jaxonsmith-njigbawr')
    ]
    
    total_rows_deleted = 0
    total_rows_updated = 0
    
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                for find_val, replace_val in replacements:
                    context.log.info(f"Processing replacement rule: '{find_val}' -> '{replace_val}'")

                    # Step 1: Delete "old" name_id if "new" name_id already exists for the same rank_type
                    delete_sql = f"""
                        DELETE FROM {TARGET_TABLE}
                        WHERE name_id = %s
                          AND EXISTS (SELECT 1 FROM {TARGET_TABLE} AS sub
                                      WHERE sub.name_id = %s AND sub.rank_type = {TARGET_TABLE}.rank_type);
                    """
                    cursor.execute(delete_sql, (find_val, replace_val))
                    deleted_count = cursor.rowcount if cursor.rowcount >= 0 else 0
                    if deleted_count > 0:
                        context.log.info(f"  Deleted {deleted_count} rows where name_id='{find_val}' because '{replace_val}' already existed for the same rank_type.")
                        total_rows_deleted += deleted_count

                    # Step 2: Update any remaining "old" name_id to "new" name_id
                    # This affects rows where find_val exists but replace_val did not (for that rank_type),
                    # or where find_val exists for rank_types not covered by an existing replace_val.
                    update_sql = f"""
                        UPDATE {TARGET_TABLE}
                        SET name_id = %s
                        WHERE name_id = %s;
                    """
                    # This update is generally safe due to the previous delete.
                    # If (replace_val, rank_type) already existed, (find_val, rank_type) would have been deleted.
                    # If (find_val, rank_type) still exists, it means (replace_val, rank_type) did not,
                    # so the update to replace_val will not violate PK constraints.
                    cursor.execute(update_sql, (replace_val, find_val))
                    updated_count = cursor.rowcount if cursor.rowcount >= 0 else 0
                    if updated_count > 0:
                        context.log.info(f"  Updated {updated_count} rows from name_id='{find_val}' to '{replace_val}'.")
                        total_rows_updated += updated_count
                
                # The connection context manager should handle commit on successful exit.
                # If explicit commit is needed: conn.commit()

        context.log.info(f"Formatting complete. Total rows deleted: {total_rows_deleted}, Total rows updated: {total_rows_updated}.")
    
        return dg.Output(
            value={"deleted_rows": total_rows_deleted, "updated_rows": total_rows_updated}, 
            metadata={
                "deleted_rows": dg.MetadataValue.int(total_rows_deleted),
                "updated_rows": dg.MetadataValue.int(total_rows_updated),
                "total_affected_rows": dg.MetadataValue.int(total_rows_deleted + total_rows_updated)
            }
        )
    except Exception as e:
        context.log.error(f"Database error during formatting: {e}")
        # Ensure to rollback or rely on connection context manager to handle rollback on error
        raise dg.Failure(f"Failed to format {TARGET_TABLE}. Error: {e}")
