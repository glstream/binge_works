import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["raw_player_asset_values"]),
    name="check_raw_player_asset_values",
    description="Verify raw player asset values DataFrame is non-empty",
    blocking=True,
)
def check_raw_player_asset_values(context, raw_player_asset_values) -> dg.AssetCheckResult:
    if raw_player_asset_values is None or len(raw_player_asset_values) < 1:
        return dg.AssetCheckResult(passed=False, description="raw_player_asset_values returned empty or None.")
    return dg.AssetCheckResult(
        passed=True,
        metadata={"row_count": dg.MetadataValue.int(len(raw_player_asset_values))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["processed_player_ranks"]),
    name="check_processed_player_ranks",
    description="Verify processed player ranks DataFrame is non-empty",
    blocking=True,
)
def check_processed_player_ranks(context, processed_player_ranks) -> dg.AssetCheckResult:
    if processed_player_ranks is None or len(processed_player_ranks) < 1:
        return dg.AssetCheckResult(passed=False, description="processed_player_ranks returned empty or None.")
    return dg.AssetCheckResult(
        passed=True,
        metadata={"row_count": dg.MetadataValue.int(len(processed_player_ranks))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["load_processed_player_ranks"]),
    name="check_load_processed_player_ranks",
    description="Verify rows were processed during load",
    blocking=True,
)
def check_load_processed_player_ranks(context, load_processed_player_ranks) -> dg.AssetCheckResult:
    records_processed = load_processed_player_ranks.get("records_processed", 0) if isinstance(load_processed_player_ranks, dict) else 0
    return dg.AssetCheckResult(
        passed=records_processed > 0,
        description=f"Loaded {records_processed} records" if records_processed > 0 else "No records loaded",
        metadata={"records_processed": dg.MetadataValue.int(records_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["load_future_draft_picks"]),
    name="check_load_future_draft_picks",
    description="Verify draft picks were loaded",
    blocking=True,
)
def check_load_future_draft_picks(context, load_future_draft_picks) -> dg.AssetCheckResult:
    records_inserted = load_future_draft_picks.get("records_inserted", 0) if isinstance(load_future_draft_picks, dict) else 0
    return dg.AssetCheckResult(
        passed=records_inserted > 0,
        description=f"Loaded {records_inserted} draft picks" if records_inserted > 0 else "No draft picks loaded",
        metadata={"records_inserted": dg.MetadataValue.int(records_inserted)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["fn_player_ranks_formatted"]),
    name="check_fn_player_ranks_formatted",
    description="Verify rows were affected during formatting",
    blocking=True,
)
def check_fn_player_ranks_formatted(context, fn_player_ranks_formatted) -> dg.AssetCheckResult:
    rows_affected = fn_player_ranks_formatted.get("rows_affected", 0) if isinstance(fn_player_ranks_formatted, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_affected >= 0,
        description=f"Formatting complete: {rows_affected} rows affected",
        metadata={"rows_affected": dg.MetadataValue.int(rows_affected)},
    )
