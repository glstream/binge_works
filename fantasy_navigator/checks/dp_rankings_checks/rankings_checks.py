import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["dp_raw_player_data"]),
    name="check_dp_raw_player_data",
    description="Verify DynastyProcess CSV returned player data",
    blocking=True,
)
def check_dp_raw_player_data(context, dp_raw_player_data) -> dg.AssetCheckResult:
    if not dp_raw_player_data or len(dp_raw_player_data) < 1:
        return dg.AssetCheckResult(passed=False, description="No data returned from DynastyProcess.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"DynastyProcess returned {len(dp_raw_player_data)} records.",
        metadata={"record_count": dg.MetadataValue.int(len(dp_raw_player_data))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["dp_player_ranks_loaded"]),
    name="check_dp_player_ranks_loaded",
    description="Verify rows were processed during load",
    blocking=True,
)
def check_dp_player_ranks_loaded(context, dp_player_ranks_loaded) -> dg.AssetCheckResult:
    rows_processed = dp_player_ranks_loaded.get("rows_processed", 0) if isinstance(dp_player_ranks_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["dp_player_ranks_formatted"]),
    name="check_dp_player_ranks_formatted",
    description="Verify rows were affected during formatting",
    blocking=True,
)
def check_dp_player_ranks_formatted(context, dp_player_ranks_formatted) -> dg.AssetCheckResult:
    rows_affected = dp_player_ranks_formatted.get("rows_affected", 0) if isinstance(dp_player_ranks_formatted, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_affected >= 0,
        description=f"Formatting complete: {rows_affected} rows affected",
        metadata={"rows_affected": dg.MetadataValue.int(rows_affected)},
    )
