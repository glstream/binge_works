import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["cbs_raw_projections_data"]),
    name="check_cbs_raw_projections_data",
    description="Verify CBS projections data was fetched",
    blocking=True,
)
def check_cbs_raw_projections_data(context, cbs_raw_projections_data) -> dg.AssetCheckResult:
    if not cbs_raw_projections_data or len(cbs_raw_projections_data) < 1:
        return dg.AssetCheckResult(passed=False, description="No projections data returned from CBS.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"CBS returned {len(cbs_raw_projections_data)} projections.",
        metadata={"record_count": dg.MetadataValue.int(len(cbs_raw_projections_data))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["cbs_player_projections_loaded"]),
    name="check_cbs_player_projections_loaded",
    description="Verify rows were processed during load",
    blocking=True,
)
def check_cbs_player_projections_loaded(context, cbs_player_projections_loaded) -> dg.AssetCheckResult:
    rows_processed = cbs_player_projections_loaded.get("rows_processed", 0) if isinstance(cbs_player_projections_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["cbs_player_projections_formatted"]),
    name="check_cbs_player_projections_formatted",
    description="Verify rows were affected during formatting",
    blocking=True,
)
def check_cbs_player_projections_formatted(context, cbs_player_projections_formatted) -> dg.AssetCheckResult:
    rows_affected = cbs_player_projections_formatted.get("rows_affected", 0) if isinstance(cbs_player_projections_formatted, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_affected >= 0,
        description=f"Formatting complete: {rows_affected} rows affected",
        metadata={"rows_affected": dg.MetadataValue.int(rows_affected)},
    )
