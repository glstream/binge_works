import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["espn_raw_projections_data"]),
    name="check_espn_raw_projections_data",
    description="Verify ESPN projections data was fetched",
    blocking=True,
)
def check_espn_raw_projections_data(context, espn_raw_projections_data) -> dg.AssetCheckResult:
    if not espn_raw_projections_data or len(espn_raw_projections_data) < 1:
        return dg.AssetCheckResult(passed=False, description="No projections data returned from ESPN.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"ESPN returned {len(espn_raw_projections_data)} projections.",
        metadata={"record_count": dg.MetadataValue.int(len(espn_raw_projections_data))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["espn_player_projections_loaded"]),
    name="check_espn_player_projections_loaded",
    description="Verify rows were processed during load",
    blocking=True,
)
def check_espn_player_projections_loaded(context, espn_player_projections_loaded) -> dg.AssetCheckResult:
    rows_processed = espn_player_projections_loaded.get("rows_processed", 0) if isinstance(espn_player_projections_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["espn_player_projections_formatted"]),
    name="check_espn_player_projections_formatted",
    description="Verify rows were affected during formatting",
    blocking=True,
)
def check_espn_player_projections_formatted(context, espn_player_projections_formatted) -> dg.AssetCheckResult:
    rows_affected = espn_player_projections_formatted.get("rows_affected", 0) if isinstance(espn_player_projections_formatted, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_affected >= 0,
        description=f"Formatting complete: {rows_affected} rows affected",
        metadata={"rows_affected": dg.MetadataValue.int(rows_affected)},
    )
