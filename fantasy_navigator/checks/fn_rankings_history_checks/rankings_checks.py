import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["raw_player_asset_values_hist"]),
    name="check_raw_player_asset_values_hist",
    description="Verify historical raw player asset values DataFrame is non-empty",
    blocking=True,
)
def check_raw_player_asset_values_hist(context, raw_player_asset_values_hist) -> dg.AssetCheckResult:
    if raw_player_asset_values_hist is None or len(raw_player_asset_values_hist) < 1:
        return dg.AssetCheckResult(passed=False, description="raw_player_asset_values_hist returned empty or None.")
    return dg.AssetCheckResult(
        passed=True,
        metadata={"row_count": dg.MetadataValue.int(len(raw_player_asset_values_hist))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["processed_player_ranks_hist"]),
    name="check_processed_player_ranks_hist",
    description="Verify historical processed player ranks DataFrame is non-empty",
    blocking=True,
)
def check_processed_player_ranks_hist(context, processed_player_ranks_hist) -> dg.AssetCheckResult:
    if processed_player_ranks_hist is None or len(processed_player_ranks_hist) < 1:
        return dg.AssetCheckResult(passed=False, description="processed_player_ranks_hist returned empty or None.")
    return dg.AssetCheckResult(
        passed=True,
        metadata={"row_count": dg.MetadataValue.int(len(processed_player_ranks_hist))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["load_sf_player_ranks_staging"]),
    name="check_load_sf_player_ranks_staging",
    description="Verify rows were processed during staging load",
    blocking=True,
)
def check_load_sf_player_ranks_staging(context, load_sf_player_ranks_staging) -> dg.AssetCheckResult:
    records_processed = load_sf_player_ranks_staging.get("records_processed", 0) if isinstance(load_sf_player_ranks_staging, dict) else 0
    return dg.AssetCheckResult(
        passed=records_processed > 0,
        description=f"Loaded {records_processed} records to staging" if records_processed > 0 else "No records loaded to staging",
        metadata={"records_processed": dg.MetadataValue.int(records_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["load_sf_player_ranks_hist"]),
    name="check_load_sf_player_ranks_hist",
    description="Verify historical ranks were loaded",
    blocking=True,
)
def check_load_sf_player_ranks_hist(context, load_sf_player_ranks_hist) -> dg.AssetCheckResult:
    # This asset returns a boolean value directly
    success = load_sf_player_ranks_hist is True if isinstance(load_sf_player_ranks_hist, bool) else False
    return dg.AssetCheckResult(
        passed=success,
        description="Historical ranks loaded successfully" if success else "Historical ranks load failed",
    )
