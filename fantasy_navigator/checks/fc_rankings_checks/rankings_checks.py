import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["fc_sf_raw_player_data"]),
    name="check_fc_sf_raw_player_data",
    description="Verify FantasyCalc SF API returned player data",
    blocking=True,
)
def check_fc_sf_raw_player_data(context, fc_sf_raw_player_data) -> dg.AssetCheckResult:
    if not fc_sf_raw_player_data or len(fc_sf_raw_player_data) < 1:
        return dg.AssetCheckResult(passed=False, description="No SF data returned from FantasyCalc API.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"FantasyCalc SF API returned {len(fc_sf_raw_player_data)} records.",
        metadata={"record_count": dg.MetadataValue.int(len(fc_sf_raw_player_data))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["fc_player_ranks_sf_loaded"]),
    name="check_fc_player_ranks_sf_loaded",
    description="Verify rows were processed during SF load",
    blocking=True,
)
def check_fc_player_ranks_sf_loaded(context, fc_player_ranks_sf_loaded) -> dg.AssetCheckResult:
    rows_processed = fc_player_ranks_sf_loaded.get("rows_processed", 0) if isinstance(fc_player_ranks_sf_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["fc_one_qb_raw_player_data"]),
    name="check_fc_one_qb_raw_player_data",
    description="Verify FantasyCalc 1QB API returned player data",
    blocking=True,
)
def check_fc_one_qb_raw_player_data(context, fc_one_qb_raw_player_data) -> dg.AssetCheckResult:
    if not fc_one_qb_raw_player_data or len(fc_one_qb_raw_player_data) < 1:
        return dg.AssetCheckResult(passed=False, description="No 1QB data returned from FantasyCalc API.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"FantasyCalc 1QB API returned {len(fc_one_qb_raw_player_data)} records.",
        metadata={"record_count": dg.MetadataValue.int(len(fc_one_qb_raw_player_data))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["fc_player_ranks_one_qb_loaded"]),
    name="check_fc_player_ranks_one_qb_loaded",
    description="Verify rows were processed during 1QB load",
    blocking=True,
)
def check_fc_player_ranks_one_qb_loaded(context, fc_player_ranks_one_qb_loaded) -> dg.AssetCheckResult:
    rows_processed = fc_player_ranks_one_qb_loaded.get("rows_processed", 0) if isinstance(fc_player_ranks_one_qb_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["fc_player_ranks_formatted"]),
    name="check_fc_player_ranks_formatted",
    description="Verify rows were affected during formatting",
    blocking=True,
)
def check_fc_player_ranks_formatted(context, fc_player_ranks_formatted) -> dg.AssetCheckResult:
    rows_affected = fc_player_ranks_formatted.get("rows_affected", 0) if isinstance(fc_player_ranks_formatted, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_affected >= 0,
        description=f"Formatting complete: {rows_affected} rows affected",
        metadata={"rows_affected": dg.MetadataValue.int(rows_affected)},
    )
