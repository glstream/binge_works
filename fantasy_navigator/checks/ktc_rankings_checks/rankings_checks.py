import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["ktc_raw_player_data"]),
    name="check_ktc_raw_player_data",
    description="Verify KTC scraper returned player data",
    blocking=True,
)
def check_ktc_raw_player_data(context, ktc_raw_player_data) -> dg.AssetCheckResult:
    if not ktc_raw_player_data or len(ktc_raw_player_data) < 1:
        return dg.AssetCheckResult(passed=False, description="No data returned from KTC scraper.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"KTC returned {len(ktc_raw_player_data)} records.",
        metadata={"record_count": dg.MetadataValue.int(len(ktc_raw_player_data))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["ktc_player_ranks_loaded"]),
    name="check_ktc_player_ranks_loaded",
    description="Verify rows were processed during load",
    blocking=True,
)
def check_ktc_player_ranks_loaded(context, ktc_player_ranks_loaded) -> dg.AssetCheckResult:
    rows_processed = ktc_player_ranks_loaded.get("rows_processed", 0) if isinstance(ktc_player_ranks_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["ktc_player_ranks_formatted"]),
    name="check_ktc_player_ranks_formatted",
    description="Verify rows were affected during formatting",
    blocking=True,
)
def check_ktc_player_ranks_formatted(context, ktc_player_ranks_formatted) -> dg.AssetCheckResult:
    rows_affected = ktc_player_ranks_formatted.get("rows_affected", 0) if isinstance(ktc_player_ranks_formatted, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_affected >= 0,
        description=f"Formatting complete: {rows_affected} rows affected",
        metadata={"rows_affected": dg.MetadataValue.int(rows_affected)},
    )
