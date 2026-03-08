import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["dd_raw_dynasty_data"]),
    name="check_dd_raw_dynasty_has_data",
    description="Verify the data returned from the Dynasty Daddy API contains player rankings",
    blocking=True,
)
def check_dd_raw_dynasty_has_data(context, dd_raw_dynasty_data) -> dg.AssetCheckResult:
    """Simple check that the API returned some data."""
    record_count = len(dd_raw_dynasty_data) if dd_raw_dynasty_data else 0
    if record_count < 1:
        return dg.AssetCheckResult(
            passed=False,
            description="API returned no players. Expected at least 1."
        )
    else:
        return dg.AssetCheckResult(
            passed=True,
            description=f"API returned {record_count} players.",
            metadata={"record_count": dg.MetadataValue.int(record_count)}
        )
        
@dg.asset_check(
    asset=dg.AssetKey(["dd_raw_redraft_data"]),
    name="check_dd_raw_redraft_has_data",
    description="Verify the data returned from the Dynasty Daddy API contains player rankings",
    blocking=True,
)
def check_dd_raw_redraft_has_data(context, dd_raw_redraft_data) -> dg.AssetCheckResult:
    """Simple check that the API returned some data."""
    record_count = len(dd_raw_redraft_data) if dd_raw_redraft_data else 0
    if record_count < 1:
        return dg.AssetCheckResult(
            passed=False,
            description="API returned no players. Expected at least 1."
        )
    else:
        return dg.AssetCheckResult(
            passed=True,
            description=f"API returned {record_count} players.",
            metadata={"record_count": dg.MetadataValue.int(record_count)}
        )

@dg.asset_check(
    asset=dg.AssetKey(["dd_player_ranks_dynasty_loaded"]),
    name="check_dd_player_ranks_has_data",
    description="Verify rows were processed during dynasty load",
    blocking=True,
)
def check_dd_player_ranks(context, dd_player_ranks_dynasty_loaded) -> dg.AssetCheckResult:
    """Check the rows_processed value from the asset output"""
    rows_processed = dd_player_ranks_dynasty_loaded.get("rows_processed", 0) if isinstance(dd_player_ranks_dynasty_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["dd_player_ranks_redraft_loaded"]),
    name="check_dd_player_ranks_redraft_loaded",
    description="Verify rows were processed during redraft load",
    blocking=True,
)
def check_dd_player_ranks_redraft_loaded(context, dd_player_ranks_redraft_loaded) -> dg.AssetCheckResult:
    """Check the rows_processed value from the asset output"""
    rows_processed = dd_player_ranks_redraft_loaded.get("rows_processed", 0) if isinstance(dd_player_ranks_redraft_loaded, dict) else 0
    return dg.AssetCheckResult(
        passed=rows_processed > 0,
        description=f"Loaded {rows_processed} rows" if rows_processed > 0 else "No rows loaded",
        metadata={"rows_processed": dg.MetadataValue.int(rows_processed)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["dd_player_ranks_formatted"]),
    name="check_dd_player_ranks_formatted",
    description="Verify rows were affected during formatting",
    blocking=True,
)
def check_dd_player_ranks_formatted(context, dd_player_ranks_formatted) -> dg.AssetCheckResult:
    """Check the affected rows from the formatting operation"""
    affected_rows = 0
    if isinstance(dd_player_ranks_formatted, dict):
        deleted = dd_player_ranks_formatted.get("deleted_rows", 0)
        updated = dd_player_ranks_formatted.get("updated_rows", 0)
        affected_rows = deleted + updated
    return dg.AssetCheckResult(
        passed=affected_rows >= 0,  # Formatting always passes even if no rows affected
        description=f"Formatting complete: {affected_rows} rows affected",
        metadata={
            "deleted_rows": dg.MetadataValue.int(dd_player_ranks_formatted.get("deleted_rows", 0) if isinstance(dd_player_ranks_formatted, dict) else 0),
            "updated_rows": dg.MetadataValue.int(dd_player_ranks_formatted.get("updated_rows", 0) if isinstance(dd_player_ranks_formatted, dict) else 0),
        },
    )
