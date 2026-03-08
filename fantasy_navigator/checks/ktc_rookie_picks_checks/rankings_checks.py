import dagster as dg


@dg.asset_check(
    asset=dg.AssetKey(["extract_one_qb_ktc_rookie_picks"]),
    name="check_extract_one_qb_ktc_rookie_picks",
    description="Verify KTC 1QB rookie picks data was extracted",
    blocking=True,
)
def check_extract_one_qb_ktc_rookie_picks(context, extract_one_qb_ktc_rookie_picks) -> dg.AssetCheckResult:
    if not extract_one_qb_ktc_rookie_picks or len(extract_one_qb_ktc_rookie_picks) < 1:
        return dg.AssetCheckResult(passed=False, description="No 1QB rookie picks data extracted from KTC.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"Extracted {len(extract_one_qb_ktc_rookie_picks)} 1QB rookie picks.",
        metadata={"record_count": dg.MetadataValue.int(len(extract_one_qb_ktc_rookie_picks))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["load_one_qb_ktc_rookie_picks"]),
    name="check_load_one_qb_ktc_rookie_picks",
    description="Verify 1QB rookie picks were loaded",
    blocking=True,
)
def check_load_one_qb_ktc_rookie_picks(context, load_one_qb_ktc_rookie_picks) -> dg.AssetCheckResult:
    records_inserted = load_one_qb_ktc_rookie_picks.get("records_inserted", 0) if isinstance(load_one_qb_ktc_rookie_picks, dict) else 0
    return dg.AssetCheckResult(
        passed=records_inserted > 0,
        description=f"Loaded {records_inserted} 1QB rookie picks" if records_inserted > 0 else "No 1QB rookie picks loaded",
        metadata={"records_inserted": dg.MetadataValue.int(records_inserted)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["extract_sf_ktc_rookie_picks"]),
    name="check_extract_sf_ktc_rookie_picks",
    description="Verify KTC SF rookie picks data was extracted",
    blocking=True,
)
def check_extract_sf_ktc_rookie_picks(context, extract_sf_ktc_rookie_picks) -> dg.AssetCheckResult:
    if not extract_sf_ktc_rookie_picks or len(extract_sf_ktc_rookie_picks) < 1:
        return dg.AssetCheckResult(passed=False, description="No SF rookie picks data extracted from KTC.")
    return dg.AssetCheckResult(
        passed=True,
        description=f"Extracted {len(extract_sf_ktc_rookie_picks)} SF rookie picks.",
        metadata={"record_count": dg.MetadataValue.int(len(extract_sf_ktc_rookie_picks))},
    )


@dg.asset_check(
    asset=dg.AssetKey(["load_sf_ktc_rookie_picks"]),
    name="check_load_sf_ktc_rookie_picks",
    description="Verify SF rookie picks were loaded",
    blocking=True,
)
def check_load_sf_ktc_rookie_picks(context, load_sf_ktc_rookie_picks) -> dg.AssetCheckResult:
    records_inserted = load_sf_ktc_rookie_picks.get("records_inserted", 0) if isinstance(load_sf_ktc_rookie_picks, dict) else 0
    return dg.AssetCheckResult(
        passed=records_inserted > 0,
        description=f"Loaded {records_inserted} SF rookie picks" if records_inserted > 0 else "No SF rookie picks loaded",
        metadata={"records_inserted": dg.MetadataValue.int(records_inserted)},
    )
