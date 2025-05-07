import dagster as dg
from dagster import AssetCheckResult

# Simple check for API extraction - verify we got at least one show
@dg.asset_check(
    asset="extract_all_popular_shows",
    name="check_api_returned_data",
    description="Verifies that the API returned at least one show"
)
def check_api_returned_data(context, extract_all_popular_shows):
    """Simple check that the API returned some data."""
    if not extract_all_popular_shows or len(extract_all_popular_shows) < 1:
        return AssetCheckResult(
            passed=False,
            description="API returned no shows. Expected at least 1."
        )
    else:
        return AssetCheckResult(
            passed=True,
            description=f"API returned {len(extract_all_popular_shows)} shows."
        )

# Simple check for staging data - verify rows were inserted
@dg.asset_check(
    asset="load_to_staging_table",
    name="check_staging_has_data",
    description="Verifies that at least one row was inserted into staging"
)
def check_staging_has_data(context, load_to_staging_table):
    """Simple check that rows were inserted into staging."""
    total_count = load_to_staging_table.get("total_count", 0)
    
    if total_count < 1:
        return AssetCheckResult(
            passed=False,
            description="No rows were inserted into staging table. Expected at least 1."
        )
    else:
        return AssetCheckResult(
            passed=True,
            description=f"Successfully loaded {total_count} rows to staging."
        )

# Simple check for production data - verify rows were inserted
@dg.asset_check(
    asset="load_to_production",
    name="check_production_has_data",
    description="Verifies that at least one row was inserted into production"
)
def check_production_has_data(context, load_to_production):
    """Simple check that rows were inserted into production."""
    total_records = load_to_production.get("total_records", 0)
    
    if total_records < 1:
        return AssetCheckResult(
            passed=False,
            description="No rows were loaded into production. Expected at least 1."
        )
    else:
        return AssetCheckResult(
            passed=True,
            description=f"Successfully loaded {total_records} rows to production."
        )

# Simple check for cleanup completion
@dg.asset_check(
    asset="cleanup_staging",
    name="check_cleanup_completed",
    description="Verifies that staging cleanup was successful"
)
def check_cleanup_completed(context, cleanup_staging):
    """Simple check that cleanup was successful."""
    cleanup_successful = cleanup_staging.get("cleanup_successful", False)
    
    if not cleanup_successful:
        return AssetCheckResult(
            passed=False,
            description="Staging cleanup may not have completed successfully."
        )
    else:
        return AssetCheckResult(
            passed=True,
            description="Staging cleanup completed successfully."
        )
        
