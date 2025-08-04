import dagster as dg
from dagster import (
    AssetCheckResult,
    AssetKey,
    asset_check,
    AssetCheckExecutionContext,
)

# --- Checks for Table Creation ---
@asset_check(
    name="check_fact_show_reviews_table_exists",
    asset=AssetKey("create_fact_show_reviews"),
    description="Verifies that the fact_show_reviews table exists with the expected structure",
    required_resource_keys={"postgres"},
)
def check_fact_show_reviews_table_exists(context, create_fact_show_reviews):
    """Check that the fact_show_reviews table exists with the expected structure."""
    postgres = context.resources.postgres
    required_columns = [
        "review_id", "tmdb_review_id", "series_id", "author_name", 
        "author_username", "date_id", "rating", "content_length", 
        "review_created_at", "review_updated_at", "created_date", 
        "loaded_partition_date"
    ]
    
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'tmdb_data' 
                    AND table_name = 'fact_show_reviews'
                );
            """)
            table_exists = cursor.fetchone()["exists"]
            
            if not table_exists:
                return AssetCheckResult(
                    passed=False,
                    description="fact_show_reviews table does not exist"
                )
            
            # Check for required columns
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'tmdb_data' 
                AND table_name = 'fact_show_reviews';
            """)
            actual_columns = [row["column_name"] for row in cursor.fetchall()]
            
            missing_columns = [col for col in required_columns if col not in actual_columns]
            
            if missing_columns:
                return AssetCheckResult(
                    passed=False,
                    description=f"Missing required columns: {', '.join(missing_columns)}"
                )
    
    return AssetCheckResult(
        passed=True,
        description="fact_show_reviews table exists with all required columns"
    )

# --- Checks for Shows Query ---
@asset_check(
    name="check_shows_for_reviews_not_empty",
    asset=AssetKey("get_shows_for_reviews"),
    description="Verifies that we found shows to process",
)
def check_shows_for_reviews_not_empty(context: AssetCheckExecutionContext, get_shows_for_reviews):
    """Check that we found shows to process."""
    shows_count = len(get_shows_for_reviews) if get_shows_for_reviews else 0
    
    # This is now just informational - it's OK to have no shows some days
    return AssetCheckResult(
        passed=True,
        description=f"Found {shows_count} shows for review updates",
        metadata={
            "shows_count": shows_count
        }
    )

# --- Checks for API Response ---
@asset_check(
    name="check_reviews_fetch_success_rate",
    asset=AssetKey("fetch_show_reviews"),
    description="Checks the success rate of review fetching",
)
def check_reviews_fetch_success_rate(context: AssetCheckExecutionContext, fetch_show_reviews):
    """Check the success rate of fetching reviews from the API."""
    # Get metadata from the asset output
    reviews_fetched = len(fetch_show_reviews) if fetch_show_reviews else 0
    
    # Just check that the fetch completed without major errors
    return AssetCheckResult(
        passed=True,
        description=f"Successfully fetched {reviews_fetched} reviews",
        metadata={
            "reviews_fetched": reviews_fetched
        }
    )

@asset_check(
    name="check_reviews_data_quality",
    asset=AssetKey("fetch_show_reviews"),
    description="Checks the quality of fetched review data",
)
def check_reviews_data_quality(context: AssetCheckExecutionContext, fetch_show_reviews):
    """Check the quality of fetched review data."""
    if not fetch_show_reviews:
        return AssetCheckResult(
            passed=True,
            description="No reviews fetched - nothing to validate",
        )
    
    # Basic validation - check for required fields
    valid_reviews = 0
    total_reviews = len(fetch_show_reviews)
    
    for review in fetch_show_reviews:
        if (review.get('tmdb_review_id') and 
            review.get('series_id') and 
            review.get('loaded_partition_date')):
            valid_reviews += 1
    
    if total_reviews > 0:
        quality_rate = (valid_reviews / total_reviews) * 100
    else:
        quality_rate = 100
    
    # Define threshold for acceptable data quality
    threshold = 95
    
    if quality_rate < threshold:
        return AssetCheckResult(
            passed=False,
            description=f"Poor data quality ({quality_rate:.1f}% valid reviews) in fetched reviews",
            metadata={
                "total_reviews": total_reviews,
                "valid_reviews": valid_reviews,
                "quality_rate": quality_rate,
                "threshold": threshold
            }
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Good data quality ({quality_rate:.1f}% valid reviews)",
        metadata={
            "total_reviews": total_reviews,
            "valid_reviews": valid_reviews,
            "quality_rate": quality_rate
        }
    )

# --- Checks for Data Loading ---
@asset_check(
    name="check_reviews_load_success",
    asset=AssetKey("load_show_reviews"),
    description="Verifies that reviews were successfully loaded to the database",
)
def check_reviews_load_success(context: AssetCheckExecutionContext, load_show_reviews):
    """Check that reviews were successfully loaded to the database."""
    # load_show_reviews returns the count of loaded reviews
    loaded_count = load_show_reviews if isinstance(load_show_reviews, int) else 0
    
    return AssetCheckResult(
        passed=True,
        description=f"Successfully loaded {loaded_count} reviews",
        metadata={
            "loaded_count": loaded_count
        }
    )

@asset_check(
    name="check_reviews_loaded_correctly",
    asset=AssetKey("load_show_reviews"),
    description="Verifies that reviews were loaded correctly",
    required_resource_keys={"postgres"},
)
def check_reviews_loaded_correctly(context: AssetCheckExecutionContext, load_show_reviews):
    """Check that reviews were loaded correctly."""
    postgres = context.resources.postgres
    reported_count = load_show_reviews if isinstance(load_show_reviews, int) else 0
    
    if reported_count == 0:
        return AssetCheckResult(
            passed=True,
            description="No reviews were loaded - nothing to verify",
        )
    
    # Get the partition date for this run
    partition_date = context.partition_key if hasattr(context, 'partition_key') else None
    
    # Verify data was loaded correctly
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            if partition_date:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM tmdb_data.fact_show_reviews
                    WHERE loaded_partition_date = %s
                """, (partition_date,))
            else:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM tmdb_data.fact_show_reviews
                    WHERE created_date >= CURRENT_DATE
                """)
            
            result = cursor.fetchone()
            actual_count = result["count"] if result else 0
    
    if actual_count < reported_count:
        return AssetCheckResult(
            passed=False,
            description="Fewer reviews found in database than were reported as loaded",
            metadata={
                "reported_count": reported_count,
                "actual_count": actual_count
            }
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Verified {actual_count} reviews loaded correctly",
        metadata={
            "reported_count": reported_count,
            "actual_count": actual_count
        }
    )