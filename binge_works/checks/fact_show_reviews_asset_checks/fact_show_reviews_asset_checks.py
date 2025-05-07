import dagster as dg
from dagster import (
    AssetCheckResult,
    AssetKey,
    asset_check,
    AssetCheckExecutionContext,
    DailyPartitionsDefinition,
    AssetIn
)
from ...constants import daily_partitions_def

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
    shows_count = len(get_shows_for_reviews)
    
    # This may not always be a failure condition - there might legitimately be days with no shows
    # So this could also be set as a warning instead of failure
    if shows_count == 0:
        return AssetCheckResult(
            passed=False,  # Or True if this is just a warning
            description="No shows found for review updates",
            metadata={
                "shows_count": shows_count
            }
        )
    
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
    additional_ins={"get_shows_for_reviews":AssetIn("get_shows_for_reviews")}
)
def check_reviews_fetch_success_rate(context: AssetCheckExecutionContext, get_shows_for_reviews, fetch_show_reviews):
    """Check the success rate of fetching reviews from the API."""
    # Get the input show count and the metadata about skipped shows
    total_shows = len(get_shows_for_reviews)
    if total_shows == 0:
        # Nothing to fetch, so it's a pass
        return AssetCheckResult(
            passed=True,
            description="No shows to fetch reviews for",
        )
    
    # Count unique series_ids in the fetched reviews
    processed_series_ids = set()
    for review in fetch_show_reviews:
        processed_series_ids.add(review)
    
    shows_with_reviews = len(processed_series_ids)
    success_rate = (shows_with_reviews / total_shows) * 100 if total_shows > 0 else 100
    
    # Define threshold for acceptable success rate (e.g., 80%)
    threshold = 80  
    
    if success_rate < threshold:
        return AssetCheckResult(
            passed=False,
            description=f"Low success rate ({success_rate:.1f}%) for fetching show reviews",
            metadata={
                "total_shows": total_shows,
                "shows_with_reviews": shows_with_reviews,
                "success_rate": success_rate,
                "threshold": threshold
            }
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Successfully fetched reviews with {success_rate:.1f}% success rate",
        metadata={
            "total_shows": total_shows,
            "shows_with_reviews": shows_with_reviews,
            "success_rate": success_rate
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
        # Nothing to check, so it's a pass
        return AssetCheckResult(
            passed=True,
            description="No reviews fetched",
        )
    
    # Count reviews with missing crucial fields
    missing_review_id = 0
    missing_series_id = 0
    missing_date_id = 0
    
    for review in fetch_show_reviews:
        context.log.info(f"Review: {fetch_show_reviews}")
    
    total_reviews = len(fetch_show_reviews)
    missing_data_rate = ((missing_review_id + missing_series_id + missing_date_id) / (total_reviews * 3)) * 100
    
    # Define threshold for acceptable missing data rate (e.g., 5%)
    threshold = 5
    
    if missing_data_rate > threshold:
        return AssetCheckResult(
            passed=False,
            description=f"Poor data quality ({missing_data_rate:.1f}% missing crucial fields) in fetched reviews",
            metadata={
                "total_reviews": total_reviews,
                "missing_review_id": missing_review_id,
                "missing_series_id": missing_series_id,
                "missing_date_id": missing_date_id,
                "missing_data_rate": missing_data_rate,
                "threshold": threshold
            }
        )
    
    return AssetCheckResult(
        passed=True,
        description="Good data quality in fetched reviews",
        metadata={
            "total_reviews": total_reviews,
            "missing_review_id": missing_review_id,
            "missing_series_id": missing_series_id,
            "missing_date_id": missing_date_id,
            "missing_data_rate": missing_data_rate
        }
    )

# --- Checks for Data Loading ---
@asset_check(
    name="check_reviews_load_success",
    asset=AssetKey("load_show_reviews"),
    description="Verifies that reviews were successfully loaded to the database",
    additional_ins={"fetch_show_reviews": AssetIn("fetch_show_reviews")},  # Assuming fetch_show_reviews is the upstream asset
)
def check_reviews_load_success(context: AssetCheckExecutionContext, fetch_show_reviews, load_show_reviews):
    """Check that reviews were successfully loaded to the database."""
    # Count the number of reviews that should have been loaded
    expected_count = len(fetch_show_reviews)
    # Get the actual count that was loaded
    actual_count = len(load_show_reviews)
    
    # If nothing was expected to load, that's fine
    if expected_count == 0:
        return AssetCheckResult(
            passed=True,
            description="No reviews to load",
        )
    
    # Check if the counts match
    if actual_count < expected_count:
        # Some reviews failed to load
        load_rate = (actual_count / expected_count) * 100
        
        # Define threshold for acceptable load rate (e.g., 95%)
        threshold = 95
        
        if load_rate < threshold:
            return AssetCheckResult(
                passed=False,
                description=f"Only {load_rate:.1f}% of reviews were loaded",
                metadata={
                    "expected_count": expected_count,
                    "actual_count": actual_count,
                    "load_rate": load_rate,
                    "threshold": threshold
                }
            )
    
    return AssetCheckResult(
        passed=True,
        description="Successfully loaded reviews",
        metadata={
            "expected_count": expected_count,
            "actual_count": actual_count,
            "load_rate": 100 * actual_count / expected_count if expected_count > 0 else 100
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
    
    if load_show_reviews == 0:
        # Nothing was loaded, so nothing to check
        return AssetCheckResult(
            passed=True,
            description="No reviews loaded",
        )
    
    # Verify data was loaded correctly
    with postgres.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM tmdb_data.fact_show_reviews
                WHERE created_date = CURRENT_DATE
            """)
            
            result = cursor.fetchone()
            count_loaded_today = result["count"] if result else 0
    
    if count_loaded_today == 0:
        return AssetCheckResult(
            passed=False,
            description="No reviews found after loading",
            metadata={
                "reported_loaded_count": load_show_reviews,
                "actual_count": count_loaded_today
            }
        )
    
    if count_loaded_today < load_show_reviews:
        return AssetCheckResult(
            passed=False,
            description="Fewer reviews found than were reported as loaded",
            metadata={
                "reported_loaded_count": load_show_reviews,
                "actual_count": count_loaded_today
            }
        )
    
    return AssetCheckResult(
        passed=True,
        description=f"Verified {count_loaded_today} reviews loaded correctly",
        metadata={
            "count_loaded": count_loaded_today
        }
    )