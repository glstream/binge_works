import dagster as dg
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json

# Import the Fleaflicker client from the backend
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../fn_app_backend'))

from fleaflicker_client import fleaflicker_client, normalize_fleaflicker_league_data, normalize_fleaflicker_roster_data

TARGET_SCHEMA = "dynastr"

@dg.asset(
    name="fleaflicker_raw_league_data",
    description="Fetches league data from Fleaflicker API for configured leagues.",
    compute_kind="python",
    metadata={"source": "fleaflicker.com"}
)
def fleaflicker_raw_league_data(context: dg.AssetExecutionContext) -> List[Dict]:
    """
    Fetches raw league data from Fleaflicker API.
    
    This asset would typically be configured with specific league IDs to monitor.
    For now, it serves as a placeholder that would need configuration.
    """
    # In a production environment, you would configure specific league IDs to monitor
    # This could be done through Dagster config or environment variables
    
    # For now, return empty list - this would be configured with actual league IDs
    context.log.info("Fleaflicker league data fetching not yet configured with specific league IDs")
    context.log.info("This asset serves as a foundation for future league data ingestion")
    
    return []


@dg.asset(
    name="fleaflicker_league_metadata_loaded",
    description="Loads Fleaflicker league metadata into the database.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    metadata={"target_table": f"{TARGET_SCHEMA}.fleaflicker_league_metadata"}
)
def fleaflicker_league_metadata_loaded(
    context: dg.AssetExecutionContext,
    fleaflicker_raw_league_data: List[Dict]
) -> dg.Output:
    """
    Processes raw league data and loads it into fleaflicker_league_metadata table.
    """
    postgres = context.resources.postgres
    
    if not fleaflicker_raw_league_data:
        context.log.info("No Fleaflicker league data to process")
        return dg.Output(
            value={"rows_processed": 0}, 
            metadata={"skipped": True, "reason": "No league data available"}
        )
    
    context.log.info(f"Processing {len(fleaflicker_raw_league_data)} league records")
    
    # Process league data for database insertion
    league_records = []
    entry_time = datetime.utcnow()
    
    for league_data in fleaflicker_raw_league_data:
        try:
            # Extract league metadata
            league_record = (
                league_data.get("league_id"),
                league_data.get("session_id"), 
                league_data.get("fleaflicker_league_id"),
                json.dumps(league_data.get("scoring_system", {})),
                json.dumps(league_data.get("roster_positions", {})),
                json.dumps(league_data.get("league_settings", {})),
                league_data.get("season"),
                league_data.get("league_type"),
                league_data.get("trade_deadline"),
                league_data.get("waiver_type"),
                entry_time,
                entry_time
            )
            league_records.append(league_record)
            
        except Exception as e:
            context.log.error(f"Error processing league data: {league_data}. Error: {e}")
            continue
    
    if not league_records:
        context.log.warning("No valid league records after processing")
        return dg.Output(
            value={"rows_processed": 0},
            metadata={"skipped": True, "reason": "No valid data after processing"}
        )
    
    # Insert into database
    insert_sql = f"""
        INSERT INTO {TARGET_SCHEMA}.fleaflicker_league_metadata (
            league_id, session_id, fleaflicker_league_id, scoring_system, 
            roster_positions, league_settings, season, league_type, 
            trade_deadline, waiver_type, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (league_id, session_id)
        DO UPDATE SET
            scoring_system = EXCLUDED.scoring_system,
            roster_positions = EXCLUDED.roster_positions,
            league_settings = EXCLUDED.league_settings,
            season = EXCLUDED.season,
            league_type = EXCLUDED.league_type,
            trade_deadline = EXCLUDED.trade_deadline,
            waiver_type = EXCLUDED.waiver_type,
            updated_at = EXCLUDED.updated_at;
    """
    
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Executing batch insert for {len(league_records)} league records")
                postgres.execute_batch(cursor, insert_sql, league_records, page_size=100)
                row_count = cursor.rowcount
                context.log.info(f"Successfully processed {row_count} league metadata records")
        
        return dg.Output(
            value={"rows_processed": len(league_records)},
            metadata={
                "table": f"{TARGET_SCHEMA}.fleaflicker_league_metadata",
                "rows_processed": dg.MetadataValue.int(len(league_records)),
                "operation": "INSERT ON CONFLICT UPDATE"
            }
        )
        
    except Exception as e:
        context.log.error(f"Database error during league metadata load: {e}")
        raise dg.Failure(f"Failed to load Fleaflicker league metadata. Error: {e}")


@dg.asset(
    name="fleaflicker_teams_loaded",
    description="Loads Fleaflicker team/roster information into the database.",
    compute_kind="postgres", 
    required_resource_keys={"postgres"},
    metadata={"target_table": f"{TARGET_SCHEMA}.fleaflicker_teams"}
)
def fleaflicker_teams_loaded(
    context: dg.AssetExecutionContext,
    fleaflicker_raw_league_data: List[Dict]
) -> dg.Output:
    """
    Processes team data from league information and loads into fleaflicker_teams table.
    """
    postgres = context.resources.postgres
    
    if not fleaflicker_raw_league_data:
        context.log.info("No Fleaflicker league data available for team processing")
        return dg.Output(
            value={"rows_processed": 0},
            metadata={"skipped": True, "reason": "No league data available"}
        )
    
    # Process team data from leagues
    team_records = []
    entry_time = datetime.utcnow()
    
    for league_data in fleaflicker_raw_league_data:
        teams = league_data.get("teams", [])
        league_id = league_data.get("league_id")
        session_id = league_data.get("session_id")
        
        for team in teams:
            try:
                team_record = (
                    team.get("team_id"),
                    league_id,
                    session_id,
                    team.get("team_name"),
                    team.get("owner_id"), 
                    team.get("owner_display_name"),
                    team.get("wins", 0),
                    team.get("losses", 0),
                    team.get("ties", 0),
                    team.get("points_for", 0.0),
                    team.get("points_against", 0.0),
                    team.get("standing"),
                    team.get("playoff_seed"),
                    entry_time,
                    entry_time
                )
                team_records.append(team_record)
                
            except Exception as e:
                context.log.error(f"Error processing team data: {team}. Error: {e}")
                continue
    
    if not team_records:
        context.log.warning("No team records found to process")
        return dg.Output(
            value={"rows_processed": 0},
            metadata={"skipped": True, "reason": "No team data found"}
        )
    
    # Insert team data
    insert_sql = f"""
        INSERT INTO {TARGET_SCHEMA}.fleaflicker_teams (
            team_id, league_id, session_id, team_name, owner_id, owner_display_name,
            wins, losses, ties, points_for, points_against, standing, playoff_seed,
            created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (team_id, league_id, session_id)
        DO UPDATE SET
            team_name = EXCLUDED.team_name,
            owner_id = EXCLUDED.owner_id,
            owner_display_name = EXCLUDED.owner_display_name,
            wins = EXCLUDED.wins,
            losses = EXCLUDED.losses,
            ties = EXCLUDED.ties,
            points_for = EXCLUDED.points_for,
            points_against = EXCLUDED.points_against,
            standing = EXCLUDED.standing,
            playoff_seed = EXCLUDED.playoff_seed,
            updated_at = EXCLUDED.updated_at;
    """
    
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Executing batch insert for {len(team_records)} team records")
                postgres.execute_batch(cursor, insert_sql, team_records, page_size=100)
                row_count = cursor.rowcount
                context.log.info(f"Successfully processed {row_count} team records")
        
        return dg.Output(
            value={"rows_processed": len(team_records)},
            metadata={
                "table": f"{TARGET_SCHEMA}.fleaflicker_teams", 
                "rows_processed": dg.MetadataValue.int(len(team_records)),
                "operation": "INSERT ON CONFLICT UPDATE"
            }
        )
        
    except Exception as e:
        context.log.error(f"Database error during teams load: {e}")
        raise dg.Failure(f"Failed to load Fleaflicker teams data. Error: {e}")


@dg.asset(
    name="fleaflicker_current_leagues_updated",
    description="Updates the current_leagues table with Fleaflicker platform data.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    deps=[fleaflicker_league_metadata_loaded],
    metadata={"target_table": f"{TARGET_SCHEMA}.current_leagues"}
)
def fleaflicker_current_leagues_updated(
    context: dg.AssetExecutionContext,
    fleaflicker_raw_league_data: List[Dict]
) -> dg.Output:
    """
    Updates the existing current_leagues table to include Fleaflicker leagues.
    """
    postgres = context.resources.postgres
    
    if not fleaflicker_raw_league_data:
        context.log.info("No Fleaflicker league data to add to current_leagues")
        return dg.Output(
            value={"rows_processed": 0},
            metadata={"skipped": True, "reason": "No league data available"}
        )
    
    # Process leagues for current_leagues table
    league_records = []
    entry_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    
    for league_data in fleaflicker_raw_league_data:
        try:
            # Map Fleaflicker data to current_leagues format
            league_record = (
                league_data.get("session_id"),
                league_data.get("user_id"),
                league_data.get("user_name"),
                league_data.get("league_id"),
                league_data.get("league_name"),
                league_data.get("avatar", ""),
                league_data.get("total_rosters"),
                league_data.get("qb_cnt", 1),
                league_data.get("rb_cnt", 2), 
                league_data.get("wr_cnt", 2),
                league_data.get("te_cnt", 1),
                league_data.get("flex_cnt", 1),
                league_data.get("sf_cnt", 0),
                league_data.get("starter_cnt"),
                league_data.get("total_roster_cnt"),
                "nfl",  # sport
                entry_time,
                league_data.get("rf_cnt", 0),
                league_data.get("league_cat", 1),  # Default to standard league
                league_data.get("league_year"),
                league_data.get("previous_league_id"),
                league_data.get("league_status", "active"),
                "fleaflicker"  # platform
            )
            league_records.append(league_record)
            
        except Exception as e:
            context.log.error(f"Error processing league for current_leagues: {league_data}. Error: {e}")
            continue
    
    if not league_records:
        context.log.warning("No league records to insert into current_leagues")
        return dg.Output(
            value={"rows_processed": 0},
            metadata={"skipped": True, "reason": "No valid league data"}
        )
    
    # Insert into current_leagues with platform field
    insert_sql = f"""
        INSERT INTO {TARGET_SCHEMA}.current_leagues (
            session_id, user_id, user_name, league_id, league_name, avatar,
            total_rosters, qb_cnt, rb_cnt, wr_cnt, te_cnt, flex_cnt, sf_cnt,
            starter_cnt, total_roster_cnt, sport, insert_date, rf_cnt, league_cat,
            league_year, previous_league_id, league_status, platform
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (session_id, league_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            user_name = EXCLUDED.user_name,
            league_name = EXCLUDED.league_name,
            avatar = EXCLUDED.avatar,
            total_rosters = EXCLUDED.total_rosters,
            qb_cnt = EXCLUDED.qb_cnt,
            rb_cnt = EXCLUDED.rb_cnt,
            wr_cnt = EXCLUDED.wr_cnt,
            te_cnt = EXCLUDED.te_cnt,
            flex_cnt = EXCLUDED.flex_cnt,
            sf_cnt = EXCLUDED.sf_cnt,
            starter_cnt = EXCLUDED.starter_cnt,
            total_roster_cnt = EXCLUDED.total_roster_cnt,
            sport = EXCLUDED.sport,
            insert_date = EXCLUDED.insert_date,
            rf_cnt = EXCLUDED.rf_cnt,
            league_cat = EXCLUDED.league_cat,
            league_year = EXCLUDED.league_year,
            previous_league_id = EXCLUDED.previous_league_id,
            league_status = EXCLUDED.league_status,
            platform = EXCLUDED.platform;
    """
    
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                context.log.info(f"Executing batch insert for {len(league_records)} current_leagues records")
                postgres.execute_batch(cursor, insert_sql, league_records, page_size=100)
                row_count = cursor.rowcount
                context.log.info(f"Successfully processed {row_count} current_leagues records")
        
        return dg.Output(
            value={"rows_processed": len(league_records)},
            metadata={
                "table": f"{TARGET_SCHEMA}.current_leagues",
                "rows_processed": dg.MetadataValue.int(len(league_records)),
                "operation": "INSERT ON CONFLICT UPDATE",
                "platform": "fleaflicker"
            }
        )
        
    except Exception as e:
        context.log.error(f"Database error during current_leagues update: {e}")
        raise dg.Failure(f"Failed to update current_leagues with Fleaflicker data. Error: {e}")


@dg.asset(
    name="fleaflicker_data_validation",
    description="Validates Fleaflicker data integrity and relationships.",
    compute_kind="postgres",
    required_resource_keys={"postgres"},
    deps=[fleaflicker_current_leagues_updated, fleaflicker_teams_loaded],
    metadata={"validation_checks": "data_integrity"}
)
def fleaflicker_data_validation(context: dg.AssetExecutionContext) -> dg.Output:
    """
    Performs data validation checks on loaded Fleaflicker data.
    """
    postgres = context.resources.postgres
    
    validation_queries = {
        "fleaflicker_leagues_count": f"""
            SELECT COUNT(*) as count 
            FROM {TARGET_SCHEMA}.current_leagues 
            WHERE platform = 'fleaflicker'
        """,
        "fleaflicker_teams_count": f"""
            SELECT COUNT(*) as count 
            FROM {TARGET_SCHEMA}.fleaflicker_teams
        """,
        "orphaned_teams": f"""
            SELECT COUNT(*) as count 
            FROM {TARGET_SCHEMA}.fleaflicker_teams ft
            LEFT JOIN {TARGET_SCHEMA}.current_leagues cl 
                ON ft.league_id = cl.league_id AND ft.session_id = cl.session_id
            WHERE cl.league_id IS NULL
        """,
        "leagues_without_metadata": f"""
            SELECT COUNT(*) as count
            FROM {TARGET_SCHEMA}.current_leagues cl
            LEFT JOIN {TARGET_SCHEMA}.fleaflicker_league_metadata flm
                ON cl.league_id = flm.league_id AND cl.session_id = flm.session_id
            WHERE cl.platform = 'fleaflicker' AND flm.league_id IS NULL
        """
    }
    
    validation_results = {}
    
    try:
        with postgres.get_connection() as conn:
            with conn.cursor() as cursor:
                for check_name, query in validation_queries.items():
                    cursor.execute(query)
                    result = cursor.fetchone()
                    count = result[0] if result else 0
                    validation_results[check_name] = count
                    context.log.info(f"Validation check '{check_name}': {count}")
        
        # Log warnings for data integrity issues
        if validation_results.get("orphaned_teams", 0) > 0:
            context.log.warning(f"Found {validation_results['orphaned_teams']} orphaned team records")
        
        if validation_results.get("leagues_without_metadata", 0) > 0:
            context.log.warning(f"Found {validation_results['leagues_without_metadata']} leagues without metadata")
        
        return dg.Output(
            value=validation_results,
            metadata={
                "validation_checks": dg.MetadataValue.json(validation_results),
                "data_quality": "validated"
            }
        )
        
    except Exception as e:
        context.log.error(f"Error during data validation: {e}")
        raise dg.Failure(f"Failed to validate Fleaflicker data. Error: {e}")