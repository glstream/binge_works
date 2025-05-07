import dagster as dg
from ..jobs.jobs import (
    ktc_rookies_job,
    fantasy_navigator_job,
    cbs_projections_job,
    espn_projections_job,
    nfl_projections_job,
    fantasy_navigator_hist_job
)

# define rookie schedules
ktc_rookies_schedule = dg.ScheduleDefinition(
    name="daily_ktc_rookies",
    cron_schedule="0 17 * * 1",  # Run at 13:00 UTC every Monday
    job=ktc_rookies_job,
    description="Update KTC rookie picks data weekly on Monday at 13:00 UTC"
)

# projecton s schedule
cbs_projections_schedule = dg.ScheduleDefinition(
    name="daily_cbs_projections",
    cron_schedule="0 17 * * 1",  # Run at 13:00 UTC every Monday
    job=cbs_projections_job,
    description="Update CBS projections data weekly on Monday at 13:00 UTC"
)

espn_projections_schedule = dg.ScheduleDefinition(
    name="daily_espn_projections",
    cron_schedule="0 17 * * 1",  # Run at 13:00 UTC every Monday
    job=cbs_projections_job,
    description="Update ESPN projections data weekly on Monday at 13:00 UTC"
)

nfl_projections_schedule = dg.ScheduleDefinition(
    name="daily_nfl_projections",
    cron_schedule="0 17 * * 1",  # Run at 13:00 UTC every Monday
    job=cbs_projections_job,
    description="Update NFL projections data weekly on Monday at 13:00 UTC"
)

fantasy_navigator_schedule = dg.ScheduleDefinition(
    name="weekly_fantasy_navigator",
    cron_schedule="0 17 * * 1",  # Run at 13:00 UTC every Monday
    job=fantasy_navigator_job,
    description="Update Fantasy Navigator rankings data weekly on Monday at 13:00 UTC"
)

fantasy_navigator_hist_schedule = dg.ScheduleDefinition(
    name="daily_fantasy_navigator_history",
    cron_schedule="0 13 * * *",  # Run at 13:00 UTC every day
    job=fantasy_navigator_hist_job,
    description="Update Fantasy Navigator historical rankings data daily at 13:00 UTC"
)