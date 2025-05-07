import dagster as dg
from ..jobs.jobs import (
    popular_people_job,
    popular_shows_job, 
    popular_movies_job,
    dim_popular_people_job, 
    dim_popular_people_history_job,
    dim_tv_genres_job,
    dim_movie_genres_job,
    bridge_person_project_job,
    bridge_show_genre_job,
    bridge_movie_genre_job,
    update_new_and_updated_movies_job,
    movie_performance_fact_job  
    )

@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[popular_people_job],
    request_job=dim_popular_people_job,
    default_status= dg.DefaultSensorStatus.RUNNING)
def trigger_dim_popular_people_assets(context):
    return dg.RunRequest(run_key=None)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[update_new_and_updated_movies_job],
    request_job=movie_performance_fact_job,
    default_status=dg.DefaultSensorStatus.RUNNING
)
def trigger_movie_performance_fact_assets(context: dg.build_sensor_context): 
    # Extract partition key from the monitored run's tags
    partition_key = context.dagster_run.tags.get(dg.PARTITION_NAME_TAG)

    if not partition_key:
        context.log.warning(
            f"Monitored run {context.dagster_run.run_id} did not have a partition key."
        )
        return dg.SkipReason(f"Monitored run {context.dagster_run.run_id} did not have a partition key.")

    # Make sure to include all necessary tags for partitioning
    return dg.RunRequest(
        run_key=f"triggered_perf_fact:{partition_key}",
        tags={dg.PARTITION_NAME_TAG: partition_key},  # Include explicit partition tag
        partition_key=partition_key 
    )

@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[popular_people_job],
    request_job=dim_popular_people_history_job,
    default_status= dg.DefaultSensorStatus.RUNNING)
def trigger_dim_popular_people_history_assets(context):
    return dg.RunRequest(run_key=None)

@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[popular_people_job],
    request_job=bridge_person_project_job,
    default_status= dg.DefaultSensorStatus.RUNNING)
def trigger_bridge_person_project_assets(context):
    return dg.RunRequest(run_key=None)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[popular_shows_job, dim_tv_genres_job],  # Monitor both upstream jobs
    request_job=bridge_show_genre_job,
    default_status=dg.DefaultSensorStatus.RUNNING
)
def trigger_bridge_show_genre_assets(context):
    return dg.RunRequest(run_key=None)

@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[popular_movies_job, dim_movie_genres_job],
    request_job=bridge_movie_genre_job,
    default_status=dg.DefaultSensorStatus.RUNNING
)
def trigger_bridge_movie_genre_assets(context):
    return dg.RunRequest(run_key=None)


