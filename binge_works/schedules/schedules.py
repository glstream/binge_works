from dagster import ScheduleDefinition, build_schedule_from_partitioned_job

# Import the jobs
from ..jobs.jobs import (
    popular_shows_job, 
    popular_people_job, 
    popular_movies_job,
    dim_movie_genres_job,
    dim_tv_genres_job,
    fact_movie_reviews_job, 
    fact_show_reviews_job,
    update_new_and_updated_movies_job,
    )

# Define schedules
popular_shows_schedule = ScheduleDefinition(
    name="daily_popular_shows",
    cron_schedule="0 13 * * *",  # Run at 5am every day
    job=popular_shows_job,
    description="Update popular shows data daily at midnight"
)


popular_people_schedule = ScheduleDefinition(
    name="daily_popular_people",
    cron_schedule="0 13 * * *",  # Run at 5am every day
    job=popular_people_job,
    description="Update the popular people data daily at 5am"
)

popular_movie_schedule = ScheduleDefinition(
    name="daily_popular_movies",
    cron_schedule="20 13 * * *",  # Run at 5am every day
    job=popular_movies_job,
    description="Update the popular movies daily at 520am"
)

dim_movie_genres_schedule = ScheduleDefinition(
    name="dim_movie_genres",
    cron_schedule="0 14 * * 1",  # Run at 2pm every Monday
    job=dim_movie_genres_job,
    description="Create the movie genres dimension table weekly on Mondays at 2pm"
)

dim_tv_genres_schedule = ScheduleDefinition(
    name="dim_tv_genres",
    cron_schedule="0 14 * * 1",  # Run at 2pm every Monday
    job=dim_tv_genres_job,
    description="Create the TV genres dimension table weekly on Mondays at 2pm"
)

fact_movie_review_schedule = build_schedule_from_partitioned_job(
    name="fact_movie_reviews",
    hour_of_day=6,  
    minute_of_hour=30 ,
    job=fact_movie_reviews_job,
    description="Update the movie reviews fact table daily at 5:30am"
)

fact_show_review_schedule = build_schedule_from_partitioned_job(
    name="fact_show_reviews",
    hour_of_day=6,  
    minute_of_hour=30,
    job=fact_show_reviews_job,
    description="Update the show reviews fact table daily at 5:45am"
)

update_new_and_updated_movies_schedule = build_schedule_from_partitioned_job(
    name="update_new_and_updated_movies_schedule",
    hour_of_day=4,  
    minute_of_hour=30,
    job=update_new_and_updated_movies_job,
    description="Create the movie augmentation table weekly on Mondays at 2pm"
)