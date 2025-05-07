from dagster import define_asset_job
from ..constants import daily_partitions_def, movie_aug_partitions_def


#DIMESNION JOBS
popular_shows_job = define_asset_job(
    name="popular_shows_job",
    selection="extract_popular_shows_metadata*",
    description="Job to update popular TV shows data daily"
)
popular_people_job = define_asset_job(
    name="popular_people_job",
    selection="truncate_popular_people_table*",
    description="Job to update popular People daily"
)

popular_movies_job = define_asset_job(
    name="popular_movies_job",
    selection="truncate_popular_movies_table*",
    description="Job to update popular Movies daily"
)

dim_popular_people_job = define_asset_job(
    name="dim_popular_people_job",
    selection="insert_dim_popular_people*",
    description="Jon for type 1 popular people dimension"
)

dim_popular_people_history_job = define_asset_job(
    name="dim_popular_people_history_job",
    selection="insert_people_history*",
    description="Jon for type 2 popular people dimension"
)

dim_movie_genres_job = define_asset_job(
    name="dim_movie_genres_job",
    selection="extract_movie_genres*",
    description="Job to create the movie genres dimension table"
)

dim_tv_genres_job = define_asset_job(
    name="dim_tv_genres_job",
    selection="extract_tv_genres*",
    description="Job to create the TV genres dimension table"
)

#Bridge JOBS

bridge_person_project_job = define_asset_job(
    name="bridge_person_project_job",
    selection="update_person_project_bridge*",
    description="Job to update the bridge table between people and projects"
)

bridge_show_genre_job = define_asset_job(
    name="bridge_show_genre_job",
    selection="update_bridge_show_genre*",
    description="Job to update the bridge table between shows and genres"
)
bridge_movie_genre_job = define_asset_job(
    name="bridge_movie_genre_job",
    selection="update_bridge_movie_genre*",
    description="Job to update the bridge table between movies and genres"
)

# FACT TABLE JOBS
fact_movie_reviews_job = define_asset_job(
    name="fact_movie_reviews_job",
    selection="get_movies_for_reviews*",
    description="Job to update the movie reviews fact table",
    partitions_def=daily_partitions_def
)

fact_show_reviews_job = define_asset_job(
    name="fact_show_reviews_job",
    selection="get_shows_for_reviews*",
    description="Job to update the show reviews fact table",
    partitions_def=daily_partitions_def
)

movie_performance_fact_job = define_asset_job(
    name="movie_performance_fact_job",
    selection=[
        "get_movie_details", 
        "load_movie_dim", 
        "load_genres_dim",
        "load_movie_performance_fact"
    ],
    description="Job to update the movie performance fact table",
    partitions_def=movie_aug_partitions_def
)

update_new_and_updated_movies_job = define_asset_job(
    name="update_new_and_updated_movies_job",
    selection=[
        "get_movie_augmentation",
        "insert_movie_augmentation",
        "get_movie_ids_for_details",
        "get_movie_details", 
        "updated_movies_api",
        "updated_movie_details",
        "load_movie_dim",
        "update_movie_dim_review_counts", 
        "load_genres_dim",
        "load_crew_dim",
        "load_cast_dim",
        "load_movie_performance_fact",
        "update_bridge_movie_genre"
    ],
    description="Job to update the movie performance fact table",
    partitions_def=movie_aug_partitions_def
)