from dagster import (
    Definitions,
    load_assets_from_package_module,
    load_asset_checks_from_package_module,
)
# import resources
from binge_works.resources.TMDBResource import TMDBResource
from binge_works.resources.PostgreSQLResource import PostgreSQLResource

# import assets
from .assets import (
    popular_shows, 
    popular_people,
    popular_movies,
    #DIMENSIONS
    dim_popular_people,
    dim_popular_people_history,
    dim_movie_genres,
    dim_tv_genres,
    movie_dim,
    genres_dim,
    cast_dim,
    crew_dim,
    create_tables,
    #BRIDGES
    bridge_person_project,
    bridge_show_genre,
    bridge_movie_genre,
    #DELTA
    updated_movies,
    #FACT
    fact_movie_reviews,
    fact_show_reviews,
    movie_performace_fact,
    #SNAPSHOTS
    new_movies,
    
)

# import asset checks
from .checks import (
    popular_shows as popular_shows_asset_checks,
    fact_show_reviews_asset_checks
)

# import constants
from .constants import (
    # resources
    postgres_connection_string,
    tmdb_api_key,
    tmdb_base_url,
    # kernels
    POPULAR_SHOWS,
    POPULAR_PEOPLE,
    POPULAR_MOVIES,
    # Dimension Tables
    DIM_POPULAR_PEOPLE_HISTORY,
    DIM_POPULAR_PEOPLE,
    DIM_MOVIE_GENRES,
    DIM_TV_GENRES,
    MOVIE_DIM,
    GENRES_DIM,
    CAST_DIM,
    CREW_DIM,
    CREATE_TABLES,
    # Bridge Tables
    BRIDGE_PERSON_PROJECT,
    BRIDGE_SHOW_GENRES,
    BRIDGE_MOVIE_GENRES,
    # Delta Tables
    UPDATED_MOVIES,
    #Fact Tables
    FACT_MOVIE_REVIEWS,
    FACT_SHOW_REVIEWS,
    MOVIE_PERFORMANCE_FACT,
    #Augmentations
    NEW_MOVIES,
)

# import jobs 
from .jobs.jobs import (
    popular_shows_job,
    popular_people_job,
    popular_movies_job,
    
    #dimension jobs
    dim_popular_people_job,
    dim_popular_people_history_job,
    dim_movie_genres_job,
    dim_tv_genres_job,
    
    #bridge jobs 
    bridge_person_project_job,
    bridge_show_genre_job,
    bridge_movie_genre_job,
    
    #fact jobs
    movie_performance_fact_job,
    fact_movie_reviews_job,
    fact_show_reviews_job,
    
    #augmentation/update jobs
    update_new_and_updated_movies_job,
)

# import schedules
from .schedules.schedules import (
    popular_shows_schedule,
    popular_people_schedule,
    popular_movie_schedule,
    dim_movie_genres_schedule,
    dim_tv_genres_schedule,
    fact_movie_review_schedule,
    fact_show_review_schedule,
    update_new_and_updated_movies_schedule
)

# import sensors
from .sensors.sensors import (
    trigger_dim_popular_people_assets, 
    trigger_dim_popular_people_history_assets,
    trigger_bridge_person_project_assets,
    trigger_bridge_show_genre_assets,
    trigger_bridge_movie_genre_assets,
    trigger_movie_performance_fact_assets
    )


# load assets from each module
popular_shows_assets = load_assets_from_package_module(popular_shows, group_name=POPULAR_SHOWS)
popular_people_assets = load_assets_from_package_module(popular_people, group_name=POPULAR_PEOPLE)
popular_movies_assets = load_assets_from_package_module(popular_movies, group_name=POPULAR_MOVIES)

#dimension assets
dim_popular_people_assets = load_assets_from_package_module(dim_popular_people, group_name=DIM_POPULAR_PEOPLE)
dim_popular_people_history_assets = load_assets_from_package_module(dim_popular_people_history, group_name=DIM_POPULAR_PEOPLE_HISTORY)
dim_movie_genres_assets = load_assets_from_package_module(dim_movie_genres, group_name=DIM_MOVIE_GENRES)
dim_tv_genres_assets = load_assets_from_package_module(dim_tv_genres, group_name=DIM_TV_GENRES)
CREATE_TABLES_assets = load_assets_from_package_module(create_tables, group_name=CREATE_TABLES) 
movie_dim_assets = load_assets_from_package_module(movie_dim, group_name=MOVIE_DIM)  
genres_dim_assets = load_assets_from_package_module(genres_dim, group_name=GENRES_DIM)
cast_dim_assets = load_assets_from_package_module(cast_dim, group_name=CAST_DIM)
crew_dim_assets = load_assets_from_package_module(crew_dim, group_name=CREW_DIM)

#bridge assets
bridge_person_project_assets = load_assets_from_package_module(bridge_person_project, group_name=BRIDGE_PERSON_PROJECT)
bridge_show_genre_assets = load_assets_from_package_module(bridge_show_genre, group_name=BRIDGE_SHOW_GENRES)
bridge_movie_genre_assets = load_assets_from_package_module(bridge_movie_genre, group_name=BRIDGE_MOVIE_GENRES)

#delta assets
updated_movies_assets = load_assets_from_package_module(updated_movies, group_name=UPDATED_MOVIES)

#fact table assets
fact_movie_reviews_assets = load_assets_from_package_module(fact_movie_reviews, group_name=FACT_MOVIE_REVIEWS)
fact_show_reviews_assets = load_assets_from_package_module(fact_show_reviews, group_name=FACT_SHOW_REVIEWS)
movie_performace_fact_assets = load_assets_from_package_module(movie_performace_fact, group_name=MOVIE_PERFORMANCE_FACT)

#snapshot assets
new_movies_assets = load_assets_from_package_module(new_movies, group_name=NEW_MOVIES)


#asset checks
popular_shows_asset_checks = load_asset_checks_from_package_module(popular_shows_asset_checks)
fact_show_reviews_asset_checks = load_asset_checks_from_package_module(fact_show_reviews_asset_checks)

#combine assets for deinition file
asset_defs = [
    #Upstream Jobs
    *popular_shows_assets,
    *popular_people_assets, 
    *popular_movies_assets,
    
    #Dimension Tables
    *dim_popular_people_assets,
    *dim_popular_people_history_assets,
    *dim_movie_genres_assets,
    *dim_tv_genres_assets,
    *movie_dim_assets,
    *CREATE_TABLES_assets,
    *genres_dim_assets,
    *cast_dim_assets,
    *crew_dim_assets,
    #Bridge Tables
    *bridge_person_project_assets,
    *bridge_show_genre_assets,
    *bridge_movie_genre_assets,
    
    #Delta Tables
    *updated_movies_assets,
    
    #Fact Tables
    *fact_movie_reviews_assets,
    *fact_show_reviews_assets,
    *movie_performace_fact_assets,
    #Snapshot Tables
    *new_movies_assets,
]

asset_check_defs = [
    *popular_shows_asset_checks,
    *fact_show_reviews_asset_checks
]

# combine jobs for definition file
job_defs = [
    popular_shows_job,
    popular_people_job,
    popular_movies_job,
    dim_popular_people_job,
    dim_popular_people_history_job,
    dim_movie_genres_job,
    dim_tv_genres_job,
    bridge_person_project_job,
    bridge_show_genre_job,
    bridge_movie_genre_job,
    fact_movie_reviews_job,
    fact_show_reviews_job,
    update_new_and_updated_movies_job,
    movie_performance_fact_job
    ]

# combine schedules for definition file
schedule_defs = [
    popular_shows_schedule,
    popular_people_schedule,
    popular_movie_schedule,
    dim_movie_genres_schedule,
    dim_tv_genres_schedule,
    fact_movie_review_schedule,
    fact_show_review_schedule,
    update_new_and_updated_movies_schedule
]


#combine sensors for definition file
sensor_defs = [
    trigger_dim_popular_people_assets,
    trigger_dim_popular_people_history_assets,
    trigger_bridge_person_project_assets,
    trigger_bridge_show_genre_assets,
    trigger_bridge_movie_genre_assets
]


# Resource definitions
resource_defs = {
    "tmdb": TMDBResource(
        api_key=tmdb_api_key,
        base_url=tmdb_base_url
    ),
    "postgres": PostgreSQLResource(
        connection_string=postgres_connection_string
    )
}

#Definition declarations
defs = Definitions(
    assets=asset_defs,
    asset_checks=asset_check_defs,
    resources=resource_defs,
    jobs=job_defs,
    schedules=schedule_defs,
    sensors=sensor_defs
)
