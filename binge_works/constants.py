# Standard libraries for all clusters
# STANDARD_LIBRARIES = [
#     {"pypi": {"package": "dagster-pipes"}},
#     {"pypi": {"package": "requests"}},  # Add this line
#     {"pypi": {"package": "simple-salesforce"}},  # Add this since your code uses it
#     {"pypi": {"package": "PyJWT"}},  # Add this since your code uses it
#     {"whl": "dbfs:/FileStore/jars/pipeline_utils-0.1-py3-none-any.whl"},
#     {"whl": "dbfs:/FileStore/jars/kvutils-0.0.1-py3-none-any.whl"},
#     {"whl": "dbfs:/FileStore/jars/salesforce_utils-0.1.0-py3-none-any.whl"}
# ]


import os
from dotenv import load_dotenv
import urllib.parse
from dagster import DailyPartitionsDefinition
import datetime

#kerenels
POPULAR_SHOWS = "popular_shows"
POPULAR_PEOPLE = "popular_people"
POPULAR_MOVIES = "popular_movies"

#Dimesnions
DIM_POPULAR_PEOPLE = "dim_popular_people"
DIM_POPULAR_PEOPLE_HISTORY = "dim_popular_people_history"
DIM_MOVIE_GENRES = "dim_movie_genres"
DIM_TV_GENRES = "dim_tv_genres"
CREATE_TABLES = "create_tables"
MOVIE_DIM = "movie_dim"
GENRES_DIM = "genres_dim"
CAST_DIM = "cast_dim"
CREW_DIM = "crew_dim"

#Bridges
BRIDGE_PERSON_PROJECT = "bridge_person_project"
BRIDGE_SHOW_GENRES = "bridge_show_genres"
BRIDGE_MOVIE_GENRES = "bridge_movie_genres"

#Facts
FACT_MOVIE_REVIEWS = "fact_movie_reviews"
FACT_SHOW_REVIEWS = "fact_show_reviews"
MOVIE_PERFORMANCE_FACT = "movie_performance_fact"

#Delta Pulls
UPDATED_MOVIES   = "updated_movies"

#snapshot
NEW_MOVIES = "new_movies"

# Load environment variables from the .env file
load_dotenv(dotenv_path=".env")

#API CONNECTION VARS
tmdb_api_key = os.getenv("TMDB_API_KEY")
if not tmdb_api_key:
    raise ValueError("TMDB_API_KEY environment variable is not set.")

tmdb_base_url = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")



# POSTGRES CONNECTION VARS
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
sslmode = os.getenv("SSLMODE")

# URL-encode the password to handle special characters
encoded_password = urllib.parse.quote_plus(db_password)

# Construct the PostgreSQL connection string with encoded password
postgres_connection_string = f"postgresql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"

daily_partitions_def = DailyPartitionsDefinition(
    start_date="2025-03-26"
)

movie_aug_partitions_def = DailyPartitionsDefinition(
    start_date="1888-01-01",
)