import os
from dotenv import load_dotenv
import urllib.parse

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


# Rankings sources
KTC_ROOKIE_PICKS = "ktc_rookie_picks"
FN_RANKINGS = "fantasy_navigator_rankings"
KTC_RANKINGS = "keep_trade_cut_rankings"
FC_RANKINGS = "fantasy_calc_rankings"
DP_RANKINGS = "dynasty_processing_rankings"
DD_RANKINGS = "dynasty_daddy_rankings"
FN_RANKINGS_HISTORY = "fantasy_navigator_rankings_history"

# Projections sources
CBS_PROJECTIONS = "cbs_projections"
ESPN_PROJECTIONS = "espn_projections"
NFL_PROJECTIONS = "nfl_projections"