import dagster as dg
import time
from typing import List, Dict, Any
import json
from ...constants import movie_aug_partitions_def

@dg.asset(name="updated_movies_api", 
        required_resource_keys={"tmdb"},
        kinds={"python", "api", "task"},
        tags={"layer": "staging"},
        partitions_def=movie_aug_partitions_def
        )
def updated_movies_api(context: dg.AssetExecutionContext):
    """
    This asset fetches the list of movies that have changed since the last run.
    It uses the TMDB API to get the list of changed movies.
    """
    
    start_time = time.time()
    # Get the TMDB API client from the resources
    tmdb = context.resources.tmdb
    page = 1
    total_pages = 1
    changed_movies = []
    
    # Loop through the pages to fetch all changed movies
    while page <= total_pages:
        context.log.info(f"Fetching page {page} of changed movies")
        # todays date
        today = time.strftime("%Y-%m-%d")
        partition_date_str = context.partition_key
        
        #last weeks date
        last_week = time.strftime("%Y-%m-%d", time.localtime(time.time() - 7 * 24 * 60 * 60))
        
        
        # Fetch the changed movies from TMDB API
        try:
            changed_movie_response = tmdb.get_changed_movies(page=page, start_date=partition_date_str, end_date=partition_date_str)
            
        except Exception as e:
            context.log.error(f"Error fetching changed movies: {e}")
            break
        
        changed_movie_data = changed_movie_response.get("results", [])
        total_pages = changed_movie_response.get("total_pages", 1)
        
        total_results = changed_movie_response.get("total_results", 0)
        
        context.log.info(f"Total pages: {total_pages}")
        context.log.info(f"Total results: {total_results}")
        
        # Append the changed movies to the list
        for changed_movie in changed_movie_data:
            changed_movies.append(changed_movie.get("id"))
        page += 1
    
    end_time = time.time()
    duration = end_time - start_time
    context.log.info(f"Execution time: {duration} seconds")
    
    return dg.Output(
        value=changed_movies,
        metadata={
            "execution_time": duration,
            "table": "tmdb_data.movie_aug",
            "operation": "fetch",
            "records_fetched": len(changed_movies)
        }
    )
        
@dg.asset(name="updated_movie_details",
        required_resource_keys={"tmdb"},
        kinds={"python", "postgres", "task"},
        tags={'layer': "silver"},
        deps=[dg.AssetKey("updated_movies_api")],
        partitions_def=movie_aug_partitions_def
)
def updated_movie_details(
    context: dg.AssetExecutionContext, 
    updated_movies_api: List[int]):
    """
    This asset inserts the changed movies into the Postgres database.
    """
    start_time = time.time()
    
    tmdb = context.resources.tmdb
    
    # context.log.info(f"Updated Movies API: {updated_movies_api}")
    
    updated_movie_details = updated_movies_api
    movie_data = []
    
    for movie_id in updated_movie_details:
        try:
            movie_details = tmdb.get_movie_details(movie_id, append_to_response="credits,production_companies")
            
            if not movie_details or not movie_details.get("id"):
                context.log.warning(f"Skipping Movie ID {movie_id}: Invalid or empty response from API.")
                continue
            # context.log.info(f"Movie Details: {json.dumps(movie_details, indent=2)}")
            budget = movie_details.get("budget", 0)
            profit = movie_details.get("revenue", 0) - budget
            roi = (profit / budget) * 100 if budget > 0 else 0
            is_profitable = profit > 0
            
            genre_list = movie_details.get("genres", [])
            cast_list = movie_details.get("credits", {}).get("cast", [])
            crew_list = movie_details.get("credits", {}).get("crew", [])
            
            # Prepare data for loading
            movie_data.append({
                "movie_id": movie_details.get("id"),
                "release_date": movie_details.get("release_date"),
                "partiton_date": context.partition_key,
                "title": movie_details.get("title"),
                "original_title": movie_details.get("original_title"),
                "original_language": movie_details.get("original_language"),
                "overview": movie_details.get("overview"),
                "tagline": movie_details.get("tagline"),
                "status": movie_details.get("status"),
                "runtime": movie_details.get("runtime"),
                "is_adult": movie_details.get("adult", False),
                "is_video": movie_details.get("video", False),
                "poster_path": movie_details.get("poster_path"),
                "backdrop_path": movie_details.get("backdrop_path"),
                "homepage": movie_details.get("homepage"),
                "imdb_id": movie_details.get("imdb_id"),
                "tmdb_popularity": movie_details.get("popularity"),
                "vote_count": movie_details.get("vote_count"),
                "vote_average": movie_details.get("vote_average"),
                "revenue": movie_details.get("revenue"),
                "profit": profit,
                "budget": budget,
                "roi": roi,
                "is_profitable": is_profitable,
                "genres": genre_list, 
                "cast": cast_list,
                "crew": crew_list,
                
            })
            # context.log.info(f"Prepared data for Movie ID {movie_id}: {movie_details}")
        
        except Exception as e:
            context.log.error(f"Error fetching details for Movie ID {movie_id}: {e}")
        
        
    end_time = time.time()
    duration = end_time - start_time

    return dg.Output(
        value=movie_data,
        metadata={
            "execution_time": duration,
            "movies_prepared": len(updated_movie_details),
            "api": "movie/movie_id",
            "operation": "request"
        }
    )