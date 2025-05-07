from dagster import ConfigurableResource
import requests
from typing import Dict, Any

class TMDBResource(ConfigurableResource):
    """Resource for TMDB API access."""
    api_key: str
    base_url: str = "https://api.themoviedb.org/3"
    
    def get_popular_shows(self, page: int = 1) -> Dict[str, Any]:
        """
        Fetch popular TV shows from TMDB API.
        
        Args:
            page: Page number to fetch
            
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/tv/popular"
        params = {
            'api_key': self.api_key,
            'page': page
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_popular_people(self, page: int = 1) -> Dict[str, Any]:
        """
        Fetch popular people from TMDB API.
        
        Args:
            page: Page number to fetch
            
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/person/popular"
        params = {
            'api_key': self.api_key,
            'page': page
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_popular_movies(self, page: int = 1) -> Dict[str, Any]:
        """
        Fetch popular movies from TMDB API.
        
        Args:
            page: Page number to fetch
            
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/movie/popular"
        params = {
            'api_key': self.api_key,
            'page': page
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_movie_reviews(self, movie_id: int, page: int = 1) -> Dict[str, Any]:
        """
        Fetch reviews for a specific movie from TMDB API.
        
        Args:
            movie_id: TMDB Movie ID
            page: Page number to fetch
            
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/movie/{movie_id}/reviews"
        params = {
            'api_key': self.api_key,
            'page': page
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_show_reviews(self, series_id: int, page: int = 1) -> Dict[str, Any]:
        """
        Fetch reviews for a specific shows from TMDB API.
        
        Args:
            series_id: TMDB Movie ID
            page: Page number to fetch
            
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/tv/{series_id}/reviews"
        params = {
            'api_key': self.api_key,
            'page': page
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()

    def get_genres_list(self, medium: str) -> Dict[str, Any]:
        """
        Fetch list of genres from TMDB API.
        
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/genre/{medium}/list"
        params = {
            'api_key': self.api_key
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def discover_movies(self, release_date:str, page: int = 1, language: str = "en") -> Dict[str, Any]:
        """
        Discover movies based on certain criteria.
        
        Args:
            page: Page number to fetch
            sort_by: Sort criteria
            language: filter for en-us
            
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/discover/movie"
        params = {
            'api_key': self.api_key,
            'page': page,
            'primary_release_date.gte': release_date,
            'primary_release_date.lte': release_date,
            'with_original_language': language,
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()

    def get_person_details(self, person_id: int) -> Dict[str, Any]:
        """
        Fetch detailed information for a specific person from TMDB API.
        
        Args:
            person_id: TMDB Person ID
                
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/person/{person_id}"
        params = {
            'api_key': self.api_key
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()

    def get_movie_details(self, movie_id: int, append_to_response: str = None) -> Dict[str, Any]:
        """
        Fetch detailed information for a specific movie from TMDB API.
        
        Args:
            movie_id: TMDB Movie ID
            append_to_response: Additional data to include in the response
                
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/movie/{movie_id}"
        params = {
            'api_key': self.api_key
        }
        
        if append_to_response:
            params['append_to_response'] = append_to_response
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_genres_list(self, medium: str) -> Dict[str, Any]:
        """
        Fetch list of genres from TMDB API.
        
        Args:
            medium: Type of media ("movie" or "tv")
                
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/genre/{medium}/list"
        params = {
            'api_key': self.api_key
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_changed_movies(self, start_date:str, end_date:str, page: int = 1) -> Dict[str, Any]:
        """
        Fetch list of changed movies from TMDB API.
        
        Args:
            page: Page number to fetch
            
        Returns:
            API response as dictionary
        """
        endpoint = f"{self.base_url}/movie/changes"
        params = {
            'api_key': self.api_key,
            'page': page,
            'start_date': start_date,
            'end_date': end_date,
        }
        
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()