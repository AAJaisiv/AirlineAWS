"""
Aviation API Client for AirlineAWS Project

This module provides a comprehensive client for interacting with aviation APIs
to collect flight data, airport information, and airline details for the
AirlineAWS data engineering pipeline.

Author: A Abhinav Jaisiv
Date: 2025
"""

import os
import time
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3
from botocore.exceptions import ClientError
import yaml

# Configure logging
logger = logging.getLogger(__name__)


class AviationAPIClient:
    """
    A comprehensive client for interacting with aviation APIs.
    
    This class handles API authentication, rate limiting, error handling,
    and data collection from multiple aviation data sources including
    Aviation Stack API and OpenSky Network API.
    
    Attributes:
        aviation_stack_key (str): API key for Aviation Stack
        opensky_username (str): Username for OpenSky Network
        opensky_password (str): Password for OpenSky Network
        rate_limit_delay (float): Delay between API calls in seconds
        max_retries (int): Maximum number of retry attempts
        session (requests.Session): HTTP session for API calls
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the Aviation API Client.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.aviation_stack_key = os.getenv('AVIATION_STACK_API_KEY')
        self.opensky_username = os.getenv('OPENSKY_USERNAME')
        self.opensky_password = os.getenv('OPENSKY_PASSWORD')
        
        # API configuration
        self.aviation_stack_base_url = self.config['api']['aviation_stack']['base_url']
        self.opensky_base_url = self.config['api']['opensky']['base_url']
        self.rate_limit_delay = 1.0  # 1 second delay between calls
        self.max_retries = self.config['data_processing']['batch']['max_retries']
        
        # Initialize session with retry strategy
        self.session = self._create_session()
        
        # Rate limiting
        self.last_request_time = 0
        
        logger.info("Aviation API Client initialized successfully")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path (str): Path to configuration file
            
        Returns:
            Dict[str, Any]: Configuration dictionary
            
        Raises:
            FileNotFoundError: If configuration file not found
            yaml.YAMLError: If configuration file is invalid
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML configuration: {e}")
            raise
    
    def _create_session(self) -> requests.Session:
        """
        Create HTTP session with retry strategy.
        
        Returns:
            requests.Session: Configured HTTP session
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _rate_limit(self):
        """
        Implement rate limiting between API calls.
        """
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: Optional[Dict] = None, 
                     auth: Optional[tuple] = None) -> Dict[str, Any]:
        """
        Make HTTP request with error handling and rate limiting.
        
        Args:
            url (str): API endpoint URL
            params (Optional[Dict]): Query parameters
            auth (Optional[tuple]): Authentication credentials
            
        Returns:
            Dict[str, Any]: API response data
            
        Raises:
            requests.RequestException: If API request fails
            ValueError: If API returns error response
        """
        self._rate_limit()
        
        try:
            response = self.session.get(url, params=params, auth=auth, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API-specific error responses
            if 'error' in data:
                raise ValueError(f"API Error: {data['error']}")
            
            logger.info(f"Successful API request to {url}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise
        except ValueError as e:
            logger.error(f"API returned error: {e}")
            raise
    
    def get_flights(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get real-time flight data from Aviation Stack API.
        
        Args:
            limit (int): Number of flights to retrieve (max 100)
            offset (int): Offset for pagination
            
        Returns:
            List[Dict[str, Any]]: List of flight data
            
        Raises:
            ValueError: If API key is not configured
        """
        if not self.aviation_stack_key:
            raise ValueError("Aviation Stack API key not configured")
        
        params = {
            'access_key': self.aviation_stack_key,
            'limit': min(limit, 100),  # API limit is 100
            'offset': offset
        }
        
        url = f"{self.aviation_stack_base_url}{self.config['api']['endpoints']['flights']}"
        
        try:
            data = self._make_request(url, params=params)
            flights = data.get('data', [])
            
            logger.info(f"Retrieved {len(flights)} flights from Aviation Stack API")
            return flights
            
        except Exception as e:
            logger.error(f"Failed to get flights: {e}")
            raise
    
    def get_airports(self, country_code: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get airport information from Aviation Stack API.
        
        Args:
            country_code (Optional[str]): ISO country code to filter airports
            
        Returns:
            List[Dict[str, Any]]: List of airport data
            
        Raises:
            ValueError: If API key is not configured
        """
        if not self.aviation_stack_key:
            raise ValueError("Aviation Stack API key not configured")
        
        params = {
            'access_key': self.aviation_stack_key
        }
        
        if country_code:
            params['country_code'] = country_code
        
        url = f"{self.aviation_stack_base_url}{self.config['api']['endpoints']['airports']}"
        
        try:
            data = self._make_request(url, params=params)
            airports = data.get('data', [])
            
            logger.info(f"Retrieved {len(airports)} airports from Aviation Stack API")
            return airports
            
        except Exception as e:
            logger.error(f"Failed to get airports: {e}")
            raise
    
    def get_airlines(self) -> List[Dict[str, Any]]:
        """
        Get airline information from Aviation Stack API.
        
        Returns:
            List[Dict[str, Any]]: List of airline data
            
        Raises:
            ValueError: If API key is not configured
        """
        if not self.aviation_stack_key:
            raise ValueError("Aviation Stack API key not configured")
        
        params = {
            'access_key': self.aviation_stack_key
        }
        
        url = f"{self.aviation_stack_base_url}{self.config['api']['endpoints']['airlines']}"
        
        try:
            data = self._make_request(url, params=params)
            airlines = data.get('data', [])
            
            logger.info(f"Retrieved {len(airlines)} airlines from Aviation Stack API")
            return airlines
            
        except Exception as e:
            logger.error(f"Failed to get airlines: {e}")
            raise
    
    def get_opensky_states(self, icao24: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get aircraft states from OpenSky Network API.
        
        Args:
            icao24 (Optional[str]): ICAO24 address of aircraft
            
        Returns:
            List[Dict[str, Any]]: List of aircraft states
            
        Raises:
            ValueError: If OpenSky credentials are not configured
        """
        if not self.opensky_username or not self.opensky_password:
            raise ValueError("OpenSky Network credentials not configured")
        
        params = {}
        if icao24:
            params['icao24'] = icao24
        
        url = f"{self.opensky_base_url}/states/all"
        auth = (self.opensky_username, self.opensky_password)
        
        try:
            data = self._make_request(url, params=params, auth=auth)
            states = data.get('states', [])
            
            logger.info(f"Retrieved {len(states)} aircraft states from OpenSky Network")
            return states
            
        except Exception as e:
            logger.error(f"Failed to get aircraft states: {e}")
            raise
    
    def get_opensky_flights(self, begin: int, end: int, 
                          icao24: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get flight data from OpenSky Network API.
        
        Args:
            begin (int): Start time as Unix timestamp
            end (int): End time as Unix timestamp
            icao24 (Optional[str]): ICAO24 address of aircraft
            
        Returns:
            List[Dict[str, Any]]: List of flight data
            
        Raises:
            ValueError: If OpenSky credentials are not configured
        """
        if not self.opensky_username or not self.opensky_password:
            raise ValueError("OpenSky Network credentials not configured")
        
        params = {
            'begin': begin,
            'end': end
        }
        
        if icao24:
            params['icao24'] = icao24
        
        url = f"{self.opensky_base_url}/flights/all"
        auth = (self.opensky_username, self.opensky_password)
        
        try:
            data = self._make_request(url, params=params, auth=auth)
            flights = data.get('data', [])
            
            logger.info(f"Retrieved {len(flights)} flights from OpenSky Network")
            return flights
            
        except Exception as e:
            logger.error(f"Failed to get flights: {e}")
            raise
    
    def collect_daily_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect comprehensive daily data from all available APIs.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary containing all collected data
        """
        logger.info("Starting daily data collection from aviation APIs")
        
        collected_data = {
            'flights': [],
            'airports': [],
            'airlines': [],
            'aircraft_states': []
        }
        
        try:
            # Collect flight data
            logger.info("Collecting flight data...")
            flights = self.get_flights(limit=100)
            collected_data['flights'] = flights
            
            # Collect airport data
            logger.info("Collecting airport data...")
            airports = self.get_airports()
            collected_data['airports'] = airports
            
            # Collect airline data
            logger.info("Collecting airline data...")
            airlines = self.get_airlines()
            collected_data['airlines'] = airlines
            
            # Collect aircraft states from OpenSky
            logger.info("Collecting aircraft states...")
            try:
                states = self.get_opensky_states()
                collected_data['aircraft_states'] = states
            except Exception as e:
                logger.warning(f"Failed to collect aircraft states: {e}")
            
            logger.info("Daily data collection completed successfully")
            return collected_data
            
        except Exception as e:
            logger.error(f"Daily data collection failed: {e}")
            raise
    
    def save_to_s3(self, data: Dict[str, List[Dict[str, Any]]], 
                   bucket_name: str, prefix: str) -> None:
        """
        Save collected data to S3 bucket.
        
        Args:
            data (Dict[str, List[Dict[str, Any]]]): Data to save
            bucket_name (str): S3 bucket name
            prefix (str): S3 key prefix
            
        Raises:
            ClientError: If S3 operation fails
        """
        s3_client = boto3.client('s3')
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        try:
            for data_type, records in data.items():
                if records:
                    # Create JSON file
                    filename = f"{data_type}_{timestamp}.json"
                    key = f"{prefix}/{data_type}/{filename}"
                    
                    # Convert to JSON string
                    json_data = json.dumps(records, default=str)
                    
                    # Upload to S3
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=key,
                        Body=json_data,
                        ContentType='application/json'
                    )
                    
                    logger.info(f"Saved {len(records)} {data_type} records to s3://{bucket_name}/{key}")
            
        except ClientError as e:
            logger.error(f"Failed to save data to S3: {e}")
            raise
    
    def get_api_usage_stats(self) -> Dict[str, Any]:
        """
        Get API usage statistics and rate limit information.
        
        Returns:
            Dict[str, Any]: API usage statistics
        """
        stats = {
            'aviation_stack': {
                'configured': bool(self.aviation_stack_key),
                'rate_limit': self.config['api']['aviation_stack']['rate_limit']
            },
            'opensky': {
                'configured': bool(self.opensky_username and self.opensky_password),
                'rate_limit': self.config['api']['opensky']['rate_limit']
            },
            'last_request_time': self.last_request_time,
            'session_requests': len(list(self.session.adapters.keys()))
        }
        
        return stats


def main():
    """
    Main function for testing the Aviation API Client.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize client
        client = AviationAPIClient()
        
        # Test API connectivity
        logger.info("Testing API connectivity...")
        
        # Get API usage stats
        stats = client.get_api_usage_stats()
        logger.info(f"API Usage Stats: {json.dumps(stats, indent=2)}")
        
        # Collect sample data
        logger.info("Collecting sample data...")
        sample_data = client.collect_daily_data()
        
        # Print summary
        for data_type, records in sample_data.items():
            logger.info(f"Collected {len(records)} {data_type} records")
        
        logger.info("Aviation API Client test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


if __name__ == "__main__":
    main() 