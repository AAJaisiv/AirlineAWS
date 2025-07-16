"""

This script is the main bridge between our pipeline and the outside world of aviation data.
It fetches flights, airports, and airlines info from public APIs, handles rate limits, retries,
and pushes the results to S3. If you ever wondered where our data journey begins—it's here.

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

# Set up logging so we can see what's happening under the hood
logger = logging.getLogger(__name__)

class AviationAPIClient:
    """
    This class is our go-to for anything related to fetching aviation data from external APIs.
    It knows how to talk to Aviation Stack and OpenSky, respects their rate limits, and
    gracefully handles hiccups (like network errors or bad responses).
    """
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Loads config, sets up API keys, and prepares a session with retry logic.
        """
        self.config = self._load_config(config_path)
        self.aviation_stack_key = os.getenv('AVIATION_STACK_API_KEY')
        self.opensky_username = os.getenv('OPENSKY_USERNAME')
        self.opensky_password = os.getenv('OPENSKY_PASSWORD')
        self.aviation_stack_base_url = self.config['api']['aviation_stack']['base_url']
        self.opensky_base_url = self.config['api']['opensky']['base_url']
        self.rate_limit_delay = 1.0  # Don't hammer the API—wait a sec between calls
        self.max_retries = self.config['data_processing']['batch']['max_retries']
        self.session = self._create_session()
        self.last_request_time = 0
        logger.info("Aviation API Client ready to fetch data!")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Loads YAML config. If it fails, we want to know why.
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"Config loaded from {config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"YAML error: {e}")
            raise

    def _create_session(self) -> requests.Session:
        """
        Sets up a requests session with retry logic. This way, if the API is flaky, we don't give up right away.
        """
        session = requests.Session()
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
        Waits if we're calling the API too quickly. Keeps us in good standing with providers.
        """
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, url: str, params: Optional[Dict] = None, auth: Optional[tuple] = None) -> Dict[str, Any]:
        """
        Actually makes the HTTP GET request, with error handling and rate limiting.
        """
        self._rate_limit()
        try:
            response = self.session.get(url, params=params, auth=auth, timeout=30)
            response.raise_for_status()
            data = response.json()
            if 'error' in data:
                # The API is telling us something went wrong
                raise ValueError(f"API Error: {data['error']}")
            logger.info(f"API call to {url} succeeded.")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Network or HTTP error: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Bad JSON in response: {e}")
            raise
        except ValueError as e:
            logger.error(f"API returned error: {e}")
            raise

    def get_flights(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Fetches real-time flight data from Aviation Stack. Limit is capped at 100 by the API.
        """
        if not self.aviation_stack_key:
            raise ValueError("Missing Aviation Stack API key. Set AVIATION_STACK_API_KEY in your environment.")
        params = {
            'access_key': self.aviation_stack_key,
            'limit': min(limit, 100),
            'offset': offset
        }
        url = f"{self.aviation_stack_base_url}{self.config['api']['endpoints']['flights']}"
        try:
            data = self._make_request(url, params=params)
            flights = data.get('data', [])
            logger.info(f"Fetched {len(flights)} flights.")
            return flights
        except Exception as e:
            logger.error(f"Couldn't fetch flights: {e}")
            raise

    def get_airports(self, country_code: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetches airport info. You can filter by country if you want.
        """
        if not self.aviation_stack_key:
            raise ValueError("Missing Aviation Stack API key. Set AVIATION_STACK_API_KEY in your environment.")
        params = {'access_key': self.aviation_stack_key}
        if country_code:
            params['country_code'] = country_code
        url = f"{self.aviation_stack_base_url}{self.config['api']['endpoints']['airports']}"
        try:
            data = self._make_request(url, params=params)
            airports = data.get('data', [])
            logger.info(f"Fetched {len(airports)} airports.")
            return airports
        except Exception as e:
            logger.error(f"Couldn't fetch airports: {e}")
            raise

    def get_airlines(self) -> List[Dict[str, Any]]:
        """
        Fetches airline info from Aviation Stack.
        """
        if not self.aviation_stack_key:
            raise ValueError("Missing Aviation Stack API key. Set AVIATION_STACK_API_KEY in your environment.")
        params = {'access_key': self.aviation_stack_key}
        url = f"{self.aviation_stack_base_url}{self.config['api']['endpoints']['airlines']}"
        try:
            data = self._make_request(url, params=params)
            airlines = data.get('data', [])
            logger.info(f"Fetched {len(airlines)} airlines.")
            return airlines
        except Exception as e:
            logger.error(f"Couldn't fetch airlines: {e}")
            raise

    def get_opensky_states(self, icao24: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Pulls live aircraft state vectors from OpenSky. Optionally filter by aircraft ICAO24.
        """
        url = f"{self.opensky_base_url}/states/all"
        auth = (self.opensky_username, self.opensky_password) if self.opensky_username and self.opensky_password else None
        params = {}
        if icao24:
            params['icao24'] = icao24
        try:
            data = self._make_request(url, params=params, auth=auth)
            states = data.get('states', [])
            logger.info(f"Fetched {len(states)} aircraft states from OpenSky.")
            return states
        except Exception as e:
            logger.error(f"Couldn't fetch OpenSky states: {e}")
            raise

    def get_opensky_flights(self, begin: int, end: int, icao24: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Gets historical flights from OpenSky between two UNIX timestamps. Optionally filter by aircraft.
        """
        url = f"{self.opensky_base_url}/flights/all"
        auth = (self.opensky_username, self.opensky_password) if self.opensky_username and self.opensky_password else None
        params = {'begin': begin, 'end': end}
        if icao24:
            params['icao24'] = icao24
        try:
            data = self._make_request(url, params=params, auth=auth)
            flights = data.get('flights', [])
            logger.info(f"Fetched {len(flights)} OpenSky flights.")
            return flights
        except Exception as e:
            logger.error(f"Couldn't fetch OpenSky flights: {e}")
            raise

    def collect_daily_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Orchestrates a daily data pull from all sources. This is the main entry point for our pipeline's data ingestion.
        """
        logger.info("Starting daily data collection...")
        all_data = {}
        try:
            all_data['flights'] = self.get_flights()
            all_data['airports'] = self.get_airports()
            all_data['airlines'] = self.get_airlines()
            all_data['opensky_states'] = self.get_opensky_states()
            logger.info("Daily data collection complete.")
            return all_data
        except Exception as e:
            logger.error(f"Daily data collection failed: {e}")
            raise

    def save_to_s3(self, data: Dict[str, List[Dict[str, Any]]], bucket_name: str, prefix: str) -> None:
        """
        Saves the collected data to S3. Each data type gets its own file.
        """
        s3 = boto3.client('s3')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        for key, records in data.items():
            s3_key = f"{prefix}/{key}_{timestamp}.json"
            try:
                s3.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=json.dumps(records),
                    ContentType='application/json'
                )
                logger.info(f"Saved {key} data to s3://{bucket_name}/{s3_key}")
            except ClientError as e:
                logger.error(f"Failed to save {key} to S3: {e}")
                raise

    def get_api_usage_stats(self) -> Dict[str, Any]:
        """
        Returns a summary of API usage. (Stub for now—could be expanded to track quotas, etc.)
        """
        # TODO: Implement real usage tracking if needed
        return {
            "aviation_stack_calls": "unknown",
            "opensky_calls": "unknown"
        }

def main():
    """
    If you run this file directly, we'll do a quick data pull and print a summary.
    """
    client = AviationAPIClient()
    try:
        data = client.collect_daily_data()
        print(f"Pulled {sum(len(v) for v in data.values())} records from all sources.")
    except Exception as e:
        print(f"Something went wrong: {e}")

if __name__ == "__main__":
    main() 