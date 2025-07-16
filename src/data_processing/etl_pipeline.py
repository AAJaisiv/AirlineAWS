"""
ETL Pipeline for AirlineAWS Project

This module provides comprehensive ETL (Extract, Transform, Load) functionality
for processing aviation data from raw sources to business-ready datasets.

Author: A Abhinav Jaisiv
Date: 2025
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import yaml
from pydantic import BaseModel, ValidationError

# Configure logging
logger = logging.getLogger(__name__)


class DataQualityConfig(BaseModel):
    """Configuration for data quality checks."""
    min_completeness: float = 0.95
    max_null_percentage: float = 0.05
    required_fields: List[str] = []
    data_types: Dict[str, str] = {}


class ETLPipeline:
    """
    Comprehensive ETL pipeline for processing aviation data.
    
    This class handles the transformation of raw data from Bronze layer
    to cleaned data in Silver layer, and business logic in Gold layer.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the ETL pipeline.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.s3_client = boto3.client('s3')
        self.data_quality_config = DataQualityConfig(
            **self.config['data_processing']['quality']
        )
        
        # S3 bucket names
        self.raw_bucket = self.config['aws']['s3']['raw_bucket']
        self.processed_bucket = self.config['aws']['s3']['processed_bucket']
        
        logger.info("ETL Pipeline initialized successfully")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path (str): Path to configuration file
            
        Returns:
            Dict[str, Any]: Configuration dictionary
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
    
    def extract_from_bronze(self, data_type: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Extract data from Bronze layer (raw data).
        
        Args:
            data_type (str): Type of data to extract (flights, airports, airlines)
            date (Optional[str]): Specific date to extract (YYYY-MM-DD format)
            
        Returns:
            List[Dict[str, Any]]: Raw data records
            
        Raises:
            ClientError: If S3 operation fails
        """
        logger.info(f"Extracting {data_type} data from Bronze layer")
        
        try:
            # List objects in S3 bucket
            prefix = f"bronze/{data_type}/"
            if date:
                prefix += f"{date}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.raw_bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No data found for {data_type} on {date}")
                return []
            
            # Extract data from all files
            all_data = []
            for obj in response['Contents']:
                try:
                    # Get object content
                    obj_response = self.s3_client.get_object(
                        Bucket=self.raw_bucket,
                        Key=obj['Key']
                    )
                    
                    # Parse JSON data
                    data = json.loads(obj_response['Body'].read().decode('utf-8'))
                    
                    # Handle different data structures
                    if isinstance(data, dict) and 'data' in data:
                        all_data.extend(data['data'])
                    elif isinstance(data, list):
                        all_data.extend(data)
                    else:
                        all_data.append(data)
                        
                except Exception as e:
                    logger.error(f"Failed to process file {obj['Key']}: {e}")
                    continue
            
            logger.info(f"Extracted {len(all_data)} {data_type} records from Bronze layer")
            return all_data
            
        except ClientError as e:
            logger.error(f"Failed to extract data from S3: {e}")
            raise
    
    def transform_flights_data(self, raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform raw flights data into cleaned format.
        
        Args:
            raw_data (List[Dict[str, Any]]): Raw flights data
            
        Returns:
            pd.DataFrame: Transformed flights data
        """
        logger.info("Transforming flights data")
        
        if not raw_data:
            logger.warning("No flights data to transform")
            return pd.DataFrame()
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(raw_data)
            
            # Add processing metadata
            df['processing_timestamp'] = datetime.now()
            df['data_source'] = 'aviation_stack'
            
            # Clean and validate flight data
            df = self._clean_flight_data(df)
            
            # Apply business rules
            df = self._apply_flight_business_rules(df)
            
            # Feature engineering
            df = self._engineer_flight_features(df)
            
            logger.info(f"Transformed {len(df)} flight records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to transform flights data: {e}")
            raise
    
    def _clean_flight_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean flight data by removing invalid records and standardizing formats.
        
        Args:
            df (pd.DataFrame): Raw flight data
            
        Returns:
            pd.DataFrame: Cleaned flight data
        """
        logger.info("Cleaning flight data")
        
        # Remove rows with missing critical fields
        critical_fields = ['flight', 'departure', 'arrival', 'airline']
        for field in critical_fields:
            if field in df.columns:
                df = df.dropna(subset=[field])
        
        # Standardize flight numbers
        if 'flight' in df.columns:
            df['flight_number'] = df['flight'].apply(
                lambda x: x.get('number', '') if isinstance(x, dict) else str(x)
            )
        
        # Clean departure and arrival data
        df = self._clean_airport_data(df, 'departure')
        df = self._clean_airport_data(df, 'arrival')
        
        # Clean airline data
        if 'airline' in df.columns:
            df['airline_name'] = df['airline'].apply(
                lambda x: x.get('name', '') if isinstance(x, dict) else str(x)
            )
            df['airline_iata'] = df['airline'].apply(
                lambda x: x.get('iata', '') if isinstance(x, dict) else ''
            )
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        logger.info(f"Cleaned flight data: {len(df)} records remaining")
        return df
    
    def _clean_airport_data(self, df: pd.DataFrame, airport_type: str) -> pd.DataFrame:
        """
        Clean airport data (departure or arrival).
        
        Args:
            df (pd.DataFrame): Flight data
            airport_type (str): 'departure' or 'arrival'
            
        Returns:
            pd.DataFrame: Flight data with cleaned airport information
        """
        if airport_type not in df.columns:
            return df
        
        # Extract airport information
        df[f'{airport_type}_airport'] = df[airport_type].apply(
            lambda x: x.get('airport', '') if isinstance(x, dict) else str(x)
        )
        df[f'{airport_type}_iata'] = df[airport_type].apply(
            lambda x: x.get('iata', '') if isinstance(x, dict) else ''
        )
        df[f'{airport_type}_icao'] = df[airport_type].apply(
            lambda x: x.get('icao', '') if isinstance(x, dict) else ''
        )
        
        # Parse timestamps
        for time_field in ['scheduled', 'estimated', 'actual']:
            field_name = f'{airport_type}_{time_field}'
            if field_name in df.columns:
                df[field_name] = pd.to_datetime(df[field_name], errors='coerce')
        
        return df
    
    def _apply_flight_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply business rules to flight data.
        
        Args:
            df (pd.DataFrame): Flight data
            
        Returns:
            pd.DataFrame: Flight data with business rules applied
        """
        logger.info("Applying business rules to flight data")
        
        # Filter out cancelled flights
        if 'departure_airport' in df.columns:
            df = df[~df['departure_airport'].str.startswith('C', na=False)]
        
        # Filter out test flights
        if 'flight_number' in df.columns:
            df = df[~df['flight_number'].str.contains('TEST', case=False, na=False)]
        
        # Add flight status
        df['flight_status'] = df.apply(self._determine_flight_status, axis=1)
        
        # Add route information
        df['route'] = df.apply(
            lambda row: f"{row.get('departure_iata', '')}-{row.get('arrival_iata', '')}", 
            axis=1
        )
        
        logger.info(f"Applied business rules: {len(df)} records remaining")
        return df
    
    def _determine_flight_status(self, row: pd.Series) -> str:
        """
        Determine flight status based on timestamps.
        
        Args:
            row (pd.Series): Flight record
            
        Returns:
            str: Flight status
        """
        now = datetime.now()
        
        # Check if flight has departed
        if pd.notna(row.get('departure_actual')):
            return 'departed'
        elif pd.notna(row.get('departure_estimated')):
            if row['departure_estimated'] < now:
                return 'delayed'
            else:
                return 'scheduled'
        else:
            return 'unknown'
    
    def _engineer_flight_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer features for flight data.
        
        Args:
            df (pd.DataFrame): Flight data
            
        Returns:
            pd.DataFrame: Flight data with engineered features
        """
        logger.info("Engineering flight features")
        
        # Time-based features
        if 'departure_scheduled' in df.columns:
            df['departure_hour'] = df['departure_scheduled'].dt.hour
            df['departure_day_of_week'] = df['departure_scheduled'].dt.dayofweek
            df['departure_month'] = df['departure_scheduled'].dt.month
            df['departure_season'] = df['departure_scheduled'].dt.month.map({
                12: 'winter', 1: 'winter', 2: 'winter',
                3: 'spring', 4: 'spring', 5: 'spring',
                6: 'summer', 7: 'summer', 8: 'summer',
                9: 'fall', 10: 'fall', 11: 'fall'
            })
        
        # Route features
        df['is_domestic'] = df.apply(
            lambda row: row.get('departure_iata', '')[:2] == row.get('arrival_iata', '')[:2],
            axis=1
        )
        
        # Delay features
        if 'departure_scheduled' in df.columns and 'departure_estimated' in df.columns:
            df['delay_minutes'] = (
                df['departure_estimated'] - df['departure_scheduled']
            ).dt.total_seconds() / 60
            df['is_delayed'] = df['delay_minutes'] > 15
        
        logger.info("Flight features engineered successfully")
        return df
    
    def validate_data_quality(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Validate data quality according to configuration.
        
        Args:
            df (pd.DataFrame): Data to validate
            data_type (str): Type of data being validated
            
        Returns:
            Dict[str, Any]: Data quality metrics
        """
        logger.info(f"Validating data quality for {data_type}")
        
        quality_metrics = {
            'total_records': len(df),
            'completeness_score': 0.0,
            'accuracy_score': 0.0,
            'consistency_score': 0.0,
            'quality_score': 0.0,
            'issues': []
        }
        
        if df.empty:
            quality_metrics['issues'].append("No data to validate")
            return quality_metrics
        
        # Calculate completeness
        completeness_scores = []
        for field in self.data_quality_config.required_fields:
            if field in df.columns:
                completeness = 1 - (df[field].isnull().sum() / len(df))
                completeness_scores.append(completeness)
            else:
                completeness_scores.append(0.0)
                quality_metrics['issues'].append(f"Required field missing: {field}")
        
        quality_metrics['completeness_score'] = sum(completeness_scores) / len(completeness_scores)
        
        # Calculate accuracy (basic checks)
        accuracy_checks = []
        
        # Check for valid IATA codes (3 characters)
        if 'departure_iata' in df.columns:
            valid_iata = df['departure_iata'].str.len() == 3
            accuracy_checks.append(valid_iata.mean())
        
        if 'arrival_iata' in df.columns:
            valid_iata = df['arrival_iata'].str.len() == 3
            accuracy_checks.append(valid_iata.mean())
        
        # Check for valid flight numbers
        if 'flight_number' in df.columns:
            valid_flight = df['flight_number'].str.len() > 0
            accuracy_checks.append(valid_flight.mean())
        
        if accuracy_checks:
            quality_metrics['accuracy_score'] = sum(accuracy_checks) / len(accuracy_checks)
        
        # Calculate consistency
        consistency_checks = []
        
        # Check for consistent route patterns
        if 'route' in df.columns:
            valid_routes = df['route'].str.contains(r'^[A-Z]{3}-[A-Z]{3}$', na=False)
            consistency_checks.append(valid_routes.mean())
        
        if consistency_checks:
            quality_metrics['consistency_score'] = sum(consistency_checks) / len(consistency_checks)
        
        # Overall quality score
        quality_metrics['quality_score'] = (
            quality_metrics['completeness_score'] * 0.4 +
            quality_metrics['accuracy_score'] * 0.4 +
            quality_metrics['consistency_score'] * 0.2
        )
        
        # Check against thresholds
        if quality_metrics['quality_score'] < self.data_quality_config.min_completeness:
            quality_metrics['issues'].append(
                f"Quality score {quality_metrics['quality_score']:.2f} below threshold "
                f"{self.data_quality_config.min_completeness}"
            )
        
        logger.info(f"Data quality validation completed: {quality_metrics['quality_score']:.2f}")
        return quality_metrics
    
    def load_to_silver(self, df: pd.DataFrame, data_type: str, 
                      quality_metrics: Dict[str, Any]) -> bool:
        """
        Load processed data to Silver layer.
        
        Args:
            df (pd.DataFrame): Processed data
            data_type (str): Type of data
            quality_metrics (Dict[str, Any]): Data quality metrics
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Loading {data_type} data to Silver layer")
        
        try:
            # Create timestamp for file naming
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            
            # Prepare data for storage
            data_to_store = {
                'data': df.to_dict('records'),
                'metadata': {
                    'data_type': data_type,
                    'processing_timestamp': timestamp,
                    'quality_metrics': quality_metrics,
                    'record_count': len(df)
                }
            }
            
            # Store in S3
            key = f"silver/{data_type}/{data_type}_{timestamp}.json"
            
            self.s3_client.put_object(
                Bucket=self.processed_bucket,
                Key=key,
                Body=json.dumps(data_to_store, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"Successfully loaded {len(df)} {data_type} records to Silver layer")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data to Silver layer: {e}")
            return False
    
    def run_etl_pipeline(self, data_type: str, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Run complete ETL pipeline for specified data type.
        
        Args:
            data_type (str): Type of data to process
            date (Optional[str]): Specific date to process
            
        Returns:
            Dict[str, Any]: Pipeline execution results
        """
        logger.info(f"Starting ETL pipeline for {data_type}")
        
        results = {
            'data_type': data_type,
            'date': date,
            'start_time': datetime.now(),
            'success': False,
            'records_processed': 0,
            'quality_metrics': {},
            'errors': []
        }
        
        try:
            # Extract
            raw_data = self.extract_from_bronze(data_type, date)
            if not raw_data:
                results['errors'].append("No data found to process")
                return results
            
            # Transform
            if data_type == 'flights':
                df = self.transform_flights_data(raw_data)
            else:
                # Handle other data types
                df = pd.DataFrame(raw_data)
            
            results['records_processed'] = len(df)
            
            # Validate
            quality_metrics = self.validate_data_quality(df, data_type)
            results['quality_metrics'] = quality_metrics
            
            # Load
            if quality_metrics['quality_score'] >= self.data_quality_config.min_completeness:
                success = self.load_to_silver(df, data_type, quality_metrics)
                results['success'] = success
            else:
                results['errors'].append("Data quality below threshold")
            
            results['end_time'] = datetime.now()
            results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info(f"ETL pipeline completed for {data_type}: {results['success']}")
            return results
            
        except Exception as e:
            logger.error(f"ETL pipeline failed for {data_type}: {e}")
            results['errors'].append(str(e))
            results['end_time'] = datetime.now()
            return results


def main():
    """
    Main function for testing the ETL pipeline.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize ETL pipeline
        pipeline = ETLPipeline()
        
        # Test with flights data
        logger.info("Testing ETL pipeline with flights data")
        results = pipeline.run_etl_pipeline('flights')
        
        # Print results
        logger.info(f"ETL Results: {json.dumps(results, default=str, indent=2)}")
        
        if results['success']:
            logger.info("ETL pipeline test completed successfully")
        else:
            logger.error("ETL pipeline test failed")
            
    except Exception as e:
        logger.error(f"ETL pipeline test failed: {e}")
        raise


if __name__ == "__main__":
    main() 