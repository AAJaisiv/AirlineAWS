"""
ETL Pipeline 

It takes raw aviation data (bronze),
cleans and transforms it (silver), and gets it ready for business and ML (gold).
If you want to know how our data gets from messy to meaningful, this is the place.

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

# Set up logging so we can see what's cooking
logger = logging.getLogger(__name__)

class DataQualityConfig(BaseModel):
    """
    Holds our data quality rules.  its like a checklist for what makes
data "good enough" to move forward.
    """
    min_completeness: float = 0.95
    max_null_percentage: float = 0.05
    required_fields: List[str] = []
    data_types: Dict[str, str] = {}

class ETLPipeline:
    """
    This class is our data chef. It extracts, transforms, and loads aviation data
    through all the layers of our data lake.
    """
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Loads config, sets up S3, and gets our quality checklist ready.
        """
        self.config = self._load_config(config_path)
        self.s3_client = boto3.client('s3')
        self.data_quality_config = DataQualityConfig(
            **self.config['data_processing']['quality']
        )
        self.raw_bucket = self.config['aws']['s3']['raw_bucket']
        self.processed_bucket = self.config['aws']['s3']['processed_bucket']
        logger.info("ETL Pipeline is ready to cook!")

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

    def extract_from_bronze(self, data_type: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Pulls raw data from S3 (bronze layer).
        """
        logger.info(f"Extracting {data_type} data from Bronze layer")
        try:
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
            all_data = []
            for obj in response['Contents']:
                try:
                    obj_response = self.s3_client.get_object(
                        Bucket=self.raw_bucket,
                        Key=obj['Key']
                    )
                    data = json.loads(obj_response['Body'].read().decode('utf-8'))
                    # Sometimes the API gives us a dict, sometimes a list. Let's handle both.
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
        Cleans and enriches flight data. This is where the magic happens—turning raw into refined.
        """
        logger.info("Transforming flights data")
        if not raw_data:
            logger.warning("No flights data to transform")
            return pd.DataFrame()
        try:
            df = pd.DataFrame(raw_data)
            df['processing_timestamp'] = datetime.now()
            df['data_source'] = 'aviation_stack'
            df = self._clean_flight_data(df)
            df = self._apply_flight_business_rules(df)
            df = self._engineer_flight_features(df)
            logger.info(f"Transformed {len(df)} flight records")
            return df
        except Exception as e:
            logger.error(f"Failed to transform flights data: {e}")
            raise

    def _clean_flight_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Drops bad rows, standardizes columns, and makes sure our data is tidy.
        """
        logger.info("Cleaning flight data")
        critical_fields = ['flight', 'departure', 'arrival', 'airline']
        for field in critical_fields:
            if field in df.columns:
                df = df.dropna(subset=[field])
        if 'flight' in df.columns:
            df['flight_number'] = df['flight'].apply(
                lambda x: x.get('number', '') if isinstance(x, dict) else str(x)
            )
        df = self._clean_airport_data(df, 'departure')
        df = self._clean_airport_data(df, 'arrival')
        if 'airline' in df.columns:
            df['airline_name'] = df['airline'].apply(
                lambda x: x.get('name', '') if isinstance(x, dict) else str(x)
            )
            df['airline_iata'] = df['airline'].apply(
                lambda x: x.get('iata', '') if isinstance(x, dict) else ''
            )
        df = df.drop_duplicates()
        logger.info(f"Cleaned flight data: {len(df)} records remaining")
        return df

    def _clean_airport_data(self, df: pd.DataFrame, airport_type: str) -> pd.DataFrame:
        """
        Standardizes airport info for either departure or arrival.
        """
        if airport_type not in df.columns:
            return df
        df[f'{airport_type}_airport'] = df[airport_type].apply(
            lambda x: x.get('airport', '') if isinstance(x, dict) else str(x)
        )
        df[f'{airport_type}_iata'] = df[airport_type].apply(
            lambda x: x.get('iata', '') if isinstance(x, dict) else ''
        )
        df[f'{airport_type}_icao'] = df[airport_type].apply(
            lambda x: x.get('icao', '') if isinstance(x, dict) else ''
        )
        for time_field in ['scheduled', 'estimated', 'actual']:
            field_name = f'{airport_type}_{time_field}'
            if field_name in df.columns:
                df[field_name] = pd.to_datetime(df[field_name], errors='coerce')
        return df

    def _apply_flight_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies business logic: filters out test/cancelled flights, adds status and route info.
        """
        logger.info("Applying business rules to flight data")
        if 'departure_airport' in df.columns:
            df = df[~df['departure_airport'].str.startswith('C', na=False)]
        if 'flight_number' in df.columns:
            df = df[~df['flight_number'].str.contains('TEST', case=False, na=False)]
        df['flight_status'] = df.apply(self._determine_flight_status, axis=1)
        df['route'] = df.apply(
            lambda row: f"{row.get('departure_iata', '')}-{row.get('arrival_iata', '')}", 
            axis=1
        )
        logger.info(f"Applied business rules: {len(df)} records remaining")
        return df

    def _determine_flight_status(self, row: pd.Series) -> str:
        """
        Looks at timestamps to figure out if a flight is scheduled, delayed, or departed.
        """
        now = datetime.now()
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
        Adds new columns for ML: hour, day, season, is_domestic, delay info, etc.
        """
        logger.info("Engineering flight features")
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
        df['is_domestic'] = df.apply(
            lambda row: row.get('departure_iata', '')[:2] == row.get('arrival_iata', '')[:2],
            axis=1
        )
        if 'departure_scheduled' in df.columns and 'departure_estimated' in df.columns:
            df['delay_minutes'] = (
                df['departure_estimated'] - df['departure_scheduled']
            ).dt.total_seconds() / 60
            df['is_delayed'] = df['delay_minutes'] > 15
        logger.info("Flight features engineered successfully")
        return df

    def validate_data_quality(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Checks if our data meets the quality bar. If not, we log why.
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
        completeness_scores = []
        for field in self.data_quality_config.required_fields:
            if field in df.columns:
                completeness = 1 - (df[field].isnull().sum() / len(df))
                completeness_scores.append(completeness)
            else:
                completeness_scores.append(0.0)
                quality_metrics['issues'].append(f"Required field missing: {field}")
        quality_metrics['completeness_score'] = sum(completeness_scores) / len(completeness_scores)
        accuracy_checks = []
        if 'departure_iata' in df.columns:
            valid_iata = df['departure_iata'].str.len() == 3
            accuracy_checks.append(valid_iata.mean())
        if 'arrival_iata' in df.columns:
            valid_iata = df['arrival_iata'].str.len() == 3
            accuracy_checks.append(valid_iata.mean())
        if 'flight_number' in df.columns:
            valid_flight = df['flight_number'].str.len() > 0
            accuracy_checks.append(valid_flight.mean())
        if accuracy_checks:
            quality_metrics['accuracy_score'] = sum(accuracy_checks) / len(accuracy_checks)
        consistency_checks = []
        if 'route' in df.columns:
            valid_routes = df['route'].str.contains(r'^[A-Z]{3}-[A-Z]{3}$', na=False)
            consistency_checks.append(valid_routes.mean())
        if consistency_checks:
            quality_metrics['consistency_score'] = sum(consistency_checks) / len(consistency_checks)
        quality_metrics['quality_score'] = (
            quality_metrics['completeness_score'] * 0.4 +
            quality_metrics['accuracy_score'] * 0.4 +
            quality_metrics['consistency_score'] * 0.2
        )
        if quality_metrics['quality_score'] < self.data_quality_config.min_completeness:
            quality_metrics['issues'].append(
                f"Quality score {quality_metrics['quality_score']:.2f} below threshold "
                f"{self.data_quality_config.min_completeness}"
            )
        logger.info(f"Data quality validation completed: {quality_metrics['quality_score']:.2f}")
        return quality_metrics

    def load_to_silver(self, df: pd.DataFrame, data_type: str, quality_metrics: Dict[str, Any]) -> bool:
        """
        Saves processed data to S3 (silver layer). If it fails, we log the error.
        """
        logger.info(f"Loading {data_type} data to Silver layer")
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            data_to_store = {
                'data': df.to_dict('records'),
                'metadata': {
                    'data_type': data_type,
                    'processing_timestamp': timestamp,
                    'quality_metrics': quality_metrics,
                    'record_count': len(df)
                }
            }
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
        Runs the full ETL process for a given data type. This is the main event.
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
            raw_data = self.extract_from_bronze(data_type, date)
            if not raw_data:
                results['errors'].append("No data found to process")
                return results
            if data_type == 'flights':
                df = self.transform_flights_data(raw_data)
            else:
                df = pd.DataFrame(raw_data)
            results['records_processed'] = len(df)
            quality_metrics = self.validate_data_quality(df, data_type)
            results['quality_metrics'] = quality_metrics
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
    we'll do a quick ETL run and print a summary.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    try:
        pipeline = ETLPipeline()
        logger.info("Testing ETL pipeline with flights data")
        results = pipeline.run_etl_pipeline('flights')
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