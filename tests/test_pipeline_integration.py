"""
Integration Tests for AirlineAWS Pipeline

This provides comprehensive integration tests for the complete
data engineering pipeline, demonstrating end-to-end functionality.

"""

import os
import sys
import json
import logging
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from data_ingestion.aviation_api_client import AviationAPIClient
from data_processing.etl_pipeline import ETLPipeline, DataQualityConfig
from machine_learning.model_trainer import ModelTrainer, ModelConfig, ModelMetrics
from monitoring.pipeline_monitor import PipelineMonitor, PipelineMetrics
from orchestration.workflow_orchestrator import (
    WorkflowOrchestrator, 
    WorkflowStatus, 
    TaskStatus
)


class TestAirlineAWSPipelineIntegration(unittest.TestCase):
    
    def setUp(self):
        """Setting up test environment."""
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Creating temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Mock configuration
        self.mock_config = {
            'aws': {
                's3': {
                    'raw_bucket': 'test-raw-bucket',
                    'processed_bucket': 'test-processed-bucket',
                    'models_bucket': 'test-models-bucket',
                    'logs_bucket': 'test-logs-bucket'
                },
                'cloudwatch': {
                    'metric_namespace': 'AirlineAWS',
                    'log_group_name': '/aws/airlineaws'
                }
            },
            'data_processing': {
                'quality': {
                    'min_completeness': 0.95,
                    'max_null_percentage': 0.05,
                    'required_fields': ['flight_number', 'departure_iata', 'arrival_iata'],
                    'data_types': {}
                }
            },
            'monitoring': {
                'alerts': {
                    'data_quality_threshold': 0.9,
                    'pipeline_failure_threshold': 0.1,
                    'model_performance_threshold': 0.8
                }
            },
            'orchestration': {
                'max_concurrent_tasks': 5
            }
        }
        
        self.sample_flight_data = [
            {
                'flight': {'number': 'AA123'},
                'departure': {
                    'airport': 'John F. Kennedy International Airport',
                    'iata': 'JFK',
                    'icao': 'KJFK',
                    'scheduled': '2025-01-15T10:00:00+00:00',
                    'estimated': '2025-01-15T10:15:00+00:00'
                },
                'arrival': {
                    'airport': 'Los Angeles International Airport',
                    'iata': 'LAX',
                    'icao': 'KLAX',
                    'scheduled': '2025-01-15T13:00:00+00:00'
                },
                'airline': {
                    'name': 'American Airlines',
                    'iata': 'AA'
                },
                'aircraft': {'icao24': 'A12345'},
                'status': 'active'
            },
            {
                'flight': {'number': 'DL456'},
                'departure': {
                    'airport': 'Hartsfield-Jackson Atlanta International Airport',
                    'iata': 'ATL',
                    'icao': 'KATL',
                    'scheduled': '2025-01-15T11:00:00+00:00',
                    'estimated': '2025-01-15T11:00:00+00:00'
                },
                'arrival': {
                    'airport': 'O\'Hare International Airport',
                    'iata': 'ORD',
                    'icao': 'KORD',
                    'scheduled': '2025-01-15T12:30:00+00:00'
                },
                'airline': {
                    'name': 'Delta Air Lines',
                    'iata': 'DL'
                },
                'aircraft': {'icao24': 'B67890'},
                'status': 'active'
            }
        ]
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('boto3.client')
    def test_complete_pipeline_integration(self, mock_boto3_client):
        """Test complete end-to-end pipeline integration."""
        self.logger.info("Starting complete pipeline integration test")
        
        # Mock AWS clients
        mock_s3 = Mock()
        mock_cloudwatch = Mock()
        mock_boto3_client.side_effect = lambda service: mock_s3 if service == 's3' else mock_cloudwatch
        
        # Mock S3 responses
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'bronze/flights/2025-01-15/flights_20250115_100000.json'}
            ]
        }
        
        mock_s3.get_object.return_value = {
            'Body': Mock(
                read=lambda: json.dumps({
                    'data': self.sample_flight_data
                }).encode('utf-8')
            )
        }
        
        mock_s3.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        
        # Mock CloudWatch responses
        mock_cloudwatch.put_metric_data.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        mock_cloudwatch.get_metric_statistics.return_value = {
            'Datapoints': [{'Average': 0.95, 'Minimum': 0.90, 'Maximum': 1.0}]
        }
        
        try:
            # 1. Test Data Ingestion
            self.logger.info("Testing data ingestion...")
            
            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.mock_config)
                
                api_client = AviationAPIClient()
                
                # Mock API response
                with patch.object(api_client, '_make_api_request') as mock_request:
                    mock_request.return_value = {
                        'success': True,
                        'data': self.sample_flight_data
                    }
                    
                    # Test data ingestion
                    result = api_client.get_flights_data(
                        access_key='test_key',
                        dep_iata='JFK',
                        arr_iata='LAX'
                    )
                    
                    self.assertTrue(result['success'])
                    self.assertEqual(len(result['data']), 2)
            
            # 2. Test Data Processing
            self.logger.info("Testing data processing...")
            
            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.mock_config)
                
                etl_pipeline = ETLPipeline()
                
                # Test ETL pipeline
                result = etl_pipeline.run_etl_pipeline('flights')
                
                self.assertIsInstance(result, dict)
                self.assertIn('success', result)
                self.assertIn('records_processed', result)
                self.assertIn('quality_metrics', result)
            
            # 3. Test Machine Learning
            self.logger.info("Testing machine learning...")
            
            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.mock_config)
                
                model_trainer = ModelTrainer()
                
                # Create model configuration
                config = ModelConfig(
                    model_type='random_forest',
                    hyperparameters={'n_estimators': 100, 'max_depth': 10},
                    feature_columns=['departure_hour', 'departure_day_of_week', 'is_domestic'],
                    target_column='airline_choice'
                )
                
                # Mock training data
                training_data = {
                    'data': [
                        {
                            'departure_hour': 10,
                            'departure_day_of_week': 1,
                            'is_domestic': True,
                            'airline_choice': 'AA'
                        },
                        {
                            'departure_hour': 11,
                            'departure_day_of_week': 2,
                            'is_domestic': True,
                            'airline_choice': 'DL'
                        }
                    ]
                }
                
                with patch.object(model_trainer, 'load_training_data') as mock_load:
                    mock_load.return_value = training_data
                    
                    # Test model training
                    features, targets = model_trainer.preprocess_data(training_data['data'], config)
                    
                    if features and targets:
                        model, metrics = model_trainer.train_model(features, targets, config)
                        
                        self.assertIsNotNone(model)
                        self.assertIsInstance(metrics, ModelMetrics)
                        self.assertGreater(metrics.accuracy, 0.0)
            
            # 4. Test Monitoring
            self.logger.info("Testing monitoring...")
            
            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.mock_config)
                
                monitor = PipelineMonitor()
                
                # Create sample metrics
                metrics = PipelineMetrics(
                    pipeline_name='test_pipeline',
                    execution_time=datetime.now(),
                    duration_seconds=120.0,
                    records_processed=1000,
                    success_rate=0.95,
                    error_count=2,
                    data_quality_score=0.92,
                    throughput_records_per_second=8.33
                )
                
                # Test metrics recording
                monitor.record_pipeline_metrics(metrics)
                
                # Test alert checking
                alerts = monitor.check_alerts(metrics)
                self.assertIsInstance(alerts, list)
                
                # Test dashboard generation
                dashboard_data = monitor.generate_dashboard_data()
                self.assertIsInstance(dashboard_data, dict)
                self.assertIn('overall_health', dashboard_data)
            
            # 5. Test Orchestration
            self.logger.info("Testing orchestration...")
            
            with patch('builtins.open', create=True) as mock_open:
                mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.mock_config)
                
                orchestrator = WorkflowOrchestrator()
                
                # Register test tasks
                def task_1(**kwargs):
                    return {'result': 'task_1_completed'}
                
                def task_2(**kwargs):
                    return {'result': 'task_2_completed'}
                
                orchestrator.register_task('task_1', 'Test Task 1', task_1)
                orchestrator.register_task('task_2', 'Test Task 2', task_2, dependencies=['task_1'])
                
                # Create workflow
                workflow_id = orchestrator.create_workflow(
                    'test_workflow',
                    ['task_1', 'task_2'],
                    {'param1': 'value1'}
                )
                
                self.assertIsInstance(workflow_id, str)
                
                # Execute workflow
                execution = orchestrator.execute_workflow(workflow_id)
                
                self.assertIsInstance(execution.status, WorkflowStatus)
                self.assertIsInstance(execution.tasks, dict)
                
                # Check task statuses
                for task_execution in execution.tasks.values():
                    self.assertIsInstance(task_execution.status, TaskStatus)
            
            self.logger.info("Complete pipeline integration test passed successfully!")
            
        except Exception as e:
            self.logger.error(f"Pipeline integration test failed: {e}")
            raise
    
    def test_data_quality_validation(self):
        """Test data quality validation functionality."""
        self.logger.info("Testing data quality validation...")
        
        # Create data quality config
        config = DataQualityConfig(
            min_completeness=0.95,
            max_null_percentage=0.05,
            required_fields=['flight_number', 'departure_iata', 'arrival_iata'],
            data_types={'flight_number': 'string', 'departure_iata': 'string'}
        )
        
        # Test with valid data
        valid_data = [
            {
                'flight_number': 'AA123',
                'departure_iata': 'JFK',
                'arrival_iata': 'LAX',
                'airline': 'AA'
            },
            {
                'flight_number': 'DL456',
                'departure_iata': 'ATL',
                'arrival_iata': 'ORD',
                'airline': 'DL'
            }
        ]
        
        # ETL pipeline for testing
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.mock_config)
            
            etl_pipeline = ETLPipeline()
            
            # Test data quality validation
            quality_metrics = etl_pipeline.validate_data_quality(valid_data, 'flights')
            
            self.assertIsInstance(quality_metrics, dict)
            self.assertIn('quality_score', quality_metrics)
            self.assertIn('completeness_score', quality_metrics)
            self.assertIn('accuracy_score', quality_metrics)
    
    def test_model_configuration(self):
        """Test model configuration and validation."""
        self.logger.info("Testing model configuration...")
        
        # Test valid configuration
        config = ModelConfig(
            model_type='random_forest',
            hyperparameters={'n_estimators': 100, 'max_depth': 10},
            feature_columns=['departure_hour', 'departure_day_of_week', 'is_domestic'],
            target_column='airline_choice',
            test_size=0.2,
            random_state=42
        )
        
        self.assertEqual(config.model_type, 'random_forest')
        self.assertEqual(len(config.feature_columns), 3)
        self.assertEqual(config.target_column, 'airline_choice')
        self.assertEqual(config.test_size, 0.2)
        
        # Test invalid configuration
        with self.assertRaises(TypeError):
            ModelConfig(
                model_type='invalid_type',
                hyperparameters='not_a_dict',
                feature_columns='not_a_list',
                target_column=123  # Should be string
            )
    
    def test_workflow_dependencies(self):
        """Test workflow dependency management."""
        self.logger.info("Testing workflow dependencies...")
        
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.mock_config)
            
            orchestrator = WorkflowOrchestrator()
            
            # Register tasks with dependencies
            def task_a(**kwargs):
                return {'result': 'task_a'}
            
            def task_b(**kwargs):
                return {'result': 'task_b'}
            
            def task_c(**kwargs):
                return {'result': 'task_c'}
            
            orchestrator.register_task('task_a', 'Task A', task_a)
            orchestrator.register_task('task_b', 'Task B', task_b, dependencies=['task_a'])
            orchestrator.register_task('task_c', 'Task C', task_c, dependencies=['task_b'])
            
            # Test valid workflow
            workflow_id = orchestrator.create_workflow(
                'test_workflow',
                ['task_a', 'task_b', 'task_c']
            )
            
            self.assertIsInstance(workflow_id, str)
            
            # Test invalid workflow (circular dependency)
            with self.assertRaises(ValueError):
                orchestrator.register_task('task_d', 'Task D', task_a, dependencies=['task_c'])
                orchestrator.register_task('task_e', 'Task E', task_a, dependencies=['task_d'])
                orchestrator.register_task('task_c', 'Task C Updated', task_c, dependencies=['task_e'])
                
                orchestrator.create_workflow(
                    'invalid_workflow',
                    ['task_c', 'task_d', 'task_e']
                )
    
    def test_monitoring_metrics(self):
        """Test monitoring metrics collection and alerting."""
        self.logger.info("Testing monitoring metrics...")
        
        # Create sample metrics
        metrics = PipelineMetrics(
            pipeline_name='test_pipeline',
            execution_time=datetime.now(),
            duration_seconds=60.0,
            records_processed=500,
            success_rate=0.98,
            error_count=1,
            data_quality_score=0.95,
            throughput_records_per_second=8.33
        )
        
        # Test metrics properties
        self.assertEqual(metrics.pipeline_name, 'test_pipeline')
        self.assertEqual(metrics.duration_seconds, 60.0)
        self.assertEqual(metrics.records_processed, 500)
        self.assertEqual(metrics.success_rate, 0.98)
        self.assertEqual(metrics.error_count, 1)
        self.assertEqual(metrics.data_quality_score, 0.95)
        self.assertEqual(metrics.throughput_records_per_second, 8.33)
        
        # Test metrics conversion to dict
        metrics_dict = {
            'pipeline_name': metrics.pipeline_name,
            'duration_seconds': metrics.duration_seconds,
            'records_processed': metrics.records_processed,
            'success_rate': metrics.success_rate,
            'error_count': metrics.error_count,
            'data_quality_score': metrics.data_quality_score,
            'throughput_records_per_second': metrics.throughput_records_per_second
        }
        
        self.assertIsInstance(metrics_dict, dict)
        self.assertEqual(metrics_dict['pipeline_name'], 'test_pipeline')


def run_integration_tests():
    """Run all integration tests."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting AirlineAWS Pipeline Integration Tests")
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestAirlineAWSPipelineIntegration)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    logger.info(f"Tests run: {result.testsRun}")
    logger.info(f"Failures: {len(result.failures)}")
    logger.info(f"Errors: {len(result.errors)}")
    
    if result.wasSuccessful():
        logger.info("All integration tests passed successfully!")
        return True
    else:
        logger.error("Some integration tests failed!")
        return False


if __name__ == '__main__':
    success = run_integration_tests()
    exit(0 if success else 1) 