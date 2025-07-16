"""
Pipeline Monitoring for AirlineAWS Project

This module provides comprehensive monitoring capabilities for the data engineering
pipeline, including metrics collection, alerting, and dashboard functionality.

Author: A Abhinav Jaisiv
Date: 2025
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import yaml
from dataclasses import dataclass, asdict

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Data class for pipeline metrics."""
    pipeline_name: str
    execution_time: datetime
    duration_seconds: float
    records_processed: int
    success_rate: float
    error_count: int
    data_quality_score: float
    throughput_records_per_second: float


@dataclass
class AlertConfig:
    """Configuration for alerting rules."""
    alert_name: str
    metric_name: str
    threshold: float
    operator: str  # 'gt', 'lt', 'eq', 'gte', 'lte'
    severity: str  # 'info', 'warning', 'error', 'critical'
    message_template: str


class PipelineMonitor:
    """
    Comprehensive monitoring system for the AirlineAWS data pipeline.
    
    This class handles metrics collection, alerting, and monitoring
    for all pipeline components including data ingestion, processing,
    and machine learning workflows.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the pipeline monitor.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.cloudwatch = boto3.client('cloudwatch')
        self.s3_client = boto3.client('s3')
        
        # Configuration
        self.namespace = self.config['aws']['cloudwatch']['metric_namespace']
        self.log_group = self.config['aws']['cloudwatch']['log_group_name']
        self.alerts_bucket = self.config['aws']['s3']['logs_bucket']
        
        # Alert configurations
        self.alert_configs = self._load_alert_configs()
        
        logger.info("Pipeline Monitor initialized successfully")
    
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
    
    def _load_alert_configs(self) -> List[AlertConfig]:
        """
        Load alert configurations from config.
        
        Returns:
            List[AlertConfig]: List of alert configurations
        """
        alert_configs = []
        
        # Data quality alerts
        alert_configs.append(AlertConfig(
            alert_name="data_quality_threshold_breach",
            metric_name="data_quality_score",
            threshold=self.config['monitoring']['alerts']['data_quality_threshold'],
            operator="lt",
            severity="error",
            message_template="Data quality score {value} is below threshold {threshold}"
        ))
        
        # Pipeline failure alerts
        alert_configs.append(AlertConfig(
            alert_name="pipeline_failure_rate_high",
            metric_name="pipeline_failure_rate",
            threshold=self.config['monitoring']['alerts']['pipeline_failure_threshold'],
            operator="gt",
            severity="warning",
            message_template="Pipeline failure rate {value} is above threshold {threshold}"
        ))
        
        # Model performance alerts
        alert_configs.append(AlertConfig(
            alert_name="model_performance_degradation",
            metric_name="model_accuracy",
            threshold=self.config['monitoring']['alerts']['model_performance_threshold'],
            operator="lt",
            severity="critical",
            message_template="Model accuracy {value} is below threshold {threshold}"
        ))
        
        return alert_configs
    
    def record_metric(self, metric_name: str, value: float, 
                     unit: str = "Count", dimensions: Optional[List[Dict[str, str]]] = None):
        """
        Record a metric to CloudWatch.
        
        Args:
            metric_name (str): Name of the metric
            value (float): Metric value
            unit (str): Unit of measurement
            dimensions (Optional[List[Dict[str, str]]]): Metric dimensions
        """
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Namespace': self.namespace,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = dimensions
            
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
            
            logger.debug(f"Recorded metric {metric_name}: {value} {unit}")
            
        except ClientError as e:
            logger.error(f"Failed to record metric {metric_name}: {e}")
    
    def record_pipeline_metrics(self, metrics: PipelineMetrics):
        """
        Record comprehensive pipeline metrics.
        
        Args:
            metrics (PipelineMetrics): Pipeline metrics to record
        """
        logger.info(f"Recording metrics for pipeline: {metrics.pipeline_name}")
        
        # Basic pipeline metrics
        dimensions = [{'Name': 'PipelineName', 'Value': metrics.pipeline_name}]
        
        self.record_metric('pipeline_duration_seconds', metrics.duration_seconds, 'Seconds', dimensions)
        self.record_metric('records_processed', metrics.records_processed, 'Count', dimensions)
        self.record_metric('success_rate', metrics.success_rate, 'Percent', dimensions)
        self.record_metric('error_count', metrics.error_count, 'Count', dimensions)
        self.record_metric('data_quality_score', metrics.data_quality_score, 'Percent', dimensions)
        self.record_metric('throughput_records_per_second', metrics.throughput_records_per_second, 'Count/Second', dimensions)
        
        # Calculate and record derived metrics
        if metrics.duration_seconds > 0:
            avg_processing_time = metrics.duration_seconds / max(metrics.records_processed, 1)
            self.record_metric('avg_processing_time_per_record', avg_processing_time, 'Seconds', dimensions)
        
        # Record timestamp for trend analysis
        self.record_metric('pipeline_execution_timestamp', 
                          metrics.execution_time.timestamp(), 'Seconds', dimensions)
    
    def check_alerts(self, metrics: PipelineMetrics) -> List[Dict[str, Any]]:
        """
        Check if any alerts should be triggered based on metrics.
        
        Args:
            metrics (PipelineMetrics): Current pipeline metrics
            
        Returns:
            List[Dict[str, Any]]: List of triggered alerts
        """
        triggered_alerts = []
        
        # Create metrics dictionary for easy lookup
        metrics_dict = {
            'data_quality_score': metrics.data_quality_score,
            'pipeline_failure_rate': 1 - metrics.success_rate,
            'model_accuracy': metrics.data_quality_score,  # Simplified for demo
            'records_processed': metrics.records_processed,
            'duration_seconds': metrics.duration_seconds
        }
        
        for alert_config in self.alert_configs:
            if alert_config.metric_name in metrics_dict:
                current_value = metrics_dict[alert_config.metric_name]
                threshold = alert_config.threshold
                
                # Check if alert should be triggered
                should_trigger = False
                
                if alert_config.operator == 'gt' and current_value > threshold:
                    should_trigger = True
                elif alert_config.operator == 'lt' and current_value < threshold:
                    should_trigger = True
                elif alert_config.operator == 'eq' and current_value == threshold:
                    should_trigger = True
                elif alert_config.operator == 'gte' and current_value >= threshold:
                    should_trigger = True
                elif alert_config.operator == 'lte' and current_value <= threshold:
                    should_trigger = True
                
                if should_trigger:
                    alert = {
                        'alert_name': alert_config.alert_name,
                        'severity': alert_config.severity,
                        'metric_name': alert_config.metric_name,
                        'current_value': current_value,
                        'threshold': threshold,
                        'message': alert_config.message_template.format(
                            value=current_value,
                            threshold=threshold
                        ),
                        'timestamp': datetime.now().isoformat(),
                        'pipeline_name': metrics.pipeline_name
                    }
                    
                    triggered_alerts.append(alert)
                    logger.warning(f"Alert triggered: {alert['message']}")
        
        return triggered_alerts
    
    def send_alert(self, alert: Dict[str, Any]) -> bool:
        """
        Send an alert through configured channels.
        
        Args:
            alert (Dict[str, Any]): Alert information
            
        Returns:
            bool: True if alert sent successfully, False otherwise
        """
        try:
            # Store alert in S3 for persistence
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            alert_key = f"alerts/{alert['alert_name']}_{timestamp}.json"
            
            self.s3_client.put_object(
                Bucket=self.alerts_bucket,
                Key=alert_key,
                Body=json.dumps(alert, default=str),
                ContentType='application/json'
            )
            
            # Log alert to CloudWatch
            logger.error(f"ALERT [{alert['severity'].upper()}]: {alert['message']}")
            
            # TODO: Implement additional alert channels
            # - SNS notifications
            # - Email alerts
            # - Slack/Teams integration
            # - PagerDuty integration
            
            logger.info(f"Alert sent successfully: {alert['alert_name']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            return False
    
    def get_pipeline_health(self, pipeline_name: str, 
                          time_range_hours: int = 24) -> Dict[str, Any]:
        """
        Get overall health status of a pipeline.
        
        Args:
            pipeline_name (str): Name of the pipeline
            time_range_hours (int): Time range to analyze in hours
            
        Returns:
            Dict[str, Any]: Pipeline health information
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=time_range_hours)
            
            # Get metrics from CloudWatch
            response = self.cloudwatch.get_metric_statistics(
                Namespace=self.namespace,
                MetricName='success_rate',
                Dimensions=[{'Name': 'PipelineName', 'Value': pipeline_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour periods
                Statistics=['Average', 'Minimum', 'Maximum']
            )
            
            if not response['Datapoints']:
                return {
                    'pipeline_name': pipeline_name,
                    'health_status': 'unknown',
                    'success_rate_avg': 0.0,
                    'success_rate_min': 0.0,
                    'success_rate_max': 0.0,
                    'data_points': 0
                }
            
            # Calculate health status
            avg_success_rate = response['Datapoints'][0]['Average']
            min_success_rate = response['Datapoints'][0]['Minimum']
            
            if avg_success_rate >= 0.95:
                health_status = 'healthy'
            elif avg_success_rate >= 0.85:
                health_status = 'warning'
            else:
                health_status = 'critical'
            
            return {
                'pipeline_name': pipeline_name,
                'health_status': health_status,
                'success_rate_avg': avg_success_rate,
                'success_rate_min': min_success_rate,
                'success_rate_max': response['Datapoints'][0]['Maximum'],
                'data_points': len(response['Datapoints']),
                'time_range_hours': time_range_hours
            }
            
        except ClientError as e:
            logger.error(f"Failed to get pipeline health: {e}")
            return {
                'pipeline_name': pipeline_name,
                'health_status': 'error',
                'error': str(e)
            }
    
    def generate_dashboard_data(self) -> Dict[str, Any]:
        """
        Generate data for monitoring dashboard.
        
        Returns:
            Dict[str, Any]: Dashboard data
        """
        logger.info("Generating dashboard data")
        
        dashboard_data = {
            'timestamp': datetime.now().isoformat(),
            'pipelines': {},
            'overall_health': 'unknown',
            'total_alerts': 0,
            'metrics_summary': {}
        }
        
        # Get health for all pipelines
        pipeline_names = ['data_ingestion', 'data_processing', 'model_training']
        
        for pipeline_name in pipeline_names:
            health = self.get_pipeline_health(pipeline_name)
            dashboard_data['pipelines'][pipeline_name] = health
        
        # Calculate overall health
        health_statuses = [p['health_status'] for p in dashboard_data['pipelines'].values()]
        
        if 'critical' in health_statuses:
            dashboard_data['overall_health'] = 'critical'
        elif 'warning' in health_statuses:
            dashboard_data['overall_health'] = 'warning'
        elif all(status == 'healthy' for status in health_statuses):
            dashboard_data['overall_health'] = 'healthy'
        else:
            dashboard_data['overall_health'] = 'unknown'
        
        # Get recent alerts count
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.alerts_bucket,
                Prefix='alerts/',
                MaxKeys=100
            )
            
            if 'Contents' in response:
                dashboard_data['total_alerts'] = len(response['Contents'])
            
        except ClientError as e:
            logger.error(f"Failed to get alerts count: {e}")
        
        logger.info(f"Dashboard data generated: overall health = {dashboard_data['overall_health']}")
        return dashboard_data
    
    def save_dashboard_data(self, dashboard_data: Dict[str, Any]) -> bool:
        """
        Save dashboard data to S3 for web dashboard consumption.
        
        Args:
            dashboard_data (Dict[str, Any]): Dashboard data to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            key = f"dashboard/dashboard_data_{timestamp}.json"
            
            self.s3_client.put_object(
                Bucket=self.alerts_bucket,
                Key=key,
                Body=json.dumps(dashboard_data, default=str),
                ContentType='application/json'
            )
            
            # Also save as latest for real-time dashboard
            self.s3_client.put_object(
                Bucket=self.alerts_bucket,
                Key='dashboard/latest_dashboard.json',
                Body=json.dumps(dashboard_data, default=str),
                ContentType='application/json'
            )
            
            logger.info("Dashboard data saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save dashboard data: {e}")
            return False
    
    def monitor_pipeline_execution(self, pipeline_name: str, 
                                 execution_func, *args, **kwargs) -> Dict[str, Any]:
        """
        Monitor a pipeline execution and record metrics.
        
        Args:
            pipeline_name (str): Name of the pipeline
            execution_func: Function to execute
            *args: Arguments for execution function
            **kwargs: Keyword arguments for execution function
            
        Returns:
            Dict[str, Any]: Execution results with monitoring data
        """
        logger.info(f"Starting monitored execution of {pipeline_name}")
        
        start_time = datetime.now()
        
        try:
            # Execute the pipeline
            result = execution_func(*args, **kwargs)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Create metrics
            metrics = PipelineMetrics(
                pipeline_name=pipeline_name,
                execution_time=start_time,
                duration_seconds=duration,
                records_processed=result.get('records_processed', 0),
                success_rate=1.0 if result.get('success', False) else 0.0,
                error_count=len(result.get('errors', [])),
                data_quality_score=result.get('quality_metrics', {}).get('quality_score', 0.0),
                throughput_records_per_second=result.get('records_processed', 0) / max(duration, 1)
            )
            
            # Record metrics
            self.record_pipeline_metrics(metrics)
            
            # Check for alerts
            alerts = self.check_alerts(metrics)
            for alert in alerts:
                self.send_alert(alert)
            
            # Add monitoring data to result
            result['monitoring'] = {
                'metrics': asdict(metrics),
                'alerts_triggered': len(alerts),
                'execution_duration': duration
            }
            
            logger.info(f"Pipeline {pipeline_name} execution monitored successfully")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Create error metrics
            metrics = PipelineMetrics(
                pipeline_name=pipeline_name,
                execution_time=start_time,
                duration_seconds=duration,
                records_processed=0,
                success_rate=0.0,
                error_count=1,
                data_quality_score=0.0,
                throughput_records_per_second=0.0
            )
            
            # Record error metrics
            self.record_pipeline_metrics(metrics)
            
            # Send error alert
            error_alert = {
                'alert_name': 'pipeline_execution_error',
                'severity': 'critical',
                'metric_name': 'pipeline_error',
                'current_value': 1,
                'threshold': 0,
                'message': f"Pipeline {pipeline_name} execution failed: {str(e)}",
                'timestamp': datetime.now().isoformat(),
                'pipeline_name': pipeline_name
            }
            
            self.send_alert(error_alert)
            
            logger.error(f"Pipeline {pipeline_name} execution failed: {e}")
            raise


def main():
    """
    Main function for testing the pipeline monitor.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize monitor
        monitor = PipelineMonitor()
        
        # Test dashboard generation
        logger.info("Testing dashboard generation")
        dashboard_data = monitor.generate_dashboard_data()
        
        # Save dashboard data
        success = monitor.save_dashboard_data(dashboard_data)
        
        if success:
            logger.info("Dashboard data saved successfully")
            logger.info(f"Dashboard data: {json.dumps(dashboard_data, indent=2)}")
        else:
            logger.error("Failed to save dashboard data")
            
    except Exception as e:
        logger.error(f"Pipeline monitor test failed: {e}")
        raise


if __name__ == "__main__":
    main() 