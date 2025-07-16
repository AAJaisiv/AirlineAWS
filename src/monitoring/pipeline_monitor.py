"""

This script is our pipeline's health dashboard and alarm system. It tracks metrics,
raises alerts if something's off, and helps us keep an eye on the whole data flow.
If you want to know if the pipeline is happy, this is where you look.

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

# Set up logging so we can see what's happening in the control room
logger = logging.getLogger(__name__)

@dataclass
class PipelineMetrics:
    """
    Holds all the numbers we care about for a pipeline run—how long it took,
how many records, how many errors, and so on.
    """
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
    """
    Defines what should trigger an alert—like a smoke detector for our data.
    """
    alert_name: str
    metric_name: str
    threshold: float
    operator: str  # 'gt', 'lt', 'eq', 'gte', 'lte'
    severity: str  # 'info', 'warning', 'error', 'critical'
    message_template: str

class PipelineMonitor:
    """
    This class is our pipeline's health monitor. It records metrics, checks for problems,
    and can even send out alerts if things go sideways.
    """
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Loads config, sets up AWS clients, and prepares alert rules.
        """
        self.config = self._load_config(config_path)
        self.cloudwatch = boto3.client('cloudwatch')
        self.s3_client = boto3.client('s3')
        self.namespace = self.config['aws']['cloudwatch']['metric_namespace']
        self.log_group = self.config['aws']['cloudwatch']['log_group_name']
        self.alerts_bucket = self.config['aws']['s3']['logs_bucket']
        self.alert_configs = self._load_alert_configs()
        logger.info("Pipeline Monitor is on duty!")

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

    def _load_alert_configs(self) -> List[AlertConfig]:
        """
        Sets up our alert rules—what to watch for and when to sound the alarm.
        """
        alert_configs = []
        alert_configs.append(AlertConfig(
            alert_name="data_quality_threshold_breach",
            metric_name="data_quality_score",
            threshold=self.config['monitoring']['alerts']['data_quality_threshold'],
            operator="lt",
            severity="error",
            message_template="Data quality score {value} is below threshold {threshold}"
        ))
        alert_configs.append(AlertConfig(
            alert_name="pipeline_failure_rate_high",
            metric_name="pipeline_failure_rate",
            threshold=self.config['monitoring']['alerts']['pipeline_failure_threshold'],
            operator="gt",
            severity="warning",
            message_template="Pipeline failure rate {value} is above threshold {threshold}"
        ))
        alert_configs.append(AlertConfig(
            alert_name="model_performance_degradation",
            metric_name="model_accuracy",
            threshold=self.config['monitoring']['alerts']['model_performance_threshold'],
            operator="lt",
            severity="critical",
            message_template="Model accuracy {value} is below threshold {threshold}"
        ))
        return alert_configs

    def record_metric(self, metric_name: str, value: float, unit: str = "Count", dimensions: Optional[List[Dict[str, str]]] = None):
        """
        Sends a metric to CloudWatch. This is how we keep score.
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
        Records all the important numbers for a pipeline run.
        """
        logger.info(f"Recording metrics for pipeline: {metrics.pipeline_name}")
        dimensions = [{'Name': 'PipelineName', 'Value': metrics.pipeline_name}]
        self.record_metric('pipeline_duration_seconds', metrics.duration_seconds, 'Seconds', dimensions)
        self.record_metric('records_processed', metrics.records_processed, 'Count', dimensions)
        self.record_metric('success_rate', metrics.success_rate, 'Percent', dimensions)
        self.record_metric('error_count', metrics.error_count, 'Count', dimensions)
        self.record_metric('data_quality_score', metrics.data_quality_score, 'Percent', dimensions)
        self.record_metric('throughput_records_per_second', metrics.throughput_records_per_second, 'Count/Second', dimensions)
        if metrics.duration_seconds > 0:
            avg_processing_time = metrics.duration_seconds / max(metrics.records_processed, 1)
            self.record_metric('avg_processing_time_per_record', avg_processing_time, 'Seconds', dimensions)
        self.record_metric('pipeline_execution_timestamp', metrics.execution_time.timestamp(), 'Seconds', dimensions)

    def check_alerts(self, metrics: PipelineMetrics) -> List[Dict[str, Any]]:
        """
        Checks if any of our alert rules are triggered by the latest metrics.
        """
        triggered_alerts = []
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
        Saves an alert to S3 and logs it. (Could be extended to email, Slack, etc.)
        """
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            alert_key = f"alerts/{alert['alert_name']}_{timestamp}.json"
            self.s3_client.put_object(
                Bucket=self.alerts_bucket,
                Key=alert_key,
                Body=json.dumps(alert, default=str),
                ContentType='application/json'
            )
            logger.error(f"ALERT [{alert['severity'].upper()}]: {alert['message']}")
            # TODO: Add more alert channels (email, Slack, etc.)
            logger.info(f"Alert sent successfully: {alert['alert_name']}")
            return True
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            return False

    def get_pipeline_health(self, pipeline_name: str, time_range_hours: int = 24) -> Dict[str, Any]:
        """
        Checks the health of a pipeline over the last N hours.
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=time_range_hours)
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
        Gathers all the health stats and alert counts for our dashboard.
        """
        logger.info("Generating dashboard data")
        dashboard_data = {
            'timestamp': datetime.now().isoformat(),
            'pipelines': {},
            'overall_health': 'unknown',
            'total_alerts': 0,
            'metrics_summary': {}
        }
        pipeline_names = ['data_ingestion', 'data_processing', 'model_training']
        for pipeline_name in pipeline_names:
            health = self.get_pipeline_health(pipeline_name)
            dashboard_data['pipelines'][pipeline_name] = health
        health_statuses = [p['health_status'] for p in dashboard_data['pipelines'].values()]
        if 'critical' in health_statuses:
            dashboard_data['overall_health'] = 'critical'
        elif 'warning' in health_statuses:
            dashboard_data['overall_health'] = 'warning'
        elif all(status == 'healthy' for status in health_statuses):
            dashboard_data['overall_health'] = 'healthy'
        else:
            dashboard_data['overall_health'] = 'unknown'
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
        Saves dashboard data to S3 so we can view it in a web dashboard.
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

    def monitor_pipeline_execution(self, pipeline_name: str, execution_func, *args, **kwargs) -> Dict[str, Any]:
        """
        Wraps a pipeline run, records metrics, and checks for alerts. Like a referee for the pipeline game.
        """
        logger.info(f"Starting monitored execution of {pipeline_name}")
        start_time = datetime.now()
        try:
            result = execution_func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
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
            self.record_pipeline_metrics(metrics)
            alerts = self.check_alerts(metrics)
            for alert in alerts:
                self.send_alert(alert)
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
            self.record_pipeline_metrics(metrics)
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
    If you run this file directly, we'll do a quick dashboard generation and print a summary.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    try:
        monitor = PipelineMonitor()
        logger.info("Testing dashboard generation")
        dashboard_data = monitor.generate_dashboard_data()
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