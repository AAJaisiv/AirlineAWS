#!/usr/bin/env python3
"""
AWS Infrastructure Deployment Script for AirlineAWS

This script deploys the complete AWS infrastructure for the AirlineAWS
data engineering pipeline using CloudFormation templates.

Author: A Abhinav Jaisiv
Date: 2025
"""

import os
import sys
import argparse
import logging
import boto3
import yaml
from botocore.exceptions import ClientError, WaiterError
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InfrastructureDeployer:
    """
    Handles deployment of AWS infrastructure for AirlineAWS project.
    
    This class manages the deployment of CloudFormation stacks, creates
    necessary AWS resources, and validates the deployment.
    """
    
    def __init__(self, region: str = 'us-east-1', profile: str = 'default'):
        """
        Initialize the infrastructure deployer.
        
        Args:
            region (str): AWS region for deployment
            profile (str): AWS profile to use
        """
        self.region = region
        self.profile = profile
        
        # Initialize AWS clients
        self.cloudformation = boto3.client('cloudformation', region_name=region)
        self.s3 = boto3.client('s3', region_name=region)
        self.secretsmanager = boto3.client('secretsmanager', region_name=region)
        self.iam = boto3.client('iam', region_name=region)
        
        # Load configuration
        self.config = self._load_config()
        
        logger.info(f"Infrastructure deployer initialized for region: {region}")
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        config_path = "config/config.yaml"
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
    
    def create_secrets(self) -> None:
        """
        Create necessary secrets in AWS Secrets Manager.
        """
        logger.info("Creating secrets in AWS Secrets Manager...")
        
        # Redshift password secret
        redshift_secret_name = f"{self.config['aws']['redshift']['cluster_identifier']}/redshift"
        
        try:
            # Generate a secure password
            import secrets
            import string
            password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))
            
            self.secretsmanager.create_secret(
                Name=redshift_secret_name,
                Description="Redshift cluster password for AirlineAWS",
                SecretString=f'{{"password": "{password}"}}'
            )
            
            logger.info(f"Created Redshift password secret: {redshift_secret_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceExistsException':
                logger.info(f"Secret {redshift_secret_name} already exists")
            else:
                logger.error(f"Failed to create secret: {e}")
                raise
    
    def validate_template(self, template_path: str) -> bool:
        """
        Validate CloudFormation template.
        
        Args:
            template_path (str): Path to CloudFormation template
            
        Returns:
            bool: True if template is valid, False otherwise
        """
        logger.info(f"Validating CloudFormation template: {template_path}")
        
        try:
            with open(template_path, 'r') as file:
                template_body = file.read()
            
            self.cloudformation.validate_template(TemplateBody=template_body)
            logger.info("CloudFormation template validation successful")
            return True
            
        except ClientError as e:
            logger.error(f"Template validation failed: {e}")
            return False
        except FileNotFoundError:
            logger.error(f"Template file not found: {template_path}")
            return False
    
    def deploy_stack(self, stack_name: str, template_path: str, 
                    parameters: Optional[Dict[str, str]] = None) -> bool:
        """
        Deploy CloudFormation stack.
        
        Args:
            stack_name (str): Name of the CloudFormation stack
            template_path (str): Path to CloudFormation template
            parameters (Optional[Dict[str, str]]): Stack parameters
            
        Returns:
            bool: True if deployment successful, False otherwise
        """
        logger.info(f"Deploying CloudFormation stack: {stack_name}")
        
        try:
            # Read template
            with open(template_path, 'r') as file:
                template_body = file.read()
            
            # Prepare parameters
            stack_parameters = []
            if parameters:
                for key, value in parameters.items():
                    stack_parameters.append({
                        'ParameterKey': key,
                        'ParameterValue': value
                    })
            
            # Check if stack exists
            try:
                self.cloudformation.describe_stacks(StackName=stack_name)
                logger.info(f"Stack {stack_name} already exists, updating...")
                operation = 'update_stack'
            except ClientError as e:
                if e.response['Error']['Code'] == 'ValidationError':
                    logger.info(f"Stack {stack_name} does not exist, creating...")
                    operation = 'create_stack'
                else:
                    raise
            
            # Deploy stack
            if operation == 'create_stack':
                response = self.cloudformation.create_stack(
                    StackName=stack_name,
                    TemplateBody=template_body,
                    Parameters=stack_parameters,
                    Capabilities=['CAPABILITY_NAMED_IAM'],
                    Tags=[
                        {
                            'Key': 'Project',
                            'Value': 'AirlineAWS'
                        },
                        {
                            'Key': 'Environment',
                            'Value': 'development'
                        }
                    ]
                )
            else:
                response = self.cloudformation.update_stack(
                    StackName=stack_name,
                    TemplateBody=template_body,
                    Parameters=stack_parameters,
                    Capabilities=['CAPABILITY_NAMED_IAM']
                )
            
            logger.info(f"Stack {operation} initiated: {response['StackId']}")
            
            # Wait for stack completion
            self._wait_for_stack_completion(stack_name)
            
            logger.info(f"Stack {stack_name} deployed successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Stack deployment failed: {e}")
            return False
    
    def _wait_for_stack_completion(self, stack_name: str) -> None:
        """
        Wait for CloudFormation stack to complete.
        
        Args:
            stack_name (str): Name of the stack to wait for
        """
        logger.info(f"Waiting for stack {stack_name} to complete...")
        
        try:
            waiter = self.cloudformation.get_waiter('stack_create_complete')
            waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 60
                }
            )
        except WaiterError as e:
            logger.error(f"Stack creation failed: {e}")
            raise
    
    def get_stack_outputs(self, stack_name: str) -> Dict[str, str]:
        """
        Get CloudFormation stack outputs.
        
        Args:
            stack_name (str): Name of the stack
            
        Returns:
            Dict[str, str]: Stack outputs
        """
        try:
            response = self.cloudformation.describe_stacks(StackName=stack_name)
            stack = response['Stacks'][0]
            
            outputs = {}
            if 'Outputs' in stack:
                for output in stack['Outputs']:
                    outputs[output['OutputKey']] = output['OutputValue']
            
            return outputs
            
        except ClientError as e:
            logger.error(f"Failed to get stack outputs: {e}")
            return {}
    
    def deploy_infrastructure(self) -> bool:
        """
        Deploy complete infrastructure for AirlineAWS.
        
        Returns:
            bool: True if deployment successful, False otherwise
        """
        logger.info("Starting AirlineAWS infrastructure deployment...")
        
        try:
            # Create secrets
            self.create_secrets()
            
            # Validate main template
            template_path = "infrastructure/cloudformation/main.yaml"
            if not self.validate_template(template_path):
                logger.error("Template validation failed")
                return False
            
            # Deploy main stack
            stack_name = "airlineaws-infrastructure"
            parameters = {
                'Environment': 'development',
                'ProjectName': 'airlineaws',
                'RedshiftNodeType': 'dc2.large',
                'RedshiftNodeCount': '2'
            }
            
            if not self.deploy_stack(stack_name, template_path, parameters):
                logger.error("Main stack deployment failed")
                return False
            
            # Get stack outputs
            outputs = self.get_stack_outputs(stack_name)
            logger.info("Stack outputs:")
            for key, value in outputs.items():
                logger.info(f"  {key}: {value}")
            
            logger.info("AirlineAWS infrastructure deployment completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Infrastructure deployment failed: {e}")
            return False
    
    def cleanup(self, stack_name: str) -> bool:
        """
        Clean up CloudFormation stack.
        
        Args:
            stack_name (str): Name of the stack to delete
            
        Returns:
            bool: True if cleanup successful, False otherwise
        """
        logger.info(f"Cleaning up stack: {stack_name}")
        
        try:
            self.cloudformation.delete_stack(StackName=stack_name)
            logger.info(f"Stack {stack_name} deletion initiated")
            
            # Wait for deletion
            waiter = self.cloudformation.get_waiter('stack_delete_complete')
            waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 60
                }
            )
            
            logger.info(f"Stack {stack_name} deleted successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Stack cleanup failed: {e}")
            return False


def main():
    """
    Main function for infrastructure deployment.
    """
    parser = argparse.ArgumentParser(description='Deploy AirlineAWS infrastructure')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--profile', default='default', help='AWS profile')
    parser.add_argument('--action', choices=['deploy', 'cleanup'], 
                       default='deploy', help='Action to perform')
    parser.add_argument('--stack-name', help='Stack name for cleanup')
    
    args = parser.parse_args()
    
    try:
        deployer = InfrastructureDeployer(region=args.region, profile=args.profile)
        
        if args.action == 'deploy':
            success = deployer.deploy_infrastructure()
            if success:
                logger.info("Infrastructure deployment completed successfully")
                sys.exit(0)
            else:
                logger.error("Infrastructure deployment failed")
                sys.exit(1)
        elif args.action == 'cleanup':
            if not args.stack_name:
                logger.error("Stack name required for cleanup")
                sys.exit(1)
            
            success = deployer.cleanup(args.stack_name)
            if success:
                logger.info("Infrastructure cleanup completed successfully")
                sys.exit(0)
            else:
                logger.error("Infrastructure cleanup failed")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Deployment script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 