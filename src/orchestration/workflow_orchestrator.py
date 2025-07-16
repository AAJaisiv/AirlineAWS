"""
Workflow Orchestrator for AirlineAWS Project

This module provides comprehensive workflow orchestration capabilities for the
data engineering pipeline, including dependency management, scheduling, and
error handling.

Author: A Abhinav Jaisiv
Date: 2025
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import yaml
from dataclasses import dataclass, asdict
from enum import Enum
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logger = logging.getLogger(__name__)


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskDefinition:
    """Definition of a workflow task."""
    task_id: str
    task_name: str
    function_name: str
    dependencies: List[str]
    timeout_seconds: int = 3600
    retry_count: int = 3
    retry_delay_seconds: int = 60
    required: bool = True
    parameters: Dict[str, Any] = None


@dataclass
class TaskExecution:
    """Task execution information."""
    task_id: str
    status: TaskStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    result: Optional[Dict[str, Any]] = None


@dataclass
class WorkflowExecution:
    """Workflow execution information."""
    workflow_id: str
    workflow_name: str
    status: WorkflowStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    tasks: Dict[str, TaskExecution]
    parameters: Dict[str, Any]
    error_message: Optional[str] = None


class WorkflowOrchestrator:
    """
    Comprehensive workflow orchestrator for the AirlineAWS data pipeline.
    
    This class manages the execution of complex workflows with dependencies,
    scheduling, error handling, and monitoring integration.
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the workflow orchestrator.
        
        Args:
            config_path (str): Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.s3_client = boto3.client('s3')
        
        # Configuration
        self.workflows_bucket = self.config['aws']['s3']['logs_bucket']
        self.max_concurrent_tasks = self.config['orchestration']['max_concurrent_tasks']
        
        # Task registry
        self.task_registry = {}
        
        # Execution tracking
        self.active_executions = {}
        self.execution_history = []
        
        logger.info("Workflow Orchestrator initialized successfully")
    
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
    
    def register_task(self, task_id: str, task_name: str, 
                     function: Callable, dependencies: List[str] = None,
                     timeout_seconds: int = 3600, retry_count: int = 3,
                     retry_delay_seconds: int = 60, required: bool = True):
        """
        Register a task for workflow execution.
        
        Args:
            task_id (str): Unique task identifier
            task_name (str): Human-readable task name
            function (Callable): Function to execute
            dependencies (List[str]): List of task IDs this task depends on
            timeout_seconds (int): Task timeout in seconds
            retry_count (int): Number of retry attempts
            retry_delay_seconds (int): Delay between retries
            required (bool): Whether task is required for workflow success
        """
        if dependencies is None:
            dependencies = []
        
        task_def = TaskDefinition(
            task_id=task_id,
            task_name=task_name,
            function_name=function.__name__,
            dependencies=dependencies,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
            retry_delay_seconds=retry_delay_seconds,
            required=required
        )
        
        self.task_registry[task_id] = {
            'definition': task_def,
            'function': function
        }
        
        logger.info(f"Registered task: {task_id} ({task_name})")
    
    def create_workflow(self, workflow_name: str, task_ids: List[str],
                       parameters: Dict[str, Any] = None) -> str:
        """
        Create a new workflow definition.
        
        Args:
            workflow_name (str): Name of the workflow
            task_ids (List[str]): List of task IDs to include
            parameters (Dict[str, Any]): Workflow parameters
            
        Returns:
            str: Workflow ID
        """
        if parameters is None:
            parameters = {}
        
        workflow_id = f"{workflow_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Validate task dependencies
        self._validate_workflow_dependencies(task_ids)
        
        workflow_config = {
            'workflow_id': workflow_id,
            'workflow_name': workflow_name,
            'task_ids': task_ids,
            'parameters': parameters,
            'created_at': datetime.now().isoformat()
        }
        
        # Store workflow definition
        try:
            key = f"workflows/{workflow_id}/workflow_definition.json"
            self.s3_client.put_object(
                Bucket=self.workflows_bucket,
                Key=key,
                Body=json.dumps(workflow_config, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"Created workflow: {workflow_id} ({workflow_name})")
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create workflow: {e}")
            raise
    
    def _validate_workflow_dependencies(self, task_ids: List[str]):
        """
        Validate that all task dependencies are satisfied.
        
        Args:
            task_ids (List[str]): List of task IDs to validate
            
        Raises:
            ValueError: If dependencies are not satisfied
        """
        # Check that all tasks are registered
        for task_id in task_ids:
            if task_id not in self.task_registry:
                raise ValueError(f"Task {task_id} is not registered")
        
        # Check dependencies
        for task_id in task_ids:
            task_def = self.task_registry[task_id]['definition']
            for dep_id in task_def.dependencies:
                if dep_id not in task_ids:
                    raise ValueError(f"Task {task_id} depends on {dep_id} which is not in workflow")
        
        # Check for circular dependencies
        self._check_circular_dependencies(task_ids)
    
    def _check_circular_dependencies(self, task_ids: List[str]):
        """
        Check for circular dependencies in the workflow.
        
        Args:
            task_ids (List[str]): List of task IDs to check
            
        Raises:
            ValueError: If circular dependencies are found
        """
        visited = set()
        rec_stack = set()
        
        def dfs(task_id):
            visited.add(task_id)
            rec_stack.add(task_id)
            
            task_def = self.task_registry[task_id]['definition']
            for dep_id in task_def.dependencies:
                if dep_id not in visited:
                    if dfs(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True
            
            rec_stack.remove(task_id)
            return False
        
        for task_id in task_ids:
            if task_id not in visited:
                if dfs(task_id):
                    raise ValueError(f"Circular dependency detected in workflow")
    
    def execute_workflow(self, workflow_id: str, 
                        parameters: Dict[str, Any] = None) -> WorkflowExecution:
        """
        Execute a workflow.
        
        Args:
            workflow_id (str): ID of the workflow to execute
            parameters (Dict[str, Any]): Execution parameters
            
        Returns:
            WorkflowExecution: Workflow execution result
        """
        if parameters is None:
            parameters = {}
        
        logger.info(f"Starting workflow execution: {workflow_id}")
        
        # Load workflow definition
        workflow_def = self._load_workflow_definition(workflow_id)
        
        # Create execution context
        execution = WorkflowExecution(
            workflow_id=workflow_id,
            workflow_name=workflow_def['workflow_name'],
            status=WorkflowStatus.RUNNING,
            start_time=datetime.now(),
            tasks={},
            parameters=parameters
        )
        
        # Initialize task executions
        for task_id in workflow_def['task_ids']:
            execution.tasks[task_id] = TaskExecution(
                task_id=task_id,
                status=TaskStatus.PENDING
            )
        
        # Store execution in active executions
        self.active_executions[workflow_id] = execution
        
        try:
            # Execute workflow
            self._execute_workflow_tasks(execution)
            
            # Update final status
            if self._all_required_tasks_completed(execution):
                execution.status = WorkflowStatus.COMPLETED
                logger.info(f"Workflow {workflow_id} completed successfully")
            else:
                execution.status = WorkflowStatus.FAILED
                execution.error_message = "Some required tasks failed"
                logger.error(f"Workflow {workflow_id} failed")
            
        except Exception as e:
            execution.status = WorkflowStatus.FAILED
            execution.error_message = str(e)
            logger.error(f"Workflow {workflow_id} failed with error: {e}")
        
        finally:
            # Finalize execution
            execution.end_time = datetime.now()
            execution.duration_seconds = (execution.end_time - execution.start_time).total_seconds()
            
            # Remove from active executions
            if workflow_id in self.active_executions:
                del self.active_executions[workflow_id]
            
            # Add to history
            self.execution_history.append(execution)
            
            # Save execution result
            self._save_execution_result(execution)
        
        return execution
    
    def _load_workflow_definition(self, workflow_id: str) -> Dict[str, Any]:
        """
        Load workflow definition from S3.
        
        Args:
            workflow_id (str): Workflow ID
            
        Returns:
            Dict[str, Any]: Workflow definition
        """
        try:
            key = f"workflows/{workflow_id}/workflow_definition.json"
            response = self.s3_client.get_object(
                Bucket=self.workflows_bucket,
                Key=key
            )
            
            return json.loads(response['Body'].read().decode('utf-8'))
            
        except Exception as e:
            logger.error(f"Failed to load workflow definition: {e}")
            raise
    
    def _execute_workflow_tasks(self, execution: WorkflowExecution):
        """
        Execute all tasks in the workflow.
        
        Args:
            execution (WorkflowExecution): Workflow execution context
        """
        workflow_def = self._load_workflow_definition(execution.workflow_id)
        task_ids = workflow_def['task_ids']
        
        # Create execution order based on dependencies
        execution_order = self._create_execution_order(task_ids)
        
        logger.info(f"Execution order: {execution_order}")
        
        # Execute tasks in order
        for task_id in execution_order:
            task_execution = execution.tasks[task_id]
            task_registry_entry = self.task_registry[task_id]
            task_def = task_registry_entry['definition']
            task_function = task_registry_entry['function']
            
            # Check if dependencies are satisfied
            if not self._dependencies_satisfied(task_id, execution):
                task_execution.status = TaskStatus.SKIPPED
                task_execution.error_message = "Dependencies not satisfied"
                continue
            
            # Execute task with retries
            success = False
            for attempt in range(task_def.retry_count + 1):
                try:
                    logger.info(f"Executing task {task_id} (attempt {attempt + 1})")
                    
                    task_execution.status = TaskStatus.RUNNING
                    task_execution.start_time = datetime.now()
                    
                    # Execute task with timeout
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(task_function, **execution.parameters)
                        result = future.result(timeout=task_def.timeout_seconds)
                    
                    task_execution.end_time = datetime.now()
                    task_execution.duration_seconds = (
                        task_execution.end_time - task_execution.start_time
                    ).total_seconds()
                    task_execution.status = TaskStatus.COMPLETED
                    task_execution.result = result
                    
                    success = True
                    logger.info(f"Task {task_id} completed successfully")
                    break
                    
                except Exception as e:
                    task_execution.retry_count = attempt
                    task_execution.error_message = str(e)
                    
                    if attempt < task_def.retry_count:
                        logger.warning(f"Task {task_id} failed (attempt {attempt + 1}), retrying in {task_def.retry_delay_seconds} seconds")
                        time.sleep(task_def.retry_delay_seconds)
                    else:
                        task_execution.status = TaskStatus.FAILED
                        logger.error(f"Task {task_id} failed after {task_def.retry_count + 1} attempts")
            
            # Update execution status
            if not success and task_def.required:
                logger.error(f"Required task {task_id} failed, stopping workflow")
                break
    
    def _create_execution_order(self, task_ids: List[str]) -> List[str]:
        """
        Create execution order based on dependencies.
        
        Args:
            task_ids (List[str]): List of task IDs
            
        Returns:
            List[str]: Ordered list of task IDs
        """
        # Topological sort
        in_degree = {task_id: 0 for task_id in task_ids}
        graph = {task_id: [] for task_id in task_ids}
        
        # Build graph
        for task_id in task_ids:
            task_def = self.task_registry[task_id]['definition']
            for dep_id in task_def.dependencies:
                if dep_id in task_ids:
                    graph[dep_id].append(task_id)
                    in_degree[task_id] += 1
        
        # Kahn's algorithm
        queue = [task_id for task_id in task_ids if in_degree[task_id] == 0]
        order = []
        
        while queue:
            task_id = queue.pop(0)
            order.append(task_id)
            
            for neighbor in graph[task_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(order) != len(task_ids):
            raise ValueError("Circular dependency detected")
        
        return order
    
    def _dependencies_satisfied(self, task_id: str, execution: WorkflowExecution) -> bool:
        """
        Check if all dependencies for a task are satisfied.
        
        Args:
            task_id (str): Task ID to check
            execution (WorkflowExecution): Workflow execution context
            
        Returns:
            bool: True if dependencies are satisfied
        """
        task_def = self.task_registry[task_id]['definition']
        
        for dep_id in task_def.dependencies:
            dep_execution = execution.tasks.get(dep_id)
            if not dep_execution or dep_execution.status != TaskStatus.COMPLETED:
                return False
        
        return True
    
    def _all_required_tasks_completed(self, execution: WorkflowExecution) -> bool:
        """
        Check if all required tasks completed successfully.
        
        Args:
            execution (WorkflowExecution): Workflow execution context
            
        Returns:
            bool: True if all required tasks completed
        """
        for task_execution in execution.tasks.values():
            task_def = self.task_registry[task_execution.task_id]['definition']
            if task_def.required and task_execution.status != TaskStatus.COMPLETED:
                return False
        
        return True
    
    def _save_execution_result(self, execution: WorkflowExecution):
        """
        Save workflow execution result to S3.
        
        Args:
            execution (WorkflowExecution): Workflow execution to save
        """
        try:
            key = f"workflows/{execution.workflow_id}/execution_result.json"
            
            # Convert execution to dictionary
            execution_dict = {
                'workflow_id': execution.workflow_id,
                'workflow_name': execution.workflow_name,
                'status': execution.status.value,
                'start_time': execution.start_time.isoformat(),
                'end_time': execution.end_time.isoformat() if execution.end_time else None,
                'duration_seconds': execution.duration_seconds,
                'parameters': execution.parameters,
                'error_message': execution.error_message,
                'tasks': {
                    task_id: {
                        'status': task.status.value,
                        'start_time': task.start_time.isoformat() if task.start_time else None,
                        'end_time': task.end_time.isoformat() if task.end_time else None,
                        'duration_seconds': task.duration_seconds,
                        'error_message': task.error_message,
                        'retry_count': task.retry_count,
                        'result': task.result
                    }
                    for task_id, task in execution.tasks.items()
                }
            }
            
            self.s3_client.put_object(
                Bucket=self.workflows_bucket,
                Key=key,
                Body=json.dumps(execution_dict, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"Execution result saved for workflow {execution.workflow_id}")
            
        except Exception as e:
            logger.error(f"Failed to save execution result: {e}")
    
    def get_workflow_status(self, workflow_id: str) -> Optional[WorkflowExecution]:
        """
        Get current status of a workflow execution.
        
        Args:
            workflow_id (str): Workflow ID
            
        Returns:
            Optional[WorkflowExecution]: Workflow execution status
        """
        # Check active executions
        if workflow_id in self.active_executions:
            return self.active_executions[workflow_id]
        
        # Check history
        for execution in self.execution_history:
            if execution.workflow_id == workflow_id:
                return execution
        
        return None
    
    def cancel_workflow(self, workflow_id: str) -> bool:
        """
        Cancel a running workflow.
        
        Args:
            workflow_id (str): Workflow ID to cancel
            
        Returns:
            bool: True if cancelled successfully
        """
        if workflow_id not in self.active_executions:
            logger.warning(f"Workflow {workflow_id} is not running")
            return False
        
        execution = self.active_executions[workflow_id]
        execution.status = WorkflowStatus.CANCELLED
        execution.end_time = datetime.now()
        execution.duration_seconds = (execution.end_time - execution.start_time).total_seconds()
        
        # Cancel running tasks
        for task_execution in execution.tasks.values():
            if task_execution.status == TaskStatus.RUNNING:
                task_execution.status = TaskStatus.CANCELLED
        
        # Remove from active executions
        del self.active_executions[workflow_id]
        
        # Add to history
        self.execution_history.append(execution)
        
        # Save result
        self._save_execution_result(execution)
        
        logger.info(f"Workflow {workflow_id} cancelled successfully")
        return True
    
    def list_workflows(self) -> List[Dict[str, Any]]:
        """
        List all workflows.
        
        Returns:
            List[Dict[str, Any]]: List of workflow information
        """
        workflows = []
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.workflows_bucket,
                Prefix='workflows/'
            )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('/workflow_definition.json'):
                        workflow_id = obj['Key'].split('/')[1]
                        try:
                            workflow_def = self._load_workflow_definition(workflow_id)
                            workflows.append({
                                'workflow_id': workflow_id,
                                'workflow_name': workflow_def['workflow_name'],
                                'created_at': workflow_def['created_at'],
                                'task_count': len(workflow_def['task_ids'])
                            })
                        except Exception as e:
                            logger.warning(f"Failed to load workflow {workflow_id}: {e}")
            
        except Exception as e:
            logger.error(f"Failed to list workflows: {e}")
        
        return workflows


def main():
    """
    Main function for testing the workflow orchestrator.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize orchestrator
        orchestrator = WorkflowOrchestrator()
        
        # Register sample tasks
        def sample_task_1(**kwargs):
            logger.info("Executing sample task 1")
            time.sleep(2)
            return {"result": "task_1_completed"}
        
        def sample_task_2(**kwargs):
            logger.info("Executing sample task 2")
            time.sleep(1)
            return {"result": "task_2_completed"}
        
        def sample_task_3(**kwargs):
            logger.info("Executing sample task 3")
            time.sleep(1)
            return {"result": "task_3_completed"}
        
        orchestrator.register_task("task_1", "Sample Task 1", sample_task_1)
        orchestrator.register_task("task_2", "Sample Task 2", sample_task_2, dependencies=["task_1"])
        orchestrator.register_task("task_3", "Sample Task 3", sample_task_3, dependencies=["task_1"])
        
        # Create and execute workflow
        workflow_id = orchestrator.create_workflow(
            "sample_workflow",
            ["task_1", "task_2", "task_3"],
            {"param1": "value1"}
        )
        
        logger.info(f"Created workflow: {workflow_id}")
        
        # Execute workflow
        execution = orchestrator.execute_workflow(workflow_id)
        
        logger.info(f"Workflow execution completed: {execution.status}")
        logger.info(f"Duration: {execution.duration_seconds} seconds")
        
        # List workflows
        workflows = orchestrator.list_workflows()
        logger.info(f"Available workflows: {len(workflows)}")
        
    except Exception as e:
        logger.error(f"Workflow orchestrator test failed: {e}")
        raise


if __name__ == "__main__":
    main() 