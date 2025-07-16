"""
Workflow Orchestration

This script is the air traffic controller for our data pipeline. It manages which
tasks run when, makes sure dependencies are respected, and keeps everything moving
smoothly. If you want to know how we keep chaos at bay, this is the place.

---
Hey there! If you're reading this, you're probably curious about how we keep our AWS data pipeline from turning into a mid-air collision. This file is the brains of the operation: it figures out what runs when, makes sure nothing steps on anything else's toes, and generally keeps the whole show on the rails (or, uh, in the air?).

You'll find a mix of classes, enums, and a main() that shows off how to use it. Comments are written like I'm talking to you at the whiteboard. If you spot something weird, it's probably because I was trying to make this as clear as possible for the next person (hi, recruiter!).
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

# Set up logging so we can see what's happening in the control tower
logger = logging.getLogger(__name__)

# --- ENUMS ---
class WorkflowStatus(Enum):
    # Workflow can be waiting, running, done, failed, or cancelled
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskStatus(Enum):
    # Same idea, but for individual tasks
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

# --- DATA CLASSES ---
@dataclass
class TaskDefinition:
    # What is this task? What does it depend on? How should it behave?
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
    # Tracks how a single run of a task went
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
    # Tracks a whole workflow run
    workflow_id: str
    workflow_name: str
    status: WorkflowStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    tasks: Dict[str, TaskExecution] = None
    parameters: Dict[str, Any] = None
    error_message: Optional[str] = None

# --- MAIN ORCHESTRATOR CLASS ---
class WorkflowOrchestrator:
    """
    This class is our pipeline's air traffic controller. It registers tasks, builds workflows,
    checks dependencies, and makes sure everything runs in the right order.
    """
    def __init__(self, config_path: str = "config/config.yaml"):
        # Load config, set up S3, and get ready to orchestrate
        self.config = self._load_config(config_path)
        self.s3_client = boto3.client('s3')
        self.workflows_bucket = self.config['aws']['s3']['logs_bucket']
        self.max_concurrent_tasks = self.config['orchestration']['max_concurrent_tasks']
        self.task_registry = {}  # All known tasks
        self.active_executions = {}  # Workflows currently running
        self.execution_history = []  # All finished workflows
        logger.info("Workflow Orchestrator is ready to direct traffic!")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        # Load YAML config. If it fails, we want to know why.
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

    def register_task(self, task_id: str, task_name: str, function: Callable, dependencies: List[str] = None,
                     timeout_seconds: int = 3600, retry_count: int = 3, retry_delay_seconds: int = 60, required: bool = True):
        # Register a task with the orchestrator. Think of this as adding a new flight to the schedule.
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

    def create_workflow(self, workflow_name: str, task_ids: List[str], parameters: Dict[str, Any] = None) -> str:
        # Create a new workflow definition and save it to S3
        if parameters is None:
            parameters = {}
        workflow_id = f"{workflow_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self._validate_workflow_dependencies(task_ids)
        workflow_config = {
            'workflow_id': workflow_id,
            'workflow_name': workflow_name,
            'task_ids': task_ids,
            'parameters': parameters,
            'created_at': datetime.now().isoformat()
        }
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
        # Make sure all tasks are registered and dependencies make sense (no cycles, no missing tasks)
        for task_id in task_ids:
            if task_id not in self.task_registry:
                raise ValueError(f"Task {task_id} is not registered")
        for task_id in task_ids:
            task_def = self.task_registry[task_id]['definition']
            for dep_id in task_def.dependencies:
                if dep_id not in task_ids:
                    raise ValueError(f"Task {task_id} depends on {dep_id} which is not in workflow")
        self._check_circular_dependencies(task_ids)

    def _check_circular_dependencies(self, task_ids: List[str]):
        # DFS to make sure there are no circular dependencies (no flights chasing their own tail)
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

    def execute_workflow(self, workflow_id: str, parameters: Dict[str, Any] = None) -> WorkflowExecution:
        # Actually run a workflow: load its definition, execute tasks in order, and track results
        if parameters is None:
            parameters = {}
        logger.info(f"Starting workflow execution: {workflow_id}")
        workflow_def = self._load_workflow_definition(workflow_id)
        execution = WorkflowExecution(
            workflow_id=workflow_id,
            workflow_name=workflow_def['workflow_name'],
            status=WorkflowStatus.RUNNING,
            start_time=datetime.now(),
            tasks={},
            parameters=parameters
        )
        for task_id in workflow_def['task_ids']:
            execution.tasks[task_id] = TaskExecution(
                task_id=task_id,
                status=TaskStatus.PENDING
            )
        self.active_executions[workflow_id] = execution
        try:
            self._execute_workflow_tasks(execution)
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
            execution.end_time = datetime.now()
            execution.duration_seconds = (execution.end_time - execution.start_time).total_seconds()
            if workflow_id in self.active_executions:
                del self.active_executions[workflow_id]
            self.execution_history.append(execution)
            self._save_execution_result(execution)
        return execution

    def _load_workflow_definition(self, workflow_id: str) -> Dict[str, Any]:
        # Load a workflow definition from S3
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
        # Run all tasks in the workflow, respecting dependencies and retries
        workflow_def = self._load_workflow_definition(execution.workflow_id)
        task_ids = workflow_def['task_ids']
        execution_order = self._create_execution_order(task_ids)
        logger.info(f"Execution order: {execution_order}")
        for task_id in execution_order:
            task_execution = execution.tasks[task_id]
            task_registry_entry = self.task_registry[task_id]
            task_def = task_registry_entry['definition']
            task_function = task_registry_entry['function']
            if not self._dependencies_satisfied(task_id, execution):
                task_execution.status = TaskStatus.SKIPPED
                task_execution.error_message = "Dependencies not satisfied"
                continue
            success = False
            for attempt in range(task_def.retry_count + 1):
                try:
                    logger.info(f"Executing task {task_id} (attempt {attempt + 1})")
                    task_execution.status = TaskStatus.RUNNING
                    task_execution.start_time = datetime.now()
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
            if not success and task_def.required:
                logger.error(f"Required task {task_id} failed, stopping workflow")
                break

    def _create_execution_order(self, task_ids: List[str]) -> List[str]:
        # Figure out the right order to run tasks, based on dependencies (topological sort)
        in_degree = {task_id: 0 for task_id in task_ids}
        graph = {task_id: [] for task_id in task_ids}
        for task_id in task_ids:
            task_def = self.task_registry[task_id]['definition']
            for dep_id in task_def.dependencies:
                if dep_id in task_ids:
                    graph[dep_id].append(task_id)
                    in_degree[task_id] += 1
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
        # Check if all dependencies for a task are done before running it
        task_def = self.task_registry[task_id]['definition']
        for dep_id in task_def.dependencies:
            dep_execution = execution.tasks.get(dep_id)
            if not dep_execution or dep_execution.status != TaskStatus.COMPLETED:
                return False
        return True

    def _all_required_tasks_completed(self, execution: WorkflowExecution) -> bool:
        # Check if all required tasks finished successfully
        for task_execution in execution.tasks.values():
            task_def = self.task_registry[task_execution.task_id]['definition']
            if task_def.required and task_execution.status != TaskStatus.COMPLETED:
                return False
        return True

    def _save_execution_result(self, execution: WorkflowExecution):
        # Save the results of a workflow run to S3
        try:
            key = f"workflows/{execution.workflow_id}/execution_result.json"
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
        # Return the current status of a workflow (if running or in history)
        if workflow_id in self.active_executions:
            return self.active_executions[workflow_id]
        for execution in self.execution_history:
            if execution.workflow_id == workflow_id:
                return execution
        return None

    def cancel_workflow(self, workflow_id: str) -> bool:
        # Cancel a running workflow (sets status, stops running tasks)
        if workflow_id not in self.active_executions:
            logger.warning(f"Workflow {workflow_id} is not running")
            return False
        execution = self.active_executions[workflow_id]
        execution.status = WorkflowStatus.CANCELLED
        execution.end_time = datetime.now()
        execution.duration_seconds = (execution.end_time - execution.start_time).total_seconds()
        for task_execution in execution.tasks.values():
            if task_execution.status == TaskStatus.RUNNING:
                task_execution.status = TaskStatus.CANCELLED
        del self.active_executions[workflow_id]
        self.execution_history.append(execution)
        self._save_execution_result(execution)
        logger.info(f"Workflow {workflow_id} cancelled successfully")
        return True

    def list_workflows(self) -> List[Dict[str, Any]]:
        # List all workflows stored in S3
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

# --- MAIN SCRIPT ENTRYPOINT ---
def main():
    """
    If you run this file directly, we'll register some sample tasks, create a workflow, and run it.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    try:
        orchestrator = WorkflowOrchestrator()
        # Here's a couple of toy tasks to show how this works
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
        # Register the tasks (like adding flights to the schedule)
        orchestrator.register_task("task_1", "Sample Task 1", sample_task_1)
        orchestrator.register_task("task_2", "Sample Task 2", sample_task_2, dependencies=["task_1"])
        orchestrator.register_task("task_3", "Sample Task 3", sample_task_3, dependencies=["task_1"])
        # Create a workflow (a flight plan)
        workflow_id = orchestrator.create_workflow(
            "sample_workflow",
            ["task_1", "task_2", "task_3"],
            {"param1": "value1"}
        )
        logger.info(f"Created workflow: {workflow_id}")
        # Run the workflow (let's see if it flies)
        execution = orchestrator.execute_workflow(workflow_id)
        logger.info(f"Workflow execution completed: {execution.status}")
        logger.info(f"Duration: {execution.duration_seconds} seconds")
        # List all workflows (what's in the hangar?)
        workflows = orchestrator.list_workflows()
        logger.info(f"Available workflows: {len(workflows)}")
    except Exception as e:
        logger.error(f"Workflow orchestrator test failed: {e}")
        raise

if __name__ == "__main__":
    main() 