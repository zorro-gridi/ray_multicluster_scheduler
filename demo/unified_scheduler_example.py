#!/usr/bin/env python3
"""
Example demonstrating the use of the unified scheduler interface
"""

import sys
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import logging
import ray

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the unified scheduler interface
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_task,
    submit_actor
)


def example_task(x, y):
    """
    Simple example task function
    """
    logger.info(f"Executing task with args: x={x}, y={y}")
    time.sleep(2)  # Simulate some work
    result = x + y
    logger.info(f"Task completed with result: {result}")
    return result


@ray.remote
class ExampleActor:
    """
    Simple example actor class
    """
    def __init__(self, initial_value):
        self.value = initial_value
        logger.info(f"Actor initialized with value: {self.value}")

    def increment(self, amount):
        """
        Increment the actor's value
        """
        logger.info(f"Incrementing value by {amount}")
        self.value += amount
        return self.value

    def get_value(self):
        """
        Get the actor's current value
        """
        logger.info(f"Getting current value: {self.value}")
        return self.value


def main():
    """
    Main function demonstrating the unified scheduler interface
    """
    logger.info("Demonstrating unified scheduler interface...")

    # Initialize the scheduler environment (single call to set up everything)
    logger.info("Initializing scheduler environment...")
    # You can specify a custom config file path if needed:
    # task_lifecycle_manager = initialize_scheduler_environment("/path/to/custom/clusters.yaml")
    task_lifecycle_manager = initialize_scheduler_environment()
    logger.info("Scheduler environment initialized successfully!")

    # Example 1: Submit a simple task without preferred cluster
    logger.info("=== Example 1: Submitting a simple task without preferred cluster ===")
    try:
        submit_task(
            func=example_task,
            args=(5, 3),
            resource_requirements={"CPU": 1, "memory": 1024 * 1024 * 1024},  # 1GB
            tags=["example", "simple"],
            name="example_addition_task"
        )
        logger.info("Simple task submitted successfully!")
    except Exception as e:
        logger.error(f"Failed to submit simple task: {e}")

    # Example 2: Submit a task with preferred cluster
    logger.info("=== Example 2: Submitting a task with preferred cluster ===")
    try:
        submit_task(
            func=example_task,
            args=(10, 20),
            resource_requirements={"CPU": 1, "memory": 1024 * 1024 * 1024},  # 1GB
            tags=["example", "preferred"],
            name="example_addition_task_preferred",
            preferred_cluster="mac"  # Prefer to run on mac cluster
        )
        logger.info("Task with preferred cluster submitted successfully!")
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Failed to submit task with preferred cluster: {e}")
        raise e

    # Example 3: Submit an actor without preferred cluster
    logger.info("=== Example 3: Submitting an actor without preferred cluster ===")
    try:
        submit_actor(
            actor_class=ExampleActor,
            args=(42,),
            resource_requirements={"CPU": 1, "memory": 1024 * 1024 * 1024},  # 1GB
            tags=["example", "actor"],
            name="example_actor"
        )
        logger.info("Actor submitted successfully!")
    except Exception as e:
        logger.error(f"Failed to submit actor: {e}")

    # Example 4: Submit an actor with preferred cluster
    logger.info("=== Example 4: Submitting an actor with preferred cluster ===")
    try:
        submit_actor(
            actor_class=ExampleActor,
            args=(100,),
            resource_requirements={"CPU": 1, "memory": 1024 * 1024 * 1024},  # 1GB
            tags=["example", "actor", "preferred"],
            name="example_actor_preferred",
            preferred_cluster="centos"  # Prefer to run on centos cluster
        )
        logger.info("Actor with preferred cluster submitted successfully!")
    except Exception as e:
        logger.error(f"Failed to submit actor with preferred cluster: {e}")

    logger.info("Unified scheduler demonstration completed!")


if __name__ == "__main__":
    main()