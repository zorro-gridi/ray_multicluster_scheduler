#!/usr/bin/env python3
"""
Simple test case for submitting a Ray job to the multicluster scheduler.
"""

import time
import ray
# Import the unified scheduler interface
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_task,
    submit_actor
)

# Simple test function to be scheduled
@ray.remote
def add_numbers(a, b):
    """Simple function that adds two numbers."""
    time.sleep(1)  # Simulate some work
    return a + b


# Simple test actor to be scheduled
@ray.remote
class CounterActor:
    """Simple actor that maintains a counter."""

    def __init__(self, initial_value=0):
        self.value = initial_value

    def increment(self, amount=1):
        """Increment the counter by the given amount."""
        self.value += amount
        return self.value

    def get_value(self):
        """Get the current value of the counter."""
        return self.value


def test_simple_job_submission():
    """Test submitting a simple job to the scheduler."""
    print("=== Simple Ray Job Submission Test ===")

    # Set up the scheduler using unified interface
    task_lifecycle_manager = initialize_scheduler_environment()

    # Give the scheduler a moment to initialize
    time.sleep(2)

    # Submit a simple task
    print("\n1. Submitting a simple addition task...")
    try:
        submit_task(
            func=add_numbers,
            args=(10, 5),
            resource_requirements={"CPU": 1},
            tags=["computation"],
            name="add_numbers_task"
        )
        print("   Task submitted successfully")
        print("   Task submitted with arguments (10, 5)")
    except Exception as e:
        print(f"   Failed to submit task: {e}")

    # Submit a simple actor
    print("\n2. Submitting a counter actor...")
    try:
        submit_actor(
            actor_class=CounterActor,
            args=(0,),
            resource_requirements={"CPU": 1},
            tags=["stateful"],
            name="counter_actor"
        )
        print("   Actor submitted successfully")
        print("   Actor submitted with initial value 0")
    except Exception as e:
        print(f"   Failed to submit actor: {e}")

    # Let the scheduler process the jobs
    print("\n3. Waiting for jobs to be processed...")
    time.sleep(10)

    print("\n4. Test completed.")
    print("   Check the scheduler logs to see which cluster processed each job.")


if __name__ == "__main__":
    test_simple_job_submission()