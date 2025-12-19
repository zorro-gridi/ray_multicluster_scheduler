"""
End-to-end test for the ray multicluster scheduler.
"""

import time
import ray
# Import the unified scheduler interface
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_task,
    submit_actor
)

# Test function to be scheduled
@ray.remote
def test_function(x, y):
    return x + y

# Test actor to be scheduled
@ray.remote
class TestActor:
    def __init__(self, value):
        self.value = value

    def increment(self, x):
        self.value += x
        return self.value

def test_e2e_workflow():
    """Test the end-to-end workflow of the scheduler."""
    print("Starting end-to-end test...")

    # Initialize the scheduler using unified interface
    task_lifecycle_manager = initialize_scheduler_environment()

    # Submit a test task
    print("Submitting test task...")
    try:
        submit_task(
            func=test_function,
            args=(5, 3),
            name="test_function"
        )
        print("Task submitted successfully")
    except Exception as e:
        print(f"Failed to submit task: {e}")

    # Submit a test actor
    print("Submitting test actor...")
    try:
        submit_actor(
            actor_class=TestActor,
            args=(10,),
            name="TestActor"
        )
        print("Actor submitted successfully")
    except Exception as e:
        print(f"Failed to submit actor: {e}")

    # Let the scheduler run for a bit
    time.sleep(5)

    print("End-to-end test completed.")

if __name__ == "__main__":
    test_e2e_workflow()