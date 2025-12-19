#!/usr/bin/env python3
"""
Demo script showing how to submit jobs to the Ray multicluster scheduler.
This is a simplified version for demonstration purposes.
"""

from ray_multicluster_scheduler.common.model import TaskDescription, ClusterMetadata


# Mock function for demonstration
def mock_add_function(a, b):
    """Mock function that simulates adding two numbers."""
    return a + b


# Mock actor for demonstration
class MockCounterActor:
    """Mock actor that simulates a counter."""

    def __init__(self, initial_value=0):
        self.value = initial_value

    def increment(self, amount=1):
        """Increment the counter."""
        self.value += amount
        return self.value


def demo_job_submission():
    """Demonstrate how to submit jobs to the scheduler."""
    print("=== Ray Multicluster Scheduler Job Submission Demo ===\n")

    # 1. Create mock cluster configurations
    print("1. Setting up mock cluster configurations...")
    cluster_configs = [
        ClusterMetadata(
            name="centos-cluster",
            head_address="192.168.5.7:32546",
            dashboard="http://192.168.5.7:31591",
            prefer=False,
            weight=1.0,
            tags=["linux", "x86_64"]
        ),
        ClusterMetadata(
            name="mac-cluster",
            head_address="192.168.5.2:32546",
            dashboard="http://192.168.5.2:8265",
            prefer=True,
            weight=1.2,
            tags=["macos", "arm64"]
        )
    ]

    print("   Created 2 mock clusters: centos-cluster and mac-cluster\n")

    # 2. Show how scheduler components would be initialized
    print("2. Scheduler components that would be initialized:")
    print("   - Cluster Metadata Manager: Manages cluster information")
    print("   - Health Checker: Monitors cluster health and resources")
    print("   - Cluster Registry: Maintains cluster metadata and snapshots")
    print("   - Connection Manager: Manages connections to Ray clusters")
    print("   - Policy Engine: Applies scheduling policies")
    print("   - Task Queue: Queues tasks for processing")
    print("   - Backpressure Controller: Controls task submission rate")
    print("   - Dispatcher: Sends tasks to selected clusters")
    print("   - Result Collector: Collects results from tasks")
    print("   - Circuit Breaker: Handles cluster failures\n")

    # 3. Create and submit a task
    print("3. Creating and submitting a sample task...")

    # Create a task description
    task_desc = TaskDescription(
        name="sample_addition_task",
        func_or_class=mock_add_function,
        args=(15, 25),
        kwargs={},
        resource_requirements={"CPU": 1},
        tags=["computation", "demo"],
        is_actor=False
    )

    print(f"   Created task: {task_desc.name}")
    print(f"   Task function: {task_desc.func_or_class.__name__}")
    print(f"   Task arguments: {task_desc.args}")
    print(f"   Task tags: {task_desc.tags}")
    print(f"   Resource requirements: {task_desc.resource_requirements}\n")

    # 4. Create and submit an actor
    print("4. Creating and submitting a sample actor...")

    # Create an actor description
    actor_desc = TaskDescription(
        name="sample_counter_actor",
        func_or_class=MockCounterActor,
        args=(100,),
        kwargs={},
        resource_requirements={"CPU": 1},
        tags=["stateful", "demo"],
        is_actor=True
    )

    print(f"   Created actor: {actor_desc.name}")
    print(f"   Actor class: {actor_desc.func_or_class.__name__}")
    print(f"   Actor arguments: {actor_desc.args}")
    print(f"   Actor tags: {actor_desc.tags}")
    print(f"   Resource requirements: {actor_desc.resource_requirements}\n")

    # 5. Summary
    print("5. How the scheduler would process these jobs:")
    print("   Step 1: Task is placed in the queue")
    print("   Step 2: Scheduler evaluates cluster health and resources")
    print("   Step 3: Policies are applied to select the best cluster")
    print("   Step 4: Task is submitted to the selected cluster")
    print("   Step 5: Result is collected and returned to the caller")
    print("   Step 6: Any failures are handled with retries or error reporting\n")

    print("=== Demo Completed ===")


if __name__ == "__main__":
    demo_job_submission()