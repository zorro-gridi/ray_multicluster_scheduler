"""Unit and integration tests for the JobSubmissionClient functionality.
"""
import pytest
import tempfile
import os
import time
from unittest.mock import Mock, patch
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.scheduler.connection.job_client_pool import JobClientPool
from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.app.client_api.unified_scheduler import UnifiedScheduler
from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, initialize_scheduler, get_job_status, stop_job


def test_job_description_creation():
    """Test JobDescription creation and conversion to task description."""
    job_desc = JobDescription(
        job_id="test_job_123",
        entrypoint="python script.py",
        runtime_env={"pip": ["torch", "numpy"]},
        metadata={"user": "test"},
        preferred_cluster="test_cluster",
        resource_requirements={"CPU": 2, "GPU": 1},
        tags=["ml", "training"]
    )

    assert job_desc.job_id == "test_job_123"
    assert job_desc.entrypoint == "python script.py"
    assert job_desc.preferred_cluster == "test_cluster"
    assert job_desc.tags == ["ml", "training"]

    # Test conversion to task description
    task_desc = job_desc.as_task_description()
    assert task_desc.task_id == "test_job_123"
    assert task_desc.preferred_cluster == "test_cluster"
    assert task_desc.resource_requirements == {"CPU": 2, "GPU": 1}
    assert task_desc.tags == ["ml", "training"]


def test_job_client_pool():
    """Test JobClientPool functionality."""
    # Mock config manager
    mock_config_manager = Mock()

    # Create JobClientPool
    pool = JobClientPool(mock_config_manager)

    # Mock cluster metadata
    from ray_multicluster_scheduler.common.model import ClusterMetadata
    cluster_metadata = ClusterMetadata(
        name="test_cluster",
        head_address="localhost:12345",
        dashboard="localhost:8265",
        prefer=False,
        weight=1.0,
        runtime_env={}
    )

    # Since we can't actually connect to a cluster, we'll test the internal structure
    with patch('ray_multicluster_scheduler.scheduler.connection.job_client_pool.JobSubmissionClient') as mock_client:
        # Mock the client instance
        mock_client_instance = Mock()
        mock_client.return_value = mock_client_instance

        # Add cluster to pool
        pool.add_cluster(cluster_metadata)

        # Verify the client was created with correct address
        mock_client.assert_called_once()
        args, kwargs = mock_client.call_args
        assert "http://localhost:8265" in str(args) or "http://localhost:8265" in str(kwargs.values())

        # Verify client is stored
        assert "test_cluster" in pool.clients
        assert pool.active_clients["test_cluster"] is True


def test_task_queue_job_support():
    """Test TaskQueue support for Job type tasks."""
    queue = TaskQueue(max_size=10)

    # Create a job description
    job_desc = JobDescription(
        job_id="test_job_123",
        entrypoint="python script.py"
    )

    # Test enqueuing job
    result = queue.enqueue_job(job_desc)
    assert result is True
    assert queue.total_job_size() == 1
    assert queue.job_size() == 1

    # Test enqueuing job with specific cluster
    job_desc2 = JobDescription(
        job_id="test_job_456",
        entrypoint="python script2.py"
    )
    result2 = queue.enqueue_job(job_desc2, "test_cluster")
    assert result2 is True
    assert queue.job_size("test_cluster") == 1

    # Test dequeuing job
    dequeued_job = queue.dequeue_job()
    assert dequeued_job.job_id == "test_job_123"
    assert queue.total_job_size() == 1  # Still 1 because we dequeued from global, but cluster queue still has 1

    dequeued_job2 = queue.dequeue_job("test_cluster")
    assert dequeued_job2.job_id == "test_job_456"
    assert queue.total_job_size() == 0


def test_policy_engine_job_scheduling():
    """Test PolicyEngine support for job scheduling."""
    # Create policy engine
    policy_engine = PolicyEngine()

    # Create a job description
    job_desc = JobDescription(
        job_id="test_job_123",
        entrypoint="python script.py",
        preferred_cluster="test_cluster"
    )

    # Mock cluster snapshots
    from ray_multicluster_scheduler.common.model import ResourceSnapshot
    cluster_snapshots = {
        "test_cluster": ResourceSnapshot(
            cluster_name="test_cluster",
            cluster_cpu_total_cores=8,
            cluster_cpu_used_cores=2,
            cluster_cpu_usage_percent=25.0,
            cluster_mem_total_mb=16000,
            cluster_mem_used_mb=4000,
            cluster_mem_usage_percent=25.0,
            node_count=2
        )
    }

    # Test job scheduling (this should reuse task scheduling logic)
    with patch.object(policy_engine, '_make_scheduling_decision') as mock_method:
        mock_decision = Mock()
        mock_decision.cluster_name = "test_cluster"
        mock_method.return_value = mock_decision

        decision = policy_engine.schedule_job(job_desc, cluster_snapshots)

        # Verify that the internal method was called with converted task description
        mock_method.assert_called_once()
        args, kwargs = mock_method.call_args
        assert len(args) >= 2  # task_desc and cluster_snapshots
        # The first argument should be a TaskDescription (converted from JobDescription)
        from ray_multicluster_scheduler.common.model import TaskDescription
        assert isinstance(args[0], TaskDescription)
        assert args[1] == cluster_snapshots


def test_dispatcher_job_dispatch():
    """Test Dispatcher support for job dispatching."""
    # Create mock connection manager
    mock_connection_manager = Mock(spec=ConnectionLifecycleManager)
    mock_connection_manager.get_job_client.return_value = Mock()

    # Create dispatcher
    dispatcher = Dispatcher(mock_connection_manager)

    # Create a job description
    job_desc = JobDescription(
        job_id="test_job_123",
        entrypoint="python script.py",
        runtime_env={"pip": ["torch"]}
    )

    # Mock the job client
    mock_job_client = Mock()
    mock_job_client.submit_job.return_value = "submitted_job_123"
    mock_connection_manager.get_job_client.return_value = mock_job_client

    # Dispatch the job
    result = dispatcher.dispatch_job(job_desc, "test_cluster")

    # Verify the job was submitted correctly
    assert result == "submitted_job_123"
    mock_job_client.submit_job.assert_called_once_with(
        entrypoint="python script.py",
        runtime_env={"pip": ["torch"]},
        metadata=None,
        job_id="test_job_123"
    )


def test_task_lifecycle_manager_job_support():
    """Test TaskLifecycleManager support for jobs."""
    # We'll create a partial mock for cluster_monitor since full initialization is complex
    mock_cluster_monitor = Mock(spec=ClusterMonitor)
    mock_cluster_monitor.get_all_cluster_info.return_value = {}
    mock_cluster_monitor.client_pool = Mock()  # Add the required client_pool attribute

    # Create task lifecycle manager
    lifecycle_manager = TaskLifecycleManager(cluster_monitor=mock_cluster_monitor)

    # Create a job description
    job_desc = JobDescription(
        job_id="test_job_123",
        entrypoint="python script.py"
    )

    # Since actual submission requires real clusters, we'll test the queueing functionality
    # by directly calling the internal methods

    # Add job to queue (this should work without actual cluster connection)
    lifecycle_manager.task_queue.enqueue_job(job_desc)
    assert lifecycle_manager.task_queue.total_job_size() == 1

    # Test that job can be retrieved from queue
    retrieved_job = lifecycle_manager.task_queue.dequeue_job()
    assert retrieved_job.job_id == "test_job_123"


def test_unified_scheduler_job_submission():
    """Test unified scheduler job submission functionality."""
    # This test will be more integration-focused
    # Since full initialization requires real clusters, we'll test the structure

    from ray_multicluster_scheduler.app.client_api.unified_scheduler import UnifiedScheduler

    scheduler = UnifiedScheduler()

    # Check that scheduler has job-related methods
    assert hasattr(scheduler, 'submit_job')

    # Check that the unified scheduler module has the submit_job function
    from ray_multicluster_scheduler.app.client_api.unified_scheduler import submit_job
    assert callable(submit_job)


def test_submit_job_api():
    """Test the submit_job API function."""
    from ray_multicluster_scheduler.app.client_api.submit_job import submit_job

    # Check that the function exists and has the right signature
    assert callable(submit_job)

    # Check that it requires the necessary parameters
    import inspect
    sig = inspect.signature(submit_job)
    params = list(sig.parameters.keys())
    assert 'entrypoint' in params


def test_job_submission_with_real_clusters():
    """Integration test for job submission with real clusters."""
    # Initialize the scheduler with real cluster configuration
    scheduler = UnifiedScheduler()

    # Initialize with default config (should load clusters.yaml)
    task_lifecycle_manager = scheduler.initialize_environment()

    # Initialize the submit_job API with the task lifecycle manager
    initialize_scheduler(task_lifecycle_manager)

    # Create a simple test script to submit as a job
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write("""
import time
import sys
print("Hello from test job!")
time.sleep(2)
print("Job completed successfully!")
""")
        script_path = f.name

    try:
        # Submit a job using the real cluster
        job_id = submit_job(
            entrypoint=f"python {script_path}",
            job_id="test_real_job_123",
            metadata={"test": "real_cluster"},
            resource_requirements={"CPU": 1}
        )

        # Verify that the job was submitted and has an ID
        assert job_id is not None
        assert job_id == "test_real_job_123"

        # Wait a bit for the job to start
        time.sleep(5)

        # Check job status
        status = get_job_status(job_id)
        print(f"Job {job_id} status: {status}")

        # Stop the job if needed (optional)
        # stop_job(job_id)

    finally:
        # Clean up the temporary script file
        os.unlink(script_path)


def test_multiple_jobs_on_different_clusters():
    """Test submitting multiple jobs to different clusters."""
    # Initialize the scheduler with real cluster configuration
    scheduler = UnifiedScheduler()

    # Initialize with default config (should load clusters.yaml)
    task_lifecycle_manager = scheduler.initialize_environment()

    # Initialize the submit_job API with the task lifecycle manager
    initialize_scheduler(task_lifecycle_manager)

    # Create a simple test script to submit as a job
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write("""
import time
import sys
print("Hello from multi-cluster test job!")
time.sleep(3)
print("Multi-cluster job completed!")
""")
        script_path = f.name

    try:
        # Submit first job without specifying cluster (let scheduler decide)
        job_id1 = submit_job(
            entrypoint=f"python {script_path}",
            job_id="multi_test_job_1",
            metadata={"test": "multi_cluster_1"}
        )

        # Submit second job with preferred cluster
        job_id2 = submit_job(
            entrypoint=f"python {script_path}",
            job_id="multi_test_job_2",
            preferred_cluster="centos",  # Use one of the configured clusters
            metadata={"test": "multi_cluster_2"}
        )

        # Verify that both jobs were submitted
        assert job_id1 is not None
        assert job_id2 is not None

        # Wait a bit for the jobs to start
        time.sleep(5)

        # Check statuses
        status1 = get_job_status(job_id1)
        status2 = get_job_status(job_id2)
        print(f"Job {job_id1} status: {status1}")
        print(f"Job {job_id2} status: {status2}")

    finally:
        # Clean up the temporary script file
        os.unlink(script_path)


if __name__ == "__main__":
    # Run the tests
    test_job_description_creation()
    test_job_client_pool()
    test_task_queue_job_support()
    test_policy_engine_job_scheduling()
    test_dispatcher_job_dispatch()
    test_task_lifecycle_manager_job_support()
    test_unified_scheduler_job_submission()
    test_submit_job_api()
    print("All unit tests passed!")

    # Optionally run integration tests with real clusters
    # Uncomment the following lines to run real cluster tests:
    # print("Running real cluster tests...")
    # test_job_submission_with_real_clusters()
    # test_multiple_jobs_on_different_clusters()
    # print("All tests passed!")