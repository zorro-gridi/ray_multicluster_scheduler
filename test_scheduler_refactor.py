"""
Unit tests for the refactored scheduler system.
Tests the new detached monitor, proper job lifecycle management, and signal handling.
"""
import unittest
import ray
import time
from unittest.mock import Mock, patch, MagicMock
from ray_multicluster_scheduler.app.client_api.submit_job import (
    submit_job, initialize_scheduler, wait_batch_jobs, BatchJobFailed, submit_batch_jobs
)
from ray_multicluster_scheduler.app.client_api.submit_task import submit_task
from ray_multicluster_scheduler.app.client_api.submit_actor import submit_actor
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor
from ray_multicluster_scheduler.scheduler.monitor.cluster_resource_monitor import ClusterResourceMonitor


class TestRefactoredScheduler(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Initialize Ray for testing
        if not ray.is_initialized():
            ray.init(local_mode=True, ignore_reinit_error=True)

        # Create a mock cluster monitor
        self.cluster_monitor = Mock(spec=ClusterMonitor)
        self.cluster_monitor.get_all_cluster_info.return_value = {
            'test_cluster': {
                'metadata': Mock(),
                'snapshot': Mock()
            }
        }

        # Create task lifecycle manager
        self.task_manager = TaskLifecycleManager(self.cluster_monitor)
        self.task_manager._initialized = True  # Bypass initialization

        # Initialize the schedulers
        initialize_scheduler(self.task_manager)

    def tearDown(self):
        """Clean up after each test method."""
        if ray.is_initialized():
            ray.shutdown()

    def test_submit_task_returns_result_immediately(self):
        """Test that submit_task properly waits for and returns task results."""
        # Mock a simple function
        def simple_func(x):
            return x * 2

        # Mock the task lifecycle manager's submit_task_and_get_future method
        with patch.object(self.task_manager, 'submit_task_and_get_future') as mock_submit:
            # Create a mock future that returns a result when ray.get is called
            mock_future = Mock()
            mock_submit.return_value = mock_future

            # Mock ray.get to return the expected result
            with patch('ray_multicluster_scheduler.app.client_api.submit_task.ray.get') as mock_ray_get:
                expected_result = 42
                mock_ray_get.return_value = expected_result

                task_id, result = submit_task(simple_func, args=(21,))

                # Verify the result is returned correctly
                self.assertEqual(result, expected_result)
                self.assertIsNotNone(task_id)
                mock_submit.assert_called_once()
                mock_ray_get.assert_called_once_with(mock_future)

    def test_submit_actor_returns_actor_handle(self):
        """Test that submit_actor properly returns actor handle."""
        # Mock an actor class
        @ray.remote
        class TestActor:
            def __init__(self, value):
                self.value = value

            def get_value(self):
                return self.value

        # Mock the task lifecycle manager's submit_task_and_get_future method
        with patch.object(self.task_manager, 'submit_task_and_get_future') as mock_submit:
            # Create a mock future that returns an actor handle when ray.get is called
            mock_future = Mock()
            mock_submit.return_value = mock_future

            # Mock ray.get to return the expected actor handle
            with patch('ray_multicluster_scheduler.app.client_api.submit_actor.ray.get') as mock_ray_get:
                mock_actor_handle = Mock()
                mock_ray_get.return_value = mock_actor_handle

                actor_id, actor_handle = submit_actor(TestActor, args=(42,))

                # Verify the actor handle is returned correctly
                self.assertEqual(actor_handle, mock_actor_handle)
                self.assertIsNotNone(actor_id)
                mock_submit.assert_called_once()
                mock_ray_get.assert_called_once_with(mock_future)

    def test_wait_batch_jobs_success(self):
        """Test that wait_batch_jobs works correctly for successful jobs."""
        mock_client = Mock()

        # Mock job statuses to transition from RUNNING to SUCCEEDED
        status_sequence = [
            {"job1": "RUNNING", "job2": "RUNNING"},
            {"job1": "SUCCEEDED", "job2": "SUCCEEDED"}
        ]
        current_status_idx = [0]  # Use list to make it mutable in nested function

        def get_status(job_id):
            statuses = status_sequence[current_status_idx[0]]
            return statuses.get(job_id, "RUNNING")

        mock_client.get_job_status.side_effect = get_status

        job_ids = ["job1", "job2"]

        # This should not raise an exception
        result = wait_batch_jobs(mock_client, job_ids, "test_cluster")

        expected_result = {"job1": "SUCCEEDED", "job2": "SUCCEEDED"}
        self.assertEqual(result, expected_result)

    def test_wait_batch_jobs_failure(self):
        """Test that wait_batch_jobs raises BatchJobFailed when jobs fail."""
        mock_client = Mock()

        # Mock job statuses to include a failure
        status_sequence = [
            {"job1": "RUNNING", "job2": "RUNNING"},
            {"job1": "FAILED", "job2": "RUNNING"}
        ]
        current_status_idx = [0]  # Use list to make it mutable in nested function

        def get_status(job_id):
            statuses = status_sequence[current_status_idx[0]]
            return statuses.get(job_id, "RUNNING")

        mock_client.get_job_status.side_effect = get_status

        job_ids = ["job1", "job2"]

        # This should raise BatchJobFailed
        with self.assertRaises(BatchJobFailed):
            wait_batch_jobs(mock_client, job_ids, "test_cluster")

    def test_submit_job_returns_job_id(self):
        """Test that submit_job returns a job ID."""
        with patch.object(self.task_manager, 'submit_job') as mock_submit:
            mock_submit.return_value = "test_job_id"

            job_id = submit_job(
                entrypoint="python test.py",
                runtime_env={"pip": ["requests"]}
            )

            self.assertEqual(job_id, "test_job_id")
            mock_submit.assert_called_once()

    def test_detached_monitor_creation(self):
        """Test that the detached monitor can be created properly."""
        # Mock cluster configs
        cluster_configs = {
            'cluster1': Mock(head_address='192.168.1.1:10001'),
            'cluster2': Mock(head_address='192.168.1.2:10001')
        }

        # Create the monitor actor
        monitor = ClusterResourceMonitor.remote(cluster_configs)

        # Verify it's running by calling ping
        result = ray.get(monitor.ping.remote())
        self.assertIn("ClusterResourceMonitor alive", result)

        # Verify initial snapshots are empty
        snapshots = ray.get(monitor.get_latest_snapshots.remote())
        self.assertEqual(len(snapshots), 0)


class TestSubmitBatchJobs(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures before each test method."""
        if not ray.is_initialized():
            ray.init(local_mode=True, ignore_reinit_error=True)

        # Create a mock cluster monitor
        self.cluster_monitor = Mock(spec=ClusterMonitor)
        self.cluster_monitor.get_all_cluster_info.return_value = {
            'test_cluster': {
                'metadata': Mock(),
                'snapshot': Mock()
            }
        }

        # Create task lifecycle manager
        self.task_manager = TaskLifecycleManager(self.cluster_monitor)
        self.task_manager._initialized = True  # Bypass initialization

        # Initialize the schedulers
        initialize_scheduler(self.task_manager)

    def tearDown(self):
        """Clean up after each test method."""
        if ray.is_initialized():
            ray.shutdown()

    @patch('ray_multicluster_scheduler.app.client_api.submit_job._task_lifecycle_manager', new_callable=Mock)
    @patch('ray_multicluster_scheduler.app.client_api.submit_job.wait_batch_jobs')
    def test_submit_batch_jobs_success(self, mock_wait, mock_task_manager):
        """Test successful batch job submission."""
        # Mock the connection manager and job client
        mock_connection_manager = Mock()
        mock_job_client = Mock()
        mock_connection_manager.get_job_client.return_value = mock_job_client
        self.task_manager.connection_manager = mock_connection_manager
        mock_task_manager.connection_manager = mock_connection_manager

        # Mock the submit_job function to return job IDs
        job_ids = ["job1", "job2", "job3"]

        def mock_submit_job(**kwargs):
            return job_ids.pop(0)

        # Mock the submit_job call in the task manager
        original_submit_job = self.task_manager.submit_job
        self.task_manager.submit_job = Mock(side_effect=[job_ids[0], job_ids[1], job_ids[2]])

        # Mock the lifecycle manager to return our task manager
        with patch('ray_multicluster_scheduler.app.client_api.submit_job.get_unified_scheduler') as mock_get_scheduler:
            mock_scheduler = Mock()
            mock_scheduler.task_lifecycle_manager = self.task_manager
            mock_get_scheduler.return_value = mock_scheduler

            # Mock wait_batch_jobs to return success
            mock_wait.return_value = {job_id: "SUCCEEDED" for job_id in ["job1", "job2", "job3"]}

            jobs = [
                {"entrypoint": "python script1.py"},
                {"entrypoint": "python script2.py"},
                {"entrypoint": "python script3.py"}
            ]

            result = submit_batch_jobs(jobs)

            self.assertEqual(result, ["job1", "job2", "job3"])
            mock_wait.assert_called_once()


if __name__ == '__main__':
    unittest.main()