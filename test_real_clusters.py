#!/usr/bin/env python3
"""
Test script for job submission with real clusters.
"""
import sys
import os
import tempfile
import time
import uuid

# Add the project root and scheduler modules to the path
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('./ray_multicluster_scheduler'))

from ray_multicluster_scheduler.app.client_api.unified_scheduler import UnifiedScheduler
from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, initialize_scheduler, get_job_status

def test_real_cluster_job_submission():
    """Test job submission with real clusters."""
    print('Testing job submission with real clusters...')

    # Initialize the scheduler with real cluster configuration
    scheduler = UnifiedScheduler()

    # Initialize with default config (should load clusters.yaml)
    task_lifecycle_manager = scheduler.initialize_environment()
    print('Scheduler initialized successfully!')

    # Initialize the submit_job API with the task lifecycle manager
    initialize_scheduler(task_lifecycle_manager)

    # Create a simple test script to submit as a job in current directory
    script_path = os.path.join(os.getcwd(), f"test_job_{str(uuid.uuid4())[:8]}.py")
    with open(script_path, 'w') as f:
        f.write('print("Hello from test job!")\nprint("Job completed successfully!")')

    try:
        # Submit a job using the real cluster
        job_id = submit_job(
            entrypoint='python ' + os.path.basename(script_path),
            job_id='test_real_job_' + str(uuid.uuid4())[:8],
            metadata={'test': 'real_cluster'},
            resource_requirements={'CPU': 1},
            runtime_env={'working_dir': os.getcwd()}
        )

        print(f'Job submitted successfully with ID: {job_id}')

        # Wait a bit for the job to start
        time.sleep(5)

        # Check job status
        status = get_job_status(job_id)
        print(f'Job {job_id} status: {status}')

    finally:
        # Clean up the temporary script file
        os.unlink(script_path)

    print('Real cluster job submission test completed!')

    # Test multiple jobs on different clusters
    print('\nTesting multiple jobs on different clusters...')

    # Create a simple test script to submit as a job in current directory
    script_path = os.path.join(os.getcwd(), f"test_job_multi_{str(uuid.uuid4())[:8]}.py")
    with open(script_path, 'w') as f:
        f.write('print("Hello from multi-cluster test job!")\nprint("Multi-cluster job completed!")')

    try:
        # Submit first job without specifying cluster (let scheduler decide)
        job_id1 = submit_job(
            entrypoint='python ' + os.path.basename(script_path),
            job_id='multi_test_job_1_' + str(uuid.uuid4())[:8],
            metadata={'test': 'multi_cluster_1'},
            runtime_env={'working_dir': os.getcwd()}
        )

        # Submit second job with preferred cluster
        job_id2 = submit_job(
            entrypoint='python ' + os.path.basename(script_path),
            job_id='multi_test_job_2_' + str(uuid.uuid4())[:8],
            preferred_cluster='centos',  # Use one of the configured clusters
            metadata={'test': 'multi_cluster_2'},
            runtime_env={'working_dir': os.getcwd()}
        )

        # Verify that both jobs were submitted
        print(f'Job 1 submitted with ID: {job_id1}')
        print(f'Job 2 submitted with ID: {job_id2}')

        # Wait a bit for the jobs to start
        time.sleep(5)

        # Check statuses
        status1 = get_job_status(job_id1)
        status2 = get_job_status(job_id2)
        print(f'Job {job_id1} status: {status1}')
        print(f'Job {job_id2} status: {status2}')

    finally:
        # Clean up the temporary script file
        os.unlink(script_path)

    print('Multiple jobs test completed!')

if __name__ == "__main__":
    test_real_cluster_job_submission()