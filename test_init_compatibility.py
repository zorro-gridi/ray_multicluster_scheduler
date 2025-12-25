#!/usr/bin/env python3
"""
Test script to verify initialize_scheduler_environment compatibility with submit_job.
"""
import sys
import os
import uuid

# Add the project root and scheduler modules to the path
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('./ray_multicluster_scheduler'))

from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
from ray_multicluster_scheduler.app.client_api.submit_job import submit_job

def test_init_compatibility():
    print('Testing initialize_scheduler_environment with submit_job compatibility...')

    # 使用 initialize_scheduler_environment 函数初始化
    task_lifecycle_manager = initialize_scheduler_environment()
    print('Scheduler environment initialized successfully via initialize_scheduler_environment!')

    # 现在测试 submit_job 是否可以正常工作（应该能够自动初始化）
    try:
        # 创建一个简单的测试脚本
        script_path = os.path.join(os.getcwd(), f"test_simple_job_{str(uuid.uuid4())[:8]}.py")
        with open(script_path, 'w') as f:
            f.write('print("Hello from simple test job!")')

        # 提交作业 - 这应该能够正常工作，因为环境已经通过 initialize_scheduler_environment 初始化
        job_id = submit_job(
            entrypoint='python ' + os.path.basename(script_path),
            job_id='simple_test_job_' + str(uuid.uuid4())[:8],
            metadata={'test': 'simple'},
            runtime_env={'working_dir': os.getcwd()}
        )

        print(f'Job submitted successfully with ID: {job_id}')

        # 清理临时文件
        os.unlink(script_path)

        print('SUCCESS: submit_job works correctly with initialize_scheduler_environment!')
        return True

    except Exception as e:
        print(f'ERROR: submit_job failed: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_init_compatibility()