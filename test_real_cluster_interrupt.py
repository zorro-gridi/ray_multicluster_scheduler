#!/usr/bin/env python3
"""
在真实集群环境中测试 submit_job 提交任务并使用 KeyboardInterrupt 中断正在运行的 ray job
"""
import time
import signal
import sys
import os
from pathlib import Path

# 添加项目路径
sys.path.insert(0, '/Users/zorro/project/pycharm/ray_multicluster_scheduler')

from ray_multicluster_scheduler.app.client_api import (
    initialize_scheduler_environment,
    submit_job,
    wait_for_all_jobs,
    get_job_status,
    stop_job
)

def create_long_running_job_script():
    """创建一个长时间运行的测试脚本"""
    script_content = '''
import time
import sys
import ray

@ray.remote
def long_running_task(task_id, duration=60):
    """模拟长时间运行的任务"""
    print(f"Task {task_id} started")
    for i in range(duration):
        print(f"Task {task_id} - Progress: {i+1}/{duration}")
        time.sleep(1)
    print(f"Task {task_id} completed")
    return f"Task {task_id} result"

def main():
    print("Starting long running Ray job...")
    print("This job will run for 60 seconds and can be interrupted with Ctrl+C")

    # 初始化 Ray
    if not ray.is_initialized():
        ray.init()

    # 提交一个长时间运行的任务
    task_ref = long_running_task.remote("test_task_1", 60)

    try:
        result = ray.get(task_ref)
        print(f"Job completed with result: {result}")
    except KeyboardInterrupt:
        print("Job interrupted by user")
        # 清理资源
        ray.shutdown()
        sys.exit(1)

    # 清理资源
    ray.shutdown()
    print("Job finished successfully")

if __name__ == "__main__":
    main()
'''

    script_path = Path("/tmp/test_long_running_job.py")
    with open(script_path, 'w') as f:
        f.write(script_content)

    return script_path

def signal_handler(signum, frame):
    """处理中断信号"""
    print(f"\nReceived signal {signum}, attempting to interrupt job...")
    # 这里我们不能直接停止job，需要通过调度器接口
    print("Please wait, attempting to stop the job gracefully...")
    # 不要直接退出，而是让主程序处理中断
    raise KeyboardInterrupt

def main():
    print("开始测试真实集群环境中的 submit_job 和 KeyboardInterrupt 中断功能")
    print("="*70)

    # 创建长时间运行的测试脚本
    job_script_path = create_long_running_job_script()
    print(f"创建测试脚本: {job_script_path}")

    try:
        # 初始化调度器环境
        print("初始化调度器环境...")
        config_file_path = Path("/Users/zorro/project/pycharm/ray_multicluster_scheduler/clusters.yaml")
        task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)
        print("调度器环境初始化完成")

        # 提交长时间运行的作业
        print("提交长时间运行的作业...")
        job_id = submit_job(
            entrypoint=f'python {job_script_path}',
            submission_id=f'test_interruption_{int(time.time() * 1000)}',
            metadata={'job_name': 'test_long_running_job', 'test_type': 'interruption'},
            resource_requirements={'CPU': 1},
            preferred_cluster='mac'  # 使用mac集群，根据配置这是偏好集群
        )

        print(f"作业提交成功，ID: {job_id}")

        # 注册信号处理器
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        print("开始轮询作业状态，按 Ctrl+C 中断作业...")
        print("注意：在真实环境中，中断信号会触发中断标志，使 wait_for_all_jobs 停止等待")

        try:
            # 等待作业完成，这将轮询作业状态
            wait_for_all_jobs(submission_ids=[job_id], check_interval=5)
            print("作业正常完成")
        except KeyboardInterrupt:
            print("\n检测到中断信号，正在尝试停止作业...")
            # 尝试停止作业
            cluster_name = 'mac'  # 根据提交时的首选集群
            stop_result = stop_job(job_id, cluster_name)
            if stop_result:
                print(f"成功发送停止命令到作业 {job_id}")
            else:
                print(f"停止作业 {job_id} 失败")
        except Exception as e:
            print(f"等待作业时发生错误: {e}")

        print("测试完成")

    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

    # 清理临时脚本
    try:
        os.remove(job_script_path)
        print(f"已清理临时脚本: {job_script_path}")
    except:
        pass

if __name__ == "__main__":
    main()