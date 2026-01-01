#!/usr/bin/env python3
"""
测试在真实集群环境中 submit_job 提交任务，并使用 KeyboardInterrupt 中断正在运行中的 ray job
"""
import time
import signal
import sys
import threading
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
    script_content = '''import time
import ray

@ray.remote
def long_running_task():
    for i in range(120):  # 120秒的任务
        print(f'Working on long task... {i+1}/120')
        time.sleep(1)
    return 'completed'

if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init()

    result = ray.get(long_running_task.remote())
    print(f'Task result: {result}')
    ray.shutdown()
'''

    # 将脚本保存在项目目录中，确保远程集群可以访问
    script_path = Path('/Users/zorro/project/pycharm/ray_multicluster_scheduler/test_scripts/interrupt_test_job.py')
    script_path.parent.mkdir(exist_ok=True)  # 创建目录如果不存在
    with open(script_path, 'w') as f:
        f.write(script_content)

    return script_path


def test_keyboard_interrupt_with_wait_for_all_jobs():
    """测试 wait_for_all_jobs 中的键盘中断功能"""
    print("开始测试 wait_for_all_jobs 中的键盘中断功能...")
    print("="*60)

    # 初始化调度器环境
    print('初始化调度器环境...')
    config_file_path = Path('/Users/zorro/project/pycharm/ray_multicluster_scheduler/clusters.yaml')
    task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)
    print('调度器环境初始化完成')

    # 创建长时间运行的测试脚本
    script_path = create_long_running_job_script()
    print(f'创建测试脚本: {script_path}')

    try:
        # 提交长时间运行的作业到centos集群
        print('提交长时间运行的作业到centos集群...')
        job_id = submit_job(
            entrypoint=f'python {script_path}',
            submission_id=f'test_interruption_job_{int(time.time() * 1000)}',
            metadata={'job_name': 'interrupt_test_job', 'test_type': 'interruption'},
            resource_requirements={'CPU': 1},
            preferred_cluster='centos'  # 使用centos集群
        )

        print(f'作业提交成功，ID: {job_id}')

        print('等待作业启动...')
        time.sleep(5)  # 等待作业启动

        # 检查作业状态
        print('检查作业状态...')
        status = get_job_status(job_id, 'centos')
        print(f'作业 {job_id} 在centos集群的状态: {status}')

        if status in ['PENDING', 'RUNNING']:
            print('\\n现在测试键盘中断功能...')
            print('模拟用户按下 Ctrl+C 中断 wait_for_all_jobs...')

            # 创建一个线程来模拟中断 - 设置中断标志
            def simulate_keyboard_interrupt():
                time.sleep(3)  # 等待3秒后触发中断
                print('\\n触发中断标志...')
                from ray_multicluster_scheduler.app.client_api.submit_job import _interrupted
                _interrupted.set()

            interrupt_thread = threading.Thread(target=simulate_keyboard_interrupt)
            interrupt_thread.start()

            try:
                # 等待作业完成 - 这里会轮询作业状态
                print('开始 wait_for_all_jobs，这将轮询作业状态...')
                wait_for_all_jobs(submission_ids=[job_id], check_interval=2)
                print('作业正常完成（这不应该发生，因为我们会中断它）')
            except KeyboardInterrupt:
                print('成功捕获到 KeyboardInterrupt 异常')
                print('wait_for_all_jobs 已被中断')
            except Exception as e:
                print(f'等待作业时发生异常: {e}')

            interrupt_thread.join(timeout=2)

            # 验证作业状态
            print('\\n检查作业的最终状态...')
            try:
                final_status = get_job_status(job_id, 'centos')
                print(f'作业 {job_id} 的最终状态: {final_status}')
            except Exception as e:
                print(f'获取最终状态失败: {e}')

        else:
            print(f'作业状态不是PENDING或RUNNING，实际状态: {status}')

    except Exception as e:
        print(f'测试过程中发生错误: {e}')
        import traceback
        traceback.print_exc()

    # 清理临时脚本
    try:
        import os
        os.remove(script_path)
        print('\\n已清理临时脚本')
    except:
        pass


def test_stop_job_functionality():
    """测试 stop_job 功能"""
    print("\\n\\n开始测试 stop_job 功能...")
    print("="*60)

    # 初始化调度器环境
    print('初始化调度器环境...')
    config_file_path = Path('/Users/zorro/project/pycharm/ray_multicluster_scheduler/clusters.yaml')
    task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)
    print('调度器环境初始化完成')

    # 创建长时间运行的测试脚本
    script_path = create_long_running_job_script()
    print(f'创建测试脚本: {script_path}')

    try:
        # 提交长时间运行的作业到centos集群
        print('提交长时间运行的作业到centos集群...')
        job_id = submit_job(
            entrypoint=f'python {script_path}',
            submission_id=f'test_stop_job_{int(time.time() * 1000)}',
            metadata={'job_name': 'stop_test_job', 'test_type': 'stop_functionality'},
            resource_requirements={'CPU': 1},
            preferred_cluster='centos'  # 使用centos集群
        )

        print(f'作业提交成功，ID: {job_id}')

        print('等待作业启动...')
        time.sleep(5)  # 等待作业启动

        # 检查作业状态
        print('检查作业状态...')
        status = get_job_status(job_id, 'centos')
        print(f'作业 {job_id} 在centos集群的状态: {status}')

        if status in ['PENDING', 'RUNNING']:
            print('\\n使用 stop_job 接口停止正在运行的作业...')
            stop_result = stop_job(job_id, 'centos')
            if stop_result:
                print(f'成功发送停止命令到作业 {job_id} (在centos集群上)')
            else:
                print(f'停止作业 {job_id} 失败')

            print('\\n等待几秒钟让停止命令生效...')
            time.sleep(5)

            print('再次检查作业状态...')
            try:
                final_status = get_job_status(job_id, 'centos')
                print(f'作业 {job_id} 的最终状态: {final_status}')

                if final_status in ['STOPPED', 'FAILED']:
                    print('作业已成功停止')
                else:
                    print(f'作业状态仍然是: {final_status}')
            except Exception as e:
                print(f'获取最终状态失败: {e}')
        else:
            print(f'作业状态不是PENDING或RUNNING，实际状态: {status}')

    except Exception as e:
        print(f'测试过程中发生错误: {e}')
        import traceback
        traceback.print_exc()

    # 清理临时脚本
    try:
        import os
        os.remove(script_path)
        print('\\n已清理临时脚本')
    except:
        pass


if __name__ == "__main__":
    print("测试真实集群环境中的 submit_job 和中断功能")
    print("="*70)

    # 测试键盘中断功能
    test_keyboard_interrupt_with_wait_for_all_jobs()

    # 测试停止作业功能
    test_stop_job_functionality()

    print("\\n\\n所有测试完成！")