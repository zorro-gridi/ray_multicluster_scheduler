#!/usr/bin/env python3
"""
自动测试用户程序中断功能的脚本
此脚本将提交一个长时间运行的作业，然后自动触发中断信号来测试优雅关闭功能
"""
import time
import signal
import sys
import os
import subprocess
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
from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler

def create_long_running_user_program():
    """创建一个模拟的长时间运行的用户程序"""
    script_content = '''#!/usr/bin/env python3
"""
模拟的用户程序 daily_model_predict.py
"""
import time
import sys

def main():
    print("Starting daily model prediction...")
    print("This is a simulation of the daily_model_predict.py script")

    # 模拟长时间运行的任务
    for i in range(120):  # 运行约120秒
        print(f"Processing batch {i+1}/120...")
        time.sleep(1)  # 每批次处理1秒

        # 每10批次输出一次进度
        if (i + 1) % 10 == 0:
            print(f"Progress: {(i+1)/120*100:.1f}% completed")

    print("Daily model prediction completed successfully!")
    return "SUCCESS"

if __name__ == "__main__":
    result = main()
    print(f"Script result: {result}")
'''

    # 将模拟脚本保存到项目目录中
    script_path = Path('/Users/zorro/project/pycharm/ray_multicluster_scheduler/test_scripts/daily_model_predict.py')
    script_path.parent.mkdir(exist_ok=True)
    with open(script_path, 'w') as f:
        f.write(script_content)

    return str(script_path)

def test_auto_interrupt():
    """自动测试中断功能"""
    print("开始自动中断功能测试...")

    # 创建模拟的用户程序
    user_program_path = create_long_running_user_program()
    print(f"创建模拟用户程序: {user_program_path}")

    # 初始化调度器环境
    print("初始化调度器环境...")
    task_lifecycle_manager = initialize_scheduler_environment()

    # 准备运行环境
    runtime_env = {
        "env_vars": {
            "PYTHONPATH": "/Users/zorro/project/pycharm/ray_multicluster_scheduler/test_scripts"
        }
    }

    # 构建执行命令
    entrypoint = f"/Users/zorro/miniconda3/envs/k8s/bin/python {user_program_path}"

    print(f"提交长时间运行的作业...")
    try:
        # 提交作业
        job_id = submit_job(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            metadata={'job_name': 'auto_interrupt_test', 'test_type': 'auto_interrupt'},
            resource_requirements={'CPU': 1},
            preferred_cluster='mac'  # 使用偏好集群
        )

        print(f'作业提交成功，ID: {job_id}')

        print('等待作业启动...')
        time.sleep(5)  # 等待作业启动

        # 检查作业状态
        print('检查作业状态...')
        scheduler = get_unified_scheduler()
        cluster_name = None
        if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
            cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

        if cluster_name:
            status = get_job_status(job_id, cluster_name)
            print(f'作业 {job_id} 在集群 {cluster_name} 的状态: {status}')
        else:
            from ray_multicluster_scheduler.app.client_api.submit_job import _get_job_status_from_all_clusters
            status = _get_job_status_from_all_clusters(job_id)
            print(f'作业 {job_id} 状态 (自动检测集群): {status}')

        # 启动一个线程来模拟中断
        import threading

        def trigger_interrupt():
            print("\n5秒后将自动触发中断...")
            time.sleep(5)  # 等待5秒后触发中断
            print("\n触发中断信号...")
            # 模拟键盘中断
            import ctypes
            import threading
            # 获取当前线程并抛出KeyboardInterrupt
            thread_id = threading.get_ident()
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_id),
                ctypes.py_object(KeyboardInterrupt))
            if res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_id), 0)

        # 启动中断线程
        interrupt_thread = threading.Thread(target=trigger_interrupt)
        interrupt_thread.daemon = True
        interrupt_thread.start()

        try:
            # 等待作业完成 - 这里会轮询作业状态
            print('开始 wait_for_all_jobs，这将轮询作业状态...')
            wait_for_all_jobs(submission_ids=[job_id], check_interval=2)
            print("作业正常完成（中断未触发或作业完成前完成）")
        except KeyboardInterrupt:
            print("\n检测到键盘中断，正在停止作业...")

            # 检查作业所在的集群
            final_cluster_name = None
            if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                final_cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

            if not final_cluster_name:
                # 需要遍历所有集群来找到作业
                from ray_multicluster_scheduler.control_plane.config import ConfigManager
                config_manager = ConfigManager()
                cluster_configs = config_manager.get_cluster_configs()

                for cluster_config in cluster_configs:
                    try:
                        job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_config.name)
                        if job_client:
                            try:
                                status = job_client.get_job_status(job_id)
                                if status in ['PENDING', 'RUNNING', 'STOPPED', 'FAILED', 'SUCCEEDED']:
                                    final_cluster_name = cluster_config.name
                                    print(f"在集群 {final_cluster_name} 上找到作业 {job_id}")
                                    break
                            except Exception:
                                continue
                    except Exception:
                        continue

            if final_cluster_name:
                print(f"尝试停止集群 {final_cluster_name} 上的作业 {job_id}")
                success = stop_job(job_id, final_cluster_name)
                if success:
                    print(f"成功发送停止命令到作业 {job_id}")
                else:
                    print(f"停止作业 {job_id} 失败")

                # 等待一段时间让停止操作生效
                time.sleep(3)

                # 再次检查状态
                final_status = get_job_status(job_id, final_cluster_name)
                print(f"作业 {job_id} 最终状态: {final_status}")
            else:
                print(f"无法确定作业 {job_id} 所在的集群")

            print("中断处理完成")
            return True
        except Exception as e:
            print(f"等待作业时发生错误: {e}")
            import traceback
            traceback.print_exc()
            return False

    except Exception as e:
        print(f"提交或运行作业时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_manual_interrupt():
    """手动测试中断功能 - 为用户提供指导"""
    print("手动中断功能测试说明:")
    print("1. 运行此命令后，作业将被提交到集群")
    print("2. 作业将在后台运行，脚本将轮询作业状态")
    print("3. 您需要手动按下 Ctrl+C 来触发中断")
    print("4. 脚本将捕获中断信号并尝试停止远程作业")
    print("\n注意：由于技术限制，自动中断测试可能不适用于所有环境")
    print("建议使用手动测试方法来验证中断功能\n")

    # 创建模拟的用户程序
    user_program_path = create_long_running_user_program()
    print(f"使用模拟用户程序: {user_program_path}")

    # 初始化调度器环境
    print("初始化调度器环境...")
    task_lifecycle_manager = initialize_scheduler_environment()

    # 准备运行环境
    runtime_env = {
        "env_vars": {
            "PYTHONPATH": "/Users/zorro/project/pycharm/ray_multicluster_scheduler/test_scripts"
        }
    }

    # 构建执行命令
    entrypoint = f"/Users/zorro/miniconda3/envs/k8s/bin/python {user_program_path}"

    print(f"提交长时间运行的作业...")
    try:
        # 提交作业
        job_id = submit_job(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            metadata={'job_name': 'manual_interrupt_test', 'test_type': 'manual_interrupt'},
            resource_requirements={'CPU': 1},
            preferred_cluster='mac'  # 使用偏好集群
        )

        print(f'作业提交成功，ID: {job_id}')

        print('等待作业启动...')
        time.sleep(5)  # 等待作业启动

        # 检查作业状态
        print('检查作业状态...')
        scheduler = get_unified_scheduler()
        cluster_name = None
        if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
            cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

        if cluster_name:
            status = get_job_status(job_id, cluster_name)
            print(f'作业 {job_id} 在集群 {cluster_name} 的状态: {status}')
        else:
            from ray_multicluster_scheduler.app.client_api.submit_job import _get_job_status_from_all_clusters
            status = _get_job_status_from_all_clusters(job_id)
            print(f'作业 {job_id} 状态 (自动检测集群): {status}')

        print('\n现在请手动按下 Ctrl+C 来中断作业等待...')
        print('脚本将捕获中断信号并尝试优雅地停止远程作业...')

        # 等待作业完成 - 这里会轮询作业状态
        print('开始 wait_for_all_jobs，这将轮询作业状态...')
        wait_for_all_jobs(submission_ids=[job_id], check_interval=2)
        print("作业正常完成（未测试中断功能）")

    except KeyboardInterrupt:
        print("\n检测到键盘中断，正在停止作业...")

        # 检查作业所在的集群
        final_cluster_name = None
        scheduler = get_unified_scheduler()
        if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
            final_cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

        if not final_cluster_name:
            # 需要遍历所有集群来找到作业
            from ray_multicluster_scheduler.control_plane.config import ConfigManager
            config_manager = ConfigManager()
            cluster_configs = config_manager.get_cluster_configs()

            for cluster_config in cluster_configs:
                try:
                    job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_config.name)
                    if job_client:
                        try:
                            status = job_client.get_job_status(job_id)
                            if status in ['PENDING', 'RUNNING', 'STOPPED', 'FAILED', 'SUCCEEDED']:
                                final_cluster_name = cluster_config.name
                                print(f"在集群 {final_cluster_name} 上找到作业 {job_id}")
                                break
                        except Exception:
                            continue
                except Exception:
                    continue

        if final_cluster_name:
            print(f"尝试停止集群 {final_cluster_name} 上的作业 {job_id}")
            success = stop_job(job_id, final_cluster_name)
            if success:
                print(f"成功发送停止命令到作业 {job_id}")
            else:
                print(f"停止作业 {job_id} 失败")

            # 等待一段时间让停止操作生效
            time.sleep(3)

            # 再次检查状态
            final_status = get_job_status(job_id, final_cluster_name)
            print(f"作业 {job_id} 最终状态: {final_status}")
        else:
            print(f"无法确定作业 {job_id} 所在的集群")

        print("中断处理完成")
        return True
    except Exception as e:
        print(f"提交或运行作业时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("自动测试用户程序中断功能")
    print("="*50)
    print("选项 1: 自动中断测试 (实验性)")
    print("选项 2: 手动中断测试 (推荐)")
    print("="*50)

    choice = input("请选择测试方式 (1 或 2，默认为 2): ").strip()

    if choice == "1":
        test_auto_interrupt()
    else:
        test_manual_interrupt()