#!/usr/bin/env python3
"""
测试用户程序 /Users/zorro/project/pycharm/Fund/Index_Markup_Forecasting/daily_model_predict.py 的中断功能
该脚本将提交用户程序作为 Ray 作业运行，并测试 KeyboardInterrupt 优雅关闭功能
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

def test_user_program_interrupt():
    """测试用户程序的中断功能"""
    print("开始测试用户程序中断功能...")

    # 初始化调度器环境
    print("初始化调度器环境...")
    task_lifecycle_manager = initialize_scheduler_environment()

    # 检查用户程序是否存在
    user_program_path = "/Users/zorro/project/pycharm/Fund/Index_Markup_Forecasting/daily_model_predict.py"
    if not os.path.exists(user_program_path):
        print(f"错误：用户程序不存在于路径: {user_program_path}")
        return False

    print(f"找到用户程序: {user_program_path}")

    # 准备运行环境 - 使用正确的 Python 解释器
    runtime_env = {
        "env_vars": {
            "PYTHONPATH": "/Users/zorro/project/pycharm/Fund/Index_Markup_Forecasting"
        }
    }

    # 构建执行命令
    entrypoint = f"/Users/zorro/miniconda3/envs/k8s/bin/python {user_program_path}"

    print(f"提交作业，entrypoint: {entrypoint}")

    try:
        # 提交作业
        job_id = submit_job(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            metadata={'job_name': 'daily_model_predict_test', 'test_type': 'interrupt_test'},
            resource_requirements={'CPU': 1},
            preferred_cluster='mac'  # 使用偏好集群
        )

        print(f'作业提交成功，ID: {job_id}')

        print('等待作业启动...')
        time.sleep(5)  # 等待作业启动

        # 检查作业状态
        print('检查作业状态...')
        # 尝试获取作业所在的集群
        from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
        scheduler = get_unified_scheduler()
        cluster_name = None
        if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
            cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

        # 如果无法获取集群信息，尝试从所有集群查找
        if not cluster_name:
            from ray_multicluster_scheduler.app.client_api.submit_job import _get_job_status_from_all_clusters
            status = _get_job_status_from_all_clusters(job_id)
            print(f'作业 {job_id} 状态 (自动检测集群): {status}')
        else:
            status = get_job_status(job_id, cluster_name)
            print(f'作业 {job_id} 在集群 {cluster_name} 的状态: {status}')

        # 测试中断功能
        print('\n现在测试键盘中断功能...')
        print('请手动按下 Ctrl+C 来中断作业等待...')

        try:
            # 等待作业完成 - 这里会轮询作业状态
            print('开始 wait_for_all_jobs，这将轮询作业状态...')
            wait_for_all_jobs(submission_ids=[job_id], check_interval=2)
            print("作业正常完成（未测试中断功能）")
        except KeyboardInterrupt:
            print("\n检测到键盘中断，正在停止作业...")

            # 检查作业所在的集群
            final_cluster_name = None
            if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                final_cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

            if not final_cluster_name:
                # 需要遍历所有集群来找到作业
                from ray_multicluster_scheduler.scheduler.connection.job_client_pool import JobClientPool
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
                time.sleep(2)

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
    print("测试用户程序中断功能")
    print("注意：此测试需要您手动按下 Ctrl+C 来触发中断")
    test_user_program_interrupt()