#!/usr/bin/env python3
"""
修正后的键盘中断测试脚本
此脚本正确处理键盘中断并停止远程作业
"""
from ray_multicluster_scheduler.app.client_api import (
    initialize_scheduler_environment,
    submit_job,
    get_job_status,
    wait_for_all_jobs,
    stop_job
)

import time
import sys
import signal
import traceback
from pathlib import Path

# NOTE: 根据平台配置参数
import platform
if platform.system() == "Darwin":
    home_dir = '/Users/zorro'
    PREFERRED_CLUSTER = 'mac'
elif platform.system() == "Linux":
    home_dir = '/home/zorro'
    PREFERRED_CLUSTER = 'centos'


env_path = Path(home_dir) / 'project/pycharm/Fund'
sys.path.insert(0, env_path.as_posix())


# predict
job_config_list = {
    'indx_bunch_daily_predict': Path(home_dir) / 'project/pycharm/Fund/Index_Markup_Forecasting/bunch_and_reverse_model/predict_cluster.py',
    'indx_markup_daily_predict': Path(home_dir) / 'project/pycharm/Fund/Index_Markup_Forecasting/index_ts_forecasting_model/cat/predict_cluster.py',
}

# 全局变量存储作业ID，用于中断处理
running_job_ids = []

def signal_handler(signum, frame):
    """信号处理器，用于优雅地停止远程作业"""
    print(f"\n收到信号 {signum}，正在停止远程作业...")

    # 停止所有正在运行的作业
    for job_id in running_job_ids:
        try:
            # 需要确定作业所在的集群
            from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
            scheduler = get_unified_scheduler()

            cluster_name = None
            if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

            if not cluster_name:
                # 如果无法确定集群，尝试从所有集群查找
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
                                    cluster_name = cluster_config.name
                                    print(f"在集群 {cluster_name} 上找到作业 {job_id}")
                                    break
                            except Exception:
                                continue
                    except Exception:
                        continue

            if cluster_name:
                print(f"正在停止集群 {cluster_name} 上的作业 {job_id}...")
                success = stop_job(job_id, cluster_name)
                if success:
                    print(f"成功发送停止命令到作业 {job_id}")
                else:
                    print(f"停止作业 {job_id} 失败")
            else:
                print(f"无法确定作业 {job_id} 所在的集群")

        except Exception as e:
            print(f"停止作业 {job_id} 时出错: {e}")

    print("中断处理完成，退出程序")
    sys.exit(0)


def main():
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config_file_path = env_path / 'Index_Markup_Forecasting/cluster.yaml'
    initialize_scheduler_environment(config_file_path=config_file_path)

    global running_job_ids
    running_job_ids = []

    # Submit a job using the real cluster
    for job_name, entrypoint in job_config_list.items():
        job_id = submit_job(
            entrypoint=f'python {entrypoint}',
            submission_id=str(int(time.time() * 1000)),
            metadata={'job_name': job_name},
            resource_requirements={'CPU': 16},
            # preferred_cluster=PREFERRED_CLUSTER,
        )
        print(f'Job submitted successfully with ID: {job_id}')
        # Wait a bit for the job to start
        time.sleep(3)
        running_job_ids.append(job_id)

    try:
        wait_for_all_jobs(submission_ids=running_job_ids, check_interval=10)
        print("所有作业已完成")
    except KeyboardInterrupt:
        # 如果在wait_for_all_jobs中捕获到中断，也会调用信号处理器
        print("检测到键盘中断...")
        signal_handler(signal.SIGINT, None)
    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        traceback.print_exc()
        # 停止所有正在运行的作业
        for job_id in running_job_ids:
            try:
                from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
                scheduler = get_unified_scheduler()

                cluster_name = None
                if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                    cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

                if cluster_name:
                    stop_job(job_id, cluster_name)
            except Exception as stop_error:
                print(f"停止作业 {job_id} 时出错: {stop_error}")


if __name__ == '__main__':
    main()