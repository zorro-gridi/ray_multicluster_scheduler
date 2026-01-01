#!/usr/bin/env python3
"""
使用装饰器的键盘中断测试脚本
此脚本使用装饰器自动处理键盘中断并停止远程作业
"""
from ray_multicluster_scheduler.app.client_api import (
    initialize_scheduler_environment,
    submit_job,
    wait_for_all_jobs,
    graceful_shutdown_on_keyboard_interrupt
)

import time
import sys
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


@graceful_shutdown_on_keyboard_interrupt
def main():
    config_file_path = env_path / 'Index_Markup_Forecasting/cluster.yaml'
    initialize_scheduler_environment(config_file_path=config_file_path)

    job_ids = []
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
        job_ids.append(job_id)

    wait_for_all_jobs(submission_ids=job_ids, check_interval=10)
    print("所有作业已完成")


if __name__ == '__main__':
    main()