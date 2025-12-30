#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
示例：提交多个job并等待它们全部完成后退出
这个方法可以避免调度器在job执行过程中退出导致队列被清理的问题
"""

import time
import uuid
from typing import List, Tuple
from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, get_job_status
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment


def submit_multiple_jobs(num_jobs: int = 3) -> List[Tuple[str, str]]:
    """
    提交多个job并返回(job_id, submission_id)列表
    """
    print(f"开始提交 {num_jobs} 个job...")

    # 初始化调度器环境
    initialize_scheduler_environment()

    job_info_list = []

    for i in range(num_jobs):
        # 生成唯一的submission_id以避免冲突
        unique_submission_id = f"wait_test_job_{uuid.uuid4().hex[:8]}_{i}"

        # 创建一个简单的entrypoint，执行一些计算并sleep一段时间
        entrypoint = f'python -c "import time; print(\\\"Job {i} starting\\\"); time.sleep(5); print(\\\"Job {i} completed\\\"); result = sum(range(10000)); print(\\\"Result:\\\", result)"'

        try:
            job_id = submit_job(
                entrypoint=entrypoint,
                submission_id=unique_submission_id
            )
            print(f"成功提交job {i+1}/{num_jobs}，ID: {job_id}")
            job_info_list.append((job_id, unique_submission_id))
        except Exception as e:
            print(f"提交job {i+1} 失败: {e}")

    print(f"成功提交 {len(job_info_list)} 个job")
    return job_info_list


def wait_for_all_jobs(job_info_list: List[Tuple[str, str]], check_interval: int = 5):
    """
    轮询等待所有job完成
    """
    print(f"\n开始轮询 {len(job_info_list)} 个job的状态，检查间隔: {check_interval}秒")

    completed_jobs = set()
    total_jobs = len(job_info_list)

    while len(completed_jobs) < total_jobs:
        still_running = []

        for job_id, submission_id in job_info_list:
            if job_id in completed_jobs:
                continue  # 已完成的job跳过

            try:
                status = get_job_status(job_id)
                print(f"Job {job_id} 状态: {status}")

                if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:  # Job完成状态
                    completed_jobs.add(job_id)
                    print(f"Job {job_id} 已完成，状态: {status}")
                else:
                    still_running.append((job_id, status))

            except Exception as e:
                print(f"获取job {job_id} 状态失败: {e}")
                still_running.append((job_id, "UNKNOWN"))

        if still_running:
            print(f"仍有 {len(still_running)} 个job在运行，继续等待...")
            print(f"  运行中的job: {[job_id for job_id, _ in still_running]}")
        else:
            print("所有job都已完成！")

        if len(completed_jobs) < total_jobs:
            print(f"等待 {check_interval} 秒后再次检查...")
            time.sleep(check_interval)

    print(f"\n所有 {total_jobs} 个job都已完成！")


def main():
    """
    主函数：提交job并等待完成
    """
    print("=== 等待所有job完成的示例 ===")

    # 提交多个job
    job_info_list = submit_multiple_jobs(num_jobs=3)

    if not job_info_list:
        print("没有成功提交任何job")
        return

    # 等待所有job完成
    wait_for_all_jobs(job_info_list)

    print("\n所有job已完成，程序可以安全退出")
    print("此时即使客户端程序退出，也不会影响队列中的其他任务")


if __name__ == "__main__":
    main()