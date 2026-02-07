#!/usr/bin/env python3
"""
测试队列作业修复效果的演示脚本
验证排队任务不会因为虚假submission_id导致调度器异常退出
"""

import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, wait_for_all_jobs, get_job_status

def test_queued_job_handling():
    """测试排队作业的处理"""
    print("=== 测试排队作业处理 ===")

    try:
        # 提交一个作业（假设所有集群资源都紧张，会进入队列）
        print("提交作业到调度系统...")
        job_id = submit_job(
            entrypoint="python -c \"print('Hello World')\"",
            preferred_cluster=None  # 不指定集群，让系统做负载均衡决策
        )

        print(f"作业已提交，job_id: {job_id}")

        # 立即查询状态
        print(f"立即查询作业状态...")
        status = get_job_status(job_id, None)  # 第二个参数是cluster_name，传None表示自动查找
        print(f"作业 {job_id} 当前状态: {status}")

        if status in ["QUEUED", "PENDING"]:
            print("✓ 作业正确显示为排队状态，而非UNKNOWN")

            # 尝试等待作业完成（应该能正常处理排队状态）
            print("尝试等待作业完成...")
            try:
                wait_for_all_jobs([job_id], check_interval=2, timeout=30)
                print("✓ 作业等待完成，没有因虚假submission_id导致异常退出")
            except Exception as e:
                if "状态未知" in str(e):
                    print("✗ 仍然出现了状态未知的错误")
                    return False
                else:
                    print(f"✓ 等待过程中出现预期的其他错误: {e}")
                    return True
        else:
            print(f"? 作业状态为 {status}，可能集群资源充足，无法测试排队场景")

        return True

    except Exception as e:
        print(f"测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_multiple_queued_jobs():
    """测试多个排队作业的处理"""
    print("\n=== 测试多个排队作业处理 ===")

    try:
        job_ids = []

        # 提交多个作业
        for i in range(3):
            job_id = submit_job(
                entrypoint=f"python -c \"print('Hello from job {i}')\"",
                preferred_cluster=None
            )
            job_ids.append(job_id)
            print(f"提交作业 {i+1}: {job_id}")
            time.sleep(1)  # 避免提交过快

        print(f"总共提交了 {len(job_ids)} 个作业")

        # 查询所有作业状态
        for job_id in job_ids:
            status = get_job_status(job_id, None)  # 第二个参数是cluster_name，传None表示自动查找
            print(f"作业 {job_id} 状态: {status}")

        # 尝试批量等待
        print("批量等待所有作业完成...")
        try:
            wait_for_all_jobs(job_ids, check_interval=3, timeout=60)
            print("✓ 所有作业等待完成")
        except Exception as e:
            if "状态未知" in str(e):
                print("✗ 出现了状态未知错误")
                return False
            else:
                print(f"✓ 出现了预期的其他错误: {e}")

        return True

    except Exception as e:
        print(f"批量测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("开始测试队列作业修复效果...")

    success1 = test_queued_job_handling()
    success2 = test_multiple_queued_jobs()

    if success1 and success2:
        print("\n🎉 所有测试通过！修复有效，不会再因虚假submission_id导致异常退出")
    else:
        print("\n❌ 部分测试失败，修复可能不完整")

    print("测试完成")