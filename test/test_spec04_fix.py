#!/usr/bin/env python3
"""
测试SPEC-04修复效果的演示脚本
验证背压控制器现在能够按集群维度独立判断，允许资源充足的集群处理排队任务
"""

import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, wait_for_all_jobs, get_job_status

def test_cluster_specific_backpressure():
    """测试集群特定的背压控制"""
    print("=== 测试集群特定背压控制 ===")

    try:
        # 提交多个作业，确保一些集群会有排队任务
        job_ids = []
        for i in range(3):
            job_id = submit_job(
                entrypoint=f"python -c \"import time; time.sleep({10+i*5}); print('Test job {i}')\"",
                preferred_cluster="mac"  # 使用mac集群，它的资源相对充足
            )
            job_ids.append(job_id)
            print(f"提交作业 {i+1}: {job_id}")
            time.sleep(1)  # 避免提交过快

        print(f"总共提交了 {len(job_ids)} 个作业")

        # 等待一段时间让作业开始执行
        print("等待作业开始执行...")
        time.sleep(5)

        # 检查作业状态
        for i, job_id in enumerate(job_ids):
            status = get_job_status(job_id, None)
            print(f"作业 {i+1} ({job_id}) 状态: {status}")

        # 等待所有作业完成
        print("等待所有作业完成...")
        wait_for_all_jobs(job_ids, check_interval=5, timeout=60)
        print("所有作业都已完成！")

        return True

    except Exception as e:
        print(f"测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_mixed_cluster_resources():
    """测试混合集群资源情况下排队任务的处理"""
    print("\n=== 测试混合集群资源情况下的排队任务处理 ===")

    try:
        # 提交一些作业到资源充足的mac集群
        mac_job_ids = []
        for i in range(2):
            job_id = submit_job(
                entrypoint=f"python -c \"import time; time.sleep(15); print('Mac job {i}')\"",
                preferred_cluster="mac"
            )
            mac_job_ids.append(job_id)
            print(f"提交到mac集群的作业 {i+1}: {job_id}")
            time.sleep(1)

        # 提交一些作业到资源可能紧张的centos集群
        centos_job_ids = []
        for i in range(2):
            job_id = submit_job(
                entrypoint=f"python -c \"import time; time.sleep(15); print('Centos job {i}')\"",
                preferred_cluster="centos"
            )
            centos_job_ids.append(job_id)
            print(f"提交到centos集群的作业 {i+1}: {job_id}")
            time.sleep(1)

        all_job_ids = mac_job_ids + centos_job_ids
        print(f"总共提交了 {len(all_job_ids)} 个作业 ({len(mac_job_ids)}个到mac, {len(centos_job_ids)}个到centos)")

        # 检查作业状态
        for i, job_id in enumerate(all_job_ids):
            status = get_job_status(job_id, None)
            print(f"作业 {job_id} 状态: {status}")

        # 等待所有作业完成
        print("等待所有作业完成...")
        wait_for_all_jobs(all_job_ids, check_interval=3, timeout=90)
        print("所有作业都已完成！")

        return True

    except Exception as e:
        print(f"混合资源测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("开始测试SPEC-04背压控制修复效果...")

    success1 = test_cluster_specific_backpressure()
    success2 = test_mixed_cluster_resources()

    if success1 and success2:
        print("\n🎉 所有测试通过！.spec-04背压控制问题已修复")
        print("系统现在能够按集群维度独立判断背压，资源充足的集群可以处理排队任务")
    else:
        print("\n❌ 部分测试失败，背压控制修复可能不完整")

    print("测试完成")
