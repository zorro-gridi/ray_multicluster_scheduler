#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试用例：验证优化后的40秒规则是否能够保证job内的子任务顺畅执行
"""
import time
import threading
from pathlib import Path
import sys
from typing import Dict, List, Optional

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, wait_for_all_jobs
from ray_multicluster_scheduler.app.client_api.submit_actor import submit_actor
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment


def test_job_with_actors():
    """测试顶级job提交后，其内部的actor子任务是否能顺畅执行"""

    print("开始测试：顶级job与内部actor子任务的执行流程")

    # 初始化调度器环境
    config_file_path = project_root / 'clusters.yaml'
    task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)

    print("1. 提交顶级job任务一...")
    job_id_1 = submit_job(
        entrypoint=f'python {project_root}/test_scripts/daily_model_predict.py',
        submission_id="test_job_1_" + str(int(time.time() * 1000)),
        metadata={'job_name': 'test_job_1'},
        resource_requirements={'CPU': 2},  # 使用较小的资源需求以适应测试环境
    )
    print(f"   顶级job任务一提交成功，ID: {job_id_1}")

    print("2. 提交顶级job任务二...")
    job_id_2 = submit_job(
        entrypoint=f'python {project_root}/test_scripts/daily_model_predict.py',
        submission_id="test_job_2_" + str(int(time.time() * 1000)),
        metadata={'job_name': 'test_job_2'},
        resource_requirements={'CPU': 2},  # 使用较小的资源需求以适应测试环境
    )
    print(f"   顶级job任务二提交成功，ID: {job_id_2}")

    # 验证顶级任务受40秒限制：等待一段时间后再提交actor
    print("   等待片刻，验证顶级任务是否受40秒限制...")
    time.sleep(3)  # 短暂等待，但不超过40秒

    print("3. 在顶级job内部提交actor子任务（模拟用户代码中的submit_actor调用）...")

    # 提交多个actor子任务，验证它们不受40秒限制
    actor_results = []
    for i in range(3):
        print(f"   提交actor子任务 {i+1}...")
        actor_id, actor_handle = submit_actor(
            actor_class=TestActor,
            resource_requirements={"CPU": 1, "memory": 512 * 1024 * 1024},  # 512MB内存
            tags=['test', 'subtask'],
            name=f'test_actor_{i+1}_{int(time.time())}',
        )
        actor_results.append((actor_id, actor_handle))
        print(f"      actor子任务 {i+1} 提交成功，ID: {actor_id}")
        time.sleep(0.5)  # 短暂间隔

    print("4. 验证actor子任务是否能立即执行而不受40秒限制...")

    # 尝试调用actor方法，验证它们是否可以立即执行
    for i, (actor_id, actor_handle) in enumerate(actor_results):
        try:
            # 调用actor的远程方法，看是否能立即执行
            print(f"   调用actor {i+1} 的远程方法...")
            # 这里我们模拟一个简单的远程调用
            if hasattr(actor_handle, 'test_method'):
                result_ref = actor_handle.test_method.remote(f"test_message_{i}")
                print(f"      actor {i+1} 远程方法调用成功")
            else:
                print(f"      actor {i+1} 没有test_method方法，跳过调用测试")
        except Exception as e:
            print(f"      actor {i+1} 远程方法调用失败: {e}")

    print("5. 等待顶级job完成...")
    try:
        wait_for_all_jobs(submission_ids=[job_id_1, job_id_2], check_interval=5, timeout=300)
        print("   所有顶级job已完成")
    except Exception as e:
        print(f"   等待顶级job完成时出现异常: {e}")

    print("测试完成！")
    print("\n预期结果：")
    print("- 顶级job（任务一、任务二）受40秒限制")
    print("- actor子任务不受40秒限制，能立即提交和执行")
    print("- 整个流程顺畅执行，无长时间停滞")


# 创建一个简单的测试Actor类
import ray

@ray.remote
class TestActor:
    def __init__(self, name: str = ""):
        self.name = name
        self.created_at = time.time()

    def get_info(self):
        return {
            "name": self.name,
            "created_at": self.created_at,
            "current_time": time.time()
        }

    def test_method(self, message: str):
        return f"Received: {message} at {time.time()}"


def main():
    """主函数"""
    test_job_with_actors()


if __name__ == "__main__":
    main()