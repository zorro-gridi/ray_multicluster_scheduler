#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试调度器在资源不足时任务排队功能的测试用例
"""

import time
import threading
from typing import Dict, Any
from ray_multicluster_scheduler.app.client_api.submit_actor import submit_actor, get_actor_status
from ray_multicluster_scheduler.app.client_api.submit_job import submit_job
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment


def test_actor_queue_functionality():
    """
    测试submit_actor在资源不足时排队功能
    """
    print("开始测试submit_actor排队功能...")

    # 初始化调度器环境
    task_lifecycle_manager = initialize_scheduler_environment()

    # 定义一个简单的函数用于测试（因为submit_actor实际是提交函数并将其作为actor运行）
    def test_function(name: str):
        print(f"Function called with name: {name}")
        return f"Hello from {name}"

    try:
        # 提交一个actor任务
        print("提交actor任务...")
        actor_id, actor_handle = submit_actor(test_function, args=("test_actor",))
        print(f"成功提交actor，ID: {actor_id}")

        # 检查任务状态
        status = get_actor_status(actor_id)
        print(f"Actor状态: {status}")

        # 等待一段时间，让调度器有机会处理任务
        print("等待调度器处理任务...")
        time.sleep(5)

        # 再次检查状态
        status = get_actor_status(actor_id)
        print(f"Actor状态: {status}")

        if status == "QUEUED":
            print("✓ Actor成功进入队列等待资源")
        elif status == "COMPLETED":
            print("✓ Actor已创建完成")
            # 由于actor_handle现在可能只是任务ID而不是实际的actor句柄，我们只检查状态
            print("✓ Actor状态检查成功")
        else:
            print(f"? Actor状态未知: {status}")

        print("submit_actor排队功能测试完成")

    except Exception as e:
        print(f"submit_actor测试失败: {e}")
        import traceback
        traceback.print_exc()


def test_job_submission():
    """
    测试submit_job功能
    """
    print("\n开始测试submit_job功能...")

    try:
        # 提交一个简单的job
        import uuid
        unique_submission_id = f"test_job_{uuid.uuid4().hex[:8]}"
        entrypoint = "python -c \"print('Hello from job'); import time; time.sleep(2); print('Job completed')\""
        job_id = submit_job(entrypoint=entrypoint, submission_id=unique_submission_id)
        print(f"成功提交job，ID: {job_id}")

        print("Job提交功能测试完成")

    except Exception as e:
        print(f"submit_job测试失败: {e}")
        import traceback
        traceback.print_exc()


def simulate_resource_constrained_environment():
    """
    模拟资源受限环境的测试
    """
    print("\n模拟资源受限环境测试...")

    # 初始化调度器
    task_lifecycle_manager = initialize_scheduler_environment()

    # 定义一个消耗资源的函数
    def resource_intensive_function(resource_tag: str):
        import time
        print(f"Processing resource intensive task: {resource_tag}")
        time.sleep(2)  # 模拟资源消耗
        return f"Resource intensive task {resource_tag} completed"

    # 提交多个actor以测试排队机制
    actors = []
    for i in range(3):  # 提交3个actor
        try:
            print(f"提交第{i+1}个资源密集型actor...")
            actor_id, actor_handle = submit_actor(resource_intensive_function, args=(f"resource_actor_{i}",))
            actors.append((actor_id, actor_handle))
            print(f"成功提交actor {i+1}，ID: {actor_id}")

            # 检查状态
            status = get_actor_status(actor_id)
            print(f"Actor {i+1} 状态: {status}")

        except Exception as e:
            print(f"提交actor {i+1} 失败: {e}")

    # 等待一段时间观察排队行为
    print("等待调度器处理所有actor...")
    time.sleep(10)

    # 检查所有actor的状态
    for i, (actor_id, _) in enumerate(actors):
        status = get_actor_status(actor_id)
        print(f"Actor {i+1} (ID: {actor_id}) 最终状态: {status}")

    print("资源受限环境测试完成")


if __name__ == "__main__":
    print("开始测试调度器排队功能...")

    # 运行测试
    test_job_submission()
    test_actor_queue_functionality()
    simulate_resource_constrained_environment()

    print("\n测试提交完成，保持运行状态以观察排队任务的处理...")
    print("调度器后台线程正在运行，等待资源可用时处理排队任务...")
    print("按 Ctrl+C 退出程序")

    try:
        # 保持程序运行，让后台线程继续处理排队的任务
        while True:
            time.sleep(10)  # 每10秒检查一次
            print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')} - 调度器正在运行...")
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        print("排队的任务将在后台继续被处理...")