#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试任务队列的去重机制
"""

from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
from ray_multicluster_scheduler.common.model import TaskDescription
from ray_multicluster_scheduler.common.model.job_description import JobDescription


def test_task_queue_dedup():
    """测试任务队列的去重功能"""
    print("开始测试任务队列去重功能...")

    # 创建任务队列实例
    queue = TaskQueue(max_size=100)

    # 创建一些测试任务
    task1 = TaskDescription(task_id="task_001", name="test_task_1")
    task2 = TaskDescription(task_id="task_002", name="test_task_2")
    task3 = TaskDescription(task_id="task_001", name="test_task_1_duplicate")  # 与task1相同的ID

    # 测试全局任务队列去重
    print("\n1. 测试全局任务队列去重:")
    result1 = queue.enqueue_global(task1)
    print(f"   添加任务 {task1.task_id}: {result1}")
    print(f"   全局队列大小: {queue.size()}")

    result2 = queue.enqueue_global(task2)
    print(f"   添加任务 {task2.task_id}: {result2}")
    print(f"   全局队列大小: {queue.size()}")

    result3 = queue.enqueue_global(task3)  # 与task1相同的ID
    print(f"   添加任务 {task3.task_id} (重复ID): {result3}")
    print(f"   全局队列大小: {queue.size()}")

    # 验证最终队列大小
    expected_size = 2  # 因为task1和task3有相同的ID，所以应该只有2个任务
    actual_size = queue.size()
    print(f"   预期队列大小: {expected_size}, 实际队列大小: {actual_size}")
    assert actual_size == expected_size, f"队列大小不匹配: 期望{expected_size}, 实际{actual_size}"
    print("   ✓ 全局任务队列去重测试通过")

    # 测试全局任务队列出队后再次入队
    print("\n2. 测试任务出队后再次入队:")
    dequeued_task = queue.dequeue_global()
    if dequeued_task:
        print(f"   出队任务: {dequeued_task.task_id}")
        print(f"   出队后全局队列大小: {queue.size()}")

        # 重新添加刚出队的任务
        result4 = queue.enqueue_global(dequeued_task)
        print(f"   重新添加任务 {dequeued_task.task_id}: {result4}")
        print(f"   重新添加后全局队列大小: {queue.size()}")

    # 测试集群任务队列去重
    print("\n3. 测试集群任务队列去重:")
    cluster_task1 = TaskDescription(task_id="cluster_task_001", name="cluster_test_task_1")
    cluster_task2 = TaskDescription(task_id="cluster_task_002", name="cluster_test_task_2")
    cluster_task3 = TaskDescription(task_id="cluster_task_001", name="cluster_test_task_1_duplicate")  # 与cluster_task1相同的ID

    result5 = queue.enqueue_cluster(cluster_task1, "cluster_a")
    print(f"   添加任务 {cluster_task1.task_id} 到集群A: {result5}")
    print(f"   集群A队列大小: {queue.size('cluster_a')}")

    result6 = queue.enqueue_cluster(cluster_task2, "cluster_a")
    print(f"   添加任务 {cluster_task2.task_id} 到集群A: {result6}")
    print(f"   集群A队列大小: {queue.size('cluster_a')}")

    result7 = queue.enqueue_cluster(cluster_task3, "cluster_a")  # 与cluster_task1相同的ID
    print(f"   添加任务 {cluster_task3.task_id} 到集群A (重复ID): {result7}")
    print(f"   集群A队列大小: {queue.size('cluster_a')}")

    expected_cluster_size = 2
    actual_cluster_size = queue.size("cluster_a")
    print(f"   预期集群A队列大小: {expected_cluster_size}, 实际队列大小: {actual_cluster_size}")
    assert actual_cluster_size == expected_cluster_size, f"集群A队列大小不匹配: 期望{expected_cluster_size}, 实际{actual_cluster_size}"
    print("   ✓ 集群任务队列去重测试通过")

    print("\n4. 测试作业队列去重:")
    # 创建一些测试作业
    job1 = JobDescription(job_id="job_001", entrypoint="python test1.py")
    job2 = JobDescription(job_id="job_002", entrypoint="python test2.py")
    job3 = JobDescription(job_id="job_001", entrypoint="python test1_duplicate.py")  # 与job1相同的ID

    result8 = queue.enqueue_global_job(job1)
    print(f"   添加作业 {job1.job_id}: {result8}")
    print(f"   全局作业队列大小: {queue.job_size()}")

    result9 = queue.enqueue_global_job(job2)
    print(f"   添加作业 {job2.job_id}: {result9}")
    print(f"   全局作业队列大小: {queue.job_size()}")

    result10 = queue.enqueue_global_job(job3)  # 与job1相同的ID
    print(f"   添加作业 {job3.job_id} (重复ID): {result10}")
    print(f"   全局作业队列大小: {queue.job_size()}")

    expected_job_size = 2
    actual_job_size = queue.job_size()
    print(f"   预期全局作业队列大小: {expected_job_size}, 实际队列大小: {actual_job_size}")
    assert actual_job_size == expected_job_size, f"作业队列大小不匹配: 期望{expected_job_size}, 实际{actual_job_size}"
    print("   ✓ 全局作业队列去重测试通过")

    # 测试集群作业队列去重
    print("\n5. 测试集群作业队列去重:")
    cluster_job1 = JobDescription(job_id="cluster_job_001", entrypoint="python cluster_test1.py")
    cluster_job2 = JobDescription(job_id="cluster_job_002", entrypoint="python cluster_test2.py")
    cluster_job3 = JobDescription(job_id="cluster_job_001", entrypoint="python cluster_test1_duplicate.py")  # 与cluster_job1相同的ID

    result11 = queue.enqueue_cluster_job(cluster_job1, "cluster_b")
    print(f"   添加作业 {cluster_job1.job_id} 到集群B: {result11}")
    print(f"   集群B作业队列大小: {queue.job_size('cluster_b')}")

    result12 = queue.enqueue_cluster_job(cluster_job2, "cluster_b")
    print(f"   添加作业 {cluster_job2.job_id} 到集群B: {result12}")
    print(f"   集群B作业队列大小: {queue.job_size('cluster_b')}")

    result13 = queue.enqueue_cluster_job(cluster_job3, "cluster_b")  # 与cluster_job1相同的ID
    print(f"   添加作业 {cluster_job3.job_id} 到集群B (重复ID): {result13}")
    print(f"   集群B作业队列大小: {queue.job_size('cluster_b')}")

    expected_cluster_job_size = 2
    actual_cluster_job_size = queue.job_size("cluster_b")
    print(f"   预期集群B作业队列大小: {expected_cluster_job_size}, 实际队列大小: {actual_cluster_job_size}")
    assert actual_cluster_job_size == expected_cluster_job_size, f"集群B作业队列大小不匹配: 期望{expected_cluster_job_size}, 实际{actual_cluster_job_size}"
    print("   ✓ 集群作业队列去重测试通过")

    print("\n所有去重测试通过！")


if __name__ == "__main__":
    test_task_queue_dedup()