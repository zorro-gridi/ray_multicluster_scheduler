#!/usr/bin/env python3
"""
资源阈值测试用例
测试当集群资源使用率超过阈值时，任务如何正确进入队列等待
"""

import sys
import time
import logging
import random
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import ray
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_task
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def sample_task(task_id, task_name, duration=None):
    """
    示例任务函数
    """
    if duration is None:
        duration = random.uniform(0.5, 2.0)  # 0.5-2秒随机时间

    logger.info(f"任务 {task_id} ({task_name}) 开始执行，预计耗时: {duration:.2f}秒")
    time.sleep(duration)
    result = {
        'task_id': task_id,
        'task_name': task_name,
        'duration': duration,
        'status': 'completed',
        'timestamp': time.time()
    }
    logger.info(f"任务 {task_id} ({task_name}) 执行完成")
    return result


def resource_threshold_test():
    """
    资源阈值测试
    """
    logger.info("=== 开始资源阈值测试 ===")
    task_lifecycle_manager = None

    try:
        # 1. 初始化调度器环境
        logger.info("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        logger.info("✅ 调度器环境初始化完成")

        # 2. 提交多个任务以测试资源阈值功能
        logger.info("2. 提交任务以测试资源阈值功能...")
        task_futures = []

        # 提交多个任务，这些任务会消耗资源
        task_configs = [
            {"name": "high_resource_task_1", "preferred_cluster": "mac", "duration": 5.0},
            {"name": "high_resource_task_2", "preferred_cluster": "centos", "duration": 5.0},
            {"name": "high_resource_task_3", "preferred_cluster": "mac", "duration": 5.0},
            {"name": "high_resource_task_4", "preferred_cluster": "centos", "duration": 5.0},
            {"name": "high_resource_task_5", "preferred_cluster": None, "duration": 5.0}
        ]

        # 先提交一些长时间运行的任务来占用资源
        for i, config in enumerate(task_configs[:3]):  # 先提交前3个任务
            try:
                task_id, future = submit_task(
                    func=sample_task,
                    args=(f"task_id_{i+1}", config["name"], config["duration"]),
                    resource_requirements={"CPU": 2},  # 请求较多CPU资源
                    tags=["resource", "test"],
                    name=config["name"],
                    preferred_cluster=config["preferred_cluster"]
                )
                task_futures.append({
                    'future': future,
                    'task_name': config["name"],
                    'task_id': task_id
                })
                logger.info(f"✅ 任务 {config['name']} 提交成功: {task_id}")
            except Exception as e:
                logger.error(f"❌ 提交任务 {config['name']} 失败: {e}")

        # 等待一小段时间让资源被占用
        time.sleep(1)

        # 3. 提交更多任务，这些任务可能会因为资源不足而进入队列
        logger.info("3. 提交更多任务测试队列功能...")
        queued_task_configs = [
            {"name": "queued_task_1", "preferred_cluster": "mac", "duration": 2.0},
            {"name": "queued_task_2", "preferred_cluster": "centos", "duration": 2.0},
            {"name": "queued_task_3", "preferred_cluster": None, "duration": 2.0}
        ]

        queued_task_futures = []
        for i, config in enumerate(queued_task_configs):
            try:
                task_id, future = submit_task(
                    func=sample_task,
                    args=(f"queued_task_id_{i+1}", config["name"], config["duration"]),
                    resource_requirements={"CPU": 1},
                    tags=["queued", "test"],
                    name=config["name"],
                    preferred_cluster=config["preferred_cluster"]
                )
                queued_task_futures.append({
                    'future': future,
                    'task_name': config["name"],
                    'task_id': task_id
                })
                logger.info(f"✅ 队列任务 {config['name']} 提交成功: {task_id}")
            except Exception as e:
                logger.error(f"❌ 提交队列任务 {config['name']} 失败: {e}")

        # 4. 等待所有任务完成
        logger.info(f"4. 等待所有任务完成...")

        # 收集所有future
        all_futures = []
        # 添加普通任务的future
        for task_info in task_futures:
            all_futures.append(task_info['future'])
        # 添加队列任务的future
        for task_info in queued_task_futures:
            all_futures.append(task_info['future'])

        # 等待所有任务完成
        if all_futures:
            # 使用ray.wait等待所有任务完成
            while all_futures:
                ready_futures, remaining_futures = ray.wait(all_futures, timeout=1.0)

                # 处理已完成的任务
                for ready_future in ready_futures:
                    # 查找对应的task_info
                    task_info = None

                    # 查找普通任务
                    for t_info in task_futures:
                        if t_info['future'] == ready_future:
                            task_info = t_info
                            break

                    # 查找队列任务
                    if task_info is None:
                        for q_info in queued_task_futures:
                            if q_info['future'] == ready_future:
                                task_info = q_info
                                break

                    try:
                        result = ray.get(ready_future)
                        if task_info:
                            logger.info(f"✅ 任务完成: {task_info['task_name']}")
                            logger.info(f"   结果: {result}")
                    except Exception as e:
                        if task_info:
                            logger.error(f"❌ 任务失败: {task_info['task_name']} - {e}")

                # 更新剩余任务列表
                all_futures = remaining_futures

                # 如果没有剩余任务，则跳出循环
                if not all_futures:
                    break

        logger.info(f"🎉 所有任务执行完成!")

        # 5. 清理资源
        logger.info("5. 清理资源...")
        if task_lifecycle_manager:
            logger.info("🛑 停止任务生命周期管理器...")
            task_lifecycle_manager.stop()

        logger.info("✅ 资源阈值测试完成")
        return True

    except Exception as e:
        logger.error(f"❌ 资源阈值测试过程中出现异常: {e}")
        import traceback
        logger.error(f"详细错误信息:\n{traceback.format_exc()}")

        # 清理资源
        if task_lifecycle_manager:
            try:
                logger.info("🛑 停止任务生命周期管理器...")
                task_lifecycle_manager.stop()
            except Exception as stop_error:
                logger.error(f"❌ 停止任务生命周期管理器时出现异常: {stop_error}")

        return False


if __name__ == "__main__":
    success = resource_threshold_test()
    if success:
        logger.info("🎉 资源阈值测试通过!")
        sys.exit(0)
    else:
        logger.error("❌ 资源阈值测试失败!")
        sys.exit(1)