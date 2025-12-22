#!/usr/bin/env python3
"""
并发任务测试用例
测试多个任务并发执行和退出机制，重点验证submit_task接口的并发支持
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

    logger.info(f"任务 {task_id} ({task_name}) 开始执行，预计耗时: {duration:.1f}s")
    time.sleep(duration)
    result = {
        "task_id": task_id,
        "task_name": task_name,
        "duration": duration,
        "status": "completed",
        "timestamp": time.time()
    }
    logger.info(f"任务 {task_id} ({task_name}) 执行完成")
    return result


def concurrent_task_test():
    """
    并发任务测试
    """
    logger.info("=== 开始并发任务测试 ===")
    task_lifecycle_manager = None

    try:
        # 1. 初始化调度器环境
        logger.info("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        logger.info("✅ 调度器环境初始化完成")

        # 2. 提交多个任务
        logger.info("2. 提交并发任务...")
        task_results = []

        # 提交更多任务以测试并发性能
        task_configs = [
            {"name": "task_1", "preferred_cluster": "mac", "duration": 1.0},
            {"name": "task_2", "preferred_cluster": "centos", "duration": 1.2},
            {"name": "task_3", "preferred_cluster": None, "duration": 0.8},  # 让调度器自动选择
            {"name": "task_4", "preferred_cluster": "mac", "duration": 1.5},
            {"name": "task_5", "preferred_cluster": "centos", "duration": 0.9},
            {"name": "task_6", "preferred_cluster": "mac", "duration": 1.1},
            {"name": "task_7", "preferred_cluster": "centos", "duration": 1.3},
            {"name": "task_8", "preferred_cluster": None, "duration": 0.7}
        ]

        task_futures = []

        for i, config in enumerate(task_configs):
            try:
                task_id, result = submit_task(
                    func=sample_task,
                    args=(f"task_{i+1}", config["name"], config["duration"]),
                    resource_requirements={"CPU": 0.5},
                    tags=["concurrent", "test", "task"],
                    name=config["name"],
                    preferred_cluster=config["preferred_cluster"]
                )
                task_futures.append({
                    'task_id': task_id,
                    'result': result,
                    'task_name': config["name"]
                })
                logger.info(f"✅ 任务 {config['name']} 提交成功: {task_id}")

            except Exception as e:
                logger.error(f"❌ 提交任务 {config['name']} 失败: {e}")

        # 3. 等待所有任务完成
        logger.info(f"3. 等待 {len(task_futures)} 个任务完成...")
        results = []
        failed_tasks = 0

        # 获取任务结果
        for task_info in task_futures:
            try:
                # 获取任务结果
                result = ray.get(task_info['result'], timeout=10.0)
                results.append(result)
                logger.info(f"✅ 任务完成: {task_info['task_name']} (ID: {task_info['task_id']})")
                logger.info(f"   结果: {result}")
            except Exception as e:
                logger.error(f"❌ 任务失败: {task_info['task_name']} (ID: {task_info['task_id']}) - {e}")
                failed_tasks += 1

        logger.info(f"🎉 任务执行完成! 成功: {len(results)}, 失败: {failed_tasks}")

        # 4. 清理资源
        logger.info("4. 清理资源...")
        if task_lifecycle_manager and hasattr(task_lifecycle_manager, 'stop'):
            logger.info("🛑 停止任务生命周期管理器...")
            task_lifecycle_manager.stop()
            logger.info("✅ 任务生命周期管理器已停止")

        # 强制关闭Ray连接
        try:
            logger.info("🔌 关闭Ray连接...")
            ray.shutdown()
            logger.info("✅ Ray连接已关闭")
        except Exception as e:
            logger.warning(f"⚠️ 关闭Ray连接时出错: {e}")

        logger.info("✅ 并发任务测试完成")
        return len(results) > 0, task_lifecycle_manager

    except Exception as e:
        logger.error(f"❌ 并发任务测试出错: {e}")
        import traceback
        logger.error(f"🔍 详细错误信息:\n{traceback.format_exc()}")
        return False, task_lifecycle_manager


def cleanup_and_exit(task_lifecycle_manager=None):
    """
    资源清理和退出
    """
    try:
        import gc
        import ray

        logger.info("🧹 开始清理资源...")

        # 停止调度器
        try:
            if task_lifecycle_manager and hasattr(task_lifecycle_manager, 'stop'):
                logger.info("🛑 停止任务生命周期管理器...")
                task_lifecycle_manager.stop()
                logger.info("✅ 任务生命周期管理器已停止")
        except Exception as e:
            logger.warning(f"⚠️ 停止任务生命周期管理器时出错: {e}")

        # 关闭Ray连接
        try:
            logger.info("🔌 关闭Ray连接...")
            ray.shutdown()
            logger.info("✅ Ray连接已关闭")
        except Exception as e:
            logger.warning(f"⚠️ 关闭Ray连接时出错: {e}")

        # 强制垃圾回收
        try:
            logger.info("🗑️ 执行垃圾回收...")
            gc.collect()
            logger.info("✅ 垃圾回收完成")
        except Exception as e:
            logger.warning(f"⚠️ 垃圾回收时出错: {e}")

        logger.info("✅ 资源清理完成")

    except Exception as e:
        logger.error(f"❌ 资源清理过程中出错: {e}")


if __name__ == "__main__":
    logger.info("🚀 开始并发任务测试...")

    try:
        # 设置超时
        import signal

        def timeout_handler(signum, frame):
            logger.error("⏰ 测试超时")
            cleanup_and_exit()
            import os
            os._exit(1)

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(120)  # 2分钟超时

        # 执行测试
        success, task_lifecycle_manager = concurrent_task_test()

        # 取消超时
        signal.alarm(0)

        # 清理资源
        cleanup_and_exit(task_lifecycle_manager)

        if success:
            logger.info("🎉 并发任务测试通过")
            sys.exit(0)
        else:
            logger.error("💥 并发任务测试失败")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("⚠️ 用户中断程序")
        cleanup_and_exit()
        import os
        os._exit(0)
    except Exception as e:
        logger.error(f"💥 程序执行出错: {e}")
        cleanup_and_exit()
        import os
        os._exit(1)