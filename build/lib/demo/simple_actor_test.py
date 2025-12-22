#!/usr/bin/env python3
"""
简化版Actor测试用例
用于隔离和测试Actor提交与退出问题
"""

import sys
import time
import logging
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import ray
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_actor
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@ray.remote
class SimpleActor:
    """
    简单的测试Actor类
    """
    def __init__(self, name):
        self.name = name
        logger.info(f"SimpleActor {self.name} 初始化完成")

    def do_work(self, task_id, duration=2):
        """
        执行简单任务
        """
        logger.info(f"Actor {self.name} 开始执行任务 {task_id}")
        time.sleep(duration)  # 模拟工作
        result = f"Actor {self.name} 完成任务 {task_id}"
        logger.info(f"Actor {self.name} 完成任务 {task_id}")
        return result

def simple_actor_test():
    """
    简单Actor测试
    """
    logger.info("=== 开始简单Actor测试 ===")

    try:
        # 1. 初始化调度器环境
        logger.info("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        logger.info("✅ 调度器环境初始化完成")

        # 2. 提交单个Actor
        logger.info("2. 提交单个Actor...")
        actor_id, actor_handle = submit_actor(
            actor_class=SimpleActor,
            args=("test_actor",),
            resource_requirements={"CPU": 1},
            tags=["test"],
            name="simple_test_actor"
        )
        logger.info(f"✅ Actor提交成功: {actor_id}")

        # 3. 执行任务
        logger.info("3. 执行Actor任务...")
        result_ref = actor_handle.do_work.remote("task_1", duration=2)
        logger.info("🚀 任务已启动")

        # 4. 获取结果
        logger.info("4. 等待任务结果...")
        result = ray.get(result_ref)
        logger.info(f"✅ 任务结果: {result}")

        # 5. 清理资源
        logger.info("5. 清理资源...")
        if task_lifecycle_manager and hasattr(task_lifecycle_manager, 'stop'):
            logger.info("🛑 停止任务生命周期管理器...")
            task_lifecycle_manager.stop()
            logger.info("✅ 任务生命周期管理器已停止")

        logger.info("✅ 简单Actor测试完成")
        return True

    except Exception as e:
        logger.error(f"❌ 简单Actor测试出错: {e}")
        import traceback
        logger.error(f"🔍 详细错误信息:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    logger.info("🚀 开始简单Actor测试...")

    try:
        # 设置超时
        import signal

        def timeout_handler(signum, frame):
            logger.error("⏰ 测试超时")
            import os
            os._exit(1)

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(60)  # 60秒超时

        # 执行测试
        success = simple_actor_test()

        # 取消超时
        signal.alarm(0)

        if success:
            logger.info("🎉 简单Actor测试通过")
            sys.exit(0)
        else:
            logger.error("💥 简单Actor测试失败")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("⚠️ 用户中断程序")
        import os
        os._exit(0)
    except Exception as e:
        logger.error(f"💥 程序执行出错: {e}")
        import os
        os._exit(1)