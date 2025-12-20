#!/usr/bin/env python3
"""
改进的并发Actor测试用例
测试多个Actor并发执行和退出机制，重点验证多集群并发支持
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
    submit_actor
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@ray.remote
class ConcurrentActor:
    """
    并发测试Actor类
    """
    def __init__(self, actor_id, name):
        self.actor_id = actor_id
        self.name = name
        logger.info(f"ConcurrentActor {self.actor_id} ({self.name}) 初始化完成")

    def process_task(self, task_name, duration=None):
        """
        处理任务
        """
        if duration is None:
            duration = random.uniform(1, 3)  # 1-3秒随机时间

        logger.info(f"Actor {self.actor_id} 开始处理任务: {task_name} (预计耗时: {duration:.1f}s)")
        time.sleep(duration)
        result = {
            "actor_id": self.actor_id,
            "task_name": task_name,
            "duration": duration,
            "status": "completed",
            "timestamp": time.time()
        }
        logger.info(f"Actor {self.actor_id} 完成任务: {task_name}")
        return result

def concurrent_actor_test():
    """
    改进的并发Actor测试
    """
    logger.info("=== 开始改进的并发Actor测试 ===")
    task_lifecycle_manager = None

    try:
        # 1. 初始化调度器环境
        logger.info("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        logger.info("✅ 调度器环境初始化完成")

        # 2. 提交多个Actor并立即执行任务
        logger.info("2. 提交Actor并执行任务...")
        task_refs = []

        # 提交3个Actor并立即执行任务，避免Actor句柄失效
        actor_configs = [
            {"name": "actor_1", "preferred_cluster": "mac"},
            {"name": "actor_2", "preferred_cluster": "centos"},
            {"name": "actor_3", "preferred_cluster": None}  # 让调度器自动选择
        ]

        actor_handles = []

        for i, config in enumerate(actor_configs):
            try:
                actor_id, actor_handle = submit_actor(
                    actor_class=ConcurrentActor,
                    args=(f"actor_{i+1}", config["name"]),
                    resource_requirements={"CPU": 1},
                    tags=["concurrent", "test"],
                    name=config["name"],
                    preferred_cluster=config["preferred_cluster"]
                )
                actor_handles.append(actor_handle)
                logger.info(f"✅ Actor {config['name']} 提交成功: {actor_id}")

                # 立即执行任务，避免Actor句柄失效
                for task_num in range(2):
                    task_name = f"{config['name']}_task_{task_num+1}"
                    # 随机任务时长
                    duration = random.uniform(1, 2)
                    task_ref = actor_handle.process_task.remote(task_name, duration)
                    task_refs.append({
                        'ref': task_ref,
                        'actor_name': config["name"],
                        'task_name': task_name
                    })
                    logger.info(f"🚀 启动任务: {task_name} (Actor: {config['name']})")

            except Exception as e:
                logger.error(f"❌ 提交Actor {config['name']} 失败: {e}")

        # 3. 等待所有任务完成
        logger.info(f"3. 等待 {len(task_refs)} 个任务完成...")
        results = []
        failed_tasks = 0

        # 分批获取结果
        for task_info in task_refs:
            try:
                result = ray.get(task_info['ref'], timeout=30.0)
                results.append(result)
                logger.info(f"✅ 任务完成: {task_info['task_name']} (Actor: {task_info['actor_name']})")
                logger.info(f"   结果: {result}")
            except Exception as e:
                logger.error(f"❌ 任务失败: {task_info['task_name']} (Actor: {task_info['actor_name']}) - {e}")
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

        logger.info("✅ 改进的并发Actor测试完成")
        return len(results) > 0, task_lifecycle_manager

    except Exception as e:
        logger.error(f"❌ 改进的并发Actor测试出错: {e}")
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
    logger.info("🚀 开始改进的并发Actor测试...")

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
        success, task_lifecycle_manager = concurrent_actor_test()

        # 取消超时
        signal.alarm(0)

        # 清理资源
        cleanup_and_exit(task_lifecycle_manager)

        if success:
            logger.info("🎉 改进的并发Actor测试通过")
            sys.exit(0)
        else:
            logger.error("💥 改进的并发Actor测试失败")
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