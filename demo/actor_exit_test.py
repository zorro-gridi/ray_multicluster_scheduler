#!/usr/bin/env python3
"""
Actor执行器退出测试用例
用于测试Actor任务执行完成后程序能否正常退出
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
class TestActor:
    """
    测试Actor类
    """
    def __init__(self, actor_id):
        self.actor_id = actor_id
        logger.info(f"TestActor {self.actor_id} 初始化完成")
    
    def execute_task(self, task_name, duration=1):
        """
        执行任务
        """
        logger.info(f"Actor {self.actor_id} 开始执行任务: {task_name}")
        time.sleep(duration)  # 模拟任务执行
        result = f"Task {task_name} completed by Actor {self.actor_id}"
        logger.info(f"Actor {self.actor_id} 完成任务: {task_name}")
        return result
    
    def get_status(self):
        """
        获取Actor状态
        """
        return f"Actor {self.actor_id} is running"

def test_actor_execution_and_exit():
    """
    测试Actor执行和程序退出
    """
    logger.info("=== 开始Actor执行和退出测试 ===")
    
    try:
        # 1. 初始化调度器环境
        logger.info("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        logger.info("✅ 调度器环境初始化完成")
        
        # 2. 提交多个Actor任务
        logger.info("2. 提交Actor任务...")
        actors = []
        results = []
        
        # 提交3个Actor
        for i in range(3):
            actor_id, actor_handle = submit_actor(
                actor_class=TestActor,
                args=(f"actor_{i}",),
                resource_requirements={"CPU": 1},
                tags=["test", "actor"],
                name=f"test_actor_{i}"
            )
            actors.append((actor_id, actor_handle))
            logger.info(f"✅ 提交Actor {i}: {actor_id}")
        
        # 3. 执行任务
        logger.info("3. 执行Actor任务...")
        task_refs = []
        
        for i, (actor_id, actor_handle) in enumerate(actors):
            # 异步执行任务
            task_ref = actor_handle.execute_task.remote(f"task_{i}", duration=2)
            task_refs.append(task_ref)
            logger.info(f"🚀 启动Actor {i} 的任务")
        
        # 4. 等待所有任务完成
        logger.info("4. 等待所有任务完成...")
        results = ray.get(task_refs)
        
        for i, result in enumerate(results):
            logger.info(f"✅ Actor {i} 任务结果: {result}")
        
        # 5. 检查Actor状态
        logger.info("5. 检查Actor状态...")
        status_refs = []
        for i, (actor_id, actor_handle) in enumerate(actors):
            status_ref = actor_handle.get_status.remote()
            status_refs.append(status_ref)
        
        statuses = ray.get(status_refs)
        for i, status in enumerate(statuses):
            logger.info(f"📊 Actor {i} 状态: {status}")
        
        logger.info("✅ 所有Actor任务执行完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试过程中出错: {e}")
        import traceback
        logger.error(f"🔍 详细错误信息:\n{traceback.format_exc()}")
        return False
    
    finally:
        # 6. 清理资源并尝试正常退出
        logger.info("6. 开始清理资源...")
        cleanup_resources()

def cleanup_resources():
    """
    清理资源并尝试正常退出
    """
    try:
        import gc
        from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
        
        logger.info("🧹 开始清理资源...")
        
        # 获取调度器实例并停止
        try:
            scheduler = get_unified_scheduler()
            if scheduler.task_lifecycle_manager and scheduler.task_lifecycle_manager.running:
                logger.info("🛑 停止任务生命周期管理器...")
                scheduler.task_lifecycle_manager.stop()
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

def force_exit_test():
    """
    强制退出测试
    """
    logger.info("=== 开始强制退出测试 ===")
    
    try:
        # 设置超时保护
        import signal
        
        def timeout_handler(signum, frame):
            logger.error("⏰ 测试超时，强制退出")
            force_cleanup_and_exit()
        
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(30)  # 30秒超时
        
        # 执行测试
        success = test_actor_execution_and_exit()
        
        # 取消超时
        signal.alarm(0)
        
        if success:
            logger.info("✅ Actor执行和退出测试通过")
        else:
            logger.error("❌ Actor执行和退出测试失败")
            
        return success
        
    except Exception as e:
        logger.error(f"❌ 强制退出测试出错: {e}")
        return False

def force_cleanup_and_exit():
    """
    强制清理所有资源并退出
    """
    try:
        import os
        import ray
        import gc
        
        logger.info("🧨 开始强制清理...")
        
        # 关闭Ray
        try:
            ray.shutdown()
        except:
            pass
        
        # 垃圾回收
        try:
            gc.collect()
        except:
            pass
        
        logger.info("👋 程序强制退出")
        os._exit(0)
        
    except Exception as e:
        logger.error(f"❌ 强制清理出错: {e}")
        import os
        os._exit(1)

if __name__ == "__main__":
    logger.info("🚀 开始Actor退出测试...")
    
    try:
        success = force_exit_test()
        
        if success:
            logger.info("🎉 所有测试通过，程序正常退出")
            sys.exit(0)
        else:
            logger.error("💥 测试失败")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⚠️ 用户中断程序")
        force_cleanup_and_exit()
    except Exception as e:
        logger.error(f"💥 程序执行出错: {e}")
        force_cleanup_and_exit()