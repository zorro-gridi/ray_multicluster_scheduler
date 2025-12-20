#!/usr/bin/env python3
"""
综合并发测试用例
同时测试submit_task和submit_actor接口的并发机制，检查接口冲突问题
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
    submit_task,
    submit_actor
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
        处理任务的方法
        """
        if duration is None:
            duration = random.uniform(0.5, 2.0)  # 0.5-2秒随机时间
            
        logger.info(f"Actor {self.actor_id} ({self.name}) 开始处理任务 {task_name}，预计耗时: {duration:.2f}秒")
        time.sleep(duration)
        
        result = {
            'actor_id': self.actor_id,
            'task_name': task_name,
            'duration': duration,
            'status': 'completed',
            'timestamp': time.time()
        }
        logger.info(f"Actor {self.actor_id} ({self.name}) 完成任务 {task_name}")
        return result


def mixed_concurrent_test():
    """
    混合并发测试 - 同时测试submit_task和submit_actor
    """
    logger.info("=== 开始混合并发测试 ===")
    task_lifecycle_manager = None
    
    try:
        # 1. 初始化调度器环境
        logger.info("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        logger.info("✅ 调度器环境初始化完成")
        
        # 2. 提交混合任务和Actor
        logger.info("2. 提交混合任务和Actor...")
        task_futures = []
        actor_handles = {}
        
        # 提交5个普通任务
        task_configs = [
            {"name": "task_1", "preferred_cluster": "mac"},
            {"name": "task_2", "preferred_cluster": "centos"},
            {"name": "task_3", "preferred_cluster": None},  # 让调度器自动选择
            {"name": "task_4", "preferred_cluster": "mac"},
            {"name": "task_5", "preferred_cluster": "centos"},
        ]
        
        for i, config in enumerate(task_configs):
            try:
                task_id, future = submit_task(
                    func=sample_task,
                    args=(f"task_id_{i+1}", config["name"]),
                    resource_requirements={"CPU": 1},
                    tags=["mixed", "test"],
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
        
        # 提交3个Actor
        actor_configs = [
            {"name": "actor_1", "preferred_cluster": "mac"},
            {"name": "actor_2", "preferred_cluster": "centos"},
            {"name": "actor_3", "preferred_cluster": None}  # 让调度器自动选择
        ]
        
        for i, config in enumerate(actor_configs):
            try:
                actor_id, actor_handle = submit_actor(
                    actor_class=ConcurrentActor,
                    args=(f"actor_id_{i+1}", config["name"]),
                    resource_requirements={"CPU": 1},
                    tags=["mixed", "test"],
                    name=config["name"],
                    preferred_cluster=config["preferred_cluster"]
                )
                actor_handles[config["name"]] = actor_handle
                logger.info(f"✅ Actor {config['name']} 提交成功: {actor_id}")
            except Exception as e:
                logger.error(f"❌ 提交Actor {config['name']} 失败: {e}")
        
        # 3. 通过Actor执行任务
        logger.info("3. 通过Actor执行任务...")
        actor_futures = []
        
        for actor_name, actor_handle in actor_handles.items():
            try:
                # 每个Actor执行2个任务
                for task_num in range(2):
                    task_name = f"{actor_name}_task_{task_num+1}"
                    # 随机任务时长
                    duration = random.uniform(0.5, 2.0)
                    future = actor_handle.process_task.remote(task_name, duration)
                    actor_futures.append({
                        'future': future,
                        'actor_name': actor_name,
                        'task_name': task_name
                    })
                    logger.info(f"🚀 启动Actor任务: {task_name} (Actor: {actor_name})")
            except Exception as e:
                logger.error(f"❌ 通过Actor {actor_name} 提交任务失败: {e}")
        
        # 4. 等待所有任务完成
        logger.info(f"4. 等待 {len(task_futures) + len(actor_futures)} 个任务完成...")
        
        # 收集所有future
        all_futures = []
        # 添加普通任务的future
        for task_info in task_futures:
            all_futures.append(task_info['future'])
        # 添加Actor任务的future
        for actor_info in actor_futures:
            all_futures.append(actor_info['future'])
        
        # 等待所有任务完成
        if all_futures:
            # 使用ray.wait等待所有任务完成
            while all_futures:
                ready_futures, remaining_futures = ray.wait(all_futures, timeout=1.0)
                
                # 处理已完成的任务
                for ready_future in ready_futures:
                    # 查找对应的task_info或actor_info
                    task_info = None
                    actor_info = None
                    
                    # 查找普通任务
                    for t_info in task_futures:
                        if t_info['future'] == ready_future:
                            task_info = t_info
                            break
                            
                    # 查找Actor任务
                    if task_info is None:
                        for a_info in actor_futures:
                            if a_info['future'] == ready_future:
                                actor_info = a_info
                                break
                    
                    try:
                        result = ray.get(ready_future)
                        if task_info:
                            logger.info(f"✅ 普通任务完成: {task_info['task_name']}")
                            logger.info(f"   结果: {result}")
                        elif actor_info:
                            logger.info(f"✅ Actor任务完成: {actor_info['task_name']} (Actor: {actor_info['actor_name']})")
                            logger.info(f"   结果: {result}")
                    except Exception as e:
                        if task_info:
                            logger.error(f"❌ 普通任务失败: {task_info['task_name']} - {e}")
                        elif actor_info:
                            logger.error(f"❌ Actor任务失败: {actor_info['task_name']} (Actor: {actor_info['actor_name']}) - {e}")
                    
                    # 从待处理列表中移除已完成的任务
                    if ready_future in all_futures:
                        all_futures.remove(ready_future)
                
                # 更新剩余任务列表
                all_futures = remaining_futures
                
                # 如果没有剩余任务，则跳出循环
                if not all_futures:
                    break
        
        logger.info(f"🎉 任务执行完成! 总共提交 {len(task_futures) + len(actor_futures)} 个任务")
        
        # 5. 清理资源
        logger.info("5. 清理资源...")
        if task_lifecycle_manager:
            logger.info("🛑 停止任务生命周期管理器...")
            task_lifecycle_manager.stop()
            
        logger.info("✅ 混合并发测试完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ 混合并发测试过程中出现异常: {e}")
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
    success = mixed_concurrent_test()
    if success:
        logger.info("🎉 所有测试通过!")
        sys.exit(0)
    else:
        logger.error("❌ 测试失败!")
        sys.exit(1)