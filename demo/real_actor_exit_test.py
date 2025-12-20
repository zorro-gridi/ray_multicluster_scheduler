#!/usr/bin/env python3
"""
真实场景下的Actor执行器退出测试
模拟用户实际使用场景，测试Actor任务执行完成后程序能否正常退出
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
class Train:
    """
    模拟用户实际使用的Train Actor类
    """
    def __init__(self, model_name="default", learning_rate=0.01):
        self.model_name = model_name
        self.learning_rate = learning_rate
        logger.info(f"Train Actor 初始化完成 - Model: {self.model_name}, LR: {self.learning_rate}")

    def train(self, idx_en, reg_model_name, **data_arg):
        """
        模拟训练任务
        """
        logger.info(f"开始训练任务 {idx_en} - Model: {reg_model_name}")
        logger.info(f"训练参数: {data_arg}")

        # 模拟训练过程
        training_time = random.uniform(1, 3)  # 1-3秒随机训练时间
        time.sleep(training_time)

        result = {
            "task_id": idx_en,
            "model": reg_model_name,
            "training_time": training_time,
            "status": "completed",
            "metrics": {
                "accuracy": random.uniform(0.8, 0.95),
                "loss": random.uniform(0.01, 0.1)
            }
        }

        logger.info(f"训练任务 {idx_en} 完成 - 结果: {result}")
        return result

    def get_status(self):
        """
        获取训练状态
        """
        return f"Train Actor is ready - Model: {self.model_name}"

def create_test_data():
    """
    创建测试数据
    """
    # 模拟用户的数据队列
    task_queues = {}

    # 创建测试索引
    test_indices = ["index_001", "index_002"]

    for idx_en in test_indices:
        # 模拟不同的数据参数
        data_args = []
        for i in range(1):  # 每个索引1个数据参数以减少测试时间
            data_arg = {
                'X_seq_len': random.randint(15, 25),
                'y_seq_len': random.randint(15, 25),
                'y_threshold': round(random.uniform(0.05, 0.08), 2),
                'batch_size': random.choice([32, 64])
            }
            data_args.append(data_arg)

        task_queues[idx_en] = {
            'reg_model_name': f"catboost_model_{idx_en}",
            'data_args': data_args,
            'completed': 0,
            'running': False
        }

    return task_queues

def train_task_fixed(idx_en, data_arg, reg_model_name, preferred_cluster=None):
    """
    修正的训练任务提交函数 - 正确处理Actor任务
    """
    logger.info(f'🖐️ 准备提交训练任务 {idx_en}, 参数: {data_arg}')

    try:
        # 通过调度器提交Actor
        actor_id, actor_handle = submit_actor(
            actor_class=Train,
            args=(),  # 构造函数参数通过kwargs传递
            kwargs={
                "model_name": reg_model_name,
                "learning_rate": 0.01
            },
            resource_requirements={"CPU": 1, "memory": 512 * 1024 * 1024},  # 512MB
            tags=["ml", "catboost", "training"],
            name=f"catboost_training_{idx_en}",
            preferred_cluster=preferred_cluster
        )

        logger.info(f"✅ Actor提交成功 {idx_en} - ID: {actor_id}")

        # 对于Actor，我们需要调用其方法来执行任务
        # 异步执行训练任务方法
        result_ref = actor_handle.train.remote(idx_en, reg_model_name=reg_model_name, **data_arg)
        logger.info(f"🚀 启动训练任务 {idx_en}")

        # 返回Actor句柄和结果引用
        return {
            'actor_id': actor_id,
            'actor_handle': actor_handle,
            'result_ref': result_ref,
            'idx_en': idx_en
        }

    except Exception as e:
        logger.error(f"❌ 提交训练任务 {idx_en} 失败: {e}")
        raise

def fixed_main_loop():
    """
    修正的主循环，正确处理Actor任务
    """
    task_lifecycle_manager = None

    try:
        # 1. 初始化调度器环境
        logger.info("1. 初始化调度器环境...")
        task_lifecycle_manager = initialize_scheduler_environment()
        logger.info("✅ 调度器环境初始化完成")

        # 2. 创建测试数据
        logger.info("2. 创建测试数据...")
        task_queues = create_test_data()
        logger.info(f"✅ 创建了 {len(task_queues)} 个测试队列")

        # 3. 执行任务调度主循环
        logger.info("3. 开始任务调度主循环...")

        # 存储所有任务的结果引用
        all_task_refs = []
        task_mapping = {}  # {result_ref: task_info}

        # 控制并发数量
        max_concurrent = 2
        active_tasks = {}

        # 统计信息
        total_tasks = sum(len(q['data_args']) for q in task_queues.values())
        completed_tasks = 0

        logger.info(f"🎯 总任务数: {total_tasks}, 最大并发数: {max_concurrent}")

        # 任务迭代器
        task_iterator = []
        for idx_en, queue_info in task_queues.items():
            for data_arg in queue_info['data_args']:
                task_iterator.append((idx_en, data_arg, queue_info['reg_model_name']))

        task_iter = iter(task_iterator)

        while completed_tasks < total_tasks:
            # 提交新任务直到达到并发限制
            while len(active_tasks) < max_concurrent and completed_tasks + len(active_tasks) < total_tasks:
                try:
                    idx_en, data_arg, reg_model_name = next(task_iter)

                    # 异步提交任务
                    task_info = train_task_fixed(idx_en, data_arg, reg_model_name)

                    # 跟踪活动任务的结果引用
                    result_ref = task_info['result_ref']
                    active_tasks[result_ref] = {
                        'idx_en': task_info['idx_en'],
                        'actor_id': task_info['actor_id']
                    }

                    logger.info(f'🚀 已提交任务 {idx_en}')

                except StopIteration:
                    break

            # 等待任务完成
            if active_tasks:
                # 等待至少一个任务完成
                ready_refs, _ = ray.wait(list(active_tasks.keys()), num_returns=1, timeout=30.0)

                # 处理完成的任务
                for ready_ref in ready_refs:
                    task_data = active_tasks.pop(ready_ref)
                    idx_en = task_data['idx_en']
                    actor_id = task_data['actor_id']

                    try:
                        # 获取结果（对result_ref调用ray.get）
                        result = ray.get(ready_ref)
                        completed_tasks += 1

                        logger.info(f'✅ 任务 {idx_en} 完成 ({completed_tasks}/{total_tasks})')
                        logger.info(f'   结果: {result}')

                        # 强制垃圾回收（每完成2个任务执行一次）
                        if completed_tasks % 2 == 0:
                            import gc
                            gc.collect()

                    except Exception as e:
                        logger.error(f'❌ 任务 {idx_en} (Actor: {actor_id}) 执行失败: {e}')
                        completed_tasks += 1
            else:
                # 没有活动任务，短暂休息
                time.sleep(0.1)

        logger.info('🎉 所有任务完成！')
        return True, task_lifecycle_manager

    except Exception as e:
        logger.error(f"❌ 真实场景测试出错: {e}")
        import traceback
        logger.error(f"🔍 详细错误信息:\n{traceback.format_exc()}")
        return False, task_lifecycle_manager

def cleanup_and_exit(task_lifecycle_manager=None):
    """
    正确的资源清理和退出函数
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
    logger.info("🚀 开始真实场景Actor退出测试...")

    try:
        # 设置超时保护
        import signal

        def timeout_handler(signum, frame):
            logger.error("⏰ 测试超时，强制退出")
            cleanup_and_exit()
            import os
            os._exit(1)

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(120)  # 2分钟超时

        # 执行测试
        success, task_lifecycle_manager = fixed_main_loop()

        # 取消超时
        signal.alarm(0)

        # 清理资源
        cleanup_and_exit(task_lifecycle_manager)

        if success:
            logger.info("🎉 真实场景测试通过，程序正常退出")
            sys.exit(0)
        else:
            logger.error("💥 真实场景测试失败")
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