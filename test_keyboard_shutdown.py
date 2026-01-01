#!/usr/bin/env python3
"""
测试系统的 keyboardshutdown 优雅退出功能
验证中断信号是否能停止集群的所有 Ray job
"""
import time
import signal
import sys
import threading
from pathlib import Path

# 添加项目路径
sys.path.insert(0, '/Users/zorro/project/pycharm/ray_multicluster_scheduler')

from ray_multicluster_scheduler.app.client_api import (
    initialize_scheduler_environment,
    submit_job,
    wait_for_all_jobs,
    get_job_status,
    stop_job
)

from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler


def _stop_job_in_all_clusters(submission_id: str) -> bool:
    """
    在所有集群中尝试停止指定的作业
    """
    try:
        scheduler = get_unified_scheduler()
        
        # 获取所有已注册的集群
        cluster_names = scheduler.task_lifecycle_manager.connection_manager.list_registered_clusters()
        
        success = False
        for cluster_name in cluster_names:
            try:
                # 尝试在每个集群中停止作业
                job_client = scheduler.task_lifecycle_manager.connection_manager.get_job_client(cluster_name)
                if job_client:
                    job_client.stop_job(submission_id)
                    print(f"成功发送停止命令到集群 {cluster_name} 的作业 {submission_id}")
                    success = True
            except Exception as e:
                print(f"在集群 {cluster_name} 中停止作业 {submission_id} 失败: {e}")
                continue
        
        return success
    except Exception as e:
        print(f"停止作业 {submission_id} 时发生错误: {e}")
        return False


def create_long_running_job_script():
    """创建一个长时间运行的测试脚本"""
    script_content = '''import time
import sys
import ray

@ray.remote
def long_running_task(task_id, duration=120):
    """模拟长时间运行的任务"""
    print(f"Long running task {task_id} started, will run for {duration} seconds")
    for i in range(duration):
        print(f"Task {task_id} - Progress: {i+1}/{duration}")
        time.sleep(1)
    print(f"Task {task_id} completed")
    return f"Task {task_id} result"

def main():
    print("Starting long running Ray job...")
    print("This job will run for 120 seconds and can be interrupted with Ctrl+C")
    
    # 初始化 Ray
    if not ray.is_initialized():
        ray.init()

    import os
    task_id = os.environ.get("RAY_TASK_ID", "test_task")
    duration = int(os.environ.get("TASK_DURATION", "120"))
    
    # 提交一个长时间运行的任务
    task_ref = long_running_task.remote(task_id, duration)

    try:
        result = ray.get(task_ref)
        print(f"Job completed with result: {result}")
    except KeyboardInterrupt:
        print("Job interrupted by user")
        # 清理资源
        ray.shutdown()
        sys.exit(1)

    # 清理资源
    ray.shutdown()
    print("Job finished successfully")

if __name__ == "__main__":
    main()
'''

    # 将脚本保存在项目目录中，确保远程集群可以访问
    script_path = Path('/Users/zorro/project/pycharm/ray_multicluster_scheduler/test_scripts/keyboard_shutdown_test_job.py')
    script_path.parent.mkdir(exist_ok=True)  # 创建目录如果不存在
    with open(script_path, 'w') as f:
        f.write(script_content)

    return script_path


def test_keyboard_shutdown_multiple_jobs():
    """测试多个作业的键盘中断功能"""
    print("开始测试多作业键盘中断功能...")
    print("="*60)

    # 创建长时间运行的测试脚本
    job_script_path = create_long_running_job_script()
    print(f"创建测试脚本: {job_script_path}")

    try:
        # 初始化调度器环境
        print("初始化调度器环境...")
        config_file_path = Path('/Users/zorro/project/pycharm/ray_multicluster_scheduler/clusters.yaml')
        task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)
        print("调度器环境初始化完成")

        # 提交多个长时间运行的作业
        print("提交多个长时间运行的作业...")
        job_ids = []
        
        for i in range(2):  # 提交2个作业进行测试
            job_id = submit_job(
                entrypoint=f'python {job_script_path}',
                submission_id=f'test_keyboard_shutdown_{i}_{int(time.time() * 1000)}',
                metadata={
                    'job_name': f'test_keyboard_shutdown_{i}',
                    'test_type': 'keyboard_shutdown',
                    'description': 'Testing keyboard interrupt functionality'
                },
                resource_requirements={'CPU': 2},
                preferred_cluster='mac' if i % 2 == 0 else 'centos'  # 在不同集群上运行
            )
            job_ids.append(job_id)
            print(f"作业 {i+1} 提交成功，ID: {job_id}")
            time.sleep(2)  # 稍微延迟，确保作业开始

        print(f"总共提交了 {len(job_ids)} 个作业")

        # 检查作业状态
        print("\n检查作业状态...")
        for idx, job_id in enumerate(job_ids):
            # 尝试获取作业所在的集群
            from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
            scheduler = get_unified_scheduler()
            cluster_name = None
            if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

            if cluster_name:
                status = get_job_status(job_id, cluster_name)
                print(f'作业 {job_id} 在集群 {cluster_name} 的状态: {status}')
            else:
                from ray_multicluster_scheduler.app.client_api.submit_job import _get_job_status_from_all_clusters
                status = _get_job_status_from_all_clusters(job_id)
                print(f'作业 {job_id} 状态 (自动检测集群): {status}')

        print('\n现在测试键盘中断功能...')
        print('模拟用户按下 Ctrl+C 中断 wait_for_all_jobs...')
        print('这将测试系统是否能优雅地处理中断并停止所有相关作业...')

        # 创建一个线程来模拟中断 - 设置中断标志
        def simulate_keyboard_interrupt():
            time.sleep(5)  # 等待5秒后触发中断
            print('\n触发中断标志...')
            from ray_multicluster_scheduler.app.client_api.submit_job import _interrupted
            _interrupted.set()

        interrupt_thread = threading.Thread(target=simulate_keyboard_interrupt)
        interrupt_thread.start()

        try:
            # 等待作业完成 - 这里会轮询作业状态
            print('开始 wait_for_all_jobs，这将轮询作业状态...')
            wait_for_all_jobs(submission_ids=job_ids, check_interval=2)
            print('所有作业正常完成（这不应该发生，因为我们会中断它）')
        except KeyboardInterrupt:
            print('成功捕获到 KeyboardInterrupt 异常')
            print('wait_for_all_jobs 已被中断')
        except Exception as e:
            print(f'等待作业时发生异常: {e}')

        interrupt_thread.join(timeout=2)

        # 验证作业状态 - 检查作业是否被正确停止
        print('\n检查作业的最终状态以验证中断是否生效...')
        all_stopped = True
        for job_id in job_ids:
            # 尝试获取作业所在的集群
            from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
            scheduler = get_unified_scheduler()
            cluster_name = None
            if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

            if cluster_name:
                status = get_job_status(job_id, cluster_name)
                print(f'作业 {job_id} 在集群 {cluster_name} 的最终状态: {status}')
            else:
                from ray_multicluster_scheduler.app.client_api.submit_job import _get_job_status_from_all_clusters
                status = _get_job_status_from_all_clusters(job_id)
                print(f'作业 {job_id} 最终状态 (自动检测集群): {status}')
            
            # 如果作业仍在运行，说明中断没有生效
            if status in ['PENDING', 'RUNNING']:
                print(f'  警告: 作业 {job_id} 仍在运行，中断可能未生效')
                all_stopped = False
            else:
                print(f'  作业 {job_id} 已停止或失败，中断生效')

        if all_stopped:
            print('\n✓ 所有作业都已停止，中断功能正常工作')
            return True
        else:
            print('\n✗ 部分作业仍在运行，中断功能可能存在问题')
            return False

    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # 清理资源
        print("\n清理测试资源...")
        try:
            # 尝试停止所有提交的作业
            for job_id in job_ids:
                try:
                    from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
                    scheduler = get_unified_scheduler()
                    cluster_name = None
                    if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                        cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)
                    
                    if cluster_name:
                        stop_job(job_id, cluster_name)
                        print(f"已尝试停止作业 {job_id} 在集群 {cluster_name}")
                    else:
                        # 尝试在所有集群中停止作业
                        _stop_job_in_all_clusters(job_id)
                        print(f"已尝试在所有集群中停止作业 {job_id}")
                except Exception as e:
                    print(f"停止作业 {job_id} 时发生错误: {e}")
        except:
            pass


def test_keyboard_shutdown_single_job():
    """测试单个作业的键盘中断功能"""
    print("开始测试单作业键盘中断功能...")
    print("="*60)

    # 创建长时间运行的测试脚本
    job_script_path = create_long_running_job_script()
    print(f"创建测试脚本: {job_script_path}")

    try:
        # 初始化调度器环境
        print("初始化调度器环境...")
        config_file_path = Path('/Users/zorro/project/pycharm/ray_multicluster_scheduler/clusters.yaml')
        task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)
        print("调度器环境初始化完成")

        # 提交长时间运行的作业
        print("提交长时间运行的作业...")
        job_id = submit_job(
            entrypoint=f'python {job_script_path}',
            submission_id=f'test_keyboard_shutdown_single_{int(time.time() * 1000)}',
            metadata={
                'job_name': 'test_keyboard_shutdown_single',
                'test_type': 'keyboard_shutdown_single',
                'description': 'Testing keyboard interrupt functionality for single job'
            },
            resource_requirements={'CPU': 1},
            preferred_cluster='mac'  # 使用mac集群
        )

        print(f"作业提交成功，ID: {job_id}")

        print("等待作业启动...")
        time.sleep(5)  # 等待作业启动

        # 检查作业状态
        print("检查作业状态...")
        # 尝试获取作业所在的集群
        from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
        scheduler = get_unified_scheduler()
        cluster_name = None
        if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
            cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)

        if cluster_name:
            status = get_job_status(job_id, cluster_name)
            print(f'作业 {job_id} 在集群 {cluster_name} 的状态: {status}')
        else:
            from ray_multicluster_scheduler.app.client_api.submit_job import _get_job_status_from_all_clusters
            status = _get_job_status_from_all_clusters(job_id)
            print(f'作业 {job_id} 状态 (自动检测集群): {status}')

        if status in ['PENDING', 'RUNNING']:
            print('\n现在测试键盘中断功能...')
            print('模拟用户按下 Ctrl+C 中断 wait_for_all_jobs...')
            print('这将测试系统是否能优雅地处理中断并停止相关作业...')

            # 创建一个线程来模拟中断 - 设置中断标志
            def simulate_keyboard_interrupt():
                time.sleep(3)  # 等待3秒后触发中断
                print('\n触发中断标志...')
                from ray_multicluster_scheduler.app.client_api.submit_job import _interrupted
                _interrupted.set()

            interrupt_thread = threading.Thread(target=simulate_keyboard_interrupt)
            interrupt_thread.start()

            try:
                # 等待作业完成 - 这里会轮询作业状态
                print('开始 wait_for_all_jobs，这将轮询作业状态...')
                wait_for_all_jobs(submission_ids=[job_id], check_interval=2)
                print('作业正常完成（这不应该发生，因为我们会中断它）')
            except KeyboardInterrupt:
                print('成功捕获到 KeyboardInterrupt 异常')
                print('wait_for_all_jobs 已被中断')
            except Exception as e:
                print(f'等待作业时发生异常: {e}')

            interrupt_thread.join(timeout=2)

            # 验证作业状态 - 检查作业是否被正确停止
            print('\n检查作业的最终状态以验证中断是否生效...')
            if cluster_name:
                status = get_job_status(job_id, cluster_name)
                print(f'作业 {job_id} 在集群 {cluster_name} 的最终状态: {status}')
            else:
                from ray_multicluster_scheduler.app.client_api.submit_job import _get_job_status_from_all_clusters
                status = _get_job_status_from_all_clusters(job_id)
                print(f'作业 {job_id} 最终状态 (自动检测集群): {status}')
            
            # 如果作业仍在运行，说明中断没有生效
            if status in ['PENDING', 'RUNNING']:
                print(f'  警告: 作业 {job_id} 仍在运行，中断可能未生效')
                return False
            else:
                print(f'  作业 {job_id} 已停止或失败，中断生效')
                return True
        else:
            print(f"作业状态不是 PENDING 或 RUNNING，而是 {status}，无法测试中断")
            return False

    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # 清理资源
        print("\n清理测试资源...")
        try:
            # 尝试停止提交的作业
            try:
                from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
                scheduler = get_unified_scheduler()
                cluster_name = None
                if hasattr(scheduler.task_lifecycle_manager, 'job_cluster_mapping'):
                    cluster_name = scheduler.task_lifecycle_manager.job_cluster_mapping.get(job_id)
                
                if cluster_name:
                    stop_job(job_id, cluster_name)
                    print(f"已尝试停止作业 {job_id} 在集群 {cluster_name}")
                else:
                    # 尝试在所有集群中停止作业
                    _stop_job_in_all_clusters(job_id)
                    print(f"已尝试在所有集群中停止作业 {job_id}")
            except Exception as e:
                print(f"停止作业 {job_id} 时发生错误: {e}")
        except:
            pass


def main():
    """主测试函数"""
    print("开始测试系统的 keyboardshutdown 优雅退出功能")
    print("="*70)
    
    success_count = 0
    total_tests = 2
    
    # 测试单个作业中断
    print("\n1. 测试单个作业中断功能:")
    if test_keyboard_shutdown_single_job():
        success_count += 1
        print("✓ 单个作业中断测试通过")
    else:
        print("✗ 单个作业中断测试失败")
    
    print("\n" + "="*70)
    
    # 测试多个作业中断
    print("\n2. 测试多个作业中断功能:")
    if test_keyboard_shutdown_multiple_jobs():
        success_count += 1
        print("✓ 多个作业中断测试通过")
    else:
        print("✗ 多个作业中断测试失败")
    
    print("\n" + "="*70)
    print(f"测试总结: {success_count}/{total_tests} 测试通过")
    
    if success_count == total_tests:
        print("✓ 所有键盘中断测试通过，系统能够优雅地处理中断信号")
        return True
    else:
        print("✗ 部分或全部测试失败，键盘中断功能存在问题")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)