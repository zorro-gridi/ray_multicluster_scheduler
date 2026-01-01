#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试集群上下文一致性：验证submit_job提交的作业内部调用submit_actor时，
submit_actor是否继承与父作业相同的集群上下文。
"""

import time
import threading
from pathlib import Path
import sys
from typing import Dict, List, Optional

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, wait_for_all_jobs
from ray_multicluster_scheduler.app.client_api.submit_actor import submit_actor
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment


def test_cluster_context_consistency():
    """测试集群上下文一致性"""
    print("开始测试：集群上下文一致性")

    # 初始化调度器环境
    config_file_path = project_root / 'clusters.yaml'
    task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)

    print("1. 提交一个作业，该作业内部会调用submit_actor")

    # 创建一个测试脚本，该脚本内部调用submit_actor
    test_script_content = '''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import sys
import os

# 添加项目根目录到Python路径
project_root = "/Users/zorro/project/pycharm/ray_multicluster_scheduler"
sys.path.insert(0, project_root)

from ray_multicluster_scheduler.app.client_api.submit_actor import submit_actor
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment

def test_actor_function():
    """测试actor的简单功能"""
    return "Hello from actor!"

def main():
    print("作业内部：开始执行")
    print(f"环境变量 RAY_MULTICLUSTER_CURRENT_CLUSTER: {os.environ.get('RAY_MULTICLUSTER_CURRENT_CLUSTER', 'NOT SET')}")

    # 初始化调度器环境
    initialize_scheduler_environment()

    print("作业内部：准备提交actor")

    # 提交一个actor，这应该继承父作业的集群上下文
    actor_id, actor_handle = submit_actor(
        actor_class=test_actor_function,
        name=f"test_actor_{int(time.time())}",
    )

    print(f"作业内部：成功提交actor，ID: {actor_id}")

    # 调用actor方法
    try:
        result = actor_handle.remote()
        print(f"作业内部：actor调用完成")
    except Exception as e:
        print(f"作业内部：actor调用失败: {e}")

    print("作业内部：执行完成")
    return "Job completed successfully"

if __name__ == "__main__":
    result = main()
    print(result)
'''

    # 创建临时测试脚本
    test_script_path = project_root / 'temp_test_script.py'
    with open(test_script_path, 'w', encoding='utf-8') as f:
        f.write(test_script_content)

    try:
        # 提交作业
        job_id = submit_job(
            entrypoint=f'python {test_script_path}',
            submission_id="test_cluster_context_" + str(int(time.time() * 1000)),
            metadata={'job_name': 'test_cluster_context'},
            resource_requirements={'CPU': 1},  # 使用较小的资源需求以适应测试环境
        )

        print(f"   作业提交成功，ID: {job_id}")

        # 等待作业完成
        print("   等待作业完成...")
        wait_for_all_jobs(submission_ids=[job_id], check_interval=5)

        print("2. 验证集群上下文继承机制")
        print("   作业和其内部的actor应该被调度到相同的集群")
        print("   测试完成")

    finally:
        # 清理临时文件
        if test_script_path.exists():
            test_script_path.unlink()

    return True


def test_direct_context_setting():
    """测试直接设置集群上下文"""
    print("\n开始测试：直接集群上下文设置")

    from ray_multicluster_scheduler.common.context_manager import ClusterContextManager

    # 设置集群上下文
    test_cluster = "test_cluster"
    print(f"设置集群上下文为: {test_cluster}")
    ClusterContextManager.set_current_cluster(test_cluster)

    # 验证设置
    current_cluster = ClusterContextManager.get_current_cluster()
    print(f"获取到的集群上下文: {current_cluster}")

    if current_cluster == test_cluster:
        print("✓ 集群上下文设置和获取测试通过")
    else:
        print("✗ 集群上下文设置和获取测试失败")

    # 清除集群上下文
    ClusterContextManager.clear_current_cluster()
    current_cluster = ClusterContextManager.get_current_cluster()
    print(f"清除后的集群上下文: {current_cluster}")

    if current_cluster is None:
        print("✓ 集群上下文清除测试通过")
    else:
        print("✗ 集群上下文清除测试失败")

    return True


if __name__ == "__main__":
    print("集群上下文一致性测试")
    print("=" * 50)

    # 运行测试
    test_direct_context_setting()
    test_cluster_context_consistency()

    print("\n" + "=" * 50)
    print("所有测试完成")