#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试自定义集群配置文件 custom_cluster.yml 的解析和使用
验证 dispatcher.py 中 _prepare_runtime_env_for_cluster_target 接口是否能准确解析到完整的 runtime_env 参数
"""

import os
import yaml
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import TaskLifecycleManager
from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor


def test_custom_cluster_config():
    """测试自定义集群配置是否能正确加载和使用"""
    print("开始测试自定义集群配置...")

    # 1. 检查自定义配置文件是否存在
    custom_config_path = "custom_cluster.yml"
    if not os.path.exists(custom_config_path):
        print(f"错误: 自定义配置文件 {custom_config_path} 不存在")
        return False

    print(f"✓ 自定义配置文件 {custom_config_path} 存在")

    # 2. 读取并解析自定义配置
    with open(custom_config_path, 'r') as f:
        custom_config = yaml.safe_load(f)

    print(f"✓ 成功解析自定义配置文件，包含 {len(custom_config.get('clusters', []))} 个集群")

    # 3. 使用自定义配置初始化调度器环境
    print("初始化调度器环境...")
    try:
        task_lifecycle_manager = initialize_scheduler_environment(config_file_path=custom_config_path)
        print("✓ 调度器环境初始化成功")
    except Exception as e:
        print(f"✗ 调度器环境初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 4. 检查集群配置是否正确加载
    cluster_info = task_lifecycle_manager.cluster_monitor.get_all_cluster_info()
    print(f"✓ 加载了 {len(cluster_info)} 个集群的信息")

    for cluster_name, info in cluster_info.items():
        metadata = info['metadata']
        print(f"  - 集群: {cluster_name}")
        print(f"    地址: {metadata.head_address}")
        print(f"    优先: {metadata.prefer}")
        print(f"    权重: {metadata.weight}")

        if hasattr(metadata, 'runtime_env') and metadata.runtime_env:
            print(f"    runtime_env 包含:")
            if 'conda' in metadata.runtime_env:
                print(f"      conda: {metadata.runtime_env['conda']}")
            if 'env_vars' in metadata.runtime_env and 'home_dir' in metadata.runtime_env['env_vars']:
                print(f"      home_dir: {metadata.runtime_env['env_vars']['home_dir']}")
            if 'env_vars' in metadata.runtime_env and 'PYTHONPATH' in metadata.runtime_env['env_vars']:
                print(f"      PYTHONPATH: {metadata.runtime_env['env_vars']['PYTHONPATH']}")
        else:
            print(f"    runtime_env: 未配置")

    # 5. 测试 dispatcher 的 _prepare_runtime_env_for_cluster_target 方法
    print("\n测试 dispatcher 的 _prepare_runtime_env_for_cluster_target 方法...")

    dispatcher = Dispatcher(task_lifecycle_manager.connection_manager)

    # 测试不同场景
    for cluster_name in cluster_info.keys():
        print(f"\n  测试集群: {cluster_name}")

        # 场景1: JobDescription 没有提供 runtime_env，使用集群默认配置
        job_desc_no_env = JobDescription(
            job_id="test_job_no_env",
            entrypoint="python test.py",
            runtime_env=None,  # 没有提供 runtime_env
            metadata={"test": "true"}
        )

        try:
            result_env = dispatcher._prepare_runtime_env_for_cluster_target(job_desc_no_env, cluster_name)
            print(f"    场景1 - Job没有提供runtime_env:")
            print(f"      结果: {result_env}")
            if result_env:
                print(f"      ✓ 成功获取集群默认的runtime_env")
            else:
                print(f"      ✗ 未能获取集群默认的runtime_env")
        except Exception as e:
            print(f"    场景1 - 错误: {e}")

        # 场景2: JobDescription 提供了 runtime_env，与集群配置合并
        user_runtime_env = {
            "pip": ["requests", "numpy"],
            "env_vars": {
                "CUSTOM_VAR": "custom_value"
            }
        }

        job_desc_with_env = JobDescription(
            job_id="test_job_with_env",
            entrypoint="python test.py",
            runtime_env=user_runtime_env,  # 提供了 runtime_env
            metadata={"test": "true"}
        )

        try:
            result_env = dispatcher._prepare_runtime_env_for_cluster_target(job_desc_with_env, cluster_name)
            print(f"    场景2 - Job提供了runtime_env:")
            print(f"      用户提供的: {user_runtime_env}")
            print(f"      最终结果: {result_env}")
            if result_env and 'pip' in result_env and 'env_vars' in result_env:
                print(f"      ✓ 成功合并了用户和集群的runtime_env配置")
                if 'CUSTOM_VAR' in result_env['env_vars'] and 'home_dir' in result_env['env_vars']:
                    print(f"      ✓ 用户配置和集群默认配置都保留")
            else:
                print(f"      ✗ runtime_env合并失败")
        except Exception as e:
            print(f"    场景2 - 错误: {e}")

    print("\n测试完成！")
    return True


if __name__ == "__main__":
    test_custom_cluster_config()