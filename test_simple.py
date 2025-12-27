"""
简化版功能测试 - 逐步验证每个组件
"""
import sys
import time

print("=" * 60)
print("开始测试 - Ray多集群调度器")
print("=" * 60)

# 测试1: 导入模块
print("\n[步骤1] 导入必要的模块...")
try:
    from ray_multicluster_scheduler.app.client_api import (
        initialize_scheduler_environment,
        submit_task
    )
    print("✅ 模块导入成功")
except Exception as e:
    print(f"❌ 模块导入失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 测试2: 初始化调度器
print("\n[步骤2] 初始化调度器环境...")
print("   配置文件: clusters.yaml")
print("   监控集群: centos")

try:
    task_manager = initialize_scheduler_environment(
        config_file_path="clusters.yaml",
        monitor_cluster_name="centos"
    )
    print("✅ 调度器初始化成功")
except Exception as e:
    print(f"❌ 调度器初始化失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 测试3: 提交一个简单任务
print("\n[步骤3] 提交测试任务...")

def simple_test():
    """最简单的测试函数"""
    return "Hello from Ray cluster!"

try:
    print("   提交任务中...")
    task_id, result = submit_task(
        simple_test,
        name="simple_test_task"
    )
    print(f"✅ 任务执行成功")
    print(f"   任务ID: {task_id}")
    print(f"   结果: {result}")
except Exception as e:
    print(f"❌ 任务提交失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
print("🎉 所有测试通过！")
print("=" * 60)

# 清理资源并正常退出
print("\n[步骤4] 清理资源...")
try:
    import ray
    # 获取unified scheduler并清理
    from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
    scheduler = get_unified_scheduler()

    # 停止健康检查线程
    if scheduler:
        scheduler.cleanup()
        print("✅ 调度器资源已清理")

    # 关闭Ray连接
    if ray.is_initialized():
        ray.shutdown()
        print("✅ Ray连接已关闭")

    print("\n测试程序正常退出")
except Exception as e:
    print(f"⚠️ 清理资源时出现警告: {e}")
    # 强制退出
    import os
    os._exit(0)
