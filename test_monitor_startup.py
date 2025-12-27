"""
测试监控器启动过程
"""
import logging

# 设置详细的日志级别
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print("=" * 60)
print("测试监控器启动")
print("=" * 60)

from ray_multicluster_scheduler.app.client_api import initialize_scheduler_environment

print("\n开始初始化调度器...")
try:
    task_manager = initialize_scheduler_environment(
        config_file_path="clusters.yaml",
        monitor_cluster_name="centos"
    )
    print("\n✅ 调度器初始化完成")
except Exception as e:
    print(f"\n❌ 调度器初始化失败: {e}")
    import traceback
    traceback.print_exc()

print("\n等待5秒后检查监控器...")
import time
time.sleep(5)

print("\n尝试连接到centos集群检查监控器...")
import ray

# 确保连接到centos集群
if ray.is_initialized():
    print("Ray已初始化，先关闭...")
    ray.shutdown()
    time.sleep(1)

print("连接到centos集群...")
ray.init(address="ray://192.168.5.7:32546", ignore_reinit_error=True)

try:
    monitor = ray.get_actor("cluster_resource_monitor")
    print("✅ 找到监控Actor!")
    result = ray.get(monitor.ping.remote())
    print(f"   Ping结果: {result}")
except ValueError as e:
    print(f"❌ 未找到监控Actor: {e}")
except Exception as e:
    print(f"❌ 检查监控器时出错: {e}")
    import traceback
    traceback.print_exc()
finally:
    ray.shutdown()

print("\n测试完成")
