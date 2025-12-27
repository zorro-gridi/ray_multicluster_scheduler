""" 测试 reset_scheduler_environment 接口
"""
import ray
import time
import yaml

print("=" * 60)
print("测试 reset_scheduler_environment 接口")
print("=" * 60)

from ray_multicluster_scheduler.app.client_api import (
    initialize_scheduler_environment,
    reset_scheduler_environment,
    submit_task
)

# 加载配置文件，获取 centos 集群的 runtime_env
def get_centos_runtime_env():
    """从 clusters.yaml 读取 centos 集群的 runtime_env 配置"""
    try:
        with open('clusters.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            for cluster in config.get('clusters', []):
                if cluster.get('name') == 'centos':
                    return cluster.get('runtime_env', {})
    except Exception as e:
        print(f"读取配置文件失败: {e}，使用默认 runtime_env")

    # 默认配置
    return {
        "conda": "ts",
        "env_vars": {
            "home_dir": "/home/zorro",
            "PYTHONPATH": "/home/zorro/project/pycharm/ray_multicluster_scheduler:${PYTHONPATH}"
        },
        "working_dir": "."
    }

CENTOS_RUNTIME_ENV = get_centos_runtime_env()
print(f"加载 centos 集群 runtime_env: {CENTOS_RUNTIME_ENV}")

# 测试1: 查看当前的 ClusterResourceMonitor 和 BackgroundHealthChecker 数量
print("\n[步骤1] 检查当前的 detached actors 数量")
try:
    # 连接到centos集群查看
    if not ray.is_initialized():
        ray.init(address="ray://192.168.5.7:32546", runtime_env=CENTOS_RUNTIME_ENV, ignore_reinit_error=True)

    import ray.util
    named_actors = ray.util.list_named_actors(all_namespaces=True)
    monitor_count = sum(1 for actor in named_actors if actor['name'] == 'cluster_resource_monitor')
    checker_count = sum(1 for actor in named_actors if actor['name'] == 'background_health_checker')

    print(f"   找到 {monitor_count} 个 ClusterResourceMonitor")
    print(f"   找到 {checker_count} 个 BackgroundHealthChecker")

    for actor in named_actors:
        if actor['name'] in ['cluster_resource_monitor', 'background_health_checker']:
            print(f"   - {actor['name']} (namespace: {actor.get('namespace', 'default')})")

    ray.shutdown()
    time.sleep(1)
except Exception as e:
    print(f"   检查失败: {e}")

# 测试2: 调用reset_scheduler_environment
print("\n[步骤2] 调用 reset_scheduler_environment()")
try:
    reset_scheduler_environment()
    print("✅ reset_scheduler_environment() 执行成功")
except Exception as e:
    print(f"❌ reset_scheduler_environment() 执行失败: {e}")
    import traceback
    traceback.print_exc()

# 等待一下确保删除完成
time.sleep(2)

# 测试3: 验证 ClusterResourceMonitor 和 BackgroundHealthChecker 是否被删除
print("\n[步骤3] 验证 detached actors 是否被删除")
try:
    # 连接到centos集群查看
    if not ray.is_initialized():
        ray.init(address="ray://192.168.5.7:32546", runtime_env=CENTOS_RUNTIME_ENV, ignore_reinit_error=True)

    import ray.util
    named_actors = ray.util.list_named_actors(all_namespaces=True)
    monitor_count = sum(1 for actor in named_actors if actor['name'] == 'cluster_resource_monitor')
    checker_count = sum(1 for actor in named_actors if actor['name'] == 'background_health_checker')

    if monitor_count == 0 and checker_count == 0:
        print("✅ 所有 detached actors 已被成功删除")
    else:
        if monitor_count > 0:
            print(f"⚠️  仍有 {monitor_count} 个 ClusterResourceMonitor 存在")
        if checker_count > 0:
            print(f"⚠️  仍有 {checker_count} 个 BackgroundHealthChecker 存在")

        for actor in named_actors:
            if actor['name'] in ['cluster_resource_monitor', 'background_health_checker']:
                print(f"   - {actor['name']} (namespace: {actor.get('namespace', 'default')})")

    ray.shutdown()
    time.sleep(1)
except Exception as e:
    print(f"   检查失败: {e}")

# 测试4: 重新初始化调度器
print("\n[步骤4] 重新初始化调度器")
try:
    task_manager = initialize_scheduler_environment(
        config_file_path="clusters.yaml",
        monitor_cluster_name="centos"
    )
    print("✅ 调度器重新初始化成功")
except Exception as e:
    print(f"❌ 调度器重新初始化失败: {e}")
    import traceback
    traceback.print_exc()

# 等待monitor启动
time.sleep(3)

# 测试5: 验证新的 ClusterResourceMonitor 和 BackgroundHealthChecker 是否创建
print("\n[步骤5] 验证新的 detached actors 是否创建")
try:
    # 连接到centos集群查看
    if ray.is_initialized():
        ray.shutdown()
        time.sleep(1)

    ray.init(address="ray://192.168.5.7:32546", runtime_env=CENTOS_RUNTIME_ENV, ignore_reinit_error=True)

    import ray.util
    named_actors = ray.util.list_named_actors(all_namespaces=True)
    monitor_count = sum(1 for actor in named_actors if actor['name'] == 'cluster_resource_monitor')
    checker_count = sum(1 for actor in named_actors if actor['name'] == 'background_health_checker')

    # 测试 ClusterResourceMonitor
    if monitor_count == 1:
        print("✅ 新的 ClusterResourceMonitor 已成功创建（只有1个）")
        try:
            MONITOR_NAMESPACE = "ray_multicluster_scheduler"
            monitor = ray.get_actor("cluster_resource_monitor", namespace=MONITOR_NAMESPACE)
            result = ray.get(monitor.ping.remote())
            print(f"   Monitor健康检查: {result}")
        except Exception as e:
            print(f"   Monitor健康检查失败: {e}")
    elif monitor_count == 0:
        print("❌ 没有找到 ClusterResourceMonitor")
    else:
        print(f"⚠️  发现 {monitor_count} 个 ClusterResourceMonitor（应该只有1个）")

    # 测试 BackgroundHealthChecker
    if checker_count == 1:
        print("✅ 新的 BackgroundHealthChecker 已成功创建（只有1个）")
        try:
            MONITOR_NAMESPACE = "ray_multicluster_scheduler"
            checker = ray.get_actor("background_health_checker", namespace=MONITOR_NAMESPACE)
            status = ray.get(checker.get_status.remote())
            print(f"   BackgroundHealthChecker状态: {status}")
        except Exception as e:
            print(f"   BackgroundHealthChecker状态检查失败: {e}")
    elif checker_count == 0:
        print("❌ 没有找到 BackgroundHealthChecker")
    else:
        print(f"⚠️  发现 {checker_count} 个 BackgroundHealthChecker（应该只有1个）")

    # 显示所有相关 actor
    for actor in named_actors:
        if actor['name'] in ['cluster_resource_monitor', 'background_health_checker']:
            print(f"   - {actor['name']} (namespace: {actor.get('namespace', 'default')})")

    ray.shutdown()
except Exception as e:
    print(f"   检查失败: {e}")
    import traceback
    traceback.print_exc()

# 测试6: 提交一个测试任务验证调度器工作正常
print("\n[步骤6] 提交测试任务验证调度器工作正常")

def test_task():
    import socket
    return {"hostname": socket.gethostname(), "message": "Hello from cluster!"}

try:
    task_id, result = submit_task(test_task, name="reset_test_task")
    print(f"✅ 任务提交成功")
    print(f"   任务ID: {task_id}")
    print(f"   执行结果: {result}")
except Exception as e:
    print(f"❌ 任务提交失败: {e}")
    import traceback
    traceback.print_exc()

# 测试7: 验证后台监控的持续运行能力
print("\n[步骤7] 验证后台监控的持续运行能力")
try:
    # 连接到centos集群
    if ray.is_initialized():
        ray.shutdown()
        time.sleep(1)

    ray.init(address="ray://192.168.5.7:32546", runtime_env=CENTOS_RUNTIME_ENV, ignore_reinit_error=True)
    MONITOR_NAMESPACE = "ray_multicluster_scheduler"

    # 获取 BackgroundHealthChecker 状态
    checker = ray.get_actor("background_health_checker", namespace=MONITOR_NAMESPACE)
    initial_status = ray.get(checker.get_status.remote())
    print(f"   初始状态: {initial_status}")

    # 等待25秒，让后台监控至少执行一次刷新
    print("   等待25秒，让后台监控执行刷新...")
    time.sleep(25)

    # 再次获取状态
    updated_status = ray.get(checker.get_status.remote())
    print(f"   更新后状态: {updated_status}")

    # 验证 iteration_count 是否增加
    if updated_status['iteration_count'] > initial_status['iteration_count']:
        print(f"✅ 后台监控正常运行：刷新次数从 {initial_status['iteration_count']} 增加到 {updated_status['iteration_count']}")
    else:
        print(f"⚠️  后台监控可能未运行：刷新次数仍为 {updated_status['iteration_count']}")

    # 验证 ClusterResourceMonitor 的快照是否更新
    monitor = ray.get_actor("cluster_resource_monitor", namespace=MONITOR_NAMESPACE)
    snapshots = ray.get(monitor.get_latest_snapshots.remote())

    if snapshots:
        print(f"✅ ClusterResourceMonitor 已有快照数据：{len(snapshots)} 个集群")
        for cluster_name, snapshot in snapshots.items():
            print(f"   - {cluster_name}: CPU={snapshot.cluster_cpu_usage_percent:.1f}%, MEM={snapshot.cluster_mem_usage_percent:.1f}%")
    else:
        print("⚠️  ClusterResourceMonitor 没有快照数据")

    ray.shutdown()
    print("✅ 后台监控持续运行验证通过")
except Exception as e:
    print(f"❌ 后台监控验证失败: {e}")
    import traceback
    traceback.print_exc()

# 清理
print("\n[步骤8] 清理资源")
try:
    from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
    scheduler = get_unified_scheduler()
    scheduler.cleanup()

    if ray.is_initialized():
        ray.shutdown()

    print("✅ 资源清理完成")
except Exception as e:
    print(f"⚠️ 清理时出现警告: {e}")

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)
