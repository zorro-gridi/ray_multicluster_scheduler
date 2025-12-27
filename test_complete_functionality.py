"""
完整功能测试 - 使用真实集群环境
测试调度器的所有核心功能，包括后台监控程序启动
"""
import ray
import time
import sys
from tools.Header_Gen import Header_Gen
from ray_multicluster_scheduler.app.client_api import (
    initialize_scheduler_environment,
    submit_task,
    submit_actor,
    submit_job,
    reset_scheduler_environment
)


def test_scheduler_initialization():
    """测试1: 调度器初始化，包括后台监控程序自动启动在centos集群"""
    print("\n" + "="*60)
    print("测试1: 调度器初始化（后台监控程序应自动启动在centos集群）")
    print("="*60)

    try:
        # 初始化调度器环境，后台监控程序应自动启动在centos集群
        task_manager = initialize_scheduler_environment(
            config_file_path="clusters.yaml",
            monitor_cluster_name="centos"  # 指定监控程序运行在centos集群
        )

        print("✅ 调度器初始化成功")
        print(f"   任务生命周期管理器: {task_manager}")
        print(f"   集群监控器: {task_manager.cluster_monitor}")
        print(f"   连接管理器: {task_manager.connection_manager}")

        # 获取集群信息
        cluster_info = task_manager.cluster_monitor.get_all_cluster_info()
        print(f"\n   配置的集群数量: {len(cluster_info)}")
        for cluster_name, info in cluster_info.items():
            metadata = info['metadata']
            snapshot = info['snapshot']
            print(f"   - 集群 [{cluster_name}]:")
            print(f"     地址: {metadata.head_address}")
            print(f"     偏好: {metadata.prefer}, 权重: {metadata.weight}")
            if snapshot:
                print(f"     CPU使用率: {snapshot.cluster_cpu_usage_percent:.1f}%")
                print(f"     内存使用率: {snapshot.cluster_mem_usage_percent:.1f}%")

        return True, task_manager
    except Exception as e:
        print(f"❌ 调度器初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False, None


def test_submit_task():
    """测试2: 提交普通任务"""
    print("\n" + "="*60)
    print("测试2: 提交普通任务")
    print("="*60)

    def simple_task(x, y):
        """简单计算任务"""
        import socket
        import os
        hostname = socket.gethostname()
        pid = os.getpid()
        result = x + y
        print(f"Task running on {hostname} (PID: {pid}), result: {result}")
        return {"result": result, "hostname": hostname, "pid": pid}

    try:
        task_id, result = submit_task(
            simple_task,
            args=(10, 20),
            name="simple_addition_task"
        )

        print(f"✅ 任务提交成功")
        print(f"   任务ID: {task_id}")
        print(f"   执行结果: {result}")
        print(f"   执行主机: {result.get('hostname', 'unknown')}")
        return True
    except Exception as e:
        print(f"❌ 任务提交失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_submit_actor():
    """测试3: 提交Actor"""
    print("\n" + "="*60)
    print("测试3: 提交Actor")
    print("="*60)

    @ray.remote
    class Counter:
        def __init__(self, initial_value=0):
            import socket
            self.value = initial_value
            self.hostname = socket.gethostname()
            print(f"Counter initialized on {self.hostname} with value {initial_value}")

        def increment(self, amount=1):
            self.value += amount
            return self.value

        def get_value(self):
            return {"value": self.value, "hostname": self.hostname}

    try:
        actor_id, actor_handle = submit_actor(
            Counter,
            args=(100,),
            name="counter_actor"
        )

        print(f"✅ Actor提交成功")
        print(f"   Actor ID: {actor_id}")

        # 测试Actor方法调用
        value1 = ray.get(actor_handle.increment.remote(10))
        print(f"   增加10后的值: {value1}")

        value2 = ray.get(actor_handle.increment.remote(5))
        print(f"   再增加5后的值: {value2}")

        final_state = ray.get(actor_handle.get_value.remote())
        print(f"   最终状态: {final_state}")
        print(f"   Actor运行主机: {final_state.get('hostname', 'unknown')}")

        return True
    except Exception as e:
        print(f"❌ Actor提交失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_submit_job():
    """测试4: 提交Job"""
    print("\n" + "="*60)
    print("测试4: 提交Job")
    print("="*60)

    try:
        # 准备测试脚本
        test_script = """
import socket
import time
from tools.Header_Gen import Header_Gen

hostname = socket.gethostname()
print(f"Job running on: {hostname}")

# 测试自定义模块导入
header = Header_Gen()
print(f"Header_Gen imported successfully: {header}")

# 执行一些计算
result = sum(range(1000))
print(f"Computation result: {result}")

print("Job completed successfully!")
"""

        # 创建临时测试脚本
        import os
        script_path = os.path.join(os.getcwd(), "temp_job_test.py")
        with open(script_path, 'w') as f:
            f.write(test_script)

        # 提交Job - 注意：不要同时指定pip和conda，因为集群配置中已经有conda
        job_id = submit_job(
            entrypoint=f"python {script_path}",
            runtime_env={
                "working_dir": "."
                # 移除pip配置，避免与集群的conda配置冲突
            }
        )

        print(f"✅ Job提交成功")
        print(f"   Job ID: {job_id}")
        print(f"   注意: Job会异步执行，可以通过Ray Dashboard查看执行状态")

        # 清理临时文件
        time.sleep(2)  # 等待Job开始执行
        if os.path.exists(script_path):
            os.remove(script_path)

        return True
    except Exception as e:
        print(f"❌ Job提交失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_preferred_cluster_scheduling():
    """测试5: 指定集群调度"""
    print("\n" + "="*60)
    print("测试5: 指定集群调度测试")
    print("="*60)

    def cluster_info_task():
        """获取当前执行集群信息"""
        import socket
        import platform
        hostname = socket.gethostname()
        system = platform.system()
        machine = platform.machine()
        return {
            "hostname": hostname,
            "system": system,
            "machine": machine
        }

    try:
        # 测试在centos集群执行
        print("\n测试5.1: 指定centos集群执行任务")
        task_id1, result1 = submit_task(
            cluster_info_task,
            name="task_on_centos",
            preferred_cluster="centos"
        )
        print(f"✅ centos集群任务完成")
        print(f"   执行主机: {result1['hostname']}")
        print(f"   系统类型: {result1['system']}/{result1['machine']}")

        # 测试在mac集群执行
        print("\n测试5.2: 指定mac集群执行任务")
        task_id2, result2 = submit_task(
            cluster_info_task,
            name="task_on_mac",
            preferred_cluster="mac"
        )
        print(f"✅ mac集群任务完成")
        print(f"   执行主机: {result2['hostname']}")
        print(f"   系统类型: {result2['system']}/{result2['machine']}")

        # 验证任务确实在不同集群执行
        if result1['system'] != result2['system']:
            print(f"\n✅ 跨集群调度验证成功！")
            print(f"   centos任务系统: {result1['system']}")
            print(f"   mac任务系统: {result2['system']}")
        else:
            print(f"\n⚠️ 两个任务可能在同一系统执行")

        return True
    except Exception as e:
        print(f"❌ 指定集群调度测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_balancing():
    """测试6: 负载均衡调度"""
    print("\n" + "="*60)
    print("测试6: 负载均衡调度（不指定集群）")
    print("="*60)

    def balanced_task(task_num):
        """负载均衡测试任务"""
        import socket
        import time
        hostname = socket.gethostname()
        time.sleep(0.5)  # 模拟计算
        return {"task_num": task_num, "hostname": hostname}

    try:
        print("\n提交10个任务进行负载均衡测试...")
        results = []
        for i in range(10):
            task_id, result = submit_task(
                balanced_task,
                args=(i,),
                name=f"balanced_task_{i}"
            )
            results.append(result)
            print(f"   任务 {i} 完成，执行主机: {result['hostname']}")

        # 统计任务分布
        from collections import Counter
        hostname_distribution = Counter([r['hostname'] for r in results])

        print(f"\n✅ 负载均衡统计:")
        for hostname, count in hostname_distribution.items():
            print(f"   主机 {hostname}: {count} 个任务 ({count*10}%)")

        return True
    except Exception as e:
        print(f"❌ 负载均衡测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_monitor_actor_check():
    """测试7: 验证后台监控Actor是否正常运行"""
    print("\n" + "="*60)
    print("测试7: 验证后台监控Actor运行状态")
    print("="*60)

    try:
        import ray

        # 监控器应该运行在centos集群，所以我们需要连接到centos集群来检查
        print("   监控器应该运行在centos集群，尝试连接...")

        # 保存当前连接状态
        was_initialized = ray.is_initialized()

        # 如果已初始化，先关闭
        if was_initialized:
            ray.shutdown()
            time.sleep(1)

        # 连接到centos集群，并配置 runtime_env
        centos_runtime_env = {
            "conda": "ts",
            "env_vars": {
                "home_dir": "/home/zorro",
                "PYTHONPATH": "/home/zorro/project/pycharm/ray_multicluster_scheduler:${PYTHONPATH}"
            },
            "working_dir": "."
        }
        ray.init(address="ray://192.168.5.7:32546", runtime_env=centos_runtime_env, ignore_reinit_error=True)
        print("   ✅ 已连接到centos集群")

        # 等待一下让连接稳定
        time.sleep(1)

        # 先列出所有actor看看
        all_actors = ray.util.list_named_actors(all_namespaces=True)
        print(f"   当前集群所有命名actor数量: {len(all_actors)}")
        monitor_found = False
        for actor_info in all_actors:
            if 'cluster_resource_monitor' in actor_info.get('name', ''):
                namespace = actor_info.get('namespace', 'default')
                if isinstance(namespace, bytes):
                    namespace = namespace.decode('utf-8')
                state = actor_info.get('state', 'unknown')
                print(f"   发现monitor: name={actor_info['name']}, namespace={namespace}, state={state}")
                monitor_found = True

        if not monitor_found:
            print("   ⚠️ 未在列表中发现cluster_resource_monitor")

        # 尝试获取监控Actor（使用固定namespace）
        MONITOR_NAMESPACE = "ray_multicluster_scheduler"
        try:
            monitor = ray.get_actor("cluster_resource_monitor", namespace=MONITOR_NAMESPACE)
            print(f"✅ 找到后台监控Actor: cluster_resource_monitor (namespace: {MONITOR_NAMESPACE})")
        except ValueError as e:
            print(f"❌ 未找到后台监控Actor: {e}")
            print("   尝试在默认namespace查找...")
            try:
                monitor = ray.get_actor("cluster_resource_monitor")
                print(f"✅ 在默认namespace找到监控Actor")
            except ValueError as e2:
                print(f"   ❌ 默认namespace也未找到: {e2}")
                # 关闭Ray连接
                ray.shutdown()
                return False

        # 检查监控器健康状态
        health_check = ray.get(monitor.ping.remote())
        print(f"   监控器健康检查: {health_check}")

        # 获取最新的集群快照
        snapshots = ray.get(monitor.get_latest_snapshots.remote())
        print(f"\n   监控器收集的集群快照数量: {len(snapshots)}")
        for cluster_name, snapshot in snapshots.items():
            print(f"   - 集群 [{cluster_name}]:")
            if snapshot:
                print(f"     CPU使用率: {snapshot.cluster_cpu_usage_percent:.1f}%")
                print(f"     内存使用率: {snapshot.cluster_mem_usage_percent:.1f}%")
            else:
                print(f"     快照为空")

        # 关闭Ray连接
        ray.shutdown()
        print("\n   ✅ 已断开centos集群连接")

        return True
    except Exception as e:
        print(f"❌ 监控Actor检查失败: {e}")
        import traceback
        traceback.print_exc()
        # 确保关闭Ray连接
        try:
            if ray.is_initialized():
                ray.shutdown()
        except:
            pass
        return False


def test_monitor_singleton():
    """测试8: 验证ClusterResourceMonitor单例模式"""
    print("\n" + "="*60)
    print("测试8: 验证ClusterResourceMonitor单例模式")
    print("="*60)

    try:
        import ray

        # 连接到centos集群
        print("   连接到centos集群...")
        if ray.is_initialized():
            ray.shutdown()
            time.sleep(1)

        # 配置 centos 集群的 runtime_env
        centos_runtime_env = {
            "conda": "ts",
            "env_vars": {
                "home_dir": "/home/zorro",
                "PYTHONPATH": "/home/zorro/project/pycharm/ray_multicluster_scheduler:${PYTHONPATH}"
            },
            "working_dir": "."
        }
        ray.init(address="ray://192.168.5.7:32546", runtime_env=centos_runtime_env, ignore_reinit_error=True)
        print("   ✅ 已连接到centos集群")

        # 检查所有namespace中的cluster_resource_monitor数量
        MONITOR_NAMESPACE = "ray_multicluster_scheduler"
        all_actors = ray.util.list_named_actors(all_namespaces=True)
        monitor_count = 0
        monitor_namespaces = []

        for actor_info in all_actors:
            if actor_info['name'] == 'cluster_resource_monitor':
                namespace = actor_info.get('namespace', 'default')
                if isinstance(namespace, bytes):
                    namespace = namespace.decode('utf-8')
                monitor_namespaces.append(namespace)
                monitor_count += 1
                print(f"   发现monitor: namespace={namespace}, state={actor_info.get('state', 'unknown')}")

        # 验证只有一个monitor
        if monitor_count == 1:
            print(f"\n✅ 单例验证通过：仅有1个ClusterResourceMonitor")
            print(f"   Namespace: {monitor_namespaces[0]}")
            success = True
        elif monitor_count == 0:
            print(f"\n❌ 单例验证失败：未找到ClusterResourceMonitor")
            success = False
        else:
            print(f"\n❌ 单例验证失败：发现{monitor_count}个ClusterResourceMonitor")
            print(f"   Namespaces: {monitor_namespaces}")
            success = False

        ray.shutdown()
        return success

    except Exception as e:
        print(f"❌ 单例验证失败: {e}")
        import traceback
        traceback.print_exc()
        try:
            if ray.is_initialized():
                ray.shutdown()
        except:
            pass
        return False


def test_reset_functionality():
    """测试9: 测试reset功能"""
    print("\n" + "="*60)
    print("测试9: 测试reset功能")
    print("="*60)

    try:
        # 执行reset
        print("   执行reset_scheduler_environment...")
        reset_scheduler_environment()
        print("   ✅ Reset执行完成")

        # 等待一段时间确保删除完成
        time.sleep(2)

        # 验证monitor是否被删除
        import ray
        print("\n   验证monitor是否被删除...")
        if not ray.is_initialized():
            # 配置 centos 集群的 runtime_env
            centos_runtime_env = {
                "conda": "ts",
                "env_vars": {
                    "home_dir": "/home/zorro",
                    "PYTHONPATH": "/home/zorro/project/pycharm/ray_multicluster_scheduler:${PYTHONPATH}"
                },
                "working_dir": "."
            }
            ray.init(address="ray://192.168.5.7:32546", runtime_env=centos_runtime_env, ignore_reinit_error=True)

        MONITOR_NAMESPACE = "ray_multicluster_scheduler"
        all_actors = ray.util.list_named_actors(all_namespaces=True)
        remaining_monitors = []

        for actor_info in all_actors:
            if actor_info['name'] == 'cluster_resource_monitor':
                namespace = actor_info.get('namespace', 'default')
                if isinstance(namespace, bytes):
                    namespace = namespace.decode('utf-8')
                remaining_monitors.append(namespace)
                print(f"   ⚠️ 发现残留monitor: namespace={namespace}")

        if len(remaining_monitors) == 0:
            print("   ✅ 所有ClusterResourceMonitor已被删除")
            success = True
        else:
            print(f"   ❌ 仍有{len(remaining_monitors)}个monitor未删除")
            success = False

        ray.shutdown()
        return success

    except Exception as e:
        print(f"❌ Reset功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        try:
            if ray.is_initialized():
                ray.shutdown()
        except:
            pass
        return False


def test_reinitialization():
    """测试10: 测试重新初始化（验证不会重复创建monitor）"""
    print("\n" + "="*60)
    print("测试10: 测试重新初始化")
    print("="*60)

    try:
        import ray

        # 第一次初始化
        print("\n第一次初始化调度器...")
        task_manager1 = initialize_scheduler_environment(
            config_file_path="clusters.yaml",
            monitor_cluster_name="centos"
        )
        print("✅ 第一次初始化成功")
        time.sleep(3)  # 等待monitor完全启动

        # 检查monitor数量 - 必须连接到centos集群
        print("\n连接到centos集群检查monitor...")
        if ray.is_initialized():
            ray.shutdown()
            time.sleep(1)

        # 配置 centos 集群的 runtime_env
        centos_runtime_env = {
            "conda": "ts",
            "env_vars": {
                "home_dir": "/home/zorro",
                "PYTHONPATH": "/home/zorro/project/pycharm/ray_multicluster_scheduler:${PYTHONPATH}"
            },
            "working_dir": "."
        }
        ray.init(address="ray://192.168.5.7:32546", runtime_env=centos_runtime_env, ignore_reinit_error=True)
        time.sleep(1)

        all_actors = ray.util.list_named_actors(all_namespaces=True)
        monitor_count_1 = sum(1 for a in all_actors if a['name'] == 'cluster_resource_monitor')
        print(f"第一次初始化后，monitor数量: {monitor_count_1}")

        # 显示所有找到的monitor
        for actor_info in all_actors:
            if actor_info['name'] == 'cluster_resource_monitor':
                namespace = actor_info.get('namespace', 'default')
                if isinstance(namespace, bytes):
                    namespace = namespace.decode('utf-8')
                print(f"  - Monitor: namespace={namespace}, state={actor_info.get('state', 'unknown')}")

        ray.shutdown()
        time.sleep(1)

        # 第二次初始化（不reset，直接再次初始化）
        print("\n第二次初始化调度器（不reset）...")
        task_manager2 = initialize_scheduler_environment(
            config_file_path="clusters.yaml",
            monitor_cluster_name="centos"
        )
        print("✅ 第二次初始化成功")
        time.sleep(3)  # 等待monitor完全启动

        # 再次检查monitor数量 - 必须连接到centos集群
        print("\n再次连接到centos集群检查monitor...")
        if ray.is_initialized():
            ray.shutdown()
            time.sleep(1)

        # 配置 centos 集群的 runtime_env
        centos_runtime_env = {
            "conda": "ts",
            "env_vars": {
                "home_dir": "/home/zorro",
                "PYTHONPATH": "/home/zorro/project/pycharm/ray_multicluster_scheduler:${PYTHONPATH}"
            },
            "working_dir": "."
        }
        ray.init(address="ray://192.168.5.7:32546", runtime_env=centos_runtime_env, ignore_reinit_error=True)
        time.sleep(1)

        all_actors = ray.util.list_named_actors(all_namespaces=True)
        monitor_count_2 = sum(1 for a in all_actors if a['name'] == 'cluster_resource_monitor')
        print(f"第二次初始化后，monitor数量: {monitor_count_2}")

        # 显示所有找到的monitor
        for actor_info in all_actors:
            if actor_info['name'] == 'cluster_resource_monitor':
                namespace = actor_info.get('namespace', 'default')
                if isinstance(namespace, bytes):
                    namespace = namespace.decode('utf-8')
                print(f"  - Monitor: namespace={namespace}, state={actor_info.get('state', 'unknown')}")

        # 验证：第二次初始化不应该创建新的monitor
        if monitor_count_2 == monitor_count_1 == 1:
            print(f"\n✅ 重新初始化验证通过：monitor数量保持为1")
            success = True
        elif monitor_count_1 == 1 and monitor_count_2 == 1:
            print(f"\n✅ 重新初始化验证通过：两次都是1个monitor")
            success = True
        else:
            print(f"\n❌ 重新初始化验证失败：第一次={monitor_count_1}, 第二次={monitor_count_2}")
            success = False

        ray.shutdown()
        return success

    except Exception as e:
        print(f"❌ 重新初始化测试失败: {e}")
        import traceback
        traceback.print_exc()
        try:
            if ray.is_initialized():
                ray.shutdown()
        except:
            pass
        return False


def main():
    """主测试流程"""
    print("\n" + "🚀"*30)
    print("Ray多集群调度器 - 完整功能测试")
    print("测试环境: 真实集群配置 (centos + mac)")
    print("🚀"*30)

    # 测试结果记录
    test_results = {}

    # 测试1: 初始化调度器
    success, task_manager = test_scheduler_initialization()
    test_results['调度器初始化'] = success

    if not success:
        print("\n❌ 调度器初始化失败，终止后续测试")
        return

    # 等待一下让监控器启动完成
    print("\n等待3秒让后台监控程序完全启动...")
    time.sleep(3)

    # 测试7: 验证监控Actor（提前测试）
    test_results['后台监控Actor验证'] = test_monitor_actor_check()

    # 测试8: 验证单例模式
    test_results['ClusterResourceMonitor单例验证'] = test_monitor_singleton()

    # 测试2: 提交任务
    test_results['提交普通任务'] = test_submit_task()

    # 测试3: 提交Actor
    test_results['提交Actor'] = test_submit_actor()

    # 测试4: 提交Job
    test_results['提交Job'] = test_submit_job()

    # 测试5: 指定集群调度
    test_results['指定集群调度'] = test_preferred_cluster_scheduling()

    # 测试6: 负载均衡
    test_results['负载均衡调度'] = test_load_balancing()

    # 测试9: Reset功能
    test_results['Reset功能'] = test_reset_functionality()

    # 测试10: 重新初始化验证
    test_results['重新初始化验证'] = test_reinitialization()

    # 打印测试总结
    print("\n" + "="*60)
    print("测试结果汇总")
    print("="*60)

    total_tests = len(test_results)
    passed_tests = sum(1 for result in test_results.values() if result)

    for test_name, result in test_results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{status} - {test_name}")

    print(f"\n总计: {passed_tests}/{total_tests} 测试通过")

    # 清理资源
    print("\n" + "="*60)
    print("清理资源")
    print("="*60)

    try:
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
    except Exception as e:
        print(f"⚠️ 清理资源时出现警告: {e}")

    if passed_tests == total_tests:
        print("\n🎉 所有测试通过！调度器功能正常！")
        return 0
    else:
        print(f"\n⚠️ 有 {total_tests - passed_tests} 个测试失败")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
