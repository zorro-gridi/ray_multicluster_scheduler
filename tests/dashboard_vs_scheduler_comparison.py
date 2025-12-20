#!/usr/bin/env python3
"""
Ray Dashboard vs 调度系统资源计算对比测试
比较Ray Dashboard显示的数据和调度系统计算的数据
"""

import sys
import os
import time
import requests
from urllib.parse import urljoin
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

import ray


def compare_dashboard_and_scheduler_resources():
    """比较Ray Dashboard和调度系统的资源计算"""
    print("=" * 60)
    print("比较Ray Dashboard和调度系统的资源计算")
    print("=" * 60)

    # 集群配置
    cluster_info = {
        "name": "mac",
        "address": "ray://192.168.5.2:32546",
        "dashboard": "http://192.168.5.2:8265"
    }

    print(f"集群名称: {cluster_info['name']}")
    print(f"Ray地址: {cluster_info['address']}")
    print(f"Dashboard地址: {cluster_info['dashboard']}")

    try:
        # 1. 通过Ray API获取资源信息（调度系统使用的方法）
        print("\n1. 通过Ray API获取资源信息:")
        ray.init(
            address=cluster_info['address'],
            ignore_reinit_error=True,
            logging_level="WARNING"
        )

        if not ray.is_initialized():
            print("❌ Ray连接失败")
            return

        print("✅ Ray连接成功")

        # 获取资源信息
        avail_resources = ray.available_resources()
        total_resources = ray.cluster_resources()
        nodes = ray.nodes()

        print(f"   可用资源: {avail_resources}")
        print(f"   总资源: {total_resources}")
        print(f"   节点数: {len(nodes)}")

        # 计算CPU使用率（调度系统的方法）
        cpu_free = avail_resources.get("CPU", 0)
        cpu_total = total_resources.get("CPU", 0)

        if cpu_total > 0:
            scheduler_cpu_util = (cpu_total - cpu_free) / cpu_total
        else:
            scheduler_cpu_util = 0

        print(f"   调度系统计算 - CPU: 可用={cpu_free}, 总计={cpu_total}")
        print(f"   调度系统计算 - CPU使用率: {scheduler_cpu_util:.2%}")

        # 2. 通过Dashboard API获取资源信息
        print("\n2. 通过Dashboard API获取资源信息:")
        try:
            # 获取节点信息
            nodes_url = urljoin(cluster_info['dashboard'], "/nodes")
            response = requests.get(nodes_url, timeout=10)

            if response.status_code == 200:
                nodes_data = response.json()
                print(f"   Dashboard节点数据: {nodes_data}")

                # 计算Dashboard显示的CPU使用率
                total_cpus = 0
                used_cpus = 0

                if isinstance(nodes_data, list):
                    for node in nodes_data:
                        # 提取CPU信息
                        if "Resources" in node:
                            resources = node["Resources"]
                            if "CPU" in resources:
                                total_cpus += resources["CPU"]
                            if "CPU" in resources and "AvailableResources" in node:
                                avail_res = node["AvailableResources"]
                                if "CPU" in avail_res:
                                    used_cpus += resources["CPU"] - avail_res["CPU"]

                if total_cpus > 0:
                    dashboard_cpu_util = used_cpus / total_cpus
                else:
                    dashboard_cpu_util = 0

                print(f"   Dashboard计算 - CPU: 已用={used_cpus}, 总计={total_cpus}")
                print(f"   Dashboard计算 - CPU使用率: {dashboard_cpu_util:.2%}")
            else:
                print(f"   ❌ 无法获取Dashboard数据，状态码: {response.status_code}")

        except Exception as e:
            print(f"   ❌ 获取Dashboard数据失败: {e}")

        # 3. 获取详细节点信息
        print("\n3. 获取详细节点信息:")
        for i, node in enumerate(nodes):
            print(f"   节点 {i+1}:")
            print(f"     节点ID: {node.get('NodeID', 'N/A')}")
            print(f"     状态: {'存活' if node.get('Alive', False) else '离线'}")
            resources = node.get('Resources', {})
            print(f"     资源: {resources}")

            # 计算该节点的CPU使用率
            cpu_total_node = resources.get('CPU', 0)
            # 注意：这里需要更复杂的逻辑来获取节点级别的可用资源

        # 4. 检查是否有特殊资源标签
        print("\n4. 检查特殊资源标签:")
        all_resource_keys = set()
        for node in nodes:
            resources = node.get('Resources', {})
            all_resource_keys.update(resources.keys())

        print(f"   所有资源键: {sorted(all_resource_keys)}")

        # 查找可能的CPU相关资源
        cpu_related_keys = [key for key in all_resource_keys if 'cpu' in key.lower() or 'CPU' in key]
        print(f"   CPU相关资源键: {cpu_related_keys}")

        # 5. 检查是否存在MacCPU等特殊资源
        print("\n5. 检查特殊CPU资源:")
        for node in nodes:
            resources = node.get('Resources', {})
            special_cpus = {}
            for key, value in resources.items():
                if 'cpu' in key.lower() and key != 'CPU':
                    special_cpus[key] = value

            if special_cpus:
                print(f"   特殊CPU资源: {special_cpus}")

        # 断开连接
        ray.shutdown()
        print("\n🔌 断开连接")

    except Exception as e:
        print(f"❌ 执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

        # 确保断开连接
        try:
            ray.shutdown()
        except:
            pass


def detailed_resource_analysis():
    """详细资源分析"""
    print("\n" + "=" * 60)
    print("详细资源分析")
    print("=" * 60)

    cluster_address = "ray://192.168.5.2:32546"
    print(f"连接到集群: {cluster_address}")

    try:
        # 连接到集群
        ray.init(
            address=cluster_address,
            ignore_reinit_error=True,
            logging_level="WARNING"
        )

        if not ray.is_initialized():
            print("❌ 连接失败")
            return

        print("✅ 连接成功")

        # 获取详细的资源信息
        print("\n获取详细资源信息:")

        # 1. 总资源
        total_resources = ray.cluster_resources()
        print(f"1. 集群总资源: {total_resources}")

        # 2. 可用资源
        avail_resources = ray.available_resources()
        print(f"2. 集群可用资源: {avail_resources}")

        # 3. 节点信息
        nodes = ray.nodes()
        print(f"3. 节点数量: {len(nodes)}")

        # 4. 详细分析每个节点
        print("\n4. 详细节点分析:")
        cluster_total_cpu = 0
        cluster_used_cpu = 0

        for i, node in enumerate(nodes):
            print(f"\n   节点 {i+1}:")
            node_id = node.get('NodeID', 'N/A')
            print(f"     节点ID: {node_id}")
            print(f"     状态: {'存活' if node.get('Alive', False) else '离线'}")

            resources = node.get('Resources', {})
            print(f"     资源详情: {resources}")

            # 计算该节点的CPU使用情况
            node_cpu_total = resources.get('CPU', 0)
            cluster_total_cpu += node_cpu_total

            print(f"     节点CPU总数: {node_cpu_total}")

            # 注意：Ray的资源模型比较复杂，可用资源是全局的而不是节点级别的

        # 5. 全局CPU使用率计算
        print("\n5. 全局CPU使用率计算:")
        global_cpu_total = total_resources.get("CPU", 0)
        global_cpu_avail = avail_resources.get("CPU", 0)

        if global_cpu_total > 0:
            global_cpu_util = (global_cpu_total - global_cpu_avail) / global_cpu_total
        else:
            global_cpu_util = 0

        print(f"   全局总CPU: {global_cpu_total}")
        print(f"   全局可用CPU: {global_cpu_avail}")
        print(f"   全局CPU使用率 (调度系统计算): {global_cpu_util:.2%}")

        # 6. 检查是否存在其他CPU资源类型
        print("\n6. 检查其他CPU资源类型:")
        cpu_resources = {}
        for key, value in total_resources.items():
            if 'cpu' in key.lower():
                cpu_resources[key] = {
                    'total': value,
                    'available': avail_resources.get(key, 0)
                }
                if value > 0:
                    util = (value - avail_resources.get(key, 0)) / value
                    cpu_resources[key]['utilization'] = util

        for key, data in cpu_resources.items():
            print(f"   {key}: 总计={data['total']}, 可用={data['available']}")
            if 'utilization' in data:
                print(f"         使用率={data['utilization']:.2%}")

        # 断开连接
        ray.shutdown()
        print("\n🔌 断开连接")

    except Exception as e:
        print(f"❌ 执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

        # 确保断开连接
        try:
            ray.shutdown()
        except:
            pass


def investigate_mac_specific_resources():
    """调查MAC特定资源"""
    print("\n" + "=" * 60)
    print("调查MAC特定资源")
    print("=" * 60)

    cluster_address = "ray://192.168.5.2:32546"
    print(f"连接到集群: {cluster_address}")

    try:
        # 连接到集群
        ray.init(
            address=cluster_address,
            ignore_reinit_error=True,
            logging_level="WARNING"
        )

        if not ray.is_initialized():
            print("❌ 连接失败")
            return

        print("✅ 连接成功")

        # 获取所有资源信息
        total_resources = ray.cluster_resources()
        avail_resources = ray.available_resources()
        nodes = ray.nodes()

        print("\n所有资源键分析:")
        all_keys = set(total_resources.keys()) | set(avail_resources.keys())
        sorted_keys = sorted(all_keys)

        print("资源键列表:")
        for key in sorted_keys:
            total_val = total_resources.get(key, 0)
            avail_val = avail_resources.get(key, 0)
            if total_val > 0:
                util = (total_val - avail_val) / total_val
                print(f"  {key}: 总计={total_val}, 可用={avail_val}, 使用率={util:.2%}")
            else:
                print(f"  {key}: 总计={total_val}, 可用={avail_val}")

        # 特别关注MAC相关资源
        print("\nMAC特定资源分析:")
        mac_related_keys = [key for key in all_keys if 'mac' in key.lower() or 'Mac' in key]
        if mac_related_keys:
            print("MAC相关资源键:")
            for key in mac_related_keys:
                total_val = total_resources.get(key, 0)
                avail_val = avail_resources.get(key, 0)
                if total_val > 0:
                    util = (total_val - avail_val) / total_val
                    print(f"  {key}: 总计={total_val}, 可用={avail_val}, 使用率={util:.2%}")
                else:
                    print(f"  {key}: 总计={total_val}, 可用={avail_val}")
        else:
            print("未找到MAC相关资源键")

        # 检查节点级别资源
        print("\n节点级别资源分析:")
        for i, node in enumerate(nodes):
            print(f"\n节点 {i+1}:")
            resources = node.get('Resources', {})
            node_id = node.get('NodeID', 'N/A')
            print(f"  节点ID: {node_id}")

            # 分析该节点的资源
            for key, value in resources.items():
                # 查找CPU相关资源
                if 'cpu' in key.lower():
                    print(f"    {key}: {value}")

        # 断开连接
        ray.shutdown()
        print("\n🔌 断开连接")

    except Exception as e:
        print(f"❌ 执行过程中出错: {e}")
        import traceback
        traceback.print_exc()

        # 确保断开连接
        try:
            ray.shutdown()
        except:
            pass


if __name__ == "__main__":
    # 比较Ray Dashboard和调度系统的资源计算
    compare_dashboard_and_scheduler_resources()

    # 详细资源分析
    detailed_resource_analysis()

    # 调查MAC特定资源
    investigate_mac_specific_resources()

    print("\n" + "=" * 60)
    print("🎉 分析完成!")
    print("=" * 60)
    print("\n可能的原因分析:")
    print("1. Ray Dashboard和调度系统可能使用不同的资源计算方法")
    print("2. MAC集群可能存在特殊的资源类型（如MacCPU）")
    print("3. Dashboard可能显示更细粒度的资源使用情况")
    print("4. 调度系统的资源计算可能存在bug或配置问题")