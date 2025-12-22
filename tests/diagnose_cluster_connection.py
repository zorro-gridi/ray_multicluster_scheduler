#!/usr/bin/env python3
"""
诊断集群连接问题
"""

import sys
import os
import time
import socket
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.control_plane.config import ConfigManager


def test_network_connectivity(host, port, timeout=5):
    """测试网络连通性"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"网络连通性测试失败: {e}")
        return False


def diagnose_cluster_connections():
    """诊断集群连接"""
    print("=" * 60)
    print("诊断集群连接")
    print("=" * 60)
    
    try:
        # 加载集群配置
        config_manager = ConfigManager()
        clusters = config_manager.load_clusters()
        
        print(f"加载到 {len(clusters)} 个集群配置:")
        
        for i, cluster in enumerate(clusters):
            print(f"\n集群 {i+1}: {cluster.name}")
            print(f"  地址: {cluster.head_address}")
            print(f"  仪表板: {cluster.dashboard}")
            print(f"  偏好: {cluster.prefer}")
            
            # 解析地址
            if ':' in cluster.head_address:
                host, port = cluster.head_address.split(':')
                port = int(port)
                print(f"  主机: {host}")
                print(f"  端口: {port}")
                
                # 测试网络连通性
                print(f"  网络连通性测试: ", end="")
                if test_network_connectivity(host, port):
                    print("✅ 可达")
                else:
                    print("❌ 不可达")
            else:
                print(f"  ❌ 地址格式不正确: {cluster.head_address}")
        
        return clusters
    except Exception as e:
        print(f"❌ 集群配置加载失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def check_cluster_status():
    """检查集群状态"""
    print("\n" + "=" * 60)
    print("检查集群状态")
    print("=" * 60)
    
    try:
        # 导入必要的模块
        from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager
        
        # 创建集群管理器
        cluster_manager = ClusterManager()
        print("✅ 集群管理器创建成功")
        
        # 刷新集群状态
        print("刷新集群状态...")
        cluster_manager.refresh_all_clusters()
        print("✅ 集群状态刷新完成")
        
        # 显示集群信息
        print(f"\n发现 {len(cluster_manager.clusters)} 个集群:")
        for name, config in cluster_manager.clusters.items():
            health = cluster_manager.health_status.get(name)
            print(f"  集群 [{name}]:")
            print(f"    地址: {config.head_address}")
            if health:
                print(f"    状态: {'🟢 健康' if health.available else '🔴 不健康'}")
                print(f"    评分: {health.score:.1f}")
                if health.resources:
                    cpu_free = health.resources.get('cpu_free', 0)
                    cpu_total = health.resources.get('cpu_total', 0)
                    print(f"    CPU: {cpu_free}/{cpu_total}")
            else:
                print("    ❌ 无法获取健康状态")
        
        return cluster_manager
    except Exception as e:
        print(f"❌ 集群状态检查失败: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # 诊断集群连接
    clusters = diagnose_cluster_connections()
    
    # 检查集群状态
    cluster_manager = check_cluster_status()
    
    print("\n" + "=" * 60)
    print("诊断完成!")
    print("=" * 60)