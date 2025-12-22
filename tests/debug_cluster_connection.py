#!/usr/bin/env python3
"""
集群连接调试工具
诊断 "Could not get client for cluster mac" 异常
"""

import sys
import os
import time
import socket
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')

from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.scheduler.cluster.cluster_manager import ClusterManager
from ray_multicluster_scheduler.scheduler.cluster.cluster_registry import ClusterRegistry
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool


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


def debug_cluster_config():
    """调试集群配置"""
    print("=" * 60)
    print("调试集群配置")
    print("=" * 60)
    
    try:
        # 加载集群配置
        config_manager = ConfigManager()
        clusters = config_manager.load_clusters()
        
        print(f"加载到 {len(clusters)} 个集群配置:")
        for i, cluster in enumerate(clusters):
            print(f"\n集群 {i+1}:")
            print(f"  名称: {cluster.name}")
            print(f"  地址: {cluster.head_address}")
            print(f"  仪表板: {cluster.dashboard}")
            print(f"  偏好: {cluster.prefer}")
            print(f"  权重: {cluster.weight}")
            print(f"  标签: {cluster.tags}")
            print(f"  Runtime Env: {cluster.runtime_env}")
            
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
        
        return clusters
    except Exception as e:
        print(f"❌ 集群配置加载失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def debug_cluster_manager():
    """调试集群管理器"""
    print("\n" + "=" * 60)
    print("调试集群管理器")
    print("=" * 60)
    
    try:
        # 创建集群管理器
        cluster_manager = ClusterManager()
        print("✅ 集群管理器创建成功")
        
        # 刷新集群状态
        print("刷新集群状态...")
        cluster_manager.refresh_clusters()
        print("✅ 集群状态刷新完成")
        
        # 显示集群信息
        print(f"\n发现 {len(cluster_manager.clusters)} 个集群:")
        for name, config in cluster_manager.clusters.items():
            print(f"  - {name}: {config.head_address}")
        
        return cluster_manager
    except Exception as e:
        print(f"❌ 集群管理器调试失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def debug_connection_lifecycle():
    """调试连接生命周期管理"""
    print("\n" + "=" * 60)
    print("调试连接生命周期管理")
    print("=" * 60)
    
    try:
        # 创建连接池
        client_pool = RayClientPool()
        print("✅ 连接池创建成功")
        
        # 创建连接生命周期管理器
        connection_manager = ConnectionLifecycleManager(client_pool)
        print("✅ 连接生命周期管理器创建成功")
        
        # 加载集群配置
        config_manager = ConfigManager()
        clusters = config_manager.load_clusters()
        
        # 注册集群
        for cluster in clusters:
            print(f"注册集群 {cluster.name}...")
            connection_manager.register_cluster(cluster)
            print(f"✅ 集群 {cluster.name} 注册成功")
        
        # 显示已注册的集群
        registered_clusters = connection_manager.list_registered_clusters()
        print(f"\n已注册的集群: {registered_clusters}")
        
        # 尝试获取连接
        for cluster_name in registered_clusters:
            print(f"\n尝试获取集群 {cluster_name} 的连接...")
            connection = connection_manager.get_connection(cluster_name)
            if connection:
                print(f"✅ 成功获取集群 {cluster_name} 的连接")
                print(f"   连接地址: {connection.get('address', 'N/A')}")
                print(f"   是否已连接: {connection.get('connected', False)}")
            else:
                print(f"❌ 无法获取集群 {cluster_name} 的连接")
        
        return connection_manager
    except Exception as e:
        print(f"❌ 连接生命周期管理调试失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def debug_ray_client_connection():
    """调试Ray客户端连接"""
    print("\n" + "=" * 60)
    print("调试Ray客户端连接")
    print("=" * 60)
    
    try:
        import ray
        
        # 加载集群配置
        config_manager = ConfigManager()
        clusters = config_manager.load_clusters()
        
        for cluster in clusters:
            print(f"\n尝试连接到集群 {cluster.name}...")
            print(f"  地址: {cluster.head_address}")
            
            # 构建Ray客户端地址
            ray_client_address = f"ray://{cluster.head_address}"
            print(f"  Ray客户端地址: {ray_client_address}")
            
            # 测试连接
            try:
                print(f"  尝试初始化Ray连接...")
                # 使用ignore_reinit_error=True避免重复初始化错误
                ray.init(
                    address=ray_client_address,
                    ignore_reinit_error=True,
                    logging_level="INFO"
                )
                print(f"  ✅ 成功连接到集群 {cluster.name}")
                
                # 获取集群信息
                print(f"  获取集群信息...")
                nodes = ray.nodes()
                print(f"  集群节点数: {len(nodes)}")
                
                # 断开连接
                print(f"  断开连接...")
                ray.shutdown()
                print(f"  ✅ 成功断开与集群 {cluster.name} 的连接")
                
            except Exception as e:
                print(f"  ❌ 连接到集群 {cluster.name} 失败: {e}")
                import traceback
                traceback.print_exc()
                
    except Exception as e:
        print(f"❌ Ray客户端连接调试失败: {e}")
        import traceback
        traceback.print_exc()


def main():
    """主函数"""
    print("开始集群连接调试...")
    
    # 1. 调试集群配置
    clusters = debug_cluster_config()
    
    # 2. 调试集群管理器
    cluster_manager = debug_cluster_manager()
    
    # 3. 调试连接生命周期管理
    connection_manager = debug_connection_lifecycle()
    
    # 4. 调试Ray客户端连接
    debug_ray_client_connection()
    
    print("\n" + "=" * 60)
    print("调试完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()