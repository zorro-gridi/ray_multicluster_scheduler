#!/usr/bin/env python
"""
测试集群连接状态的脚本
"""

import ray
import time
import socket
from ray_multicluster_scheduler.control_plane.config import ConfigManager
from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import RayClientPool
from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
from ray_multicluster_scheduler.common.model import ClusterMetadata


def test_network_connectivity(host, port):
    """测试网络连接性"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  # 5秒超时
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"网络连接测试失败: {e}")
        return False


def test_ray_cluster_connection(cluster_metadata: ClusterMetadata):
    """测试单个Ray集群的连接"""
    print(f"\n测试集群: {cluster_metadata.name}")
    print(f"地址: {cluster_metadata.head_address}")
    
    # 测试网络连接
    host, port_str = cluster_metadata.head_address.split(':')
    port = int(port_str)
    
    print(f"1. 测试网络连接到 {host}:{port}...")
    if test_network_connectivity(host, port):
        print("   ✅ 网络连接正常")
    else:
        print("   ❌ 网络连接失败")
        return False
    
    # 测试Ray连接
    print(f"2. 测试Ray客户端连接...")
    try:
        ray_address = f"ray://{cluster_metadata.head_address}"
        print(f"   连接地址: {ray_address}")
        
        # 尝试连接
        ray.init(
            address=ray_address,
            ignore_reinit_error=True,
            logging_level=40  # 只显示错误日志
        )
        
        # 检查是否连接成功
        if ray.is_initialized():
            print("   ✅ Ray连接成功")
            
            # 获取集群信息
            try:
                cluster_resources = ray.cluster_resources()
                print(f"   集群资源: {cluster_resources}")
                
                # 获取节点信息
                nodes = ray.nodes()
                print(f"   节点数量: {len(nodes)}")
                
                # 尝试运行一个简单的任务
                @ray.remote
                def test_task():
                    return "Hello from " + ray.cluster_resources().get('cluster_name', 'unknown')
                
                result = ray.get(test_task.remote())
                print(f"   测试任务结果: {result}")
                
            except Exception as e:
                print(f"   ⚠️ 获取集群信息时出错: {e}")
            
            # 关闭连接
            ray.shutdown()
            return True
        else:
            print("   ❌ Ray连接失败")
            return False
            
    except Exception as e:
        print(f"   ❌ Ray连接失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("🔍 集群连接测试工具")
    print("="*50)
    
    # 加载集群配置
    config_manager = ConfigManager()
    cluster_configs = config_manager.get_cluster_configs()
    
    print(f"发现 {len(cluster_configs)} 个集群配置:")
    for config in cluster_configs:
        print(f"  - {config.name}: {config.head_address}")
    
    # 测试每个集群的连接
    for cluster_config in cluster_configs:
        success = test_ray_cluster_connection(cluster_config)
        if not success:
            print(f"❌ 集群 {cluster_config.name} 连接失败")
        else:
            print(f"✅ 集群 {cluster_config.name} 连接成功")
        print("-" * 50)
    
    print("\n📊 测试完成")


if __name__ == "__main__":
    main()