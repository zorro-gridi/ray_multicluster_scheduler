"""
测试代码：删除当前集群配置的所有集群环境中的actor
"""
import ray
import yaml
import os
from typing import Dict, Any, List


def load_cluster_configs(config_path: str = "clusters.yaml") -> List[Dict[str, Any]]:
    """加载集群配置"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
    return config_data.get('clusters', [])


def delete_all_actors_from_cluster(cluster_config: Dict[str, Any]) -> int:
    """
    从指定集群中删除所有actors

    Args:
        cluster_config: 集群配置字典

    Returns:
        int: 删除的actor数量
    """
    cluster_name = cluster_config['name']
    head_address = cluster_config['head_address']
    runtime_env = cluster_config.get('runtime_env', {})

    print(f"正在连接到集群 {cluster_name} ({head_address})...")

    try:
        # 连接到指定集群
        ray.init(
            address=f"ray://{head_address}",
            runtime_env=runtime_env,
            ignore_reinit_error=True
        )

        # 获取所有命名actors
        named_actors = ray.util.list_named_actors(all_namespaces=True)
        print(f"在集群 {cluster_name} 中找到 {len(named_actors)} 个命名actors")

        deleted_count = 0
        for actor_info in named_actors:
            actor_name = actor_info['name']
            namespace = actor_info['namespace']

            print(f"  尝试删除 actor: {actor_name} (namespace: {namespace})")

            try:
                # 获取actor句柄并杀死它
                actor_handle = ray.get_actor(actor_name, namespace=namespace)
                ray.kill(actor_handle)
                print(f"    ✓ 成功删除 actor: {actor_name}")
                deleted_count += 1
            except Exception as e:
                print(f"    ⚠ 删除 actor {actor_name} 失败: {e}")

        # 关闭当前集群连接
        ray.shutdown()
        print(f"集群 {cluster_name} 连接已关闭")

        return deleted_count

    except Exception as e:
        print(f"连接到集群 {cluster_name} 时出错: {e}")
        if ray.is_initialized():
            ray.shutdown()
        return 0


def delete_all_actors_from_all_clusters(config_path: str = "clusters.yaml") -> Dict[str, int]:
    """
    从所有集群中删除所有actors

    Args:
        config_path: 集群配置文件路径

    Returns:
        Dict[str, int]: 每个集群删除的actor数量
    """
    print("开始删除所有集群中的actors...")

    cluster_configs = load_cluster_configs(config_path)
    if not cluster_configs:
        print("未找到集群配置")
        return {}

    print(f"加载了 {len(cluster_configs)} 个集群配置")

    results = {}
    total_deleted = 0

    for cluster_config in cluster_configs:
        cluster_name = cluster_config['name']
        print(f"\n处理集群: {cluster_name}")

        deleted_count = delete_all_actors_from_cluster(cluster_config)
        results[cluster_name] = deleted_count
        total_deleted += deleted_count

        print(f"集群 {cluster_name} 完成，删除了 {deleted_count} 个actors")

    print(f"\n总计删除了 {total_deleted} 个actors")
    print("所有集群的删除结果:")
    for cluster_name, count in results.items():
        print(f"  {cluster_name}: {count} 个")

    return results


def main():
    """主函数"""
    print("=== 删除所有集群中的actors测试 ===")

    try:
        # 删除所有集群中的actors
        results = delete_all_actors_from_all_clusters()

        print("\n=== 测试完成 ===")
        print("删除统计:")
        for cluster_name, count in results.items():
            print(f"  {cluster_name}: {count} 个actors")

    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()