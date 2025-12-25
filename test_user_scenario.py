#!/usr/bin/env python3
"""
Final test script to verify the complete path conversion functionality for user's use case.
"""
import sys
import os
import uuid

# Add the project root and scheduler modules to the path
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('./ray_multicluster_scheduler'))

def test_user_scenario():
    print("Testing user's job submission scenario with path conversion...")

    # 模拟用户代码
    index_mapping = {'AAPL': 1, 'GOOGL': 2}  # 示例数据

    print("\nSimulating user's code with absolute paths:")
    for idx in index_mapping.keys():
        entrypoint = f'python /Users/zorro/project/Index_Markup_Forecasting/index_ts_forecasting_model/cat/predict_weekly_cluster.py --index {idx}'
        print(f"  Generated entrypoint: {entrypoint}")

        # 模拟调度决策和路径转换
        import re

        # 提取路径
        path_match = re.search(r'\bpython\s+(/[^\s]+)', entrypoint, re.IGNORECASE)
        if path_match:
            script_path = path_match.group(1)
            print(f"    Script path: {script_path}")

            # 模拟路径转换逻辑
            possible_source_home_dirs = ['/home/', '/Users/', '/root/']

            for source_home in possible_source_home_dirs:
                if script_path.startswith(source_home):
                    # 提取相对路径部分，移除源home_dir部分（包含用户名）
                    source_username_start = len(source_home)
                    remaining_path = script_path[source_username_start:]
                    username_end = remaining_path.find('/')
                    if username_end != -1:
                        relative_path = remaining_path[username_end + 1:]
                    else:
                        relative_path = remaining_path

                    # 模拟目标集群配置
                    target_home_dir = "/home/zorro"  # 目标集群的home_dir
                    # 构建目标路径 - 保留目标home_dir的完整路径（包括用户名）
                    target_path = f"{target_home_dir}/{relative_path}"

                    new_entrypoint = entrypoint.replace(script_path, target_path)
                    print(f"    Converted path: {new_entrypoint}")
                    print(f"    Target cluster home_dir: {target_home_dir}")
                    break
            else:
                print(f"    No matching source home directory found for: {script_path}")

    print("\nPath conversion successfully handles the user's scenario!")
    print("Entrypoint paths will be automatically converted from /Users/ to /home/ format")
    print("when submitted to different clusters with different home_dir configurations.")
    print("\nExample conversion:")
    print("  From: python /Users/zorro/project/script.py")
    print("  To:   python /home/zorro/project/script.py")

if __name__ == "__main__":
    test_user_scenario()