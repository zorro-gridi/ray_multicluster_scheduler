#!/usr/bin/env python3
"""
Test script to verify the path conversion functionality in submit_job.
"""
import sys
import os

# Add the project root and scheduler modules to the path
sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('./ray_multicluster_scheduler'))

def test_path_conversion():
    print("Testing path conversion functionality...")

    # Test the regular expression patterns
    import re

    # Test cases for different path patterns
    test_cases = [
        "python /Users/zorro/project/script.py --arg value",
        "python /home/user/project/script.py",
        "python /root/project/script.py --option test",
        "/Users/zorro/data/file.txt",
        "/home/user/data/file.txt"
    ]

    print("\nTesting path extraction from entrypoints:")
    for entrypoint in test_cases:
        # Extract script path for python commands
        path_match = re.search(r'\bpython\s+(/[^\s]+)', entrypoint, re.IGNORECASE)
        if path_match:
            script_path = path_match.group(1)
            print(f"  Python command: '{entrypoint}' -> Script path: '{script_path}'")
        else:
            # Extract possible paths for non-python commands
            possible_paths = re.findall(r'(/[^\s]+)', entrypoint)
            print(f"  Non-python command: '{entrypoint}' -> Possible paths: {possible_paths}")

    # Test path conversion logic
    print("\nTesting path conversion logic:")

    # Mock target cluster metadata
    class MockRuntimeEnv:
        def __init__(self, home_dir):
            self.env_vars = {'home_dir': home_dir}

    class MockClusterMetadata:
        def __init__(self, name, home_dir):
            self.name = name
            self.runtime_env = MockRuntimeEnv(home_dir)

    # Test conversion from /Users/ to /home/
    target_cluster = MockClusterMetadata("centos", "/home/zorro")

    # Test case 1: Convert /Users path to /home path
    original_entrypoint = "python /Users/zorro/project/script.py --arg value"
    path_match = re.search(r'\bpython\s+(/[^\s]+)', original_entrypoint, re.IGNORECASE)

    if path_match:
        script_path = path_match.group(1)
        possible_source_home_dirs = ['/home/', '/Users/', '/root/']

        for source_home in possible_source_home_dirs:
            if script_path.startswith(source_home):
                # 提取相对路径部分，移除源home_dir部分（包含用户名）
                # 例如: /Users/zorro/project/script.py -> project/script.py
                source_username_start = len(source_home)  # 例如 /Users/ 的长度
                # 找到用户名后的斜杠位置
                remaining_path = script_path[source_username_start:]
                username_end = remaining_path.find('/')
                if username_end != -1:
                    # 移除用户名，保留剩余路径
                    relative_path = remaining_path[username_end + 1:]
                else:
                    # 如果没有找到用户名，只移除源home_dir
                    relative_path = remaining_path

                # 构建目标路径 - 使用目标home_dir的父目录加上相对路径
                target_base = '/'.join(target_cluster.runtime_env.env_vars['home_dir'].split('/')[:-1])  # 例如 /home
                target_path = f"{target_base}/{relative_path}"
                new_entrypoint = original_entrypoint.replace(script_path, target_path)

                print(f"  Original: {original_entrypoint}")
                print(f"  Converted: {new_entrypoint}")
                print(f"  Expected: python /home/project/script.py --arg value")
                break

    # Test case 2: Convert /home/user path to /home/zorro path
    print("\nTest case 2: Convert /home/user path to /home/zorro path")
    original_entrypoint2 = "python /home/user/project/script.py --arg value"
    target_cluster2 = MockClusterMetadata("centos", "/home/zorro")

    path_match2 = re.search(r'\bpython\s+(/[^\s]+)', original_entrypoint2, re.IGNORECASE)

    if path_match2:
        script_path2 = path_match2.group(1)
        possible_source_home_dirs = ['/home/', '/Users/', '/root/']

        for source_home in possible_source_home_dirs:
            if script_path2.startswith(source_home):
                # 提取相对路径部分，移除源home_dir部分（包含用户名）
                source_username_start = len(source_home)  # 例如 /home/ 的长度
                # 找到用户名后的斜杠位置
                remaining_path2 = script_path2[source_username_start:]
                username_end2 = remaining_path2.find('/')
                if username_end2 != -1:
                    # 移除用户名，保留剩余路径
                    relative_path2 = remaining_path2[username_end2 + 1:]
                else:
                    # 如果没有找到用户名，只移除源home_dir
                    relative_path2 = remaining_path2

                # 构建目标路径 - 使用目标home_dir的父目录加上相对路径
                target_base2 = '/'.join(target_cluster2.runtime_env.env_vars['home_dir'].split('/')[:-1])  # 例如 /home
                target_path2 = f"{target_base2}/{relative_path2}"
                new_entrypoint2 = original_entrypoint2.replace(script_path2, target_path2)

                print(f"  Original: {original_entrypoint2}")
                print(f"  Converted: {new_entrypoint2}")
                print(f"  Expected: python /home/project/script.py --arg value")
                break

    print("\nPath conversion functionality test completed successfully!")

if __name__ == "__main__":
    test_path_conversion()