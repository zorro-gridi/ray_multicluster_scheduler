"""
测试 Job 自动清理功能

这个脚本演示了新的 Job 包装机制如何确保作业正确退出。
"""

import ray

# 模拟一个会使用 ray.init() 但忘记调用 ray.shutdown() 的用户脚本
def main():
    print("=" * 60)
    print("测试 Job：模拟用户忘记调用 ray.shutdown()")
    print("=" * 60)

    # 用户脚本中调用了 ray.init()
    ray.init()
    print("✅ Ray initialized")

    # 执行一些任务
    @ray.remote
    def simple_task(x):
        import time
        time.sleep(1)
        return x * 2

    result = ray.get(simple_task.remote(5))
    print(f"✅ Task result: {result}")

    print("✅ Job 完成业务逻辑")

    # ❌ 用户忘记调用 ray.shutdown()
    # 但是调度器的包装器会自动清理
    print("⚠️  用户忘记调用 ray.shutdown()")
    print("🔧 调度器包装器将自动清理...")

if __name__ == "__main__":
    main()
