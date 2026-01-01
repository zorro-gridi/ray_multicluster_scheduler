import time
import sys
import ray

@ray.remote
def long_running_task(task_id, duration=120):
    """模拟长时间运行的任务"""
    print(f"Long running task {task_id} started, will run for {duration} seconds")
    for i in range(duration):
        print(f"Task {task_id} - Progress: {i+1}/{duration}")
        time.sleep(1)
    print(f"Task {task_id} completed")
    return f"Task {task_id} result"

def main():
    print("Starting long running Ray job...")
    print("This job will run for 120 seconds and can be interrupted with Ctrl+C")
    
    # 初始化 Ray
    if not ray.is_initialized():
        ray.init()

    import os
    task_id = os.environ.get("RAY_TASK_ID", "test_task")
    duration = int(os.environ.get("TASK_DURATION", "120"))
    
    # 提交一个长时间运行的任务
    task_ref = long_running_task.remote(task_id, duration)

    try:
        result = ray.get(task_ref)
        print(f"Job completed with result: {result}")
    except KeyboardInterrupt:
        print("Job interrupted by user")
        # 清理资源
        ray.shutdown()
        sys.exit(1)

    # 清理资源
    ray.shutdown()
    print("Job finished successfully")

if __name__ == "__main__":
    main()
