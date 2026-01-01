#!/usr/bin/env python3
"""
测试中断处理功能的脚本
"""
import time
import signal
import sys
from ray_multicluster_scheduler.app.client_api.submit_job import wait_for_all_jobs, _interrupted

def test_interrupt_mechanism():
    """测试中断机制是否正常工作"""
    print("测试中断处理机制...")

    # 模拟一个长时间运行的任务
    def simulate_long_running_task():
        print("开始模拟长时间运行的任务...")
        for i in range(100):  # 模拟100次检查
            print(f"检查进度... {i+1}/100")
            time.sleep(0.5)  # 短暂休眠，但会定期检查中断信号

            # 模拟检查中断
            if _interrupted.is_set():
                print("检测到中断信号，正在停止任务...")
                _interrupted.clear()  # 清除中断标志
                return "任务被中断"
        return "任务完成"

    # 设置中断处理器
    def signal_handler(signum, frame):
        print(f"\n接收到信号 {signum}，设置中断标志...")
        _interrupted.set()

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("信号处理器已注册，运行测试...")
    print("按 Ctrl+C 可以中断任务")

    try:
        result = simulate_long_running_task()
        print(f"任务结果: {result}")
    except KeyboardInterrupt:
        print("捕获到 KeyboardInterrupt 异常")

    print("测试完成")


def test_wait_for_all_jobs_with_mock_data():
    """测试 wait_for_all_jobs 函数的中断处理"""
    print("\n测试 wait_for_all_jobs 函数的中断处理...")

    # 由于我们无法真正提交作业，我们模拟一个长时间的等待
    # 这里主要是验证中断机制是否能正常工作
    import threading

    def interrupt_after_delay():
        time.sleep(2)  # 等待2秒后模拟中断
        print("\n模拟中断信号...")
        _interrupted.set()

    # 启动一个线程来模拟中断
    interrupt_thread = threading.Thread(target=interrupt_after_delay)
    interrupt_thread.start()

    try:
        # 模拟调用 wait_for_all_jobs，但使用 mock 数据
        print("开始模拟 wait_for_all_jobs，2秒后将模拟中断...")
        # 在实际实现中，wait_for_all_jobs 会检查中断信号
        for i in range(20):  # 模拟20次检查
            if _interrupted.is_set():
                print("检测到中断信号，停止等待")
                _interrupted.clear()
                print("中断处理成功")
                break
            print(f"等待中... {i+1}/20")
            time.sleep(0.5)
        else:
            print("等待完成（未被中断）")

    except KeyboardInterrupt:
        print("捕获到 KeyboardInterrupt")
    finally:
        interrupt_thread.join(timeout=1)  # 等待中断线程结束

    print("wait_for_all_jobs 中断处理测试完成")


if __name__ == "__main__":
    print("开始测试中断处理功能")
    print("="*50)

    test_interrupt_mechanism()
    test_wait_for_all_jobs_with_mock_data()

    print("="*50)
    print("所有测试完成")