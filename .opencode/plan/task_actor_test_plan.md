# 测试计划：submit_task 和 submit_actor 接口

## 问题分析

### 现状
- ✅ 有 `submit_job` 的测试（test_queue_job_fix.py, test_spec03_fix.py, test_spec04_fix.py）
- ❌ **缺少** `submit_task` 和 `submit_actor` 的测试
- ❌ **缺少** 并发场景的测试
- ❌ **缺少** 队列处理机制的验证测试

### 已修复的并发问题
1. **TaskDescription.is_processing** 标记防止重复执行
2. **及时从 queued_tasks 移除** 已处理的任务
3. **finally 块** 确保标记总是被清除

---

## 需要测试的场景

### 1. 并发保护测试（高优先级）

#### 测试 1.1: 单个任务并发执行保护
**目标**: 验证 `is_processing` 标记防止任务重复执行

**步骤**:
1. 提交一个任务 `task_id=A`
2. 模拟两个并发处理路径同时尝试处理任务A
3. 验证只有一个路径成功执行
4. 验证另一个路径被 `is_processing` 检查阻止

**预期结果**:
- 只执行一次任务
- 日志显示 "任务 X 已在处理中，跳过重复执行"
- 返回正确的结果

**关键断言**:
```python
assert result_count == 1  # 任务只执行一次
assert "已处理中，跳过重复执行" in logs
```

---

#### 测试 1.2: 多个任务并发处理
**目标**: 验证多个任务并发时各自独立保护

**步骤**:
1. 同时提交多个任务（A, B, C）
2. 触发队列重新评估
3. Worker Loop 同时运行
4. 验证每个任务只执行一次

**预期结果**:
- 每个任务执行一次
- 没有任务重复执行
- 没有任务被跳过

---

### 2. 队列处理测试（中优先级）

#### 测试 2.1: 任务进入队列机制
**目标**: 验证任务在资源不足时正确进入队列

**步骤**:
1. 模拟所有集群资源超过 70% 阈值
2. 提交一个任务
3. 验证任务进入队列（而非立即执行）
4. 验证 `_task_results[task_id] == task_id` （表示排队）

**预期结果**:
- 任务进入 `queued_tasks`
- 任务进入 `TaskQueue.global_queue`
- 返回 `(task_id, task_id)` 表示排队
- `get_task_status(task_id)` 返回 "QUEUED"

**关键断言**:
```python
assert task_id in scheduler.task_lifecycle_manager.queued_tasks
assert len(scheduler.task_lifecycle_manager.task_queue.global_queue) > 0
assert get_task_status(task_id) == "QUEUED"
```

---

#### 测试 2.2: 从队列取出并执行
**目标**: 验证任务从队列正确取出并执行

**步骤**:
1. 提交任务进入队列
2. 等待资源恢复
3. Worker Loop 从队列取出任务
4. 验证任务被正确调度
5. 验证任务从 `queued_tasks` 移除

**预期结果**:
- 任务从 `TaskQueue.global_queue` 移除
- 任务从 `queued_tasks` 移除
- 任务在目标集群执行
- 返回正确的执行结果

**关键断言**:
```python
assert task_id not in scheduler.task_lifecycle_manager.queued_tasks
assert task_id not in scheduler.task_lifecycle_manager.task_queue.global_queue_ids
assert result is not None and result != task_id  # 不是排队标记
```

---

### 3. 资源恢复测试（中优先级）

#### 测试 3.1: 资源恢复后重新评估
**目标**: 验证 15 秒周期性重新评估机制

**步骤**:
1. 提交任务 A、B、C（都进入队列）
2. 等待 16 秒（超过 15 秒评估周期）
3. 模拟一个集群资源恢复（使用率 < 70%）
4. 验证 `_re_evaluate_queued_tasks` 被触发
5. 验证至少一个任务被调度

**预期结果**:
- 重新评估日志显示 "重新评估 X 个排队任务的调度可能性"
- 至少一个任务被调度执行
- 剩余任务仍在队列中

**关键断言**:
```python
assert "重新评估" in logs
assert executed_count > 0
assert remaining_in_queue < initial_queue_size
```

---

#### 测试 3.2: Worker Loop 和 Re-evaluation 协作
**目标**: 验证两个处理路径不会冲突

**步骤**:
1. 提交任务 A、B 到队列
2. 在同一时间：
   - Worker Loop 尝试取出任务
   - Re-evaluation 触发
3. 验证最终状态一致

**预期结果**:
- 没有任务重复执行
- 没有任务丢失
- 最终 `queued_tasks` 状态正确

---

### 4. Actor 特定测试（高优先级）

#### 测试 4.1: Actor 并发创建保护
**目标**: 验证 Actor 创建不会被并发重复执行

**步骤**:
1. 提交 Actor 到队列
2. 模拟两个并发处理路径
3. 验证只有一个 Actor 被创建
4. 验证 `ActorHandle` 正确返回

**预期结果**:
- 只创建一个 Actor
- 只返回一个 `ActorHandle`
- 日志显示跳过重复执行

**关键断言**:
```python
assert actor_handle_count == 1
assert hasattr(actor_handle, '_actor_id')
assert "已处理中，跳过重复执行" in logs
```

---

#### 测试 4.2: Actor 队列和执行
**目标**: 验证 Actor 从队列正确创建

**步骤**:
1. 提交 Actor（资源紧张）
2. 验证 Actor 进入队列
3. 资源恢复后，Actor 被创建
4. 验证 `_task_results` 存储 `ActorHandle`

**预期结果**:
- Actor 排队时 `get_actor_status(actor_id)` 返回 "QUEUED"
- Actor 执行后 `_task_results[actor_id]` 是 `ActorHandle`
- Actor 正确返回到客户端

**关键断言**:
```python
assert get_actor_status(actor_id) == "QUEUED"  # 初始状态
assert isinstance(actor_handle, (ClientActorHandle, ActorHandle))  # 最终状态
assert hasattr(actor_handle, 'remote')  # 可调用远程方法
```

---

### 5. finally 块测试（中优先级）

#### 测试 5.1: 异常时标记清除
**目标**: 验证 finally 块确保 `is_processing` 标记总是被清除

**步骤**:
1. 模拟任务执行过程中抛出异常
2. 验证 finally 块执行
3. 验证 `is_processing` 被重置为 False
4. 提交相同任务，验证可以重新处理

**预期结果**:
- 异常被正确捕获和处理
- `is_processing` 被重置
- 相同任务可以重新提交并处理

**关键断言**:
```python
assert task_desc.is_processing == False  # 最终状态
assert not "已处理中，跳过重复执行" in retry_logs
```

---

## 测试文件结构

### 文件 1: `demo/test_task_queue_fix.py`
**测试内容**:
- Test 1.1: 单任务并发保护
- Test 1.2: 多任务并发处理
- Test 2.1: 任务进入队列
- Test 2.2: 从队列取出执行
- Test 5.1: finally 块异常清除

**Mock 需求**:
- 模拟 Ray `remote()` 执行
- 模拟资源快照（超过/低于阈值）
- 模拟并发触发

---

### 文件 2: `demo/test_actor_queue_fix.py`
**测试内容**:
- Test 1.1: Actor 并发创建保护
- Test 4.2: Actor 队列和执行
- Test 5.1: Actor 异常时标记清除

**Mock 需求**:
- 模拟 Actor 类和 `ActorHandle`
- 模拟集群资源状态

---

### 文件 3: `demo/test_concurrent_task_fix.py`
**测试内容**:
- Test 3.1: 资源恢复后重新评估
- Test 3.2: Worker Loop 和 Re-evaluation 协作

**Mock 需求**:
- 模拟时间流逝（15 秒评估周期）
- 模拟资源状态变化

---

## 实现策略

### 策略 1: 使用 Mock 和 Fixture
```python
import pytest
from unittest.mock import Mock, patch

@pytest.fixture
def mock_scheduler():
    """创建模拟调度器用于测试"""
    scheduler = Mock()
    scheduler.task_lifecycle_manager = Mock()
    return scheduler

@pytest.fixture
def mock_cluster_snapshots():
    """模拟集群资源快照"""
    return {
        'cluster1': ResourceSnapshot(
            cluster_name='cluster1',
            cluster_cpu_usage_percent=80.0,  # 超过阈值
            cluster_mem_usage_percent=85.0
        ),
        'cluster2': ResourceSnapshot(
            cluster_name='cluster2',
            cluster_cpu_usage_percent=60.0,  # 低于阈值
            cluster_mem_usage_percent=65.0
        )
    }
```

---

### 策略 2: 使用线程测试并发
```python
import threading

def test_concurrent_task_execution(mock_scheduler, mock_cluster_snapshots):
    """测试任务并发执行保护"""
    task = TaskDescription(
        task_id="test_task",
        func_or_class=lambda: "result",
        is_processing=False
    )

    results = []
    errors = []

    def process_task():
        try:
            result = scheduler._process_task(task, mock_cluster_snapshots, None)
            results.append(result)
        except Exception as e:
            errors.append(e)

    # 创建两个并发线程处理同一任务
    t1 = threading.Thread(target=process_task)
    t2 = threading.Thread(target=process_task)
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()

    # 验证只有一个成功执行
    success_count = sum(1 for r in results if r is not None)
    assert success_count == 1  # ✅ 只执行一次
    
    # 验证另一个被阻止
    skip_count = len(errors)
    assert "已处理中，跳过重复执行" in str(errors)
```

---

## 测试优先级

| 优先级 | 测试场景 | 文件 | 复杂度 |
|---------|----------|------|--------|
| 🔴 高 | 并发保护（Task） | test_task_queue_fix.py | 中 |
| 🔴 高 | 并发保护（Actor） | test_actor_queue_fix.py | 中 |
| 🟡 中 | 队列处理机制 | test_task_queue_fix.py | 低 |
| 🟡 中 | 资源恢复重评估 | test_concurrent_task_fix.py | 中 |
| 🟢 低 | finally 块异常清除 | test_task_queue_fix.py | 低 |

---

## 执行计划

1. **创建 `demo/test_task_queue_fix.py`**
   - 实现 5 个 Task 相关测试
   - 使用 Mock 和 threading 测试并发

2. **创建 `demo/test_actor_queue_fix.py`**
   - 实现 3 个 Actor 相关测试
   - 模拟 Actor Handle 行为

3. **创建 `demo/test_concurrent_task_fix.py`**
   - 实现 2 个并发协调测试
   - 模拟时间流逝和资源变化

4. **运行所有测试**
   ```bash
   pytest test/test_task_queue_fix.py -v
   pytest test/test_actor_queue_fix.py -v
   pytest test/test_concurrent_task_fix.py -v
   ```

5. **验证修复**
   - 所有测试通过
   - 日志正确显示并发保护生效
   - 没有任务重复执行

---

## 成功标准

- ✅ 所有测试通过（`pytest`）
- ✅ 日志显示 `is_processing` 保护生效
- ✅ 没有任务重复执行
- ✅ 队列状态一致性正确
- ✅ 异常处理和 finally 块正确工作

---

## 与现有测试对比

| 方面 | 现有 Job 测试 | 需要 Task/Actor 测试 |
|------|----------------|---------------------|
| 并发保护 | ❌ 未测试 | ✅ 需要测试 |
| 队列机制 | ✅ 已测试 | ✅ 需要测试 |
| 资源恢复 | ✅ 已测试 | ✅ 需要测试 |
| 映射关系 | ✅ 已测试 | N/A |
| finally 清除 | ❌ 未测试 | ✅ 需要测试 |
