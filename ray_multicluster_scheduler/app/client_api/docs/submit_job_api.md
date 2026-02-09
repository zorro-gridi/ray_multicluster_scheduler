# submit_job 接口文档

## 概述

`submit_job` 是 Ray 多集群调度器的客户端 API 接口，用于向可用的 Ray 集群提交作业。该接口基于 Ray 的 `JobSubmissionClient`，提供了一种跨集群调度和执行作业的简化方式。

### 主要功能
- 跨多个 Ray 集群自动调度作业
- 智能集群选择：根据资源可用性、集群偏好和健康状态
- 支持资源需求规格和标签系统
- 提供作业状态查询、停止和监控功能
- 集成键盘中断优雅关闭机制

## 快速开始

### 基本使用示例

```python
from ray_multicluster_scheduler.app.client_api.submit_job import submit_job, get_job_status, wait_for_all_jobs

# 初始化调度器环境（需要集群配置文件）
from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
task_lifecycle_manager = initialize_scheduler_environment(config_file_path="clusters.yaml")

# 提交作业
entrypoint = "python /absolute/path/to/script.py --arg1 value1 --arg2 value2"
submission_id = submit_job(
    entrypoint=entrypoint,
    job_id="my_job_001",
    preferred_cluster="gpu_cluster",
    resource_requirements={"CPU": 4, "GPU": 1, "memory": 16.0}
)

print(f"作业提交成功，Submission ID: {submission_id}")

# 等待作业完成
wait_for_all_jobs([submission_id], check_interval=10)

# 查询作业状态
status = get_job_status(submission_id, "gpu_cluster")
print(f"作业状态: {status}")
```

### 使用装饰器自动处理键盘中断

```python
from ray_multicluster_scheduler.app.client_api.submit_job import graceful_shutdown_on_keyboard_interrupt

@graceful_shutdown_on_keyboard_interrupt
def my_workflow():
    # 提交多个作业
    job1 = submit_job("python /path/to/job1.py", job_id="job1")
    job2 = submit_job("python /path/to/job2.py", job_id="job2")

    # 等待作业完成
    wait_for_all_jobs([job1, job2])
    print("所有作业完成")

if __name__ == "__main__":
    # 初始化调度器
    from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
    initialize_scheduler_environment()

    # 运行工作流（Ctrl+C 会自动停止远程作业）
    my_workflow()
```

## API 详细说明

### submit_job 函数

```python
def submit_job(
    entrypoint: str,
    runtime_env: Optional[Dict] = None,
    job_id: Optional[str] = None,
    metadata: Optional[Dict] = None,
    submission_id: Optional[str] = None,
    preferred_cluster: Optional[str] = None,
    resource_requirements: Optional[Dict[str, float]] = None,
    tags: Optional[list] = None
) -> str
```

#### 参数说明

**必选参数：**

1. **entrypoint** (`str`)
   - 作业执行的命令字符串
   - **必须使用绝对路径** 指定脚本位置
   - 示例：`"python /home/user/scripts/train.py --epochs 100 --batch_size 32"`
   - 支持的命令格式：
     - `python <绝对路径脚本> <参数>`
     - 其他可执行命令 `<可执行文件绝对路径> <参数>`

**可选参数：**

2. **runtime_env** (`Dict`, 可选)
   - 作业运行时环境配置
   - 如果未提供，调度器将使用目标集群的默认配置
   - 示例：
     ```python
     {
         "pip": ["numpy>=1.21.0", "pandas>=1.3.0"],
         "conda": {"dependencies": ["python=3.9"]},
         "env_vars": {"OMP_NUM_THREADS": "1"}
     }
     ```

3. **job_id** (`str`, 可选)
   - 作业的唯一标识符
   - 如果未提供，系统会自动生成：`"job_{uuid}"`

4. **metadata** (`Dict`, 可选)
   - 作业的元数据信息
   - 示例：`{"user": "alice", "project": "ml_training", "priority": "high"}`

5. **submission_id** (`str`, 可选)
   - 提交 ID，用于跟踪作业提交
   - 如果未提供，系统会自动生成：`"sub_{uuid}"`

6. **preferred_cluster** (`str`, 可选)
   - 首选的集群名称
   - 如果指定集群不可用，调度器会自动回退到其他可用集群
   - 示例：`"gpu_cluster"`, `"cpu_cluster"`

7. **resource_requirements** (`Dict[str, float]`, 可选)
   - 作业的资源需求规格
   - 支持的资源类型：
     - `"CPU"`: CPU 核心数（浮点数）
     - `"GPU"`: GPU 数量（整数或浮点数）
     - `"memory"`: 内存需求（GiB，浮点数）
   - 示例：`{"CPU": 8.0, "GPU": 2.0, "memory": 32.0}`

8. **tags** (`list`, 可选)
   - 作业标签列表，用于分类和过滤
   - 示例：`["training", "gpu_required", "high_priority"]`

#### 返回值
- **类型**: `str`
- **说明**: 作业的提交 ID (submission_id)
- **用途**: 用于后续的作业状态查询、停止操作

#### 异常
- `RuntimeError`: 调度器未初始化时抛出
- `ValueError`: entrypoint 包含相对路径时抛出
- 其他异常：网络连接、资源不足等问题

### 其他相关函数

#### get_job_status

```python
def get_job_status(submission_id: str, cluster_name: str) -> Any
```

查询作业状态。

**参数：**
- `submission_id`: 提交时返回的 submission_id
- `cluster_name`: 作业所在的集群名称

**返回值：**
- Ray Job 状态字符串：
  - `"PENDING"`: 等待中
  - `"RUNNING"`: 运行中
  - `"SUCCEEDED"`: 成功完成
  - `"FAILED"`: 执行失败
  - `"STOPPED"`: 被停止
  - `"UNKNOWN"`: 状态未知

#### stop_job

```python
def stop_job(submission_id: str, cluster_name: str) -> bool
```

停止正在运行的作业。

**参数：**
- `submission_id`: 要停止的作业 submission_id
- `cluster_name`: 作业所在的集群名称

**返回值：**
- `True`: 停止成功
- `False`: 停止失败

#### wait_for_all_jobs

```python
def wait_for_all_jobs(
    submission_ids: list,
    check_interval: int = 5,
    timeout: Optional[int] = None
) -> bool
```

轮询等待所有作业完成。

**参数：**
- `submission_ids`: 要等待的 submission_id 列表
- `check_interval`: 检查间隔（秒），默认 5 秒
- `timeout`: 超时时间（秒），None 表示无超时

**返回值：**
- `True`: 所有作业成功完成
- 异常：
  - `RuntimeError`: 任一作业失败时抛出
  - `KeyboardInterrupt`: 用户中断操作时抛出
  - `TimeoutError`: 等待超时时抛出

#### get_job_info

```python
def get_job_info(submission_id: str, cluster_name: str) -> Any
```

获取作业的详细信息。

**参数：**
- `submission_id`: 作业 submission_id
- `cluster_name`: 作业所在的集群名称

**返回值：**
- 作业详细信息，包括状态、日志等

### 装饰器

#### graceful_shutdown_on_keyboard_interrupt

```python
@graceful_shutdown_on_keyboard_interrupt
def user_function(*args, **kwargs):
    # 用户代码
```

为函数添加键盘中断优雅关闭功能。

**功能：**
- 当用户按 Ctrl+C 时，自动停止该函数中提交的所有远程作业
- 防止作业在集群中继续运行而浪费资源

## 高级用法

### 批量提交作业

```python
# 批量提交多个作业
jobs = []
for i in range(5):
    entrypoint = f"python /path/to/train.py --job_id job_{i}"
    submission_id = submit_job(
        entrypoint=entrypoint,
        job_id=f"batch_job_{i}",
        resource_requirements={"CPU": 2, "memory": 8.0},
        tags=["batch_training", f"iteration_{i}"]
    )
    jobs.append(submission_id)
    print(f"提交作业 {i}: {submission_id}")

# 等待所有作业完成
try:
    wait_for_all_jobs(jobs, check_interval=10, timeout=3600)
    print("所有批量作业完成")
except RuntimeError as e:
    print(f"部分作业失败: {e}")
```

### 自定义运行时环境

```python
# 为特定作业定制运行时环境
custom_env = {
    "pip": [
        "torch==2.0.0",
        "torchvision==0.15.0",
        "transformers==4.30.0"
    ],
    "env_vars": {
        "CUDA_VISIBLE_DEVICES": "0,1",
        "PYTHONPATH": "/home/user/code:$PYTHONPATH"
    },
    "working_dir": "/home/user/project",
    "excludes": ["*.log", "tmp/*"]
}

submission_id = submit_job(
    entrypoint="python /home/user/project/main.py",
    runtime_env=custom_env,
    job_id="torch_training",
    preferred_cluster="gpu_cluster",
    resource_requirements={"CPU": 16, "GPU": 2, "memory": 64.0}
)
```

### 作业依赖和顺序执行

```python
# 实现作业间的依赖关系
preprocess_id = submit_job(
    entrypoint="python /path/to/preprocess.py --input data.csv",
    job_id="preprocess",
    tags=["preprocessing"]
)

# 等待预处理完成
wait_for_all_jobs([preprocess_id])

# 提交训练作业
train_id = submit_job(
    entrypoint="python /path/to/train.py --model bert",
    job_id="train",
    metadata={"depends_on": preprocess_id},
    tags=["training"]
)

# 等待训练完成
wait_for_all_jobs([train_id])

# 提交评估作业
eval_id = submit_job(
    entrypoint="python /path/to/evaluate.py --model output/model.pt",
    job_id="evaluate",
    metadata={"depends_on": train_id},
    tags=["evaluation"]
)
```

## 调度策略说明

### 集群选择逻辑

1. **首选集群优先**：如果指定了 `preferred_cluster`，调度器会优先尝试该集群
2. **资源匹配**：调度器会检查集群是否满足作业的资源需求
3. **负载均衡**：在没有首选集群时，调度器会选择资源最充足的集群
4. **健康状态**：考虑集群的健康评分，避免使用不健康的集群

### 运行时环境处理

- 如果未指定 `runtime_env`，调度器会使用目标集群的默认配置
- 如果指定了 `runtime_env`，调度器会在调度决策后，将作业的运行时环境与集群配置合并
- 确保作业在目标集群上使用正确的依赖和环境变量

## 错误处理

### 常见错误及解决方法

1. **entrypoint 路径错误**
   ```python
   # 错误：使用相对路径
   submit_job("python script.py")  # ValueError

   # 正确：使用绝对路径
   submit_job("python /home/user/script.py")
   ```

2. **调度器未初始化**
   ```python
   # 错误：未初始化调度器就提交作业
   submit_job("python /path/script.py")  # RuntimeError

   # 正确：先初始化
   from ray_multicluster_scheduler.app.client_api.unified_scheduler import initialize_scheduler_environment
   initialize_scheduler_environment()
   submit_job("python /path/script.py")
   ```

3. **集群不可用**
   ```python
   # 指定不存在的集群
   try:
       submit_job("python /path/script.py", preferred_cluster="non_existent_cluster")
   except Exception as e:
       print(f"集群不可用: {e}")
       # 调度器会自动尝试其他可用集群
   ```

4. **资源不足**
   ```python
   # 请求超出集群可用资源的作业
   try:
       submit_job("python /path/script.py", resource_requirements={"CPU": 1000})
   except Exception as e:
       print(f"资源不足: {e}")
       # 考虑减少资源需求或等待集群资源释放
   ```

### 调试建议

1. **启用详细日志**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **检查作业日志**
   ```python
   job_info = get_job_info(submission_id, cluster_name)
   if job_info and 'logs' in job_info:
       print(f"作业日志:\n{job_info['logs']}")
   ```

3. **监控调度过程**
   ```python
   # 在提交前查看集群状态
   from ray_multicluster_scheduler.app.client_api.unified_scheduler import get_unified_scheduler
   scheduler = get_unified_scheduler()
   clusters = scheduler.list_clusters()
   print(f"可用集群: {clusters}")
   ```

## 性能注意事项

### 最佳实践

1. **批量提交**：对于大量小作业，使用批量提交减少调度开销
2. **合理设置检查间隔**：根据作业时长设置合适的 `check_interval`
3. **使用标签系统**：合理使用标签便于作业管理和过滤
4. **资源需求优化**：准确指定资源需求，避免过度分配
5. **超时设置**：为长时间作业设置合理的超时时间

### 资源管理

- 提交作业前检查集群资源状态
- 使用 `resource_requirements` 准确描述需求
- 考虑作业优先级，为高优先级作业预留资源

## 配置要求

### 集群配置文件

调度器需要集群配置文件来初始化。配置文件示例：

```yaml
# clusters.yaml
clusters:
  cpu_cluster:
    head_address: "123.456.789.0:10001"
    runtime_env:
      pip:
        - "numpy"
        - "pandas"
      env_vars:
        - "PYTHONPATH=/opt/code"
    prefer: true
    weight: 1.0
    tags: ["cpu", "stable"]

  gpu_cluster:
    head_address: "123.456.789.1:10002"
    runtime_env:
      pip:
        - "torch"
        - "torchvision"
      conda:
        dependencies: ["cudatoolkit=11.7"]
    prefer: false
    weight: 0.8
    tags: ["gpu", "experimental"]
```

### 初始化流程

```python
# 1. 指定配置文件初始化
initialize_scheduler_environment(config_file_path="clusters.yaml")

# 2. 或不指定配置文件（使用默认位置）
initialize_scheduler_environment()

# 3. 懒初始化（首次调用 submit_job 时自动初始化）
# 需要确保集群配置文件在默认位置
```

## 版本兼容性

### Ray 版本要求
- 支持 Ray 2.0+ 版本
- 需要 `ray[job-submission]` 包

### Python 版本要求
- Python 3.8+
- 支持异步操作

## 安全注意事项

1. **认证与授权**：确保集群配置中的访问权限正确
2. **路径安全**：只允许绝对路径，防止路径遍历攻击
3. **环境变量**：谨慎设置环境变量，避免暴露敏感信息
4. **资源限制**：合理设置资源配额，防止资源耗尽攻击

## 故障排除

### 常见问题

**Q: 作业提交后立即失败**
- 检查 entrypoint 路径是否正确
- 检查目标集群是否可访问
- 查看作业日志获取详细错误信息

**Q: 作业长时间处于 PENDING 状态**
- 检查集群资源是否充足
- 检查是否有其他作业占用资源
- 考虑调整资源需求或选择其他集群

**Q: 无法连接到调度器**
- 检查集群配置文件是否正确
- 检查网络连接是否正常
- 确认 Ray 集群 head 节点是否运行

**Q: 键盘中断后作业仍在运行**
- 确保使用了 `graceful_shutdown_on_keyboard_interrupt` 装饰器
- 检查信号处理器是否正确注册
- 手动调用 `stop_job` 停止作业

### 获取帮助

1. 查看详细日志：`logging.basicConfig(level=logging.DEBUG)`
2. 检查集群状态：`get_unified_scheduler().list_clusters()`
3. 联系系统管理员获取集群配置信息