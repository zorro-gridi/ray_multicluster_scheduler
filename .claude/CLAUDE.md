# CLAUDE.md

此文件为 Claude Code (claude.ai/code) 在此代码库中工作时提供指导。

## 项目概述

Ray 多集群调度器是一个位于 Ray 内部调度器之上的上层调度框架。它管理多个异构 Ray 集群（CentOS x86_64 和 macOS ARM64）之间的任务，提供统一的资源管理、负载均衡和任务编排。

可以将其理解为"面向 Ray 集群的 Kubernetes 风格控制平面" - Ray 负责单集群执行，而本系统负责选择使用哪个 Ray 集群。

## 开发环境

- **Conda 环境**: 使用 `k8s` 虚拟环境
- **Python 版本**: 3.10.14
- **主要依赖**: `ray==2.30.0`

## 常用命令

```bash
# 开发模式安装（推荐）
pip install -e .

# 带开发依赖安装
pip install -e ".[dev]"

# 运行所有测试
pytest

# 运行单个测试文件
pytest path/to/test_file.py

# 运行单个测试函数
pytest path/to/test_file.py::test_function_name

# 带覆盖率报告
pytest --cov=ray_multicluster_scheduler

# 启动调度器
ray-multicluster-scheduler
# 或
python -m ray_multicluster_scheduler.main
```

## 系统架构

```
应用层 (app/client_api/)
    ↓
调度控制平面 (scheduler/)
    ↓
通用基础设施 (common/, control_plane/)
    ↓
Ray 数据平面 (Ray 集群)
```

### 核心组件

1. **应用层** (`app/client_api/`)
   - `submit_task.py` - Ray 远程函数提交
   - `submit_actor.py` - Ray Actor 提交
   - `submit_job.py` - 基于 JobSubmissionClient 的作业提交
   - `unified_scheduler.py` - 所有提交类型的统一接口

2. **调度核心** (`scheduler/`)
   - **生命周期管理器** (`lifecycle/task_lifecycle_manager.py`) - 核心协调器
   - **策略引擎** (`policy/policy_engine.py`) - 多因子调度决策
   - **队列管理** (`queue/task_queue.py`) - 全局和集群专属队列，支持去重
   - **分发器** (`scheduler_core/dispatcher.py`) - 在目标集群上执行任务
   - **集群监控器** (`monitor/cluster_monitor.py`) - 健康检查和资源监控
   - **连接管理** (`connection/`) - Ray Client 和 JobSubmissionClient 连接池

3. **通用基础设施** (`common/`)
   - 数据模型：TaskDescription, JobDescription, ClusterMetadata, ResourceSnapshot
   - 异常层次结构：SchedulerError 基类
   - 结构化日志
   - 熔断器实现容错

## 任务提交流程

1. 用户调用 `submit_job()` / `submit_task()` / `submit_actor()`
2. 任务/作业加入相应队列（全局或集群专属）
3. `TaskLifecycleManager` 工作循环处理队列
4. `PolicyEngine` 做出调度决策
5. `Dispatcher` 提交到目标集群
6. 收集并返回结果

## 关键调度规则

### 资源阈值
- CPU、GPU、内存默认 70% 阈值
- 当所有集群超过阈值时，任务进入队列等待

### 40秒规则
- 同一集群 40 秒内只能接收一个顶级任务
- 子任务不受此限制
- 防止快速提交导致的资源竞争

### 任务类型
- **Task** - `ray.remote()` 函数/Actor
- **Job** - JobSubmissionClient 提交的作业
- 两类任务有独立的队列和调度逻辑

### 调度优先级
1. 首选集群（如指定且可用）
2. 标签亲和匹配（架构特定）
3. 基于资源可用性的负载均衡
4. 轮询回退

## 集群配置

配置解析优先级：
1. 显式指定的 `config_file_path` 参数
2. `CLUSTER_CONFIG_FILE` 环境变量
3. `./clusters.yaml`（当前工作目录）
4. 包根目录的 `clusters.yaml`
5. `~/.ray_scheduler/clusters.yaml`
6. `/etc/ray_scheduler/clusters.yaml`
7. 默认回退配置

示例 `clusters.yaml`：
```yaml
clusters:
  - name: centos
    head_address: 192.168.5.7:32546
    dashboard: http://192.168.5.7:31591
    prefer: false
    weight: 1.0
    runtime_env:
      conda: ts
      env_vars:
        home_dir: /home/zorro
    tags: [linux, x86_64]
```

## 代码规范

### 日志格式
```
<Module>.<SubModule>... - <message>
```
- 关键数字信息使用 emoji 图标标注
- 字典/列表使用 pprint 友好输出
- 模块级日志记录器：`logger = get_logger(__name__)`

### 类型注解
- 所有公共函数都需要类型注解
- 使用 `typing` 模块：`Dict`, `List`, `Optional`, `Any`, `Callable`, `Union`

### 命名约定
- 类名：PascalCase（如 `PolicyEngine`, `TaskQueue`）
- 函数/变量：snake_case（如 `dispatch_task`）
- 常量：UPPER_CASE（如 `RESOURCE_THRESHOLD`）
- 私有方法：_snake_case

### 异常处理
- 所有异常必须打印堆栈跟踪
- 自定义异常层次继承自 `SchedulerError`
- 不得在不记录日志的情况下吞掉异常

### 并发编程
- 使用 `threading.Lock()` 和 `threading.Condition()` 保证线程安全
- 共享状态通过上下文管理器保护
- 在队列层面实现去重

### 文档规范
- Google 风格的文档字符串，包含 Args/Returns/Raises
- 复杂逻辑使用中文注释（解释 WHY，而非 WHAT）
- 禁止硬编码 - 使用数据类或配置文件

## 测试规范

- **单元测试**：Bug 修复、新功能、优化
- **集成测试**：模块集成、接口调用
- **演示文件**：`demo/` 目录中的复杂场景测试

### 测试异常分析
- 当接口返回空或抛出异常时，逐步分析堆栈跟踪
- 沿调用链检查每个接口的返回结果
- 找出根本原因，而不是简单地通过捕获异常来规避

## 重要设计模式

- UnifiedScheduler 使用单例模式
- 使用条件变量实现线程安全队列
- 基于策略的可插拔调度
- 连接的延迟初始化
- 面向集群感知操作的上下文管理
