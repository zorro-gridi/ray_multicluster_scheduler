# 异常分析输出格式

本文档定义异常分析完成后的标准输出格式。

## 标准报告模板

```markdown
## 异常分析报告

### 异常摘要
- **异常类型**: `ExceptionType`
- **发生位置**: `module.py:line`
- **错误消息**: [完整错误消息]

### 调用链分析
[关键调用链路，标明每层的输入/输出]

### 根本原因
[确定的根因解释]

### 解决方案
1. [方案描述]
2. [代码修改建议]
3. [验证方法]

### 相关资源
- 相关文件: [文件列表]
- 相关案例: [知识库链接]
```

## 各部分详细说明

### 异常摘要

简洁描述异常的基本信息，包括：
- **异常类型**: 完整的异常类名（如 `TypeError`）
- **发生位置**: 文件路径和行号（如 `scheduler/dispatcher.py:142`）
- **错误消息**: 完整的错误消息文本

### 调用链分析

展示从入口点到异常点的关键调用路径：

```markdown
### 调用链分析

```
submit_job() [task_id=123]
  ↓
TaskLifecycleManager.dispatch_task()
  ↓
Dispatcher.submit_to_cluster() [cluster=centos]
  ↓ ❌ ConnectionError: Connection refused
```

**参数传递**:
- `submit_job` 接收 `task_id=123`
- `dispatch_task` 选择目标集群 `centos`
- `submit_to_cluster` 尝试连接时失败
```

### 根本原因

清晰解释问题的根本原因，包括：

1. **直接原因**: 立即导致异常的操作
2. **深层原因**: 为什么直接原因会发生
3. **系统性问题**: 是否存在设计或架构层面的问题

```markdown
### 根本原因

**直接原因**: 尝试连接到 `centos:32546` 时连接被拒绝

**深层原因**: 端口配置错误，实际 Ray head 服务运行在 `32548` 端口

**系统性问题**: 集群配置文件 `clusters.yaml` 中的端口硬编码，缺少配置验证机制
```

### 解决方案

提供可操作的解决方案，包括：

1. **问题描述**: 需要修复什么
2. **代码变更**: 具体的代码修改建议
3. **验证方法**: 如何验证修复有效

```markdown
### 解决方案

**1. 修正端口配置**
- 修改 `clusters.yaml` 中 centos 集群的 `head_address` 端口为 `32548`

**2. 添加配置验证**
```python
# 在 scheduler/cluster_monitor.py 中添加端口验证
def validate_cluster_config(config: ClusterMetadata):
    """验证集群配置的连通性"""
    try:
        socket.create_connection((host, port), timeout=5)
        return True
    except OSError:
        return False
```

**3. 验证方法**
```bash
# 运行连接测试
python -m scheduler.test_cluster_connection centos
```
```

### 相关资源

列出相关的文件和知识库链接：

```markdown
### 相关资源

**相关文件**:
- `ray_multicluster_scheduler/scheduler/dispatcher.py:142`
- `ray_multicluster_scheduler/control_plane/config/cluster_loader.py:45`

**相关案例**:
- [集群连接超时案例](../../.knowledge-base/exceptions/cluster-connection/001-timeout.md)
- [端口配置错误案例](../../.knowledge-base/exceptions/cluster-connection/002-port-config.md)
```

## 简化输出格式（用于快速分析）

对于简单的异常，可以使用简化格式：

```markdown
## 快速分析

**问题**: `TypeError: 'NoneType' object is not subscriptable`
**位置**: `task_queue.py:87`

**根因**: `get_cluster_queue()` 返回 `None` 而不是空字典

**修复**:
```python
# 修改前
queue = self.get_cluster_queue(cluster)
return queue[task_id]

# 修改后
queue = self.get_cluster_queue(cluster) or {}
return queue.get(task_id)
```
```
