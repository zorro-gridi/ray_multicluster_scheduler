---
exception_type: [异常类型，如 ClusterConnectionError]
symptoms:
  - [症状1]
  - [症状2]
module: [发生异常的模块，如 dispatcher.py]
related_files:
  - [相关代码文件路径1]
  - [相关代码文件路径2]
tags: [标签1, 标签2, 标签3]
created: YYYY-MM-DD
verified: false          # 是否已验证（测试通过）
verified_date:           # 验证通过日期（如已验证）
resolution_time:         # 解决耗时（如 30m, 2h）
status: [draft/resolved/verified]
---

# [异常标题]

## 异常类型
`SchedulerError` > `[具体异常子类]`

## 典型错误信息
```
[完整异常堆栈]
```

## 问题场景
[描述异常发生的场景、触发条件]

## 根本原因
[分析异常的根本原因]

## 调用链分析
[关键调用链路和参数传递]

## 解决方案

### 方案1: [方案名称]
- **描述**: [方案描述]
- **实现步骤**:
  1. [步骤1]
  2. [步骤2]
- **代码变更**:
  ```python
  # [代码变更示例]
  ```

### 方案2: [方案名称]
...

## 预防措施
[如何预防此类异常]

## 相关案例
- [链接到相关案例]

## 验证方法
[如何验证修复是否有效]
