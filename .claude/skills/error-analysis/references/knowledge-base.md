# 知识库引用指南

本文档说明如何在异常分析过程中引用项目的异常处理知识库。

## 知识库结构

```
.knowledge-base/
├── exceptions/                        # 异常案例库
│   ├── cluster-connection/            # 集群连接类异常
│   ├── resource-scheduling/           # 资源调度类异常
│   ├── concurrency/                   # 并发执行类异常
│   ├── configuration/                 # 配置传递类异常
│   ├── policy-engine/                 # 策略引擎类异常
│   └── circuit-breaker/               # 熔断器类异常
├── patterns/                          # 异常模式库
├── solutions/                         # 解决方案库
└── templates/                         # 文档模板
```

## 异常分类映射

| 异常类型 | 知识库目录 | 典型异常 |
|----------|------------|----------|
| 集群连接失败 | `cluster-connection/` | `ClusterConnectionError`、连接超时 |
| 资源调度问题 | `resource-scheduling/` | `NoHealthyClusterError`、资源阈值 |
| 并发执行问题 | `concurrency/` | 任务重复、队列状态不一致 |
| 配置传递问题 | `configuration/` | `ConfigurationError`、runtime_env |
| 策略引擎问题 | `policy-engine/` | `PolicyEvaluationError` |
| 熔断器问题 | `circuit-breaker/` | 熔断器状态转换、恢复超时 |

## 搜索相似案例

### 按异常类型搜索

```bash
# 搜索特定异常类型
grep -r "exception_type: ClusterConnectionError" .knowledge-base/exceptions/
```

### 按症状标签搜索

```bash
# 搜索包含特定标签的案例
grep -r "tags:.*连接" .knowledge-base/exceptions/
grep -r "tags:.*超时" .knowledge-base/exceptions/
grep -r "tags:.*并发" .knowledge-base/exceptions/
```

### 按模块搜索

```bash
# 搜索特定模块的异常案例
grep -r "module: dispatcher" .knowledge-base/exceptions/
grep -r "module: task_queue" .knowledge-base/exceptions/
```

## 使用知识库案例

### 分析流程

1. **识别异常类型** → 定位到对应的分类目录
2. **搜索症状标签** → 查找匹配的历史案例
3. **引用解决方案** → 参考历史案例的根因分析和解决方案

### 案例模板

每个案例包含以下部分：

```yaml
---
exception_type: [异常类型]
symptoms:
  - [症状1]
  - [症状2]
tags: [标签1, 标签2, 标签3]
verified: true/false
---

# 异常标题
## 典型错误信息
## 根本原因
## 解决方案
## 验证方法
```

### 引用已验证案例

优先引用 `verified: true` 的案例，因为这些案例已经过测试验证：

```
参考已验证案例：
- [案例文件路径](../../.knowledge-base/exceptions/cluster-connection/001-verified.md)
```

## 添加新案例

分析完成后，如果这是新类型的异常，考虑添加到知识库：

1. 复制模板：`[.knowledge-base/templates/case-template.md](../../.knowledge-base/templates/case-template.md)`
2. 填写异常信息
3. 保存到对应的分类目录
4. 更新该分类的 `index.md`

## 相关资源链接

- [知识库概述](../../.knowledge-base/README.md)
- [异常案例索引](../../.knowledge-base/exceptions/index.md)
- [案例模板](../../.knowledge-base/templates/case-template.md)
