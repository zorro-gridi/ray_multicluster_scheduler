# 异常处理知识库

## 概述

本知识库用于维护 Ray 多集群调度器项目的异常处理操作知识。当项目运行发生异常，经过异常分析并成功修复之后，将异常问题描述、问题根因和针对该问题的解决方案作为异常处理案例知识库持久化保存，给后续出现的同类问题提供问题分析的思路和操作指引。

## 目录结构

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
└── templates/                         # 文档模板
```

## 异常分类

### 1. 集群连接类 (cluster-connection)
- `ClusterConnectionError` - 集群连接失败
- JobSubmissionClient 创建失败
- Ray Client 连接超时
- 端口配置错误

### 2. 资源调度类 (resource-scheduling)
- `NoHealthyClusterError` - 无健康集群可用
- 所有集群资源超过阈值
- 任务进入队列但无法调度

### 3. 并发执行类 (concurrency)
- 任务重复执行
- 队列状态不一致
- 缺少并发标记

### 4. 配置传递类 (configuration)
- `ConfigurationError` - 配置错误
- runtime_env 参数传递错误
- 跨集群配置不匹配

### 5. 策略引擎类 (policy-engine)
- `PolicyEvaluationError` - 策略评估失败
- 调度决策错误

### 6. 熔断器类 (circuit-breaker)
- 熔断器状态转换
- 恢复超时

## 使用方式

### 异常处理闭环工作流

```
1. 异常发生
   ↓
2. debugger 子代理分析
   - 搜索知识库找相似案例
   - 输出根因分析和解决方案
   ↓
3. 实施修复（主对话）
   - 根据建议修改代码
   ↓
4. 测试验证（主对话或 test-runner 子代理）
   - 运行测试验证修复有效性
   ↓
5. 用户验收确认
   - 用户确认问题已解决
   ↓
6. Stop Hook 自动触发知识捕获
   - 自动检测测试结果
   - 询问是否保存为已验证案例
   - 支持保存为 verified 或 draft 状态
   ↓
7. 知识积累
   - 新案例添加到知识库
   - 供后续异常分析参考
```

### 查找相似案例

当遇到异常时，`debugger` 子代理会自动：
1. 根据异常类型定位到对应分类目录
2. 搜索症状标签匹配的案例
3. 引用历史案例的根因分析和解决方案

### 添加新案例

**方式一：自动化捕获（推荐）**
- 完成异常修复和验证后，Stop Hook 会自动提示保存
- 支持混合模式：自动检测测试结果 + 用户最终确认
- 自动标记验证状态（verified/draft）

**方式二：手动创建**
- 使用 `.knowledge-base/templates/case-template.md` 模板
- 填写异常信息、根因、解决方案
- 更新对应分类的 index.md

## 索引

- [异常案例总览](exceptions/index.md)
- [异常模式库](patterns/index.md)
- [解决方案库](solutions/index.md)
