# 异常模式库

本目录包含常见异常调试模式和方法论。

## 模式列表

| 模式 | 描述 | 文件 |
|------|------|------|
| 调用链分析 | 沿堆栈跟踪分析每个接口的返回结果 | [call-chain-analysis.md](call-chain-analysis.md) |
| 参数丢失追踪 | 追踪参数在调用链中的传递 | [parameter-loss-tracing.md](parameter-loss-tracing.md) |
| 状态转换调试 | 调试状态机转换问题 | [state-transition-debugging.md](state-transition-debugging.md) |

## 使用方式

当 `debugger` 分析异常时，会参考这些模式来：
1. 确定分析策略
2. 选择合适的调试工具
3. 定位关键检查点
