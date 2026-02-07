---
paths: ray_multicluster_scheduler/**/*.py
---

# 异常处理规范

## 通用处理规范
- 所有异常必须显式打印异常堆栈信息
- 自定义异常层次继承自 `SchedulerError`
- 不得在不记录日志的情况下吞掉异常
