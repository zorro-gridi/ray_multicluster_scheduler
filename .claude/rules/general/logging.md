---
paths: ray_multicluster_scheduler/**/*.py
---

# 日志管理规范

## 日志打印格式
- 控制台信息打印格式: `<Module>.<SubModule>... - <message>`
- 关键数字信息使用适宜的 emoji 图标标注
- 字典、列表等数据类型，使用 pprint 友好输出

## 日志记录器
- 使用模块级日志记录器：`logger = get_logger(__name__)`
