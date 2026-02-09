---
paths: |
  - ray_multicluster_scheduler/**/*.py
  - ray_multicluster_scheduler/**/*.yaml
---

# Documentation Sync Rule

## Trigger
每当执行以下类型的代码变更时，必须同步更新文档：
- 功能新增（Feature Addition）
- 功能优化（Feature Optimization）

---

## Contextual Requirements

1. **位置邻近性（Proximity Rule）**
   文档必须保存在对应模块的目录中，例如：
   - `ray_multicluster_scheduler/scheduler/README.md`
   - `ray_multicluster_scheduler/executor/docs/api.md`

2. **同步更新（Strict Sync Rule）**
   严禁仅更新代码而不更新相关接口、类或方法的说明。
   任何公共 API、方法签名、参数、返回值、异常、配置项发生变化，文档必须同步修改。

---

## Rule Actions

1. **检测变更（Change Detection）**
   在提交前，必须识别并列出：
   - 受影响的公开接口（APIs）
   - 公共方法（Public Methods）
   - 数据结构（Data Models / Schemas）
   - 配置项（Config Fields）

2. **文档维护（Documentation Maintenance）**
   - **新增功能**：
     在模块目录下创建或追加 Markdown 文档，至少包含：
     - 接口定义
     - 参数说明（类型 / 是否必填 / 默认值 / 含义）
     - 返回值示例
     - 错误码 / 异常说明
   - **功能优化**：
     同步修正已有文档中的：
     - 行为逻辑描述
     - 参数变化
     - 性能说明或限制条件

3. **格式标准（Format Standard）**
   - 接口文档需遵循项目标准格式，例如：
     - Swagger / OpenAPI 风格
     - 或标准 Markdown 表格说明
   - 文档底部必须包含更新时间标记：

     ```md
     Last Updated: YYYY-MM-DD
     ```

4. **依赖关系说明（Module Dependency Declaration）【新增规则】**
   文档中必须**明确说明当前模块的上下游依赖关系**：

   - 🔼 上游模块（Inputs / Callers）
   - 🔽 下游模块（Outputs / Callees）

   至少包含以下信息：
   - 依赖模块名称与路径
   - 依赖类型：
     - API 调用
     - 数据结构依赖
     - 事件触发
     - 配置读取
   - 关键交互接口或数据格式示例

   推荐写法示例：

   ```md
   ### Module Dependencies

   #### Upstream (Inputs)
   - scheduler/api.py
     - 调用方法: `submit_job(job_config: dict)`
     - 输入数据: JobSpec v2

   #### Downstream (Outputs)
   - executor/worker_pool.py
     - 调用方法: `dispatch(task_bundle)`
     - 输出数据: TaskBundle

## Verification（提交前自检清单）

在提交任务前，必须回答以下问题：

* [ ] 我是否已在对应的模块文件夹中更新了 README 或相关 `.md` 文件？
* [ ] 文档中的参数类型和数量是否与最新代码完全一致？
* [ ] 是否已补充或修正模块的上下游依赖说明？
* [ ] 是否更新了 `Last Updated` 日期？

---

## Enforcement

如发现以下情况之一，视为违规提交：

* 代码新增功能但未新增或更新文档
* 公共接口发生变化但文档未同步
* 模块文档缺失上下游依赖说明

违规提交必须在合入前整改完成。
