# 异常案例库索引

## 按异常类型浏览

| 分类 | 说明 | 案例数量 |
|------|------|----------|
| [cluster-connection](cluster-connection/) | 集群连接类异常 | 0 |
| [resource-scheduling](resource-scheduling/) | 资源调度类异常 | 0 |
| [concurrency](concurrency/) | 并发执行类异常 | 0 |
| [configuration](configuration/) | 配置传递类异常 | 0 |
| [policy-engine](policy-engine/) | 策略引擎类异常 | 0 |
| [circuit-breaker](circuit-breaker/) | 熔断器类异常 | 0 |

## 快速检索

### 按症状标签搜索

使用以下命令在知识库中搜索症状标签：

```bash
# 搜索包含特定标签的案例
grep -r "tags:.*连接" .knowledge-base/exceptions/
```

### 按异常类型搜索

```bash
# 搜索特定异常类型的案例
grep -r "exception_type: ClusterConnectionError" .knowledge-base/exceptions/
```

## 添加新案例

1. 复制 [模板文件](../templates/case-template.md)
2. 填写异常信息
3. 保存到对应的异常分类目录
4. 更新分类索引文件
