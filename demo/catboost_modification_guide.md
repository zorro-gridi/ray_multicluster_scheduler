# CatBoost Index Prediction 程序修改指南

为了使原始的 CatBoost Index Prediction 程序能够在我们的 Ray 多集群调度框架上运行，需要对原始程序进行以下修改：

## 1. 移除 Ray 初始化相关代码

### 需要移除的代码：

```python
# %% 移除以下代码段
import os
os.environ["RAY_DEFAULT_PYTHON_VERSION_MATCH_LEVEL"] = "minor"

import ray

# ...

import ray
from ray.util import ActorPool
```

### 原因：
- 我们的调度框架会统一管理 Ray 集群的连接和初始化
- 多个 ray.init() 调用会导致冲突
- 调度器会自动处理集群连接和任务分发

## 2. 结构化主逻辑为可调用函数

### 需要修改的部分：

原始程序的全局作用域代码需要封装到函数中：

```python
# 原始代码结构类似：
# %%
mysql_db_client = DB_Client(con_type='mysql_centos')
# ... 其他全局代码 ...

# %%
parser = argparse.ArgumentParser()
# ... 解析参数 ...

# %%
@ray.remote
class Train:
    # ... 类定义 ...

def train_task(idx_en, data_arg, reg_model_name):
    # ... 函数定义 ...

def run():
    # ... 主运行逻辑 ...

if __name__ == '__main__':
    run()
```

### 修改建议：

将全局代码封装到一个主函数中：

```python
def main_catboost_training():
    # %% 将 mysql_db_client 初始化移到函数内部
    mysql_db_client = DB_Client(con_type='mysql_centos')

    # ... 其他全局代码 ...

    # %% 参数解析也移到函数内部
    parser = argparse.ArgumentParser()
    # ... 参数解析逻辑 ...

    # %% 主运行逻辑
    def train_task(idx_en, data_arg, reg_model_name):
        # ... 函数定义 ...

    def run():
        # ... 主运行逻辑 ...

    run()

if __name__ == '__main__':
    main_catboost_training()
```

## 3. 移除或修改 Ray 特定的装饰器和调用

### 需要修改的代码：

```python
# 修改 @ray.remote 装饰器的使用
# 原始代码：
@ray.remote
class Train:
    def train(self, indexname_en, X_seq_len=15, y_seq_len=15, y_threshold=0.06, reg_model_name=None):
        # ... 方法实现 ...

# 修改后的代码：
class Train:
    def train(self, indexname_en, X_seq_len=15, y_seq_len=15, y_threshold=0.06, reg_model_name=None):
        # ... 方法实现保持不变 ...
```

### 修改 train_task 函数中的 Ray 调用：

```python
# 原始代码：
def train_task(idx_en, data_arg, reg_model_name):
    '''
    Desc:
        完整的单个训练任务处理
    '''
    logging.warning(f'🖐️ 重新训练 {idx_en} 模型, 参数: {data_arg}')
    # 执行实际训练任务, remote 返回的result是rayRefObj的id
    train_actor = Train.remote()
    result = train_actor.train.remote(idx_en, reg_model_name=reg_model_name, **data_arg)
    # 返回一个简单可哈希的标识（这里使用任务ID字符串）
    return result

# 修改后的代码：
def train_task(idx_en, data_arg, reg_model_name):
    '''
    Desc:
        完整的单个训练任务处理
    '''
    logging.warning(f'🖐️ 重新训练 {idx_en} 模型, 参数: {data_arg}')
    # 执行实际训练任务
    train_instance = Train()
    # 直接调用方法而不是使用 .remote()
    result = train_instance.train(idx_en, reg_model_name=reg_model_name, **data_arg)
    return result
```

## 4. 修改主循环中的 Ray 特定调用

### 需要修改的代码：

```python
# 原始代码中的 Ray 特定部分：
while True:
    # 检查已完成任务
    ready_ids, _ = ray.wait(list(active_tasks.keys()), num_returns=len(active_tasks), timeout=1.0)
    for task_id in ready_ids:
        idx_en = active_tasks.pop(task_id)
        task_result = ray.get(task_id)  # 获取结果
        # ... 其他处理 ...

# 修改建议：
# 使用传统的并发控制方式或者适配我们调度框架的任务管理方式
```

## 5. 适配我们调度框架的资源需求

在通过我们的调度框架提交任务时，可以通过 `submit_task` 函数的 `resource_requirements` 参数指定资源需求：

```python
# 在 submit_catboost_job.py 中：
submit_task(
    func=main_catboost_training,
    args=(),
    kwargs={},
    resource_requirements={
        "CPU": 4,
        "memory": 8 * 1024 * 1024 * 1024,  # 8GB
        "GPU": 1  # 如果需要 GPU
    },
    tags=["ml", "catboost", "training"],
    name="catboost_index_prediction"
)
```

## 6. 环境变量和路径处理

确保环境变量和路径处理兼容我们的调度框架：

```python
# 原始代码：
home_dir = os.environ['home_dir']
env_path = Path(home_dir) / 'project/pycharm/Fund'
sys.path.append(env_path.as_posix())
os.chdir(env_path.as_posix())

# 可能需要修改为：
home_dir = os.environ.get('home_dir', '/default/path')
env_path = Path(home_dir) / 'project/pycharm/Fund'
# 确保路径存在并且可以访问
if env_path.exists():
    sys.path.append(env_path.as_posix())
    os.chdir(env_path.as_posix())
else:
    # 处理路径不存在的情况
    logging.warning(f"Environment path {env_path} does not exist")
```

## 总结

通过以上修改，原始的 CatBoost Index Prediction 程序就可以适配我们的 Ray 多集群调度框架。主要修改点包括：

1. 移除所有 Ray 初始化和连接相关的代码
2. 将全局代码封装到函数中
3. 移除或修改 Ray 特定的装饰器和调用
4. 适配我们调度框架的资源需求声明方式
5. 确保环境变量和路径处理的兼容性

这样修改后，程序就可以通过我们的 `submit_catboost_job.py` 脚本提交到多集群环境中执行。