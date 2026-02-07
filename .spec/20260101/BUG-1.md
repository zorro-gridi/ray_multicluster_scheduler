# 问题类型
多集群调度过程中，ray job 执行 FAILED，异常退出

# 用户程序代码上下文
## 主控程序核心代码
job_config_list = {
    'indx_bunch_daily_predict': Path(home_dir) / 'project/pycharm/Fund/Index_Markup_Forecasting/bunch_and_reverse_model/predict_cluster.py',
    'indx_markup_daily_predict': Path(home_dir) / 'project/pycharm/Fund/Index_Markup_Forecasting/index_ts_forecasting_model/cat/predict_cluster.py',
    }

def main():
    config_file_path = env_path / 'Index_Markup_Forecasting/cluster.yaml'
    initialize_scheduler_environment(config_file_path=config_file_path)

    job_ids = []
    # Submit a job using the real cluster
    for job_name, entrypoint in job_config_list.items():
        job_id = submit_job(
            entrypoint=f'python {entrypoint}',
            submission_id=str(int(time.time() * 1000)),
            metadata={'job_name': job_name},
            resource_requirements={'CPU': 16},
            # preferred_cluster=PREFERRED_CLUSTER,
        )
        print(f'Job submitted successfully with ID: {job_id}')
        # Wait a bit for the job to start
        time.sleep(3)
        job_ids.append(job_id)

    wait_for_all_jobs(submission_ids=job_ids, check_interval=10)

## 任务一：project/pycharm/Fund/Index_Markup_Forecasting/bunch_and_reverse_model/predict_cluster.py 核心代码
def infer_task():
    randstr = str(random.randint(1e10, 9e10))
    actor_id, actor_handle = submit_actor(
        actor_class=Infer,
        resource_requirements={"CPU": 1, "memory": 1 * 1024 * 1024 * 1024},
        tags=['tradebot', 'fund'],
        name=f'bounch_predict_daily_{randstr}',
        # preferred_cluster=PREFERRED_CLUSTER,
        # preferred_cluster='mac',
        )
    return actor_id, actor_handle

results_ref = []
for idx in index_list:
    actor_id, actor_handle = infer_task()
    objRef = actor_handle.infer.remote(idx, split_name='test')
    results_ref.append(objRef)
    time.sleep(0.2)

resutls = list(ray.get(results_ref))
task_lifecycle_manager.stop()

## 任务二：project/pycharm/Fund/Index_Markup_Forecasting/index_ts_forecasting_model/cat/predict_cluster.py 核心代码
def infer_task():
    randstr = str(random.randint(1000, 9999))
    actor_id, actor_handle = submit_actor(
        actor_class=Infer,
        resource_requirements={"CPU": 1, "memory": 1 * 1024 * 1024 * 1024},
        tags=['tradebot', 'fund'],
        name=f'idx_predict_daily_{randstr}',
        # preferred_cluster=PREFERRED_CLUSTER,
        )
    return actor_id, actor_handle


def main():
    config_file_path = env_path / 'Index_Markup_Forecasting/cluster.yaml'
    task_lifecycle_manager = initialize_scheduler_environment(config_file_path=config_file_path)

    results_ref = []
    for idx in index_mapping.keys():
        actor_id, actor_handle = infer_task()
        objRef = actor_handle.infer.remote(idx)
        results_ref.append(objRef)

    print(list(ray.get(results_ref)))
    task_lifecycle_manager.stop()

# 提交方式
用户在 mac 集群的物理机上通过主控程序调用 submit_job 接口提交的以上两个任务

# ray dashboard 动态描述
- 1. python /home/zorro//project/pycharm/Fund/Index_Markup_Forecasting/bunch_and_reverse_model/predict_cluster.py 任务调度到了 centos 集群，runtime_env 配置
conda: ts
env_vars:
  home_dir: /home/zorro/
  PYTHONPATH: |
    /home/zorro/project/pycharm/Fund:
    /home/zorro/project/pycharm/tools:
    /home/zorro/project/pycharm/rlops:
    /home/zorro/project/pycharm/mlops:
    /home/zorro/project/pycharm/ray_multicluster_scheduler:
    ${PYTHONPATH}
- 2. 控制台显示 project/pycharm/Fund/Index_Markup_Forecasting/index_ts_forecasting_model/cat/predict_cluster.py 决策调度到 mac 集群，但是任务很快因为环境配置错误失败，因为 mac 集群任务调度 runtime_env 显示的是 centos 的配置
- 3. 用户主控程序通过 submit_job 接口提交任务，在centos和mac集群上还启动了以下的伴生程序
* /Users/zorro/miniconda3/envs/k8s/bin/python3.10 -m ray.util.client.server --address=127.0.0.1:6379 --host=0.0.0.0 --port=23056 --mode=specific-server
* /home/ray/anaconda3/bin/python -m ray.util.client.server --address=192.168.0.145:6379 --host=0.0.0.0 --port=23002 --mode=specific-server

# 问题原因
任务二提交时，经过负载均衡调度决策后，被提交到 mac 集群调度，但任务的 runtime_env 参数配置仍然使用了 centos 集群的配置信息，导致 mac 集群执行环境不匹配，任务执行失败。
以下用户的自定义集群配置文件部分关于 runtime_env 的配置内容：
- centos 集群 runtime_env 配置
conda: ts
env_vars:
  home_dir: /home/zorro/
  PYTHONPATH: |
    /home/zorro/project/pycharm/Fund:
    /home/zorro/project/pycharm/tools:
    /home/zorro/project/pycharm/rlops:
    /home/zorro/project/pycharm/mlops:
    /home/zorro/project/pycharm/ray_multicluster_scheduler:
    ${PYTHONPATH}
- 实际 mac 集群应该需要的 runtime_env 配置：
（差异主要是 conda 环境，和home_dir目录）
conda: k8s
env_vars:
  home_dir: /Users/zorro/
  PYTHONPATH: |
    /Users/zorro/project/pycharm/Fund:
    /Users/zorro/project/pycharm/tools:
    /Users/zorro/project/pycharm/rlops:
    /Users/zorro/project/pycharm/mlops:
    /Users/zorro/project/pycharm/ray_multicluster_scheduler:
    ${PYTHONPATH}

#  参数传递规范
- 1. submit_task/actor/job 接口都不需要 runtime_env 参数的显式传参，它主要是在 ray init 或 ray job submitssion client 连接初始化过程中，通过指定的 cluster_name 读取用户自定义或系统默认的集群配置文件信息，并在调用过程中，自动获取配置文件的 runtime_env 参数配置

# 任务需求
分析为什么任务二提交到了 mac 集群调度时，仍然使用了任务一调度时使用的 centos 集群的 runtime_env 配置。请根据 runtime__env 参数的传递流程，仔细分析相关接口调用过程中，runtime_env 的生成与传递过程，找出参数传递过程中的问题