# 问题描述
- 1. ray job 进程的守护进程与自身不在同一个集群内。例如，任务一经过负载均衡策略被调度到 centos 集群，但是任务一的守护进程缺可能运行在 mac 集群。或者说，所有ray job的守护进程可能都在同一个集群内，导致该集群的负载压力过大。

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
- 1. 用户主控程序通过 submit_job 接口向集群提交任务，每一个任务都会启动了一个如下的守护进程
## mac 集群的守护进程入口
* /Users/zorro/miniconda3/envs/k8s/bin/python3.10 -m ray.util.client.server --address=127.0.0.1:6379 --host=0.0.0.0 --port=23056 --mode=specific-server
## centos 集群的守护进程入口
* /home/ray/anaconda3/bin/python -m ray.util.client.server --address=192.168.0.145:6379 --host=0.0.0.0 --port=23002 --mode=specific-server

# 任务需求
请分析用户描述的问题场景，仔细分析系统相关模块的实现，并向用户解释这种问题的原因