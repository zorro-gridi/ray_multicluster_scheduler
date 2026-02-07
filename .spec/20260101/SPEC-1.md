# 问题描述
- 1. KeyboardInterrupt 优雅关闭没有效果。用户通过以下主控程序提交ray job任务，job启动运行后，用户希望中途终止 ray job 运行，在终端发出 KeyboardInterrupt 信号后，终端并没有显示出发任务终止信号，终止并清理 ray 环境。

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