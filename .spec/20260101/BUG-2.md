# 问题描述
用户通过 submit_job 接口连续向集群提交两个任务，任务一调度到了centos集群，任务二调度到了mac集群。在两个任务运行一段时间后，负载均衡策略检测到
“01/01/2026 02:41:46 AM 没有可用的集群进行评分评估
01/01/2026 02:41:46 AM 没有找到满足资源需求的集群”
此后，两个job便一直不再继续执行，看起来处于等待集群资源的状态。其实，查看ray dashboard或系统资源快照，此时所有集群的资源还是充足的。

# 可能的原因
- 1. 可能集群每 40 秒限制提交一个新任务的规则导致集群调度器长期被限制调度，造成一种永久暂停的状态，无法继续执行任务
- 2. 其它可能的原因，需要你仔细分析验证

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
- 1. centos / mac 集群都出现 “没有可用的集群进行评分评估，没有找到满足资源需求的集群” 的控制台打印信息
- 2. 两个集群的ray job一直处于running状态，但是实际都没有继续向下执行

# 任务需求
根据调度器的任务调度策略，仔细分析为什么任务执行之后便很快出现“没有找到满足资源需求的集群”的问题，给出分析报告