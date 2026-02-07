# 问题描述
当系统所有集群资源使用率显示超过阈值时，提交任务，控制台显示没有可用资源，任务进入了待执行队列，但是接着调度器就退出了，待排队任务队列消失，后续并没有执行队列中的任务。

# 控制台信息
2025-12-29 22:18:15,886 - ray_multicluster_scheduler.scheduler.policy.policy_engine - INFO - 未指定首选集群，使用负载均衡策略选择最优集群
2025-12-29 22:18:15,887 - ray_multicluster_scheduler.scheduler.policy.policy_engine - WARNING - 所有集群资源使用率都超过阈值，任务 job_0876f2ad833c4ea0b3d7 将进入队列等待

# 任务需求
分析这是什么原因？先不要改动系统代码