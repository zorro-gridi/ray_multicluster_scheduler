# 问题发现
调度器在初始化启动时，存在接口被大量重复调用

# 重复调用接口列表
- 1. ray_multicluster_scheduler.scheduler.health.health_checker
- 2. ray_multicluster_scheduler.scheduler.connection.ray_client_pool

# 大量重复调用日志
# ray_client_pool 重复调用
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - Initializing cluster connections...
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.connection.ray_client_pool - INFO - Cluster centos already in pool, updating configuration
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.connection.ray_client_pool - INFO - Added cluster centos to connection pool
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.connection.connection_lifecycle - INFO - Registered and connected to cluster centos
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.connection.ray_client_pool - INFO - Cluster mac already in pool, updating configuration
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.connection.ray_client_pool - INFO - Added cluster mac to connection pool
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.connection.connection_lifecycle - INFO - Registered and connected to cluster mac
2025-12-29 12:22:04,917 - ray_multicluster_scheduler.scheduler.connection.job_client_pool - INFO - Added cluster centos to job client pool metadata
2025-12-29 12:22:04,922 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - Task lifecycle manager started
2025-12-29 12:22:04,930 - ray_multicluster_scheduler.scheduler.connection.connection_lifecycle - INFO - Added cluster centos to job client pool
2025-12-29 12:22:04,930 - ray_multicluster_scheduler.scheduler.connection.job_client_pool - INFO - Added cluster mac to job client pool metadata
2025-12-29 12:22:04,930 - ray_multicluster_scheduler.scheduler.connection.connection_lifecycle - INFO - Added cluster mac to job client pool
2025-12-29 12:22:04,930 - ray_multicluster_scheduler.scheduler.connection.connection_lifecycle - INFO - Initialized job client pool with 2 clusters
2025-12-29 12:22:04,930 - ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager - INFO - Initialized connections to 2 clusters

## health_checker 重复调用
2025-12-29 12:22:04,934 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster centos
2025-12-29 12:22:05,039 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [centos] 资源状态: CPU=15.18/20.00 (24.08% 已使用), 内存=10.99/31.19GiB (64.76% 已使用), GPU=0/0, 节点数=0, 评分=12.4
2025-12-29 12:22:05,039 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for centos with stats: CPU usage=24.08%, Mem usage=64.76%, Score=12.4
2025-12-29 12:22:05,039 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster mac
2025-12-29 12:22:05,052 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [centos] 资源状态: CPU=15.18/20.00 (24.08% 已使用), 内存=10.99/31.19GiB (64.76% 已使用), GPU=0/0, 节点数=0, 评分=12.4
2025-12-29 12:22:05,054 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for centos with stats: CPU usage=24.08%, Mem usage=64.76%, Score=12.4
2025-12-29 12:22:05,054 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster mac
2025-12-29 12:22:05,070 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=2.06/8.00 (74.25% 已使用), 内存=11.14/16.00GiB (30.38% 已使用), GPU=0/0, 节点数=0, 评分=1.2
2025-12-29 12:22:05,070 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for mac with stats: CPU usage=74.25%, Mem usage=30.38%, Score=1.2
2025-12-29 12:22:05,099 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=2.06/8.00 (74.25% 已使用), 内存=11.14/16.00GiB (30.38% 已使用), GPU=0/0, 节点数=0, 评分=1.2
2025-12-29 12:22:05,099 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for mac with stats: CPU usage=74.25%, Mem usage=30.38%, Score=1.2
2025-12-29 12:22:05,100 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster centos
2025-12-29 12:22:05,136 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [centos] 资源状态: CPU=15.18/20.00 (24.08% 已使用), 内存=10.99/31.19GiB (64.76% 已使用), GPU=0/0, 节点数=0, 评分=12.4
2025-12-29 12:22:05,136 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for centos with stats: CPU usage=24.08%, Mem usage=64.76%, Score=12.4
2025-12-29 12:22:05,136 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Starting health check for cluster mac
2025-12-29 12:22:05,187 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - 集群 [mac] 资源状态: CPU=2.06/8.00 (74.25% 已使用), 内存=11.14/16.00GiB (30.38% 已使用), GPU=0/0, 节点数=0, 评分=1.2
2025-12-29 12:22:05,187 - ray_multicluster_scheduler.scheduler.health.health_checker - INFO - Created snapshot for mac with stats: CPU usage=74.25%, Mem usage=30.38%, Score=1.2

# 改善目标
请严格自查以上接口在调度器启动到运行结束的完整调用链，在一个任务周期中，接口只允许调用一次，避免重复的打印日志
