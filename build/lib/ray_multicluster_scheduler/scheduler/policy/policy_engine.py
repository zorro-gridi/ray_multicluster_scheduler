"""Policy engine for combining multiple scheduling policies."""

from typing import Dict, List, Optional
from ray_multicluster_scheduler.common.model import TaskDescription, SchedulingDecision, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.common.model.job_description import JobDescription
from ray_multicluster_scheduler.scheduler.policy.enhanced_score_based_policy import EnhancedScoreBasedPolicy
from ray_multicluster_scheduler.scheduler.policy.tag_affinity_policy import TagAffinityPolicy
from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import ClusterSubmissionHistory
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import PolicyEvaluationError
from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import ClusterMonitor

logger = get_logger(__name__)


class PolicyEngine:
    """Engine for evaluating and combining multiple scheduling policies."""

    # 定义资源使用率阈值，超过此阈值时任务将进入队列等待
    RESOURCE_THRESHOLD = 0.7  # 70%

    def __init__(self, cluster_monitor: ClusterMonitor):
        if cluster_monitor is None:
            raise ValueError("cluster_monitor cannot be None. Strategy engine requires valid cluster state parameters.")

        self.cluster_monitor = cluster_monitor
        self.policies = []
        # Initialize cluster metadata
        self._cluster_metadata = {}

        # Initialize default policies
        self.score_policy = EnhancedScoreBasedPolicy()
        self.tag_policy = TagAffinityPolicy({})  # Will be updated dynamically

        # Register default policies
        self.policies.append(self.score_policy)
        self.policies.append(self.tag_policy)

        # Initialize cluster submission history
        self.cluster_submission_history = ClusterSubmissionHistory()

        # Initialize round-robin counter for fallback cluster selection
        self._round_robin_counter = 0

    def update_cluster_metadata(self, cluster_metadata: Dict[str, ClusterMetadata]):
        """Update cluster metadata for policies that need it."""
        self.tag_policy = TagAffinityPolicy(cluster_metadata)
        # Replace the tag policy in the policies list
        for i, policy in enumerate(self.policies):
            if isinstance(policy, TagAffinityPolicy):
                self.policies[i] = self.tag_policy
                break

        # Store cluster metadata for enhanced scoring
        self._cluster_metadata = cluster_metadata

    def add_policy(self, policy):
        """Add a custom policy to the engine."""
        self.policies.append(policy)

    def remove_policy(self, policy):
        """Remove a policy from the engine."""
        if policy in self.policies:
            self.policies.remove(policy)

    def schedule(self, task_desc: TaskDescription) -> SchedulingDecision:
        """Evaluate all policies and make a scheduling decision according to specified rules."""
        # 在调度时直接获取最新的集群资源快照和集群元数据
        cluster_info = self.cluster_monitor.get_all_cluster_info() if self.cluster_monitor else {}
        cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                          if info['snapshot'] is not None}
        cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}

        # 更新集群元数据
        self._cluster_metadata = cluster_metadata

        return self._make_scheduling_decision(task_desc, cluster_snapshots)

    def schedule_job(self, job_desc: JobDescription) -> SchedulingDecision:
        """Schedule a job using the same policies as tasks, by converting the job to a task description."""
        # 在调度时直接获取最新的集群资源快照和集群元数据
        cluster_info = self.cluster_monitor.get_all_cluster_info() if self.cluster_monitor else {}
        cluster_snapshots = {name: info['snapshot'] for name, info in cluster_info.items()
                          if info['snapshot'] is not None}
        cluster_metadata = {name: info['metadata'] for name, info in cluster_info.items()}

        # 更新集群元数据
        self._cluster_metadata = cluster_metadata

        # Convert job description to task description to reuse existing scheduling logic
        task_desc = job_desc.as_task_description()
        return self._make_scheduling_decision(task_desc, cluster_snapshots)

    def _make_scheduling_decision(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision:
        """Internal method to make scheduling decisions for both tasks and jobs."""

        # Rule 1: When preferred_cluster is specified, prioritize that cluster
        if task_desc.preferred_cluster:
            logger.info(f"检测到用户指定的首选集群: {task_desc.preferred_cluster}")
            # Check if the preferred cluster is healthy
            if task_desc.preferred_cluster in cluster_snapshots:
                # 获取首选集群的资源信息
                snapshot = cluster_snapshots[task_desc.preferred_cluster]
                # 使用新的资源指标
                cpu_used_cores = snapshot.cluster_cpu_used_cores
                cpu_total_cores = snapshot.cluster_cpu_total_cores
                cpu_available = cpu_total_cores - cpu_used_cores
                cpu_total = cpu_total_cores

                # GPU资源暂时不可用，使用默认值
                gpu_available = 0
                gpu_total = 0

                # 检查资源使用率是否超过阈值
                cpu_utilization = cpu_used_cores / cpu_total_cores if cpu_total_cores > 0 else 0
                gpu_utilization = 0  # GPU指标暂不可用

                # 检查内存使用率
                mem_utilization = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

                # 检查集群上次任务提交时间是否超过40秒
                # 根据任务描述中的信息判断是否为顶级任务
                is_top_level_task = getattr(task_desc, 'is_top_level_task', True)
                if not self.cluster_submission_history.is_cluster_available_and_record(task_desc.preferred_cluster, is_top_level_task):
                    if is_top_level_task:  # 只有顶级任务才显示40秒限制警告
                        remaining_time = self.cluster_submission_history.get_remaining_wait_time(task_desc.preferred_cluster)
                        logger.warning(f"首选集群 {task_desc.preferred_cluster} 在40秒内已提交过任务，"
                                     f"还需等待 {remaining_time:.2f} 秒，任务将进入该集群的待执行队列等待")
                        # 返回首选集群名称，让任务进入该集群的队列等待
                        return SchedulingDecision(
                            task_id=task_desc.task_id,
                            cluster_name=task_desc.preferred_cluster,
                            reason=f"首选集群 {task_desc.preferred_cluster} 在40秒内已提交过任务，还需等待 {remaining_time:.2f} 秒，任务进入该集群的待执行队列等待"
                        )
                    else:  # 子任务不受40秒限制
                        logger.info(f"子任务，跳过40秒限制检查，任务 {task_desc.task_id} 将调度到首选集群 {task_desc.preferred_cluster}")
                elif cpu_utilization > self.RESOURCE_THRESHOLD or gpu_utilization > self.RESOURCE_THRESHOLD or mem_utilization > self.RESOURCE_THRESHOLD:
                    logger.warning(f"用户指定的首选集群 {task_desc.preferred_cluster} 资源使用率超过阈值 "
                                 f"(CPU: {cpu_utilization:.2%}, GPU: {gpu_utilization:.2%}, 内存: {mem_utilization:.2%})，任务将进入该集群的待执行队列等待")
                    # 返回首选集群名称，让任务进入该集群的队列等待
                    return SchedulingDecision(
                        task_id=task_desc.task_id,
                        cluster_name=task_desc.preferred_cluster,
                        reason=f"首选集群 {task_desc.preferred_cluster} 资源紧张，任务进入该集群的待执行队列等待"
                    )
                else:
                    logger.info(f"任务 {task_desc.task_id} 将调度到用户指定的首选集群 [{task_desc.preferred_cluster}]: "
                               f"可用资源 - CPU: {cpu_available}, GPU: {gpu_available}")

                    # 40秒规则检查和时间记录已在上面的原子操作中完成

                    return SchedulingDecision(
                        task_id=task_desc.task_id,
                        cluster_name=task_desc.preferred_cluster,
                        reason=f"选择了用户指定的首选集群 {task_desc.preferred_cluster}，"
                               f"可用资源: CPU={cpu_available}, GPU={gpu_available}"
                    )
            else:
                logger.error(f"用户指定的首选集群 {task_desc.preferred_cluster} 不在线或无法连接")
                # 首选集群完全不可用，直接抛出异常
                raise PolicyEvaluationError(f"用户指定的首选集群 {task_desc.preferred_cluster} 不在线或无法连接")
        else:
            logger.info(f"未指定首选集群，使用负载均衡策略选择最优集群")

        # Rule 2: When preferred_cluster is not specified or unavailable, use load balancing
        # 检查所有集群的资源使用率是否都超过阈值
        all_clusters_over_threshold = True
        available_clusters = []

        # 先过滤掉40秒内已提交任务的集群
        filtered_cluster_snapshots = {}
        for cluster_name, snapshot in cluster_snapshots.items():
            if self.cluster_submission_history.is_cluster_available(cluster_name):
                filtered_cluster_snapshots[cluster_name] = snapshot
                # 使用新的资源指标
                cpu_used_cores = snapshot.cluster_cpu_used_cores
                cpu_total_cores = snapshot.cluster_cpu_total_cores
                cpu_available = cpu_total_cores - cpu_used_cores
                cpu_total = cpu_total_cores

                # GPU资源暂时不可用，使用默认值
                gpu_available = 0
                gpu_total = 0

                # 检查资源使用率是否超过阈值
                cpu_utilization = cpu_used_cores / cpu_total_cores if cpu_total_cores > 0 else 0
                gpu_utilization = 0  # GPU指标暂不可用

                # 检查内存使用率
                mem_utilization = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

                if cpu_utilization <= self.RESOURCE_THRESHOLD and gpu_utilization <= self.RESOURCE_THRESHOLD and mem_utilization <= self.RESOURCE_THRESHOLD:
                    all_clusters_over_threshold = False
                    available_clusters.append(cluster_name)
            else:
                remaining_time = self.cluster_submission_history.get_remaining_wait_time(cluster_name)
                logger.info(f"集群 {cluster_name} 在40秒内已提交过任务，还需等待 {remaining_time:.2f} 秒，将从可用集群列表中排除")

        # 如果所有集群都超过阈值，则任务进入队列
        if all_clusters_over_threshold and filtered_cluster_snapshots:
            logger.warning(f"所有集群资源使用率都超过阈值，任务 {task_desc.task_id} 将进入队列等待")
            return SchedulingDecision(
                task_id=task_desc.task_id,
                cluster_name="",
                reason="所有集群资源使用率都超过阈值，任务进入队列等待"
            )

        # Rule 3: 如果没有指定首选集群，且存在资源使用率超过阈值的集群，则执行负载均衡策略
        if not task_desc.preferred_cluster:
            # 检查是否有任何集群的资源使用率超过阈值
            for cluster_name, snapshot in filtered_cluster_snapshots.items():
                # 检查CPU、GPU和内存使用率
                cpu_utilization = snapshot.cluster_cpu_used_cores / snapshot.cluster_cpu_total_cores if snapshot.cluster_cpu_total_cores > 0 else 0
                gpu_utilization = 0  # GPU指标暂不可用
                mem_utilization = snapshot.cluster_mem_usage_percent / 100.0 if snapshot.cluster_mem_total_mb > 0 else 0

                if cpu_utilization > self.RESOURCE_THRESHOLD or gpu_utilization > self.RESOURCE_THRESHOLD or mem_utilization > self.RESOURCE_THRESHOLD:
                    logger.info(f"检测到集群 {cluster_name} 资源使用率超过阈值，触发负载均衡策略")
                    logger.info(f"  - CPU使用率: {cpu_utilization:.2%} (阈值: {self.RESOURCE_THRESHOLD:.2%})")
                    logger.info(f"  - 内存使用率: {mem_utilization:.2%} (阈值: {self.RESOURCE_THRESHOLD:.2%})")
                    break  # 找到一个超阈值的集群就足够触发负载均衡
        else:
            logger.info(f"任务指定了首选集群 {task_desc.preferred_cluster}，不应用负载均衡策略")

        # 在进行策略评估之前，我们需要考虑如何确保任务在集群间均匀分布
        # 首先，收集所有策略的决策
        policy_decisions = []

        for policy in self.policies:
            try:
                if isinstance(policy, EnhancedScoreBasedPolicy):
                    # 使用过滤后的集群快照进行策略评估
                    decision = policy.evaluate(task_desc, filtered_cluster_snapshots, self._cluster_metadata)
                else:
                    # 使用过滤后的集群快照进行策略评估
                    decision = policy.evaluate(task_desc, filtered_cluster_snapshots)
                policy_decisions.append(decision)
                logger.debug(f"策略 {policy.__class__.__name__} 决策: {decision}")
            except Exception as e:
                logger.error(f"策略 {policy.__class__.__name__} 评估失败: {e}")
                import traceback
                traceback.print_exc()
                # Continue with other policies even if one fails

        # Combine decisions - in this simple implementation, we prioritize tag affinity
        # if it provides a specific cluster, otherwise fall back to score-based policy
        final_decision = self._combine_decisions(task_desc, policy_decisions)

        if not final_decision or not final_decision.cluster_name:
            # Rule 3: If no specific cluster was recommended by policies, use cluster monitor to select best cluster
            if self.cluster_monitor:
                try:
                    requirements = {}
                    if task_desc.resource_requirements:
                        requirements["resources"] = task_desc.resource_requirements
                    if task_desc.tags:
                        requirements["tags"] = task_desc.tags

                    # 获取最佳集群，但需要确保该集群不在40秒内已提交任务的列表中
                    best_cluster = self.cluster_monitor.get_best_cluster(requirements)

                    # 如果最佳集群在40秒内已提交任务，则需要从可用集群中选择
                    if best_cluster and not self.cluster_submission_history.is_cluster_available(best_cluster):
                        logger.info(f"最佳集群 {best_cluster} 在40秒内已提交过任务，正在从其他可用集群中选择")

                        # 从过滤后的集群中选择最佳集群
                        best_cluster = None
                        best_score = -1
                        for cluster_name, snapshot in filtered_cluster_snapshots.items():
                            # 计算集群评分（简单使用CPU可用资源作为评分）
                            cpu_available = snapshot.cluster_cpu_total_cores - snapshot.cluster_cpu_used_cores
                            if cpu_available > best_score:
                                best_score = cpu_available
                                best_cluster = cluster_name

                    if best_cluster:
                        # 使用原子操作检查并记录40秒规则
                        is_top_level_task = getattr(task_desc, 'is_top_level_task', True)
                        if self.cluster_submission_history.is_cluster_available_and_record(best_cluster, is_top_level_task):
                            # 获取所选集群的资源信息
                            cluster_info = self.cluster_monitor.get_all_cluster_info()
                            cluster_data = cluster_info.get(best_cluster)
                            if cluster_data and cluster_data['snapshot']:
                                snapshot = cluster_data['snapshot']
                                # 使用新的资源指标
                                cpu_available = snapshot.cluster_cpu_total_cores - snapshot.cluster_cpu_used_cores
                                gpu_available = 0  # GPU指标暂不可用

                                logger.info(f"任务 {task_desc.task_id} 通过负载均衡算法调度到最佳集群 [{best_cluster}]: "
                                           f"可用资源 - CPU: {cpu_available}, GPU: {gpu_available}")

                                return SchedulingDecision(
                                    task_id=task_desc.task_id,
                                    cluster_name=best_cluster,
                                    reason=f"通过负载均衡算法选择的最佳集群 {best_cluster}，"
                                           f"可用资源: CPU={cpu_available}, GPU={gpu_available}"
                                )
                            else:
                                return SchedulingDecision(
                                    task_id=task_desc.task_id,
                                    cluster_name=best_cluster,
                                    reason=f"通过负载均衡算法选择的最佳集群 {best_cluster}"
                                )
                        else:
                            # 集群在原子操作检查时发现不可用，重新选择
                            is_top_level_task = getattr(task_desc, 'is_top_level_task', True)
                            if is_top_level_task:  # 只有顶级任务才会被拒绝
                                logger.warning(f"最佳集群 {best_cluster} 在原子检查时发现不可用，任务进入队列")
                                best_cluster = None
                            else:  # 子任务不会被40秒限制拒绝
                                logger.info(f"子任务，跳过40秒限制检查，任务 {task_desc.task_id} 将调度到集群 {best_cluster}")
                                return SchedulingDecision(
                                    task_id=task_desc.task_id,
                                    cluster_name=best_cluster,
                                    reason=f"子任务直接调度到集群 {best_cluster}"
                                )
                except Exception as e:
                    logger.error(f"使用集群监视器选择最佳集群失败: {e}")
                    import traceback
                    traceback.print_exc()

            # Fallback to selecting any healthy cluster
            # 实现轮询机制以确保任务在集群间分布
            if filtered_cluster_snapshots:
                # 使用轮询计数器在可用集群间轮询分配任务
                available_cluster_names = list(filtered_cluster_snapshots.keys())

                # 尝试轮询选择，使用原子操作确保40秒规则
                for attempt in range(len(available_cluster_names)):
                    cluster_idx = (self._round_robin_counter + attempt) % len(available_cluster_names)
                    selected_cluster = available_cluster_names[cluster_idx]

                    # 使用原子操作检查并记录
                    is_top_level_task = getattr(task_desc, 'is_top_level_task', True)
                    if self.cluster_submission_history.is_cluster_available_and_record(selected_cluster, is_top_level_task):
                        # 成功选择集群，更新轮询计数器
                        self._round_robin_counter = (cluster_idx + 1) % len(available_cluster_names)

                        logger.info(f"任务 {task_desc.task_id} 调度到回退集群 {selected_cluster}")

                        return SchedulingDecision(
                            task_id=task_desc.task_id,
                            cluster_name=selected_cluster,
                            reason=f"回退选择集群 {selected_cluster} (轮询分配)"
                        )

                # 如果所有集群都在40秒限制内，顶级任务进入队列，子任务直接选择集群
                is_top_level_task = getattr(task_desc, 'is_top_level_task', True)
                if is_top_level_task:
                    logger.warning(f"所有可用集群都在40秒限制内，任务 {task_desc.task_id} 进入队列等待")
                    return SchedulingDecision(
                        task_id=task_desc.task_id,
                        cluster_name="",
                        reason="所有可用集群都在40秒限制内，任务进入队列等待"
                    )
                else:
                    # 子任务不受40秒限制，选择第一个可用集群
                    selected_cluster = available_cluster_names[0]
                    logger.info(f"子任务 {task_desc.task_id} 直接调度到集群 {selected_cluster}，跳过40秒限制")
                    return SchedulingDecision(
                        task_id=task_desc.task_id,
                        cluster_name=selected_cluster,
                        reason=f"子任务直接调度到集群 {selected_cluster}"
                    )

        # 如果策略引擎提供了决策，需要使用原子操作确保40秒规则
        if final_decision and final_decision.cluster_name:
            # 使用原子操作检查并记录提交时间
            is_top_level_task = getattr(task_desc, 'is_top_level_task', True)
            if self.cluster_submission_history.is_cluster_available_and_record(final_decision.cluster_name, is_top_level_task):
                logger.info(f"任务 {task_desc.task_id} 通过策略决策调度到集群 {final_decision.cluster_name}")
                return final_decision
            else:
                # 策略推荐的集群在原子检查时发现不可用，只有顶级任务才进入队列
                is_top_level_task = getattr(task_desc, 'is_top_level_task', True)
                if is_top_level_task:
                    logger.warning(f"策略推荐的集群 {final_decision.cluster_name} 在原子检查时发现不可用，任务进入队列")
                    return SchedulingDecision(
                        task_id=task_desc.task_id,
                        cluster_name="",
                        reason=f"策略推荐的集群 {final_decision.cluster_name} 在40秒限制内"
                    )
                else:
                    # 子任务不受40秒限制，直接调度
                    logger.info(f"子任务 {task_desc.task_id} 直接调度到集群 {final_decision.cluster_name}，跳过40秒限制")
                    return SchedulingDecision(
                        task_id=task_desc.task_id,
                        cluster_name=final_decision.cluster_name,
                        reason=f"子任务直接调度到集群 {final_decision.cluster_name}"
                    )

        logger.info(f"最终调度决策: 无可用集群，任务进入队列")
        return SchedulingDecision(
            task_id=task_desc.task_id,
            cluster_name="",
            reason="无可用集群，任务进入队列等待"
        )

    def _combine_decisions(
        self, task_desc: TaskDescription, policy_decisions: List[SchedulingDecision],
                        ) -> SchedulingDecision:
        """Combine decisions from multiple policies."""
        # First, check if tag affinity policy provided a specific cluster
        for decision in policy_decisions:
            if decision.cluster_name and "tag affinity" in decision.reason.lower():
                return decision

        # If no tag affinity, check score-based policy
        for decision in policy_decisions:
            if decision.cluster_name and ("resource availability" in decision.reason.lower() or "评分策略" in decision.reason.lower() or "增强版评分策略" in decision.reason.lower()):
                return decision

        # Return None to indicate no specific decision was made
        return SchedulingDecision(
            task_id=task_desc.task_id,
            cluster_name="",
            reason="策略未推荐特定集群"
        )