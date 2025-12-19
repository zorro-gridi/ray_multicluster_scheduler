"""
Policy engine for combining multiple scheduling policies.
"""

from typing import Dict, List, Optional
from ray_multicluster_scheduler.common.model import TaskDescription, SchedulingDecision, ResourceSnapshot, ClusterMetadata
from ray_multicluster_scheduler.scheduler.policy.score_based_policy import ScoreBasedPolicy
from ray_multicluster_scheduler.scheduler.policy.tag_affinity_policy import TagAffinityPolicy
from ray_multicluster_scheduler.common.logging import get_logger
from ray_multicluster_scheduler.common.exception import PolicyEvaluationError

logger = get_logger(__name__)


class PolicyEngine:
    """Engine for evaluating and combining multiple scheduling policies."""

    def __init__(self, cluster_monitor=None):
        self.cluster_monitor = cluster_monitor
        self.policies = []

        # Initialize default policies
        self.score_policy = ScoreBasedPolicy()
        self.tag_policy = TagAffinityPolicy({})  # Will be updated dynamically

        # Register default policies
        self.policies.append(self.score_policy)
        self.policies.append(self.tag_policy)

    def update_cluster_metadata(self, cluster_metadata: Dict[str, ClusterMetadata]):
        """Update cluster metadata for policies that need it."""
        self.tag_policy = TagAffinityPolicy(cluster_metadata)
        # Replace the tag policy in the policies list
        for i, policy in enumerate(self.policies):
            if isinstance(policy, TagAffinityPolicy):
                self.policies[i] = self.tag_policy
                break

    def add_policy(self, policy):
        """Add a custom policy to the engine."""
        self.policies.append(policy)

    def remove_policy(self, policy):
        """Remove a policy from the engine."""
        if policy in self.policies:
            self.policies.remove(policy)

    def schedule(self, task_desc: TaskDescription, cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision:
        """Evaluate all policies and make a scheduling decision according to specified rules."""

        # Rule 1: When preferred_cluster is specified, prioritize that cluster
        if task_desc.preferred_cluster:
            logger.info(f"检测到用户指定的首选集群: {task_desc.preferred_cluster}")
            # Check if the preferred cluster is healthy
            if task_desc.preferred_cluster in cluster_snapshots:
                # 获取首选集群的资源信息
                snapshot = cluster_snapshots[task_desc.preferred_cluster]
                cpu_available = snapshot.available_resources.get("CPU", 0)
                cpu_total = snapshot.total_resources.get("CPU", 0)
                gpu_available = snapshot.available_resources.get("GPU", 0)
                gpu_total = snapshot.total_resources.get("GPU", 0)

                # 检查资源使用率是否超过80%
                cpu_utilization = (cpu_total - cpu_available) / cpu_total if cpu_total > 0 else 0
                gpu_utilization = (gpu_total - gpu_available) / gpu_total if gpu_total > 0 else 0

                if cpu_utilization > 0.8 or gpu_utilization > 0.8:
                    logger.warning(f"用户指定的首选集群 {task_desc.preferred_cluster} 资源使用率超过80% "
                                 f"(CPU: {cpu_utilization:.2f}, GPU: {gpu_utilization:.2f})，回退到负载均衡调度")
                else:
                    logger.info(f"任务 {task_desc.task_id} 将调度到用户指定的首选集群 [{task_desc.preferred_cluster}]: "
                               f"可用资源 - CPU: {cpu_available}, GPU: {gpu_available}")

                    return SchedulingDecision(
                        task_id=task_desc.task_id,
                        cluster_name=task_desc.preferred_cluster,
                        reason=f"选择了用户指定的首选集群 {task_desc.preferred_cluster}，"
                               f"可用资源: CPU={cpu_available}, GPU={gpu_available}"
                    )
            else:
                logger.warning(f"用户指定的首选集群 {task_desc.preferred_cluster} 不可用，回退到负载均衡调度")
        else:
            logger.info(f"未指定首选集群，使用负载均衡策略选择最优集群")

        # Rule 2: When preferred_cluster is not specified or unavailable, use load balancing
        # Collect decisions from all policies
        policy_decisions = []

        for policy in self.policies:
            try:
                decision = policy.evaluate(task_desc, cluster_snapshots)
                policy_decisions.append(decision)
                logger.debug(f"策略 {policy.__class__.__name__} 决策: {decision}")
            except Exception as e:
                logger.error(f"策略 {policy.__class__.__name__} 评估失败: {e}")
                import traceback
                traceback.print_exc()
                # Continue with other policies even if one fails

        # Combine decisions - in this simple implementation, we prioritize tag affinity
        # if it provides a specific cluster, otherwise fall back to score-based policy
        final_decision = self._combine_decisions(task_desc, policy_decisions, cluster_snapshots)

        if not final_decision or not final_decision.cluster_name:
            # Rule 3: If no specific cluster was recommended by policies, use cluster monitor to select best cluster
            if self.cluster_monitor:
                try:
                    requirements = {}
                    if task_desc.resource_requirements:
                        requirements["resources"] = task_desc.resource_requirements
                    if task_desc.tags:
                        requirements["tags"] = task_desc.tags

                    best_cluster = self.cluster_monitor.get_best_cluster(requirements)
                    if best_cluster:
                        # 获取所选集群的资源信息
                        cluster_info = self.cluster_monitor.get_all_cluster_info()
                        cluster_data = cluster_info.get(best_cluster)
                        if cluster_data and cluster_data['snapshot']:
                            snapshot = cluster_data['snapshot']
                            cpu_available = snapshot.available_resources.get("CPU", 0)
                            gpu_available = snapshot.available_resources.get("GPU", 0)

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
                except Exception as e:
                    logger.error(f"使用集群监视器选择最佳集群失败: {e}")
                    import traceback
                    traceback.print_exc()

            # Fallback to selecting any healthy cluster
            for cluster_name in cluster_snapshots.keys():
                return SchedulingDecision(
                    task_id=task_desc.task_id,
                    cluster_name=cluster_name,
                    reason=f"回退选择第一个可用集群 {cluster_name}"
                )

        logger.info(f"最终调度决策: {final_decision}")
        return final_decision

    def _combine_decisions(self, task_desc: TaskDescription, policy_decisions: List[SchedulingDecision],
                          cluster_snapshots: Dict[str, ResourceSnapshot]) -> SchedulingDecision:
        """Combine decisions from multiple policies."""
        # First, check if tag affinity policy provided a specific cluster
        for decision in policy_decisions:
            if decision.cluster_name and "tag affinity" in decision.reason.lower():
                return decision

        # If no tag affinity, check score-based policy
        for decision in policy_decisions:
            if decision.cluster_name and "resource availability" in decision.reason.lower():
                return decision

        # Return None to indicate no specific decision was made
        return SchedulingDecision(
            task_id=task_desc.task_id,
            cluster_name="",
            reason="策略未推荐特定集群"
        )