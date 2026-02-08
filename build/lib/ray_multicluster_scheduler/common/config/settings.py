"""
统一参数管理模块 - 调度系统全部常量参数

支持三层配置（优先级从高到低）：
1. 环境变量 SCHEDULER_*
2. 配置文件 (~/.ray_scheduler/settings.yaml 或通过 --config-file 指定)
3. 代码默认值

Example:
    from ray_multicluster_scheduler.common.config import settings

    # 使用默认值
    print(settings.SUBMISSION_WAIT_TIME)  # 40.0

    # 从环境变量覆盖
    # export SCHEDULER_SUBMISSION_WAIT_TIME=60.0
    print(settings.SUBMISSION_WAIT_TIME)  # 60.0

    # 从配置文件加载
    # settings.load("/path/to/settings.yaml")
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import os
import yaml
from pathlib import Path


class ExecutionMode:
    """任务执行模式枚举"""
    SERIAL = "serial"      # 串行模式：集群一次只允许一个任务
    PARALLEL = "parallel" # 并行模式：允许同时多个任务（受资源限制）


@dataclass
class SchedulerSettings:
    """
    统一参数管理模块 - 调度系统全部常量参数

    Attributes:
        EXECUTION_MODE: 任务执行模式（serial/parallel）
        SUBMISSION_WAIT_TIME: 40秒规则 - 同一集群连续提交顶级任务的最小间隔（秒）
        RESOURCE_THRESHOLD: 资源使用率阈值（70%）
        RESOURCE_CHECK_INTERVAL: 资源检查间隔（秒）
        JOB_COMPLETION_TIMEOUT: 等待Job完成的超时时间（秒）
        TASK_COMPLETION_TIMEOUT: 等待Task完成的超时时间（秒）
        ACTOR_READY_TIMEOUT: 等待Actor就绪的超时时间（秒）
        DEFAULT_TASK_TIMEOUT: 默认任务超时时间（秒）
        CONNECTION_TIMEOUT: Ray连接超时时间（秒）
        JOB_CLIENT_TIMEOUT: Job客户端超时时间（秒）
        JOB_CLIENT_CACHE_TIMEOUT: Job客户端缓存超时时间（秒）
        JOB_STATUS_CHECK_INTERVAL: 检查Job状态的间隔（秒）
        HEALTH_CHECK_INTERVAL: 健康检查间隔（秒）
        CACHE_TIMEOUT: 集群快照缓存时间（秒）
        EMPTY_QUEUE_SLEEP: 队列为空时主循环休眠时间（秒）
        QUEUE_CONDITION_WAIT_TIMEOUT: 队列条件变量等待超时时间（秒）
        ACTOR_NOTIFICATION_WAIT: Actor通知等待超时时间（秒）
        JOB_POLL_CHECK_INTERVAL: 轮询Job状态默认检查间隔（秒）
        TASK_QUEUE_MAX_SIZE: 任务队列最大容量
        TASK_QUEUE_DEFAULT_MAX_SIZE: 任务队列默认最大容量
        CIRCUIT_BREAKER_FAILURE_THRESHOLD: 熔断器故障阈值（失败次数）
        CIRCUIT_BREAKER_RECOVERY_TIMEOUT: 熔断器恢复超时时间（秒）
        RETRY_SLEEP: 连接重试时的休眠时间（秒）
        BACKOFF_TIME: 背压激活时的退避时间（秒）
        WORKER_THREAD_JOIN_TIMEOUT: 工作线程优雅关闭等待时间（秒）
        ERROR_LOOP_SLEEP: 错误状态下短暂延迟（秒）
        REPEATED_ERROR_SLEEP: 重复错误时休眠时间（秒）
        CLUSTER_WEIGHT_DEFAULT: 集群权重默认值
        PROMETHEUS_QUERY_TIMEOUT: Prometheus HTTP查询超时时间（秒）
        JOB_CLIENT_CONNECTION_TEST_TIMEOUT: Job客户端连接测试超时时间（秒）
    """

    # ==================== 调度策略常量 ====================
    EXECUTION_MODE: str = "serial"
    SUBMISSION_WAIT_TIME: float = 60
    RESOURCE_THRESHOLD: float = 0.7
    RESOURCE_CHECK_INTERVAL: int = 5

    # ==================== 超时管理常量 ====================
    JOB_COMPLETION_TIMEOUT: float = 600.0
    TASK_COMPLETION_TIMEOUT: float = 600.0
    ACTOR_READY_TIMEOUT: int = 300
    DEFAULT_TASK_TIMEOUT: int = 300
    CONNECTION_TIMEOUT: int = 300
    JOB_CLIENT_TIMEOUT: int = 300
    JOB_CLIENT_CACHE_TIMEOUT: int = 300

    # ==================== 轮询与间隔常量 ====================
    JOB_STATUS_CHECK_INTERVAL: int = 15
    HEALTH_CHECK_INTERVAL: int = 30
    CACHE_TIMEOUT: float = 5.0
    EMPTY_QUEUE_SLEEP: float = 15.0
    QUEUE_CONDITION_WAIT_TIMEOUT: float = 1.0
    ACTOR_NOTIFICATION_WAIT: float = 1.0
    JOB_POLL_CHECK_INTERVAL: int = 5

    # ==================== 队列管理常量 ====================
    TASK_QUEUE_MAX_SIZE: int = 1000
    TASK_QUEUE_DEFAULT_MAX_SIZE: int = 100

    # ==================== 熔断器常量 ====================
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = 5
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT: float = 60.0

    # ==================== 重试与退避常量 ====================
    RETRY_SLEEP: float = 0.5
    BACKOFF_TIME: float = 30.0
    WORKER_THREAD_JOIN_TIMEOUT: float = 5.0
    ERROR_LOOP_SLEEP: float = 0.1
    REPEATED_ERROR_SLEEP: float = 1.0

    # ==================== 其他常量 ====================
    CLUSTER_WEIGHT_DEFAULT: float = 1.0
    PROMETHEUS_QUERY_TIMEOUT: int = 20
    JOB_CLIENT_CONNECTION_TEST_TIMEOUT: int = 5

    # ==================== 配置加载 ====================
    _config_file: Optional[str] = field(default=None, repr=False)

    def load(self, config_file: Optional[str] = None) -> "SchedulerSettings":
        """
        从配置文件加载配置

        Args:
            config_file: 配置文件路径，如果为 None 则使用默认路径
                        (~/.ray_scheduler/settings.yaml)

        Returns:
            self
        """
        # 1. 从配置文件加载
        self._load_from_file(config_file)

        # 2. 从环境变量加载覆盖
        self._load_from_env()

        return self

    def _load_from_file(self, config_file: Optional[str] = None) -> None:
        """从 YAML 配置文件加载"""
        # 确定配置文件路径
        if config_file:
            config_path = Path(config_file)
        else:
            config_path = Path.home() / ".ray_scheduler" / "settings.yaml"

        if not config_path.exists():
            return

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f) or {}

            # 将配置应用到对应属性
            for key, value in config_data.items():
                if hasattr(self, key.upper()):
                    setattr(self, key.upper(), value)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to load config file {config_path}: {e}")

    def _load_from_env(self) -> None:
        """从环境变量加载配置覆盖"""
        env_prefix = "SCHEDULER_"

        for key, default_value in self._get_defaults().items():
            env_key = f"{env_prefix}{key}"
            env_value = os.environ.get(env_key)

            if env_value is not None:
                converted_value = self._convert_value(env_value, default_value)
                setattr(self, key, converted_value)

    def _get_defaults(self) -> Dict[str, Any]:
        """获取所有默认值"""
        return {
            "EXECUTION_MODE": "serial",
            "SUBMISSION_WAIT_TIME": 40.0,
            "RESOURCE_THRESHOLD": 0.7,
            "RESOURCE_CHECK_INTERVAL": 5,
            "JOB_COMPLETION_TIMEOUT": 600.0,
            "TASK_COMPLETION_TIMEOUT": 600.0,
            "ACTOR_READY_TIMEOUT": 300,
            "DEFAULT_TASK_TIMEOUT": 300,
            "CONNECTION_TIMEOUT": 300,
            "JOB_CLIENT_TIMEOUT": 300,
            "JOB_CLIENT_CACHE_TIMEOUT": 300,
            "JOB_STATUS_CHECK_INTERVAL": 5,
            "HEALTH_CHECK_INTERVAL": 30,
            "CACHE_TIMEOUT": 5.0,
            "EMPTY_QUEUE_SLEEP": 15.0,
            "QUEUE_CONDITION_WAIT_TIMEOUT": 1.0,
            "ACTOR_NOTIFICATION_WAIT": 1.0,
            "JOB_POLL_CHECK_INTERVAL": 5,
            "TASK_QUEUE_MAX_SIZE": 1000,
            "TASK_QUEUE_DEFAULT_MAX_SIZE": 100,
            "CIRCUIT_BREAKER_FAILURE_THRESHOLD": 5,
            "CIRCUIT_BREAKER_RECOVERY_TIMEOUT": 60.0,
            "RETRY_SLEEP": 0.5,
            "BACKOFF_TIME": 30.0,
            "WORKER_THREAD_JOIN_TIMEOUT": 5.0,
            "ERROR_LOOP_SLEEP": 0.1,
            "REPEATED_ERROR_SLEEP": 1.0,
            "CLUSTER_WEIGHT_DEFAULT": 1.0,
            "PROMETHEUS_QUERY_TIMEOUT": 10,
            "JOB_CLIENT_CONNECTION_TEST_TIMEOUT": 5,
        }

    def _convert_value(self, value: str, default: Any) -> Any:
        """将字符串值转换为正确的类型"""
        if isinstance(default, bool):
            return value.lower() in ("true", "1", "yes")
        elif isinstance(default, int):
            return int(value)
        elif isinstance(default, float):
            return float(value)
        elif isinstance(default, str):
            return value
        else:
            return value

    def to_dict(self) -> Dict[str, Any]:
        """将配置导出为字典"""
        return {k: getattr(self, k) for k in self._get_defaults().keys()}


# 全局单例 - 自动加载配置
settings = SchedulerSettings()
# 自动加载环境变量和配置文件
settings.load()
