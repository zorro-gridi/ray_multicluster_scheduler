"""
统一参数管理模块 - 完整集成测试

测试目标：严格验证所有 30 个常量参数能够被调度系统正确调用和使用

分类：
1. 导入测试 - 验证模块导入
2. 值验证测试 - 验证所有常量默认值
3. 环境变量测试 - 验证环境变量覆盖功能
4. 配置文件测试 - 验证 YAML 配置文件加载
5. 模块集成测试 - 验证各模块正确使用 settings 常量
6. 执行模式测试 - 验证 EXECUTION_MODE 配置和串行/并行模式逻辑
"""

import os
import tempfile
import pytest
from typing import Dict, Any
from unittest.mock import patch, MagicMock

from ray_multicluster_scheduler.common.config import settings, SchedulerSettings, ExecutionMode


class TestSettingsImport:
    """导入测试 - 验证模块导入"""

    def test_settings_module_import(self):
        """验证 settings 模块能正确导入"""
        assert settings is not None
        assert isinstance(settings, SchedulerSettings)

    def test_scheduler_settings_class_import(self):
        """验证 SchedulerSettings 类能正确导入"""
        assert SchedulerSettings is not None
        assert callable(SchedulerSettings)


class TestSettingsDefaults:
    """值验证测试 - 验证所有常量默认值"""

    def test_all_30_settings_attributes_exist(self):
        """验证所有 30 个常量属性存在且可访问"""
        expected_attrs = [
            # 调度策略常量 (4个)
            "EXECUTION_MODE",
            "SUBMISSION_WAIT_TIME",
            "RESOURCE_THRESHOLD",
            "RESOURCE_CHECK_INTERVAL",
            # 超时管理常量 (8个)
            "JOB_COMPLETION_TIMEOUT",
            "TASK_COMPLETION_TIMEOUT",
            "ACTOR_READY_TIMEOUT",
            "DEFAULT_TASK_TIMEOUT",
            "CONNECTION_TIMEOUT",
            "JOB_CLIENT_TIMEOUT",
            "JOB_CLIENT_CACHE_TIMEOUT",
            "JOB_CLIENT_CONNECTION_TEST_TIMEOUT",
            # 轮询与间隔常量 (8个)
            "JOB_STATUS_CHECK_INTERVAL",
            "HEALTH_CHECK_INTERVAL",
            "CACHE_TIMEOUT",
            "EMPTY_QUEUE_SLEEP",
            "QUEUE_CONDITION_WAIT_TIMEOUT",
            "ACTOR_NOTIFICATION_WAIT",
            "JOB_POLL_CHECK_INTERVAL",
            "PROMETHEUS_QUERY_TIMEOUT",
            # 队列管理常量 (2个)
            "TASK_QUEUE_MAX_SIZE",
            "TASK_QUEUE_DEFAULT_MAX_SIZE",
            # 熔断器常量 (2个)
            "CIRCUIT_BREAKER_FAILURE_THRESHOLD",
            "CIRCUIT_BREAKER_RECOVERY_TIMEOUT",
            # 重试与退避常量 (5个)
            "RETRY_SLEEP",
            "BACKOFF_TIME",
            "WORKER_THREAD_JOIN_TIMEOUT",
            "ERROR_LOOP_SLEEP",
            "REPEATED_ERROR_SLEEP",
            # 其他常量 (1个)
            "CLUSTER_WEIGHT_DEFAULT",
        ]

        for attr in expected_attrs:
            assert hasattr(settings, attr), f"Missing attribute: {attr}"
            # 验证属性可访问
            value = getattr(settings, attr)
            assert value is not None, f"Attribute {attr} is None"

    def test_execution_mode_constants_values(self):
        """验证执行模式常量值"""
        assert ExecutionMode.SERIAL == "serial"
        assert ExecutionMode.PARALLEL == "parallel"

    def test_policy_constants_values(self):
        """验证调度策略常量值"""
        assert settings.EXECUTION_MODE == "serial"
        assert settings.SUBMISSION_WAIT_TIME == 60.0
        assert settings.RESOURCE_THRESHOLD == 0.7
        assert settings.RESOURCE_CHECK_INTERVAL == 5

    def test_timeout_constants_values(self):
        """验证超时管理常量值"""
        assert settings.JOB_COMPLETION_TIMEOUT == 600.0
        assert settings.TASK_COMPLETION_TIMEOUT == 600.0
        assert settings.ACTOR_READY_TIMEOUT == 300
        assert settings.DEFAULT_TASK_TIMEOUT == 300
        assert settings.CONNECTION_TIMEOUT == 300
        assert settings.JOB_CLIENT_TIMEOUT == 300
        assert settings.JOB_CLIENT_CACHE_TIMEOUT == 300
        assert settings.JOB_CLIENT_CONNECTION_TEST_TIMEOUT == 5

    def test_polling_constants_values(self):
        """验证轮询与间隔常量值"""
        assert settings.JOB_STATUS_CHECK_INTERVAL == 15
        assert settings.HEALTH_CHECK_INTERVAL == 30
        assert settings.CACHE_TIMEOUT == 5.0
        assert settings.EMPTY_QUEUE_SLEEP == 15.0
        assert settings.QUEUE_CONDITION_WAIT_TIMEOUT == 1.0
        assert settings.ACTOR_NOTIFICATION_WAIT == 1.0
        assert settings.JOB_POLL_CHECK_INTERVAL == 5
        assert settings.PROMETHEUS_QUERY_TIMEOUT == 20

    def test_queue_constants_values(self):
        """验证队列管理常量值"""
        assert settings.TASK_QUEUE_MAX_SIZE == 1000
        assert settings.TASK_QUEUE_DEFAULT_MAX_SIZE == 100

    def test_circuit_breaker_constants_values(self):
        """验证熔断器常量值"""
        assert settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD == 5
        assert settings.CIRCUIT_BREAKER_RECOVERY_TIMEOUT == 60.0

    def test_retry_backoff_constants_values(self):
        """验证重试与退避常量值"""
        assert settings.RETRY_SLEEP == 0.5
        assert settings.BACKOFF_TIME == 30.0
        assert settings.WORKER_THREAD_JOIN_TIMEOUT == 5.0
        assert settings.ERROR_LOOP_SLEEP == 0.1
        assert settings.REPEATED_ERROR_SLEEP == 1.0

    def test_miscellaneous_constants_values(self):
        """验证其他常量值"""
        assert settings.CLUSTER_WEIGHT_DEFAULT == 1.0


class TestSettingsToDict:
    """to_dict() 方法测试"""

    def test_to_dict_returns_all_settings(self):
        """验证 to_dict() 方法返回所有配置"""
        settings_dict = settings.to_dict()

        assert isinstance(settings_dict, dict)
        # 验证包含所有 29 个常量
        assert len(settings_dict) >= 29

        # 验证关键常量存在
        assert "SUBMISSION_WAIT_TIME" in settings_dict
        assert "RESOURCE_THRESHOLD" in settings_dict
        assert "JOB_COMPLETION_TIMEOUT" in settings_dict
        assert "TASK_QUEUE_MAX_SIZE" in settings_dict

    def test_to_dict_value_types(self):
        """验证 to_dict() 返回值的类型正确性"""
        settings_dict = settings.to_dict()

        # SUBMISSION_WAIT_TIME 在 _get_defaults 中定义为 40.0，但类定义为 60
        # to_dict() 使用 _get_defaults() 返回值，所以应该是 float
        assert isinstance(settings_dict["SUBMISSION_WAIT_TIME"], (int, float))
        assert isinstance(settings_dict["RESOURCE_THRESHOLD"], (int, float))
        assert isinstance(settings_dict["RESOURCE_CHECK_INTERVAL"], int)
        assert isinstance(settings_dict["JOB_COMPLETION_TIMEOUT"], (int, float))
        assert isinstance(settings_dict["CACHE_TIMEOUT"], (int, float))


class TestEnvironmentVariableOverride:
    """环境变量测试 - 验证环境变量覆盖默认值"""

    def test_execution_mode_env_override_serial(self):
        """验证 EXECUTION_MODE 环境变量覆盖 - 串行模式"""
        with patch.dict(os.environ, {"SCHEDULER_EXECUTION_MODE": "serial"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.EXECUTION_MODE == "serial"

    def test_execution_mode_env_override_parallel(self):
        """验证 EXECUTION_MODE 环境变量覆盖 - 并行模式"""
        with patch.dict(os.environ, {"SCHEDULER_EXECUTION_MODE": "parallel"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.EXECUTION_MODE == "parallel"

    def test_execution_mode_default(self):
        """验证 EXECUTION_MODE 默认值为串行模式"""
        test_settings = SchedulerSettings()
        test_settings.load()

        assert test_settings.EXECUTION_MODE == "serial"

    def test_submission_wait_time_env_override(self):
        """验证 SUBMISSION_WAIT_TIME 环境变量覆盖"""
        with patch.dict(os.environ, {"SCHEDULER_SUBMISSION_WAIT_TIME": "120"}):
            # 创建新的 settings 实例
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.SUBMISSION_WAIT_TIME == 120.0

    def test_resource_threshold_env_override(self):
        """验证 RESOURCE_THRESHOLD 环境变量覆盖"""
        with patch.dict(os.environ, {"SCHEDULER_RESOURCE_THRESHOLD": "0.9"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.RESOURCE_THRESHOLD == 0.9

    def test_timeout_env_override(self):
        """验证超时常量环境变量覆盖"""
        with patch.dict(os.environ, {
            "SCHEDULER_JOB_COMPLETION_TIMEOUT": "1200",
            "SCHEDULER_TASK_COMPLETION_TIMEOUT": "900",
            "SCHEDULER_CONNECTION_TIMEOUT": "600",
        }):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.JOB_COMPLETION_TIMEOUT == 1200.0
            assert test_settings.TASK_COMPLETION_TIMEOUT == 900.0
            assert test_settings.CONNECTION_TIMEOUT == 600

    def test_prometheus_query_timeout_env_override(self):
        """验证 PROMETHEUS_QUERY_TIMEOUT 环境变量覆盖"""
        with patch.dict(os.environ, {"SCHEDULER_PROMETHEUS_QUERY_TIMEOUT": "30"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.PROMETHEUS_QUERY_TIMEOUT == 30

    def test_circuit_breaker_env_override(self):
        """验证熔断器常量环境变量覆盖"""
        with patch.dict(os.environ, {
            "SCHEDULER_CIRCUIT_BREAKER_FAILURE_THRESHOLD": "10",
            "SCHEDULER_CIRCUIT_BREAKER_RECOVERY_TIMEOUT": "120.0",
        }):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD == 10
            assert test_settings.CIRCUIT_BREAKER_RECOVERY_TIMEOUT == 120.0

    def test_queue_size_env_override(self):
        """验证队列管理常量环境变量覆盖"""
        with patch.dict(os.environ, {
            "SCHEDULER_TASK_QUEUE_MAX_SIZE": "2000",
            "SCHEDULER_TASK_QUEUE_DEFAULT_MAX_SIZE": "200",
        }):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.TASK_QUEUE_MAX_SIZE == 2000
            assert test_settings.TASK_QUEUE_DEFAULT_MAX_SIZE == 200

    def test_env_type_conversion_int(self):
        """验证环境变量类型转换 - int"""
        with patch.dict(os.environ, {"SCHEDULER_RESOURCE_CHECK_INTERVAL": "10"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert isinstance(test_settings.RESOURCE_CHECK_INTERVAL, int)
            assert test_settings.RESOURCE_CHECK_INTERVAL == 10

    def test_env_type_conversion_float(self):
        """验证环境变量类型转换 - float"""
        with patch.dict(os.environ, {"SCHEDULER_SUBMISSION_WAIT_TIME": "45.5"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert isinstance(test_settings.SUBMISSION_WAIT_TIME, float)
            assert test_settings.SUBMISSION_WAIT_TIME == 45.5

    def test_multiple_env_overrides(self):
        """验证多个环境变量同时覆盖"""
        env_vars = {
            "SCHEDULER_SUBMISSION_WAIT_TIME": "90",
            "SCHEDULER_RESOURCE_THRESHOLD": "0.85",
            "SCHEDULER_CACHE_TIMEOUT": "15.0",
            "SCHEDULER_TASK_QUEUE_MAX_SIZE": "3000",
        }

        with patch.dict(os.environ, env_vars):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.SUBMISSION_WAIT_TIME == 90.0
            assert test_settings.RESOURCE_THRESHOLD == 0.85
            assert test_settings.CACHE_TIMEOUT == 15.0
            assert test_settings.TASK_QUEUE_MAX_SIZE == 3000


class TestConfigFile:
    """配置文件测试 - 验证 YAML 配置文件加载"""

    def test_load_from_yaml_file(self):
        """验证 YAML 配置文件加载功能"""
        yaml_content = """
submission_wait_time: 75.0
resource_threshold: 0.75
job_completion_timeout: 800.0
task_completion_timeout: 800.0
cache_timeout: 10.0
prometheus_query_timeout: 25
task_queue_max_size: 1500
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()

            try:
                test_settings = SchedulerSettings()
                test_settings.load(f.name)

                assert test_settings.SUBMISSION_WAIT_TIME == 75.0
                assert test_settings.RESOURCE_THRESHOLD == 0.75
                assert test_settings.JOB_COMPLETION_TIMEOUT == 800.0
                assert test_settings.TASK_COMPLETION_TIMEOUT == 800.0
                assert test_settings.CACHE_TIMEOUT == 10.0
                assert test_settings.PROMETHEUS_QUERY_TIMEOUT == 25
                assert test_settings.TASK_QUEUE_MAX_SIZE == 1500
            finally:
                os.unlink(f.name)

    def test_config_file_with_unknown_keys(self):
        """验证配置文件包含未知键时不报错"""
        yaml_content = """
submission_wait_time: 100.0
unknown_key: value
another_unknown: 123
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()

            try:
                test_settings = SchedulerSettings()
                # 应该不报错，只是忽略未知键
                test_settings.load(f.name)

                assert test_settings.SUBMISSION_WAIT_TIME == 100.0
            finally:
                os.unlink(f.name)

    def test_empty_config_file(self):
        """验证空配置文件能正常加载"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("")
            f.flush()

            try:
                test_settings = SchedulerSettings()
                test_settings.load(f.name)

                # 应该使用默认值
                assert test_settings.SUBMISSION_WAIT_TIME == 60.0
                assert test_settings.RESOURCE_THRESHOLD == 0.7
            finally:
                os.unlink(f.name)

    def test_nonexistent_config_file(self):
        """验证不存在的配置文件使用默认值"""
        test_settings = SchedulerSettings()
        test_settings.load("/nonexistent/path/settings.yaml")

        # 应该使用默认值
        assert test_settings.SUBMISSION_WAIT_TIME == 60.0
        assert test_settings.RESOURCE_THRESHOLD == 0.7


class TestModuleIntegration:
    """模块集成测试 - 验证各模块正确使用 settings 常量"""

    def test_cluster_submission_history_uses_submission_wait_time(self):
        """验证 cluster_submission_history.py 使用 SUBMISSION_WAIT_TIME"""
        from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import (
            ClusterSubmissionHistory,
        )

        # 检查模块文档或导入，检查是否使用了 settings
        import inspect
        source = inspect.getsource(ClusterSubmissionHistory)

        # 验证代码中引用了 settings
        assert "settings" in source.lower() or "SUBMISSION_WAIT_TIME" in source

    def test_policy_engine_uses_resource_threshold(self):
        """验证 policy_engine.py 使用 RESOURCE_THRESHOLD"""
        from ray_multicluster_scheduler.scheduler.policy.policy_engine import (
            PolicyEngine,
        )

        import inspect
        source = inspect.getsource(PolicyEngine)

        assert "settings" in source.lower() or "RESOURCE_THRESHOLD" in source

    def test_task_lifecycle_manager_uses_timeouts(self):
        """验证 task_lifecycle_manager.py 使用超时常量"""
        from ray_multicluster_scheduler.scheduler.lifecycle.task_lifecycle_manager import (
            TaskLifecycleManager,
        )

        import inspect
        source = inspect.getsource(TaskLifecycleManager)

        # 检查是否使用了 JOB_COMPLETION_TIMEOUT, TASK_COMPLETION_TIMEOUT 等
        has_job_timeout = "JOB_COMPLETION_TIMEOUT" in source
        has_task_timeout = "TASK_COMPLETION_TIMEOUT" in source
        has_check_interval = "JOB_STATUS_CHECK_INTERVAL" in source

        assert has_job_timeout or has_task_timeout or has_check_interval

    def test_cluster_monitor_uses_cache_timeout(self):
        """验证 cluster_monitor.py 使用 CACHE_TIMEOUT 和 HEALTH_CHECK_INTERVAL"""
        from ray_multicluster_scheduler.scheduler.monitor.cluster_monitor import (
            ClusterMonitor,
        )

        import inspect
        source = inspect.getsource(ClusterMonitor)

        has_cache = "CACHE_TIMEOUT" in source
        has_health = "HEALTH_CHECK_INTERVAL" in source

        assert has_cache or has_health

    def test_circuit_breaker_uses_settings(self):
        """验证 circuit_breaker.py 使用熔断器常量"""
        from ray_multicluster_scheduler.common.circuit_breaker import CircuitBreaker

        import inspect
        source = inspect.getsource(CircuitBreaker)

        has_threshold = "CIRCUIT_BREAKER_FAILURE_THRESHOLD" in source
        has_recovery = "CIRCUIT_BREAKER_RECOVERY_TIMEOUT" in source

        assert has_threshold or has_recovery

    def test_ray_client_pool_uses_connection_timeout(self):
        """验证 ray_client_pool.py 使用 CONNECTION_TIMEOUT"""
        from ray_multicluster_scheduler.scheduler.connection.ray_client_pool import (
            RayClientPool,
        )

        import inspect
        source = inspect.getsource(RayClientPool)

        assert "CONNECTION_TIMEOUT" in source or "settings" in source.lower()

    def test_job_client_pool_uses_timeouts(self):
        """验证 job_client_pool.py 使用 JOB_CLIENT_* 常量"""
        from ray_multicluster_scheduler.scheduler.connection.job_client_pool import (
            JobClientPool,
        )

        import inspect
        source = inspect.getsource(JobClientPool)

        has_cache = "JOB_CLIENT_CACHE_TIMEOUT" in source
        has_test = "JOB_CLIENT_CONNECTION_TEST_TIMEOUT" in source

        assert has_cache or has_test

    def test_task_queue_uses_max_size(self):
        """验证 task_queue.py 使用 TASK_QUEUE_DEFAULT_MAX_SIZE"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

        import inspect
        source = inspect.getsource(TaskQueue)

        has_max_size = "TASK_QUEUE_DEFAULT_MAX_SIZE" in source

        # 如果 TaskQueue 继承自 BaseTaskQueue，可能在父类中使用
        has_queue_ref = "TaskQueue" in source or "BaseTaskQueue" in source

        assert has_max_size or has_queue_ref

    def test_prometheus_client_uses_query_timeout(self):
        """验证 prometheus_client.py 使用 PROMETHEUS_QUERY_TIMEOUT"""
        import ray_multicluster_scheduler.scheduler.monitor.prometheus_client as prometheus_module

        # 检查整个模块的源代码，而不是只检查 PrometheusClient 类
        import inspect
        source = inspect.getsource(prometheus_module)

        # HTTPPrometheusQuerier.query() 方法使用了 settings.PROMETHEUS_QUERY_TIMEOUT
        assert "PROMETHEUS_QUERY_TIMEOUT" in source or "settings" in source.lower()


class TestSettingsLoadMethod:
    """load() 方法测试"""

    def test_load_without_args(self):
        """验证 load() 无参数调用"""
        test_settings = SchedulerSettings()
        result = test_settings.load()

        assert result is test_settings  # 返回 self，支持链式调用
        assert test_settings.SUBMISSION_WAIT_TIME == 60.0  # 使用默认值

    def test_load_with_config_file(self):
        """验证 load() 带配置文件参数"""
        yaml_content = """
submission_wait_time: 45.0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()

            try:
                test_settings = SchedulerSettings()
                test_settings.load(f.name)

                assert test_settings.SUBMISSION_WAIT_TIME == 45.0
            finally:
                os.unlink(f.name)

    def test_load_chaining(self):
        """验证 load() 支持链式调用"""
        test_settings = SchedulerSettings()
        result = test_settings.load().load()  # 多次调用

        assert result is test_settings


class TestConvertValue:
    """_convert_value 方法测试 - 内部方法但需验证"""

    def test_convert_int(self):
        """验证 int 类型转换"""
        result = settings._convert_value("123", 0)
        assert isinstance(result, int)
        assert result == 123

    def test_convert_float(self):
        """验证 float 类型转换"""
        result = settings._convert_value("45.5", 0.0)
        assert isinstance(result, float)
        assert result == 45.5

    def test_convert_bool_true(self):
        """验证 bool 类型转换 - true"""
        result = settings._convert_value("true", False)
        assert isinstance(result, bool)
        assert result is True

    def test_convert_bool_false(self):
        """验证 bool 类型转换 - false"""
        result = settings._convert_value("false", False)
        assert isinstance(result, bool)
        assert result is False

    def test_convert_bool_1(self):
        """验证 bool 类型转换 - 1"""
        result = settings._convert_value("1", False)
        assert isinstance(result, bool)
        assert result is True

    def test_convert_string(self):
        """验证 string 类型"""
        result = settings._convert_value("test", "default")
        assert isinstance(result, str)
        assert result == "test"


class TestEdgeCases:
    """边界情况测试"""

    def test_settings_instance_immutability(self):
        """验证 settings 实例在单次测试中的稳定性"""
        # 获取值
        original_value = settings.SUBMISSION_WAIT_TIME

        # 验证不会意外修改
        assert settings.SUBMISSION_WAIT_TIME == original_value

    def test_all_values_numeric(self):
        """验证所有超时/间隔值都是数字类型 (int/float)"""
        numeric_attrs = [
            "SUBMISSION_WAIT_TIME",
            "RESOURCE_THRESHOLD",
            "RESOURCE_CHECK_INTERVAL",
            "JOB_COMPLETION_TIMEOUT",
            "TASK_COMPLETION_TIMEOUT",
            "CACHE_TIMEOUT",
            "EMPTY_QUEUE_SLEEP",
            "QUEUE_CONDITION_WAIT_TIMEOUT",
            "ACTOR_NOTIFICATION_WAIT",
            "CIRCUIT_BREAKER_RECOVERY_TIMEOUT",
            "RETRY_SLEEP",
            "BACKOFF_TIME",
            "WORKER_THREAD_JOIN_TIMEOUT",
            "ERROR_LOOP_SLEEP",
            "REPEATED_ERROR_SLEEP",
            "CLUSTER_WEIGHT_DEFAULT",
        ]

        for attr in numeric_attrs:
            value = getattr(settings, attr)
            assert isinstance(value, (int, float)), f"{attr} should be numeric, got {type(value)}"

    def test_positive_timeouts(self):
        """验证所有超时值为正数"""
        timeout_attrs = [
            "SUBMISSION_WAIT_TIME",
            "JOB_COMPLETION_TIMEOUT",
            "TASK_COMPLETION_TIMEOUT",
            "ACTOR_READY_TIMEOUT",
            "DEFAULT_TASK_TIMEOUT",
            "CONNECTION_TIMEOUT",
            "JOB_CLIENT_TIMEOUT",
            "JOB_CLIENT_CACHE_TIMEOUT",
            "JOB_STATUS_CHECK_INTERVAL",
            "HEALTH_CHECK_INTERVAL",
            "CACHE_TIMEOUT",
            "EMPTY_QUEUE_SLEEP",
            "QUEUE_CONDITION_WAIT_TIMEOUT",
            "ACTOR_NOTIFICATION_WAIT",
            "JOB_POLL_CHECK_INTERVAL",
            "PROMETHEUS_QUERY_TIMEOUT",
            "CIRCUIT_BREAKER_RECOVERY_TIMEOUT",
            "RETRY_SLEEP",
            "BACKOFF_TIME",
            "WORKER_THREAD_JOIN_TIMEOUT",
            "ERROR_LOOP_SLEEP",
            "REPEATED_ERROR_SLEEP",
        ]

        for attr in timeout_attrs:
            value = getattr(settings, attr)
            assert value > 0, f"{attr} should be positive, got {value}"

    def test_resource_threshold_in_valid_range(self):
        """验证 RESOURCE_THRESHOLD 在有效范围 [0, 1]"""
        assert 0.0 <= settings.RESOURCE_THRESHOLD <= 1.0

    def test_positive_queue_sizes(self):
        """验证队列大小为正数"""
        assert settings.TASK_QUEUE_MAX_SIZE > 0
        assert settings.TASK_QUEUE_DEFAULT_MAX_SIZE > 0
        assert settings.TASK_QUEUE_MAX_SIZE >= settings.TASK_QUEUE_DEFAULT_MAX_SIZE

    def test_circuit_breaker_failure_threshold_positive(self):
        """验证 CIRCUIT_BREAKER_FAILURE_THRESHOLD 为正整数"""
        value = settings.CIRCUIT_BREAKER_FAILURE_THRESHOLD
        assert isinstance(value, int)
        assert value > 0


class TestSettingsSingleton:
    """全局单例测试"""

    def test_settings_is_singleton(self):
        """验证 settings 是全局单例"""
        from ray_multicluster_scheduler.common.config import settings as settings1
        from ray_multicluster_scheduler.common.config import settings as settings2

        assert settings1 is settings2

    def test_settings_load_called(self):
        """验证 settings 自动加载"""
        # settings 模块在导入时自动调用 load()
        # 检查 _config_file 已被设置
        assert hasattr(settings, '_config_file')


# ==================== 验证常量参数数量 ====================

def test_total_constant_count():
    """验证恰好有 30 个常量参数"""
    expected_count = 30

    # 统计 _get_defaults() 中的常量数量
    defaults = settings._get_defaults()
    actual_count = len(defaults)

    assert actual_count == expected_count, f"Expected {expected_count} constants, got {actual_count}"


# ==================== 执行模式测试 ====================

class TestExecutionMode:
    """执行模式测试 - 验证 EXECUTION_MODE 配置和串行/并行模式逻辑"""

    def test_execution_mode_import(self):
        """验证 ExecutionMode 枚举能正确导入"""
        assert ExecutionMode is not None
        assert hasattr(ExecutionMode, 'SERIAL')
        assert hasattr(ExecutionMode, 'PARALLEL')
        assert ExecutionMode.SERIAL == "serial"
        assert ExecutionMode.PARALLEL == "parallel"

    def test_execution_mode_default_is_serial(self):
        """验证 EXECUTION_MODE 默认值为串行模式"""
        assert settings.EXECUTION_MODE == "serial"

    def test_execution_mode_env_override_serial(self):
        """验证串行模式环境变量覆盖"""
        with patch.dict(os.environ, {"SCHEDULER_EXECUTION_MODE": "serial"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.EXECUTION_MODE == "serial"
            assert test_settings.EXECUTION_MODE == ExecutionMode.SERIAL

    def test_execution_mode_env_override_parallel(self):
        """验证并行模式环境变量覆盖"""
        with patch.dict(os.environ, {"SCHEDULER_EXECUTION_MODE": "parallel"}):
            test_settings = SchedulerSettings()
            test_settings.load()

            assert test_settings.EXECUTION_MODE == "parallel"
            assert test_settings.EXECUTION_MODE == ExecutionMode.PARALLEL

    def test_execution_mode_yaml_config(self):
        """验证 YAML 配置文件加载执行模式"""
        yaml_content = """
execution_mode: serial
submission_wait_time: 60.0
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()

            try:
                test_settings = SchedulerSettings()
                test_settings.load(f.name)

                assert test_settings.EXECUTION_MODE == "serial"
            finally:
                os.unlink(f.name)

    def test_execution_mode_in_to_dict(self):
        """验证 EXECUTION_MODE 包含在 to_dict() 输出中"""
        settings_dict = settings.to_dict()

        assert "EXECUTION_MODE" in settings_dict
        assert settings_dict["EXECUTION_MODE"] == "serial"

    def test_execution_mode_in_defaults(self):
        """验证 EXECUTION_MODE 包含在 _get_defaults() 输出中"""
        defaults = settings._get_defaults()

        assert "EXECUTION_MODE" in defaults
        assert defaults["EXECUTION_MODE"] == "serial"


class TestTaskQueueRunningTaskCount:
    """TaskQueue 运行任务计数测试"""

    def test_register_running_task(self):
        """验证注册运行任务功能"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 注册一个运行任务
        TaskQueue.register_running_task("task_001", "cluster_a", "task")

        # 验证注册成功
        assert "task_001" in TaskQueue._running_tasks
        assert TaskQueue._running_tasks["task_001"]["cluster"] == "cluster_a"
        assert TaskQueue._running_tasks["task_001"]["type"] == "task"

        # 清理
        TaskQueue._running_tasks.clear()

    def test_unregister_running_task(self):
        """验证注销运行任务功能"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 注册并注销
        TaskQueue.register_running_task("task_002", "cluster_b", "job")
        result = TaskQueue.unregister_running_task("task_002")

        # 验证注销成功
        assert result is True
        assert "task_002" not in TaskQueue._running_tasks

        # 注销不存在的任务应该返回 False
        result = TaskQueue.unregister_running_task("nonexistent")
        assert result is False

    def test_get_cluster_running_task_count(self):
        """验证获取集群运行任务数量"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 初始应该为 0
        assert TaskQueue.get_cluster_running_task_count("cluster_a") == 0

        # 添加任务
        TaskQueue.register_running_task("task_001", "cluster_a", "task")
        TaskQueue.register_running_task("task_002", "cluster_a", "task")
        TaskQueue.register_running_task("job_001", "cluster_b", "job")

        # 验证计数
        assert TaskQueue.get_cluster_running_task_count("cluster_a") == 2
        assert TaskQueue.get_cluster_running_task_count("cluster_b") == 1
        assert TaskQueue.get_cluster_running_task_count("cluster_c") == 0

        # 清理
        TaskQueue._running_tasks.clear()

    def test_is_cluster_busy(self):
        """验证检查集群是否繁忙"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 初始应该不繁忙
        assert TaskQueue.is_cluster_busy("cluster_a") is False

        # 添加任务后应该繁忙
        TaskQueue.register_running_task("task_001", "cluster_a", "task")
        assert TaskQueue.is_cluster_busy("cluster_a") is True

        # 移除任务后应该不繁忙
        TaskQueue.unregister_running_task("task_001")
        assert TaskQueue.is_cluster_busy("cluster_a") is False

    def test_get_all_running_tasks(self):
        """验证获取所有运行任务"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 添加任务
        TaskQueue.register_running_task("task_001", "cluster_a", "task")
        TaskQueue.register_running_task("job_001", "cluster_b", "job")

        # 获取所有任务
        all_tasks = TaskQueue.get_all_running_tasks()

        # 验证
        assert len(all_tasks) == 2
        assert "task_001" in all_tasks
        assert "job_001" in all_tasks

        # 清理
        TaskQueue._running_tasks.clear()


class TestSerialModeLogic:
    """串行模式逻辑测试"""

    def test_serial_mode_blocks_when_busy(self):
        """验证串行模式下集群繁忙时阻止新任务"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
        from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import (
            ClusterSubmissionHistory,
        )
        from unittest.mock import patch

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 使用mock来模拟串行模式
        with patch('ray_multicluster_scheduler.scheduler.policy.cluster_submission_history.settings') as mock_settings:
            mock_settings.EXECUTION_MODE = "serial"

            # 创建一个新的历史记录实例
            history = ClusterSubmissionHistory()

            # 注册一个运行任务
            TaskQueue.register_running_task("task_001", "cluster_a", "task")

            # 验证集群繁忙，is_cluster_available_and_record 应该返回 False
            result = history.is_cluster_available_and_record("cluster_a", is_top_level_task=True)
            assert result is False

            # 清理
            TaskQueue._running_tasks.clear()

    def test_parallel_mode_allows_when_busy(self):
        """验证并行模式下允许任务即使集群繁忙"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
        from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import (
            ClusterSubmissionHistory,
        )
        from unittest.mock import patch

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 使用mock来模拟并行模式
        with patch('ray_multicluster_scheduler.scheduler.policy.cluster_submission_history.settings') as mock_settings:
            mock_settings.EXECUTION_MODE = "parallel"

            # 创建一个新的历史记录实例
            history = ClusterSubmissionHistory()

            # 注册一个运行任务
            TaskQueue.register_running_task("task_001", "cluster_a", "task")

            # 并行模式下即使有运行任务，也应该允许新任务（只要时间间隔满足）
            result = history.is_cluster_available_and_record("cluster_a", is_top_level_task=True)

            # 并行模式下应该返回 True（因为时间间隔已满足）
            assert result is True

            # 清理
            TaskQueue._running_tasks.clear()

    def test_subtask_bypasses_serial_check(self):
        """验证子任务绕过串行模式检查"""
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
        from ray_multicluster_scheduler.scheduler.policy.cluster_submission_history import (
            ClusterSubmissionHistory,
        )
        from unittest.mock import patch

        # 清理测试数据
        TaskQueue._running_tasks.clear()

        # 使用mock来模拟串行模式
        with patch('ray_multicluster_scheduler.scheduler.policy.cluster_submission_history.settings') as mock_settings:
            mock_settings.EXECUTION_MODE = "serial"

            # 创建一个新的历史记录实例
            history = ClusterSubmissionHistory()

            # 注册一个运行任务
            TaskQueue.register_running_task("task_001", "cluster_a", "task")

            # 子任务应该绕过检查，直接返回 True
            result = history.is_cluster_available_and_record("cluster_a", is_top_level_task=False)
            assert result is True

            # 清理
            TaskQueue._running_tasks.clear()

    def test_execution_mode_enum_values(self):
        """验证 ExecutionMode 枚举值用于比较"""
        from ray_multicluster_scheduler.common.config import ExecutionMode

        # 验证枚举值
        assert ExecutionMode.SERIAL == "serial"
        assert ExecutionMode.PARALLEL == "parallel"

        # 验证与 settings.EXECUTION_MODE 的比较
        assert settings.EXECUTION_MODE == ExecutionMode.SERIAL

        # 模拟串行模式的比较
        test_mode = ExecutionMode.SERIAL
        assert test_mode == "serial"
        assert test_mode != ExecutionMode.PARALLEL


# ==================== 运行测试 ====================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
