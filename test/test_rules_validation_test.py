"""
test_rules_validation_test.py

单元测试模块，用于验证 calculate_cluster_metrics 函数的正确性。
"""

import pytest

from ray_multicluster_scheduler.test_rules_validation import calculate_cluster_metrics


class TestCalculateClusterMetrics:
    """测试 calculate_cluster_metrics 函数的各类场景。"""

    def test_healthy_cluster(self):
        """
        测试健康集群场景

        验证当 CPU 和内存使用率都较低时，返回 healthy 状态。
        """
        result = calculate_cluster_metrics("test-cluster", 0.3, 0.4, 5)

        assert result["score"] < 0.5
        assert result["status"] == "healthy"
        assert len(result["recommendations"]) == 0

    def test_warning_cluster(self):
        """
        测试警告状态集群场景

        验证当资源使用率中等时，返回 warning 状态并提供优化建议。
        """
        result = calculate_cluster_metrics("test-cluster", 0.7, 0.75, 50)

        assert 0.5 <= result["score"] < 0.8
        assert result["status"] == "warning"
        assert len(result["recommendations"]) > 0

    def test_critical_cluster(self):
        """
        测试严重状态集群场景

        验证当资源使用率很高时，返回 critical 状态并提供紧急建议。
        """
        result = calculate_cluster_metrics("test-cluster", 0.9, 0.95, 120)

        assert result["score"] >= 0.8
        assert result["status"] == "critical"
        assert len(result["recommendations"]) >= 2

    def test_invalid_cpu_usage(self):
        """
        测试 CPU 使用率参数验证

        验证当 CPU 使用率超出有效范围时，函数抛出 ValueError。
        """
        with pytest.raises(ValueError) as exc_info:
            calculate_cluster_metrics("test-cluster", 1.5, 0.5, 10)

        assert "CPU 使用率" in str(exc_info.value)
        assert "0.0-1.0" in str(exc_info.value)

    def test_invalid_memory_usage(self):
        """
        测试内存使用率参数验证

        验证当内存使用率超出有效范围时，函数抛出 ValueError。
        """
        with pytest.raises(ValueError) as exc_info:
            calculate_cluster_metrics("test-cluster", 0.5, -0.1, 10)

        assert "内存使用率" in str(exc_info.value)
        assert "0.0-1.0" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
