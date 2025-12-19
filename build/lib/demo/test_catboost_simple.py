#!/usr/bin/env python3
"""
Simple test script to verify CatBoost job functionality without connecting to Ray clusters
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_simplified_catboost_directly():
    """
    Test the simplified CatBoost script directly
    """
    logger.info("Testing simplified CatBoost script directly...")

    try:
        # Import and run the simplified CatBoost script
        from demo.catboost_simplified import main as catboost_main

        # Run the simplified CatBoost training
        result = catboost_main()

        logger.info(f"Simplified CatBoost test completed with result: {result}")
        return True

    except Exception as e:
        logger.error(f"Simplified CatBoost test failed: {e}")
        return False


def test_scheduler_components():
    """
    Test that we can import scheduler components without connecting to clusters
    """
    logger.info("Testing scheduler component imports...")

    try:
        # Test importing scheduler components
        from ray_multicluster_scheduler.app.client_api.submit_task import initialize_scheduler
        from ray_multicluster_scheduler.scheduler.scheduler_core.task_lifecycle import TaskLifecycleManager
        from ray_multicluster_scheduler.control_plane.config import ConfigManager
        from ray_multicluster_scheduler.scheduler.cluster.cluster_metadata import ClusterMetadataManager
        from ray_multicluster_scheduler.scheduler.health.health_checker import HealthChecker
        from ray_multicluster_scheduler.scheduler.cluster.cluster_registry import ClusterRegistry
        from ray_multicluster_scheduler.scheduler.policy.policy_engine import PolicyEngine
        from ray_multicluster_scheduler.scheduler.queue.task_queue import TaskQueue
        from ray_multicluster_scheduler.scheduler.queue.backpressure_controller import BackpressureController
        from ray_multicluster_scheduler.scheduler.connection.connection_lifecycle import ConnectionLifecycleManager
        from ray_multicluster_scheduler.scheduler.scheduler_core.dispatcher import Dispatcher
        from ray_multicluster_scheduler.scheduler.scheduler_core.result_collector import ResultCollector
        from ray_multicluster_scheduler.common.circuit_breaker import ClusterCircuitBreakerManager

        logger.info("All scheduler components imported successfully!")
        return True

    except Exception as e:
        logger.error(f"Scheduler component import test failed: {e}")
        return False


if __name__ == "__main__":
    logger.info("Starting simple CatBoost tests...")

    # Test 1: Direct execution of simplified CatBoost script
    logger.info("=== Test 1: Direct execution of simplified CatBoost script ===")
    success1 = test_simplified_catboost_directly()

    # Test 2: Scheduler component imports
    logger.info("=== Test 2: Scheduler component imports ===")
    success2 = test_scheduler_components()

    # Summary
    logger.info("=== Test Summary ===")
    logger.info(f"Direct execution test: {'PASSED' if success1 else 'FAILED'}")
    logger.info(f"Component import test: {'PASSED' if success2 else 'FAILED'}")

    if success1 and success2:
        logger.info("All tests passed!")
        sys.exit(0)
    else:
        logger.error("Some tests failed!")
        sys.exit(1)