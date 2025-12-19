#!/usr/bin/env python3
"""
Test script to verify CatBoost job submission to Ray Multicluster Scheduler
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


def test_catboost_job_submission():
    """
    Test the CatBoost job submission to the scheduler
    """
    logger.info("Testing CatBoost job submission to Ray Multicluster Scheduler...")

    try:
        # Import and run the submission script
        from demo.submit_catboost_job import main as submit_main

        # Run the submission
        submit_main()

        logger.info("CatBoost job submission test completed successfully!")
        return True

    except Exception as e:
        logger.error(f"CatBoost job submission test failed: {e}")
        return False


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


if __name__ == "__main__":
    logger.info("Starting CatBoost scheduler tests...")

    # Test 1: Direct execution of simplified CatBoost script
    logger.info("=== Test 1: Direct execution of simplified CatBoost script ===")
    success1 = test_simplified_catboost_directly()

    # Test 2: Submission through scheduler
    logger.info("=== Test 2: Submission through scheduler ===")
    success2 = test_catboost_job_submission()

    # Summary
    logger.info("=== Test Summary ===")
    logger.info(f"Direct execution test: {'PASSED' if success1 else 'FAILED'}")
    logger.info(f"Scheduler submission test: {'PASSED' if success2 else 'FAILED'}")

    if success1 and success2:
        logger.info("All tests passed!")
        sys.exit(0)
    else:
        logger.error("Some tests failed!")
        sys.exit(1)