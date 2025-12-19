#!/usr/bin/env python3
"""
Submit CatBoost Index Prediction Job to Ray Multicluster Scheduler
Using the new unified scheduler interface
"""

import os
import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Import our unified scheduler interface
from ray_multicluster_scheduler.app.client_api.unified_scheduler import (
    initialize_scheduler_environment,
    submit_task
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def submit_catboost_training_job():
    """
    Submit the CatBoost training job to the multicluster scheduler using unified interface
    """
    logger.info("Submitting CatBoost training job using unified scheduler interface...")

    # Initialize scheduler environment (single call to set up everything)
    task_lifecycle_manager = initialize_scheduler_environment()

    # Define resource requirements for the job
    resource_requirements = {
        "CPU": 2,
        "memory": 4 * 1024 * 1024 * 1024,  # 4GB
    }

    # Define tags for the job
    tags = ["ml", "catboost", "training", "finance"]

    # Submit the job as a task
    try:
        submit_task(
            func=run_catboost_training,  # This imports and runs the simplified CatBoost script
            args=(),
            kwargs={},
            resource_requirements=resource_requirements,
            tags=tags,
            name="catboost_index_prediction_training"
        )
        logger.info("CatBoost training job submitted successfully.")
    except Exception as e:
        logger.error(f"Failed to submit CatBoost training job: {e}")
        raise


def run_catboost_training():
    """
    Wrapper function to run the CatBoost training job.
    This function imports and executes the simplified CatBoost script.
    """
    logger.info("Running CatBoost training job...")

    try:
        # Import the simplified CatBoost training function
        from demo.catboost_simplified import main as catboost_main

        # Execute the training logic
        result = catboost_main()

        logger.info("CatBoost training job completed.")
        return result
    except Exception as e:
        logger.error(f"CatBoost training job failed: {e}")
        raise


def main():
    """
    Main function to submit the CatBoost job to the scheduler
    """
    try:
        submit_catboost_training_job()
        logger.info("Job submission process completed.")
    except Exception as e:
        logger.error(f"Job submission failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()