"""
Logging configuration for the ray multicluster scheduler.
"""

import logging
import os


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structured logging for the scheduler."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            # TODO: Add file handler for production use
        ]
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name."""
    return logging.getLogger(name)