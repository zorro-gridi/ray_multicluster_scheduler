"""
Exception hierarchy for the ray multicluster scheduler.
"""

class SchedulerError(Exception):
    """Base exception for all scheduler-related errors."""
    pass


class NoHealthyClusterError(SchedulerError):
    """Raised when no healthy clusters are available for scheduling."""
    pass


class TaskSubmissionError(SchedulerError):
    """Raised when a task fails to be submitted to a cluster."""
    pass


class PolicyEvaluationError(SchedulerError):
    """Raised when a scheduling policy fails to evaluate."""
    pass


class ClusterConnectionError(SchedulerError):
    """Raised when connection to a cluster fails."""
    pass


class ConfigurationError(SchedulerError):
    """Raised when there is an error in scheduler configuration."""
    pass