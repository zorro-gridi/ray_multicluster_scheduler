# Client API for submitting tasks and actors to the ray multicluster scheduler

from .submit_task import initialize_scheduler, submit_task
from .submit_actor import initialize_scheduler as initialize_actor_scheduler, submit_actor
from .submit_job import initialize_scheduler as initialize_job_scheduler, submit_job, get_job_status, wait_for_all_jobs, stop_job, graceful_shutdown_on_keyboard_interrupt
from .unified_scheduler import (
    UnifiedScheduler,
    get_unified_scheduler,
    initialize_scheduler_environment,
    submit_task as unified_submit_task,
    submit_actor as unified_submit_actor
)

__all__ = [
    "initialize_scheduler",
    "initialize_actor_scheduler",
    "initialize_job_scheduler",
    "submit_task",
    "submit_actor",
    "submit_job",
    "get_job_status",
    "wait_for_all_jobs",
    "stop_job",
    "graceful_shutdown_on_keyboard_interrupt",
    "UnifiedScheduler",
    "get_unified_scheduler",
    "initialize_scheduler_environment",
    "unified_submit_task",
    "unified_submit_actor"
]