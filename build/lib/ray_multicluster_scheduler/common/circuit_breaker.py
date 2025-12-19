"""
Circuit breaker implementation for handling cluster failures.
"""

import time
from enum import Enum
from typing import Dict, Optional
from ray_multicluster_scheduler.common.logging import get_logger

logger = get_logger(__name__)


class CircuitState(Enum):
    """Enumeration of circuit breaker states."""
    CLOSED = 1
    OPEN = 2
    HALF_OPEN = 3


class CircuitBreaker:
    """Circuit breaker for handling cluster failures."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        """
        Initialize the circuit breaker.

        Args:
            failure_threshold: Number of failures before opening the circuit
            recovery_timeout: Time in seconds before attempting to close the circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = CircuitState.CLOSED

    def call(self, func, *args, **kwargs):
        """
        Call a function through the circuit breaker.

        Args:
            func: Function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function call

        Raises:
            Exception: If the circuit is open or the function raises an exception
        """
        if self.state == CircuitState.OPEN:
            # Check if we should transition to half-open
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                logger.info("Circuit breaker transitioning to HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except Exception as e:
            self.on_failure()
            raise e

    def on_success(self):
        """Handle a successful call."""
        self.failure_count = 0
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            logger.info("Circuit breaker transitioning to CLOSED state after successful call")

    def on_failure(self):
        """Handle a failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker transitioning to OPEN state after {self.failure_count} failures")

    def is_closed(self) -> bool:
        """Check if the circuit breaker is closed."""
        return self.state == CircuitState.CLOSED

    def is_open(self) -> bool:
        """Check if the circuit breaker is open."""
        return self.state == CircuitState.OPEN

    def is_half_open(self) -> bool:
        """Check if the circuit breaker is half-open."""
        return self.state == CircuitState.HALF_OPEN


class ClusterCircuitBreakerManager:
    """Manages circuit breakers for multiple clusters."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        """
        Initialize the cluster circuit breaker manager.

        Args:
            failure_threshold: Number of failures before opening a cluster's circuit
            recovery_timeout: Time in seconds before attempting to close a cluster's circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}

    def get_circuit_breaker(self, cluster_name: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a cluster."""
        if cluster_name not in self.circuit_breakers:
            self.circuit_breakers[cluster_name] = CircuitBreaker(
                failure_threshold=self.failure_threshold,
                recovery_timeout=self.recovery_timeout
            )
        return self.circuit_breakers[cluster_name]

    def call_cluster(self, cluster_name: str, func, *args, **kwargs):
        """
        Call a function for a specific cluster through its circuit breaker.

        Args:
            cluster_name: Name of the cluster
            func: Function to call
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function call

        Raises:
            Exception: If the cluster's circuit is open or the function raises an exception
        """
        circuit_breaker = self.get_circuit_breaker(cluster_name)
        return circuit_breaker.call(func, *args, **kwargs)