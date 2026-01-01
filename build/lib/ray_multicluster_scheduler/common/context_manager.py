"""
Context manager for maintaining cluster context across task submissions.
This module provides thread-local storage for tracking the current cluster context,
ensuring that child tasks and actors inherit the same cluster as their parent job.
"""

import threading


class ClusterContextManager:
    """Manages cluster context across threads using thread-local storage."""
    
    _local = threading.local()
    
    @classmethod
    def set_current_cluster(cls, cluster_name: str):
        """Set the current cluster context for the current thread."""
        cls._local.current_cluster = cluster_name
    
    @classmethod
    def get_current_cluster(cls) -> str:
        """Get the current cluster context for the current thread."""
        return getattr(cls._local, 'current_cluster', None)
    
    @classmethod
    def clear_current_cluster(cls):
        """Clear the current cluster context for the current thread."""
        if hasattr(cls._local, 'current_cluster'):
            delattr(cls._local, 'current_cluster')