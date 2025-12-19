"""
Configuration management for the ray multicluster scheduler.
"""

import os
import yaml
from typing import List, Dict, Any, Optional
from pathlib import Path
from ray_multicluster_scheduler.common.model import ClusterMetadata


# Default configuration
DEFAULT_CONFIG = {
    "health_check_interval": 30,  # seconds
    "task_queue_max_size": 1000,
    "default_task_timeout": 300,  # seconds
}


class ConfigManager:
    """Manages scheduler configuration from environment variables or defaults."""

    def __init__(self, config_file_path: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            config_file_path (str, optional): Path to the cluster configuration YAML file.
                If not provided, the system will attempt to locate the configuration file
                in common locations or fall back to default configuration.
        """
        self.config = DEFAULT_CONFIG.copy()
        self.config_file_path = config_file_path
        self._load_from_env()

    def _load_from_env(self):
        """Load configuration from environment variables."""
        for key in DEFAULT_CONFIG:
            env_key = f"SCHEDULER_{key.upper()}"
            env_value = os.environ.get(env_key)
            if env_value is not None:
                # Try to convert to appropriate type
                try:
                    if isinstance(DEFAULT_CONFIG[key], int):
                        self.config[key] = int(env_value)
                    elif isinstance(DEFAULT_CONFIG[key], float):
                        self.config[key] = float(env_value)
                    else:
                        self.config[key] = env_value
                except ValueError:
                    self.config[key] = env_value

    def _find_config_file(self) -> str:
        """Find the cluster configuration file."""
        # Use provided config file path if available
        if self.config_file_path:
            return self.config_file_path

        # Get config file path from environment variable
        config_file = os.environ.get("CLUSTER_CONFIG_FILE")

        # If no environment variable is set, try to find the config file in common locations
        if not config_file:
            # Try to find the config file in the package directory first
            package_dir = Path(__file__).parent.parent
            possible_paths = [
                Path.cwd() / "clusters.yaml",   # In the current working directory
                package_dir / "clusters.yaml",  # In the package root
                Path.home() / ".ray_scheduler" / "clusters.yaml",  # In user home directory
                Path("/etc/ray_scheduler/clusters.yaml"),  # In system-wide config directory
            ]

            for path in possible_paths:
                if path.exists():
                    config_file = str(path)
                    break

            # If still not found, use the default package directory path
            if not config_file:
                config_file = str(package_dir / "clusters.yaml")

        return config_file

    def get_cluster_configs(self) -> List[ClusterMetadata]:
        """Get configured cluster metadata from YAML config file."""
        config_file = self._find_config_file()

        # Check if config file exists
        if not os.path.exists(config_file):
            # Fallback to default configuration if config file doesn't exist
            print(f"Warning: Config file {config_file} not found, using default configuration")
            return [
                ClusterMetadata(
                    name="centos",
                    head_address="192.168.5.7:32546",
                    dashboard="http://192.168.5.7:31591",
                    prefer=False,
                    weight=1.0,
                    home_dir="/home/zorro",  # 从配置文件读取的默认值
                    conda="ts",  # 添加conda属性
                    tags=["linux", "x86_64"]
                ),
                ClusterMetadata(
                    name="mac",
                    head_address="192.168.5.2:32546",
                    dashboard="http://192.168.5.2:8265",
                    prefer=True,
                    weight=1.2,
                    home_dir="/Users/zorro",  # 从配置文件读取的默认值
                    conda="k8s",  # 添加conda属性
                    tags=["macos", "arm64"]
                )
            ]

        # Load cluster configurations from YAML file
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)

            clusters = []
            for cluster_data in config.get('clusters', []):
                clusters.append(ClusterMetadata(
                    name=cluster_data['name'],
                    head_address=cluster_data['head_address'],
                    dashboard=cluster_data['dashboard'],
                    prefer=cluster_data.get('prefer', False),
                    weight=cluster_data.get('weight', 1.0),
                    home_dir=cluster_data.get('home_dir'),  # 从配置文件读取，没有默认值
                    conda=cluster_data.get('conda'),  # 从配置文件读取conda属性
                    tags=cluster_data.get('tags', [])
                ))

            return clusters

        except Exception as e:
            print(f"❌ Error loading cluster configurations from {config_file}: {e}")
            # Return default configuration if loading fails
            import traceback
            traceback.print_exe()
            raise e