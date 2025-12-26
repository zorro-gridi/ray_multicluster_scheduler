"""
Network utilities for the ray multicluster scheduler.
Provides common functions for address validation, connection testing, and network operations.
"""

import socket
import logging
from typing import Optional


def validate_remote_address(address: str) -> bool:
    """
    Validate that the address is not a local address.

    Args:
        address: The address string to validate (e.g., 'ray://192.168.5.2:32546',
                 'http://192.168.5.2:8265', '192.168.5.2:32546')

    Returns:
        bool: True if the address is valid and not local, False otherwise
    """
    logger = logging.getLogger(__name__)

    # 解析地址以获取主机名/IP
    if address.startswith('ray://'):
        host_port = address[6:]  # 移除'ray://'
    elif address.startswith('http://'):
        host_port = address[7:]  # 移除'http://'
    elif address.startswith('https://'):
        host_port = address[8:]  # 移除'https://'
    else:
        host_port = address

    # 分离主机和端口
    if ':' in host_port:
        host = host_port.split(':')[0]
    else:
        host = host_port

    # 检查是否为本地地址
    try:
        ip = socket.gethostbyname(host)
        # 检查IP是否为本地地址
        if ip.startswith('127.'):
            logger.warning(f"Address {address} resolves to localhost ({ip}), which may not be accessible from this context")
            return False
    except socket.gaierror:
        logger.warning(f"Could not resolve hostname: {host}")
        return False

    return True


def test_http_connection(address: str, timeout: int = 5) -> bool:
    """
    Test if the HTTP address is accessible.

    Args:
        address: The HTTP address to test (e.g., 'http://192.168.5.2:8265')
        timeout: Connection timeout in seconds (default: 5)

    Returns:
        bool: True if the address is accessible, False otherwise
    """
    try:
        import requests
        # 尝试连接到HTTP端点
        response = requests.get(f"{address}/api/jobs/", timeout=timeout)
        # 如果返回200或401（认证）等，说明端口是通的
        return response.status_code in [200, 401, 403, 405]
    except:
        # 如果requests不可用或连接失败，返回False
        return False