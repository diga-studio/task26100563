"""分类升级策略（唯一保留的策略）。"""

CONFIDENTIAL = "Confidential"
STORAGE_LIMIT_GB = 25.0


def is_violation(storage_gb: float, classification: str) -> bool:
    """storage_gb 大于 25 且 classification 不是 Confidential 即违规。"""
    return storage_gb > STORAGE_LIMIT_GB and classification != CONFIDENTIAL
