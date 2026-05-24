# -*- coding: utf-8 -*-
"""
===================================
数据源策略层 - 包初始化
===================================

本包实现策略模式管理多个数据源，实现：
1. 统一的数据获取接口
2. 自动故障切换
3. 防封禁流控策略

数据源优先级：
1. AkshareFetcher (Priority 1) - 来自 akshare 库
2. TushareFetcher (Priority 2) - 来自 tushare 库
"""

from .base import BaseFetcher, DataFetcherManager
from .akshare_fetcher import AkshareFetcher
from .tushare_fetcher import TushareFetcher
from .common import extract_last_segment_standard
from .cache_manager import cache_manager, incremental_updater
from .data_quality import (
    data_validator, data_cleaner, version_manager,
    DataValidator, DataCleaner,
    DataQualityLevel, ValidationError
)
from .data_processor import DataComparator, data_comparator, ValidationResult
from .monitor import monitor, performance_logger, Alert, AlertLevel, AlertType

__all__ = [
    'BaseFetcher',
    'DataFetcherManager',
    'AkshareFetcher',
    'TushareFetcher',
    'cache_manager',
    'incremental_updater',
    'data_validator',
    'data_cleaner',
    'version_manager',
    'DataValidator',
    'DataCleaner',
    'DataComparator',
    'data_comparator',
    'DataQualityLevel',
    'ValidationResult',
    'ValidationError',
    'monitor',
    'performance_logger',
    'Alert',
    'AlertLevel',
    'AlertType'
]
