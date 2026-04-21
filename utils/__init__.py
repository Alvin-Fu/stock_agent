# -*- coding: utf-8 -*-
"""
工具包入口
统一导出所有工具函数，方便全项目调用
"""

# 从配置模块导出核心函数
from .config import (
    load_config,
    get_model_config,
    get_all_kb_config,
    get_kb_config,
    get_all_agent_config,
    get_agent_config,
    get_db_config,
    get_stock_tools_config,
    get_search_config,
)
from .common import TASK_NAME_DAILY_TASK, parse_row_date
# 从文件工具模块导出函数（后续扩展用）
from .file_tools import load_documents_from_dir, split_documents
# 从日志模块导出核心函数
from .logger import setup_logger

# 定义公共导出接口（控制哪些函数能被外部导入）
__all__ = [
    # 配置相关
    "load_config",
    "get_model_config",
    "get_all_kb_config",
    "get_kb_config",
    "get_all_agent_config",
    "get_agent_config",
    # 工具相关
    "get_db_config",
    "get_stock_tools_config",
    "get_search_config",
    # 日志相关
    "setup_logger",
    # 文件工具相关
    "load_documents_from_dir",
    "split_documents",
    "parse_row_date"
]

# 包元信息（可选）
__version__ = "1.0.0"
__author__ = "alvin"