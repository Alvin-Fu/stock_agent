# -*- coding: utf-8 -*-
"""
项目核心层入口
统一导出：基类、向量库、嵌入模型、LLM模型等核心组件
"""

from .base_agent import BaseAgent
# --------------------- 1. 导出基类（知识库 + Agent）---------------------
from .base_knowledge import BaseKnowledge
# --------------------- 2. 导出向量模型工具 ---------------------
from .embeddings import get_embeddings
# --------------------- 4. 导出LLM大模型（后续问答Agent必备）---------------------
from .llm import get_llm, get_ds
# --------------------- 3. 导出远程 Chroma 向量库核心函数 ---------------------
from .vector_store import (
    get_remote_chroma_client,
    create_remote_chroma
)

# --------------------- 5. 导出新增的核心模块 ---------------------
from .state_manager import (
    state_validator,
    StateValidator,
    StateTransitionManager,
    StateCache,
    AgentStatus,
    AgentExecutionRecord
)

from .agent_coordination import (
    AgentMessage,
    MessageType,
    Priority,
    MessageBus,
    ParallelExecutor,
    AgentResultAggregator,
    message_bus,
    parallel_executor,
    result_aggregator
)

from .business_analyzer import (
    valuation_analyzer,
    risk_analyzer,
    time_series_analyzer,
    ValuationAnalyzer,
    RiskAnalyzer,
    TimeSeriesAnalyzer,
    ValuationMetrics,
    ValuationLevel,
    RiskLevel
)

from .data_processor import (
    data_validator,
    data_cleaner,
    data_comparator,
    DataValidator,
    DataCleaner,
    DataComparator,
    DataQuality,
    ValidationResult
)

from .plugin_system import (
    plugin_registry,
    config_manager,
    agent_factory,
    ab_test_manager,
    PluginRegistry,
    ConfigManager,
    PluginType,
    PluginMetadata
)

# --------------------- 定义对外公共接口（规范导入）---------------------
__all__ = [
    # 基类
    "BaseKnowledge",
    "BaseAgent",
    # 嵌入模型
    "get_embeddings",
    # 远程Chroma向量库
    "get_remote_chroma_client",
    "create_remote_chroma",
    # 大模型
    "get_llm",
    "get_ds",
    # 状态管理
    "state_validator",
    "StateValidator",
    "StateTransitionManager",
    "StateCache",
    "AgentStatus",
    "AgentExecutionRecord",
    # Agent协作
    "AgentMessage",
    "MessageType",
    "Priority",
    "MessageBus",
    "ParallelExecutor",
    "AgentResultAggregator",
    "message_bus",
    "parallel_executor",
    "result_aggregator",
    # 业务分析
    "valuation_analyzer",
    "risk_analyzer",
    "time_series_analyzer",
    "ValuationAnalyzer",
    "RiskAnalyzer",
    "TimeSeriesAnalyzer",
    "ValuationMetrics",
    "ValuationLevel",
    "RiskLevel",
    # 数据处理
    "data_validator",
    "data_cleaner",
    "data_comparator",
    "DataValidator",
    "DataCleaner",
    "DataComparator",
    "DataQuality",
    "ValidationResult",
    # 插件系统
    "plugin_registry",
    "config_manager",
    "agent_factory",
    "ab_test_manager",
    "PluginRegistry",
    "ConfigManager",
    "PluginType",
    "PluginMetadata"
]