# -*- coding: utf-8 -*-
"""
项目核心层入口
统一导出：基类、向量库、嵌入模型、LLM模型等核心组件
（state_manager / agent_coordination / business_analyzer / data_processor /
 plugin_system 等增强模块暂无生产调用方，不在此处导入，需要时按模块路径显式引用）
"""

from .base_agent import BaseAgent
from .base_knowledge import BaseKnowledge
from .embeddings import get_embeddings
from .llm import get_llm, get_ds
from .vector_store import (
    get_remote_chroma_client,
    create_remote_chroma
)

__all__ = [
    "BaseKnowledge",
    "BaseAgent",
    "get_embeddings",
    "get_remote_chroma_client",
    "create_remote_chroma",
    "get_llm",
    "get_ds",
]
