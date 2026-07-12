# -*- coding: utf-8 -*-
"""
项目核心层入口
统一导出：知识库基类、向量库、嵌入模型、LLM模型等核心组件
"""

from .base_knowledge import BaseKnowledge
from .embeddings import get_embeddings
from .llm import get_llm, get_ds
from .vector_store import (
    get_remote_chroma_client,
    create_remote_chroma
)

__all__ = [
    "BaseKnowledge",
    "get_embeddings",
    "get_remote_chroma_client",
    "create_remote_chroma",
    "get_llm",
    "get_ds",
]
