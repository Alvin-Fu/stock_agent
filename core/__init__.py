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
    "get_ds"
]