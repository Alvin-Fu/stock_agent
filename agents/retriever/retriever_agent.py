"""
检索 Agent
职责：从向量库中检索相关文档，支持混合检索和重排序
"""

from typing import Dict, Any, List
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END
from langgraph.graph.state import CompiledStateGraph

from agents.base import AgentState
from core import get_embeddings
from core.vector_store import get_remote_chroma_client
from .query_rewriter import QueryRewriter
from .reranker import Reranker
from core.llm import get_default_llm
from utils.logger import logger
from utils.config import get_retriever_config
from functools import partial

# 获取检索配置
retriever_config = get_retriever_config()
TOP_K_RETRIEVAL = retriever_config.get('top_k', 3)
USE_RERANKER = retriever_config.get('use_reranker', False)



class RetrieverAgent:
    """知识检索 Agent"""

    def __init__(self, collection_name: str = "collection_stock"):
        self.name = "retrieve"
        self.collection_name = collection_name
        self.embeddings = get_embeddings()
        self.vector_store = self._load_vector_store()
        self.query_rewriter = QueryRewriter()
        self.reranker = Reranker() if USE_RERANKER else None
        self.graph = self._build_graph()
    
    def _load_vector_store(self):
        """加载向量存储"""
        try:
            vector_store = get_remote_chroma_client(self.collection_name, self.embeddings)
            logger.info(f"✅ 成功加载向量存储：{self.collection_name}")
            return vector_store
        except Exception as e:
            logger.error(f"❌ 加载向量存储失败：{e}")
            raise

    def _build_graph(self) -> CompiledStateGraph:
        """构建状态图"""
        workflow = StateGraph(AgentState)
        retrieve_func = partial(self.retrieve_node)
        workflow.add_node("retrieve", retrieve_func)
        workflow.set_entry_point("retrieve")
        workflow.add_edge("retrieve", END)
        return workflow.compile()

    def retrieve_node(self, state: AgentState) -> Dict[str, Any]:
        """执行检索"""
        question = state.get("question", "")
        logger.info(f"开始检索，原始问题: {question[:80]}...")

        try:
            # 1. 查询改写/扩展（可选）
            queries = [question]
            try:
                rewritten = self.query_rewriter.rewrite(question)
                if rewritten and rewritten != question:
                    queries.append(rewritten)
                    logger.debug(f"查询改写: {rewritten[:80]}...")
            except Exception as e:
                logger.warning(f"查询改写失败: {e}")

            # 2. 执行向量检索（可扩展为混合检索）
            all_docs = []
            try:
                for q in queries[:2]:  # 最多用两个查询
                    docs = self.vector_store.similarity_search(q, k=TOP_K_RETRIEVAL * 2)
                    all_docs.extend(docs)
            except Exception as e:
                logger.error(f"向量检索失败: {e}")
                return {
                    "documents": [],
                    "error": f"检索失败: {e}",
                    "intermediate_steps": [("retriever", {"error": str(e)})],
                }

            # 去重（按内容哈希）
            seen = set()
            unique_docs = []
            for doc in all_docs:
                content_hash = hash(doc.page_content[:200])
                if content_hash not in seen:
                    seen.add(content_hash)
                    unique_docs.append(doc)

            # 3. 重排序（如果启用）
            if self.reranker and unique_docs:
                try:
                    unique_docs = self.reranker.rerank(question, unique_docs, top_k=TOP_K_RETRIEVAL)
                except Exception as e:
                    logger.warning(f"重排序失败，使用原始排序: {e}")
                    unique_docs = unique_docs[:TOP_K_RETRIEVAL]
            else:
                unique_docs = unique_docs[:TOP_K_RETRIEVAL]

            logger.info(f"检索完成，返回 {len(unique_docs)} 个文档")

            return {
                "documents": unique_docs,
                "current_node": "retriever",
                "intermediate_steps": [("retriever", {"query": question, "docs_count": len(unique_docs)})],
            }
        except Exception as e:
            logger.error(f"检索节点执行失败: {e}")
            return {
                "documents": [],
                "error": f"检索执行失败: {e}",
                "intermediate_steps": [("retriever", {"error": str(e)})],
            }

    def invoke(self, state: AgentState) -> AgentState:
        return self.graph.invoke(state)


def create_retriever_node():
    agent = RetrieverAgent()
    return agent.retrieve_node