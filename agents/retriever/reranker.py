"""
重排序模块：对检索结果进行精排
"""

from typing import List
from langchain_core.documents import Document
from sentence_transformers import CrossEncoder
from utils.config import get_retriever_config

# 获取重排序模型配置
retriever_config = get_retriever_config()
RERANKER_MODEL = retriever_config.get('reranker_model', 'deepseek-r1:14b')


class Reranker:
    def __init__(self, model_name: str = RERANKER_MODEL):
        self.model = CrossEncoder(model_name)

    def rerank(self, query: str, documents: List[Document], top_k: int = 5) -> List[Document]:
        if not documents:
            return []

        # 准备 (query, doc) 对
        pairs = [[query, doc.page_content[:1000]] for doc in documents]
        scores = self.model.predict(pairs)

        # 按分数排序
        scored_docs = list(zip(scores, documents))
        scored_docs.sort(key=lambda x: x[0], reverse=True)

        return [doc for _, doc in scored_docs[:top_k]]