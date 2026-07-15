"""
重排序模块：对检索结果进行精排
注意：sentence_transformers 是可选依赖，仅在 use_reranker=true 时需要。
未安装时重排序自动降级，不影响向量检索主干。
"""

from typing import List
from langchain_core.documents import Document
from utils.config import get_retriever_config
from utils.logger import logger

# 获取重排序模型配置
retriever_config = get_retriever_config()
# 必须是 CrossEncoder 类模型（如 BAAI/bge-reranker-base / BAAI/bge-reranker-v2-m3），
# 不能填 Ollama LLM 名（deepseek-r1:14b 是生成模型，不能做重排序）
RERANKER_MODEL = retriever_config.get('reranker_model', 'BAAI/bge-reranker-base')


class Reranker:
    def __init__(self, model_name: str = RERANKER_MODEL):
        self.model_name = model_name
        self.model = None

    def _load_model(self):
        """延迟加载：仅在首次 rerank 时加载模型，避免导入时 crash"""
        if self.model is not None:
            return
        try:
            from sentence_transformers import CrossEncoder
            self.model = CrossEncoder(self.model_name)
            logger.info(f"[Reranker] 重排序模型加载成功: {self.model_name}")
        except ImportError:
            raise RuntimeError(
                "重排序需要 sentence_transformers 库，请执行: "
                "pip install sentence-transformers"
            )
        except Exception as e:
            raise RuntimeError(f"重排序模型加载失败 ({self.model_name}): {e}")

    def rerank(self, query: str, documents: List[Document], top_k: int = 5) -> List[Document]:
        if not documents:
            return []

        try:
            self._load_model()
        except Exception as e:
            logger.warning(f"[Reranker] 模型加载失败，退回原始排序: {e}")
            return documents[:top_k]

        # 准备 (query, doc) 对
        pairs = [[query, doc.page_content[:1000]] for doc in documents]
        scores = self.model.predict(pairs)

        # 按分数排序
        scored_docs = list(zip(scores, documents))
        scored_docs.sort(key=lambda x: x[0], reverse=True)

        return [doc for _, doc in scored_docs[:top_k]]