from abc import ABC, abstractmethod

from langchain_core.documents import Document


class BaseKnowledge(ABC):
    """知识库抽象基类：所有知识库必须实现以下方法"""
    def __init__(self, config):
        self.config = config
        self.name = config["name"]
        self.vector_store = None  # Chroma向量库

    @abstractmethod
    def load_and_split_documents(self) -> list[Document]:
        """加载+切分文档"""
        pass

    @abstractmethod
    def build_vector_store(self):
        """构建向量库"""
        pass

    @abstractmethod
    def get_retriever(self, **kwargs):
        """获取检索器"""
        pass