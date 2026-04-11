# -*- coding: utf-8 -*-
"""
股票知识库 实现类
继承BaseKnowledge，实现标准化接口
适配：远程Docker Chroma + 多知识库隔离
"""
from core import BaseKnowledge, get_embeddings, create_remote_chroma, get_remote_chroma_client
from utils import load_documents_from_dir, split_documents, logger

class StockKnowledge(BaseKnowledge):
    """
    股票专业知识库
    支持：股票知识、研报、行情文档、交易规则等
    """
    def __init__(self, config: dict):
        # 1. 保存配置
        self.config = config

        # 2. 初始化 collection_name (修复之前的报错)
        self.collection_name = config.get("collection_name", "stock_data")

        # 3. 关键：初始化 vector_store 属性
        # 你可以先设为 None，让 get_retriever 里的懒加载逻辑生效
        self.vector_store = None

        # 或者在这里直接初始化（取决于你的逻辑）
        # self.vector_store = init_vector_store(self.collection_name)

    def load_and_split_documents(self):
        """
        实现基类方法：加载股票文档 + 文本切分
        """
        logger.info(f"📈 加载【{self.name}】文档...")

        # 1. 从配置路径加载所有股票文档
        docs = load_documents_from_dir(self.config["data_path"])

        # 2. 按配置切分文本
        split_docs = split_documents(
            documents=docs,
            chunk_size=self.config["chunk_size"],
            chunk_overlap=self.config["chunk_overlap"]
        )
        return split_docs

    def build_vector_store(self):
        """
        实现基类方法：构建远程Chroma向量库
        """
        logger.info(f"🚀 构建【{self.name}】远程向量库...")

        # 1. 获取嵌入模型
        embeddings = get_embeddings()

        # 2. 加载并切分文档
        split_docs = self.load_and_split_documents()

        # 3. 写入远程Docker Chroma（独立集合）
        self.vector_store = create_remote_chroma(
            split_docs=split_docs,
            embedding_function=embeddings,
            collection_name=self.collection_name
        )
        return self.vector_store

    def get_retriever(self, **kwargs):
        """
        实现基类方法：获取检索器（Agent问答使用）
        """
        if not self.vector_store:
            embeddings = get_embeddings()
            # 连接已存在的远程集合
            self.vector_store = get_remote_chroma_client(
                self.collection_name,
                embeddings
            )
        # 返回检索器（取最相关的3条）
        return self.vector_store.as_retriever(** kwargs)