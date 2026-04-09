# -*- coding: utf-8 -*-
from core import BaseAgent, get_llm
from langchain_classic.chains import RetrievalQA
from langchain_core.prompts import PromptTemplate
from utils.logger import logger


class QAAgent(BaseAgent):
    """标准知识库问答Agent"""

    def __init__(self, config, knowledge_registry):
        super().__init__(config, knowledge_registry)
        self.llm = get_llm()  # 远程Ollama
        self.all_kb = knowledge_registry.get_all_knowledge()

    def get_qa_chain(self, kb_id: str):
        """绑定指定知识库，创建问答链"""
        kb = self.all_kb.get(kb_id)
        if not kb:
            raise ValueError(f"知识库 {kb_id} 不存在")

        # 提示词：只根据知识库回答
        prompt = PromptTemplate(
            template="""
            你是专业助手，仅根据上下文回答问题，不要编造。
            上下文：{context}
            问题：{question}
            答案：
            """,
            input_variables=["context", "question"]
        )

        return RetrievalQA.from_chain_type(
            llm=self.llm,
            chain_type="stuff",
            retriever=kb.get_retriever(),
            chain_type_kwargs={"prompt": prompt},
            return_source_documents=True
        )

    def run(self, query: str, kb_id: str = "kb_stock"):
        """执行问答：默认查询股票知识库"""
        logger.info(f"📝 问答Agent处理问题：{query}")
        query = query.encode('utf-8', 'ignore').decode('utf-8')
        qa_chain = self.get_qa_chain(kb_id)
        result = qa_chain.invoke({"query": query})
        return result