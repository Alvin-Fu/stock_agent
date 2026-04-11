# -*- coding: utf-8 -*-
"""
🔥 中央路由大脑 Agent
系统核心：自动识别问题 → 路由到对应知识库 → 生成答案
"""
from core import BaseAgent, get_ds
from langchain_classic.chains import RetrievalQA,create_retrieval_chain
from langchain_core.prompts import PromptTemplate
from langchain_classic.chains.combine_documents import create_stuff_documents_chain
from utils.logger import logger


class RouterBrainAgent(BaseAgent):
    def __init__(self, config, knowledge_registry):
        super().__init__(config, knowledge_registry)
        self.llm = get_ds()  # 远程Ollama大模型
        self.kb_map = knowledge_registry.get_all_knowledge()

    def _route_to_knowledge_base(self, query: str) -> str:
        """
        🔥 大脑核心：根据问题自动判断属于哪个知识库
        """
        route_prompt = PromptTemplate(
            template="""
            你是路由助手，判断用户问题属于哪个知识库，只能返回关键词：股票 / 产品 / 技术
            问题：{query}
            返回：
            """,
            input_variables=["query"]
        )
        # LLM判断分类
        category = self.llm.invoke(route_prompt.format(query=query)).content.strip()
        logger.info(f"🧠 大脑路由判断：问题：{query} → 分类：{category}")
        kb_id = self.kb_map.get(category, "kb_stock")
        logger.info(f"🧠 大脑路由判断：问题 → {category} → 知识库：{kb_id}")
        return kb_id

    def _get_qa_chain(self, kb_id: str):
        """绑定对应知识库的问答链"""
        kb = self.kb_map[kb_id]
        prompt = PromptTemplate(
            template="""
            你是专业助手，**仅根据上下文回答**，不编造内容。
            回答简洁、专业、准确。
            上下文：{context}
            问题：{question}
            答案：
            """,
            input_variables=["context", "question"]
        )
        from langchain_core.callbacks.streaming_stdout import StreamingStdOutCallbackHandler

        # 在你的 _get_qa_chain 方法中修改
        streaming_llm = self.llm.bind(
            streaming=True,
            callbacks=[StreamingStdOutCallbackHandler()]  # 强制将流输出到标准输出
        )
        retriever = kb.get_retriever(search_kwargs={"k": 3})

        logger.info(f"🧠 中央大脑获取检索器：{retriever}")
        return RetrievalQA.from_chain_type(
            llm=streaming_llm,
            chain_type="stuff",
            retriever=retriever,
            chain_type_kwargs={"prompt": prompt},
            return_source_documents=True
        )



    def _get_qa_chain2(self, kb_id, query: str):
        # 1. 获取 retriever
        kb = self.kb_map[kb_id]
        retriever = kb.get_retriever(search_kwargs={"k": 3})

        # 2. 定义 Prompt (注意：这里需要包含 {context} 和 {input} 变量)
        prompt = PromptTemplate(
            template="""
                            你是专业助手，**仅根据上下文回答**，不编造内容。
                            回答简洁、专业、准确。
                            上下文：{context}
                            问题：{input}
                            答案：
                            """,
            input_variables=["context", "input"]
        )
        question_answer_chain = create_stuff_documents_chain(self.llm, prompt)
        rag_chain = create_retrieval_chain(retriever, question_answer_chain)
        logger.info(f"🧠 中央大脑获取检索链：{rag_chain}")
        return rag_chain

    def run(self, query: str):
        """
        大脑统一入口：接收问题 → 路由 → 检索 → 回答
        """
        logger.info(f"🧠 中央大脑收到问题：{query}")
        query = query.encode('utf-8', 'ignore').decode('utf-8')
        # 1. 自动路由到对应知识库
        kb_id = self._route_to_knowledge_base(query)
        logger.info(f"🧠 中央大脑路由结果：{kb_id}")
        # 2. 获取对应问答链
        qa_chain = self._get_qa_chain2(kb_id, query)

        # 3. 执行问答
        result = qa_chain.invoke({"input": query})
        logger.info(f"🧠 中央大脑问答结果：{result}")
        return result
