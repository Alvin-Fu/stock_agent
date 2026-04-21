# -*- coding: utf-8 -*-
"""
🔥 中央路由大脑 Agent
系统核心：自动识别问题 → 路由到对应知识库 → 生成答案
"""
from core import BaseAgent, get_llm
from langchain_classic.chains import create_retrieval_chain
from langchain_core.prompts import PromptTemplate
from langchain_classic.memory import ConversationBufferMemory
from langchain_classic.agents import create_structured_chat_agent, AgentExecutor
from langchain_classic.chains.combine_documents import create_stuff_documents_chain
from langchain_classic import hub
from utils.logger import logger
from tools import all_stock_tools
import re


class RouterBrainAgent(BaseAgent):
    def __init__(self, config, knowledge_registry):
        super().__init__(config, knowledge_registry)
        self.llm = get_llm()  # 远程Ollama大模型
        self.kb_map = knowledge_registry.get_all_knowledge()

        self.tools = all_stock_tools
        self.tool_agent = self._init_tool_agent()
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        # 记录当前分析的股票代码（多轮复用）
        self.current_stock = None

    def _init_tool_agent(self):
        prompt = hub.pull("hwchase17/structured-chat-agents")
        agent = create_structured_chat_agent(llm=self.llm, tools=self.tools, prompt=prompt)
        return AgentExecutor(agent=agent, tools=self.tools, verbose=True, handle_parsing_errors=True)

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
        response = self.llm.invoke(route_prompt.format(query=query))
        category = response.content.strip() if hasattr(response, 'content') else response.strip()
        logger.info(f"🧠 大脑路由判断：问题：{query} → 分类：{category}")
        kb_id = self.kb_map.get(category, "kb_stock")
        logger.info(f"🧠 大脑路由判断：问题 → {category} → 知识库：{kb_id}")
        return kb_id

    def _get_qa_chain(self, kb_id):
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

    # ===================== 核心：分析股票（工具+知识库）=====================
    def analyze_stock(self, stock_code: str, period: str, kb_chain):
        """
        统一分析入口：
        1. 拉取K线数据
        2. 拉取分析知识
        3. AI合并分析
        """
        logger.info(f"📊 开始分析 {stock_code} {period}")
        daily = self.tool_agent.invoke({"input": f"获取{stock_code}日线数据"})["output"]
        weekly = self.tool_agent.invoke({"input": f"获取{stock_code}周线数据"})["output"]
        monthly = self.tool_agent.invoke({"input": f"获取{stock_code}月线数据"})["output"]
        all_kline = f"{daily}\n\n{weekly}\n\n{monthly}"

        # 2. 从知识库获取分析规则
        analysis_result = kb_chain.invoke({
            "input": f"股票K线走势分析方法、技术指标判断规则、估值方法，MACD等"
        })
        analysis_rule = analysis_result.get("answer", analysis_result.get("result", ""))

        # 3. 大模型整合分析
        final_prompt = f"""
        你是专业股票分析师，请根据【分析规则】和【K线数据】给出专业分析。
        对股票 {stock_code} 做**日线+周线+月线综合技术分析**。

        输出结构：
        1. 趋势判断（短/中/长）
        2. 支撑位 & 压力位
        3. 多周期共振情况
        4. 风险提示
        5. 综合结论

        【分析规则】
        {analysis_rule}

        【真实K线数据】
        {all_kline}

        请输出专业分析：
        """
        return self.llm.invoke(final_prompt)

    def extract_stock_code(self, query: str) -> str:
        """从问题里提取6位股票代码"""
        match = re.search(r'\d{6}', query)
        return match.group(0) if match else "002594"

    def run(self, query: str):
        """
        大脑统一入口：接收问题 → 路由 → 检索 → 回答
        """
        code = self.extract_stock_code(query)
        if not code:
            return "请输入6位股票代码，例如：分析000001"
        logger.info(f"🧠 中央大脑收到问题：{query}")
        query = query.encode('utf-8', 'ignore').decode('utf-8')
        # 1. 自动路由到对应知识库
        kb_id = self._route_to_knowledge_base(query)
        logger.info(f"🧠 中央大脑路由结果：{kb_id}")
        # 2. 获取对应问答链
        qa_chain = self._get_qa_chain(kb_id)
        return self.analyze_stock(code, "日线", qa_chain)
