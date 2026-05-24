# -*- coding: utf-8 -*-
"""
🔥 中央路由大脑 Agent
系统核心：自动识别问题 → 路由到对应知识库 → 生成答案
"""
from core import BaseAgent, get_llm
from langchain_classic.chains import create_retrieval_chain
from langchain_core.prompts import PromptTemplate
from langchain_classic.memory import ConversationBufferMemory
from langchain_classic.chains.combine_documents import create_stuff_documents_chain
from utils.logger import logger
from tools import StockTools
from tools.company_code_validator import validate_and_correct_companies
import re
import unicodedata
import json


class RouterBrainAgent(BaseAgent):
    def __init__(self, config, knowledge_registry):
        super().__init__(config, knowledge_registry)
        self.llm = get_llm()  # 远程Ollama大模型
        self.kb_map = knowledge_registry.get_all_knowledge()

        # 直接创建 StockTools 实例用于数据获取
        self.stock_tools = StockTools()

        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        # 记录当前分析的股票代码（多轮复用）
        self.current_stock = None
        
        # Agent引用
        self.agents = {}

    def set_agents(self, agents_dict):
        """设置可用的Agent"""
        self.agents = agents_dict

    def _clean_unicode(self, text: str) -> str:
        """
        清理无效的 Unicode 字符（如 surrogate pairs）
        """
        if not text:
            return text
        
        # 移除无效的 surrogate characters
        cleaned = ''.join(c for c in text if not (0xD800 <= ord(c) <= 0xDFFF))
        
        # 标准化 Unicode
        try:
            cleaned = unicodedata.normalize('NFKC', cleaned)
        except:
            pass
        
        # 确保可以编码为 UTF-8
        try:
            cleaned.encode('utf-8').decode('utf-8')
        except UnicodeEncodeError:
            cleaned = text.encode('utf-8', 'replace').decode('utf-8')
        
        return cleaned

    def _route_to_knowledge_base(self, query: str) -> str:
        """
        🔥 大脑核心：根据问题自动判断属于哪个知识库
        """
        route_prompt = PromptTemplate(
            template="""
            你是路由助手，根据问题内容判断其类型，只能返回以下关键词之一：
            - 股票：涉及具体股票代码、股票分析、个股技术面/基本面分析
            - 产业链：涉及行业产业链分析、上下游公司、产业生态、产业链全景
            - 技术：涉及技术指标、技术分析方法、编程技术问题
            - 产品：涉及产品功能、使用方法、产品介绍
            
            问题：{query}
            返回：
            """,
            input_variables=["query"]
        )
        # LLM判断分类
        response = self.llm.invoke(route_prompt.format(query=query))
        category = response.content.strip() if hasattr(response, 'content') else response.strip()
        
        # 关键词匹配兜底
        if "产业链" in query or "产业" in query:
            category = "产业链"
        elif re.search(r'\d{6}', query):
            category = "股票"
        
        logger.info(f"🧠 大脑路由判断：问题 → 分类：{category}")
        
        # 根据分类选择知识库
        kb_mapping = {
            "股票": "kb_stock",
            "产业链": "kb_stock",  # 产业链分析也使用股票知识库
            "技术": "kb_stock",
            "产品": "kb_stock"
        }
        
        kb_id = kb_mapping.get(category, "kb_stock")
        logger.info(f"🧠 大脑路由判断：问题 → 分类：{category} → 知识库：{kb_id}")
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
        logger.info(f"🧠 中央大脑获取检索链")
        return rag_chain

    def _fetch_kline_data(self, stock_code: str) -> dict:
        """
        直接使用 StockTools 获取K线数据，不通过 Agent
        """
        result = {
            "daily": "",
            "weekly": "",
            "monthly": ""
        }

        # 获取日线数据
        try:
            df = self.stock_tools.fetch_and_save_stock_daily_data(stock_code)
            if df is not None and not df.empty:
                result["daily"] = df.head(30).to_string()
                logger.info(f"✅ 获取 {stock_code} 日线数据成功")
            else:
                result["daily"] = "日线数据为空"
        except Exception as e:
            logger.error(f"获取日线数据失败: {e}")
            result["daily"] = f"日线数据获取失败: {str(e)}"

        # 获取周线数据
        try:
            df = self.stock_tools.fetch_and_save_stock_weekly_data(stock_code)
            if df is not None and not df.empty:
                result["weekly"] = df.head(20).to_string()
                logger.info(f"✅ 获取 {stock_code} 周线数据成功")
            else:
                result["weekly"] = "周线数据为空"
        except Exception as e:
            logger.error(f"获取周线数据失败: {e}")
            result["weekly"] = f"周线数据获取失败: {str(e)}"

        # 获取月线数据
        try:
            df = self.stock_tools.fetch_and_save_stock_monthly_data(stock_code)
            if df is not None and not df.empty:
                result["monthly"] = df.head(12).to_string()
                logger.info(f"✅ 获取 {stock_code} 月线数据成功")
            else:
                result["monthly"] = "月线数据为空"
        except Exception as e:
            logger.error(f"获取月线数据失败: {e}")
            result["monthly"] = f"月线数据获取失败: {str(e)}"

        return result

    def extract_company_stock_codes(self, chain_analysis_text: str) -> list:
        """
        从产业链分析文本中提取股票代码，并通过Tushare验证
        """
        extract_prompt = f"""
        从以下产业链分析文本中，提取所有提到的上市公司及其股票代码。
        请返回JSON格式，格式如下：
        {{
            "companies": [
                {{
                    "name": "公司名称",
                    "code": "股票代码"
                }}
            ]
        }}
        
        请尽可能提取更多相关上市公司。
        如果文本中没有明确提到股票代码，请只返回公司名称。
        
        产业链分析文本：
        {chain_analysis_text}
        """
        
        companies = []
        try:
            response = self.llm.invoke(extract_prompt)
            content = response.content if hasattr(response, 'content') else response
            
            # 尝试解析JSON
            json_match = re.search(r'(\{[\s\S]*\})', content)
            if json_match:
                data = json.loads(json_match.group(1))
                companies = data.get('companies', [])
        except Exception as e:
            logger.error(f"提取股票代码失败: {e}")
        
        # 通过Tushare验证和修正公司代码
        return validate_and_correct_companies(companies)

    # ===================== 核心：分析股票（工具+知识库）=====================
    def analyze_stock(self, stock_code: str, period: str, kb_chain=None):
        """
        统一分析入口：
        1. 拉取K线数据
        2. 拉取分析知识
        3. AI合并分析
        """
        logger.info(f"📊 开始分析 {stock_code} {period}")

        # 直接使用 StockTools 获取数据，不通过 Agent
        kline_data = self._fetch_kline_data(stock_code)

        all_kline = f"""
【日线数据】
{kline_data['daily']}

【周线数据】
{kline_data['weekly']}

【月线数据】
{kline_data['monthly']}
        """

        # 2. 从知识库获取分析规则
        analysis_rule = "使用标准股票分析方法"
        if kb_chain:
            try:
                analysis_result = kb_chain.invoke({
                    "input": f"股票K线走势分析方法、技术指标判断规则、估值方法，MACD等"
                })
                analysis_rule = analysis_result.get("answer", analysis_result.get("result", ""))
            except Exception as e:
                logger.error(f"获取分析规则失败: {e}")

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

    def analyze_industry_chain(self, query: str):
        """
        产业链分析入口：先分析产业链，再分析重要公司
        """
        logger.info(f"📊 开始产业链分析：{query}")
        
        # 步骤1：获取初始产业链分析
        chain_analysis = self._get_initial_chain_analysis(query)
        
        # 步骤2：从产业链分析中提取重要公司
        companies = self.extract_company_stock_codes(chain_analysis)
        logger.info(f"📋 从产业链分析中提取到 {len(companies)} 家公司")
        
        # 步骤3：对每家重要公司进行分析
        company_analyses = []
        for company in companies:
            code = company.get('code', '')
            name = company.get('name', '')
            if code and len(code) >= 6:
                try:
                    logger.info(f"🔍 分析公司: {name}({code})")
                    stock_analysis = self._analyze_single_stock(code)
                    company_analyses.append({
                        'name': name,
                        'code': code,
                        'analysis': stock_analysis
                    })
                except Exception as e:
                    logger.error(f"分析公司 {name}({code}) 失败: {e}")
        
        # 步骤4：整合产业链分析和公司分析，生成最终报告
        final_report = self._generate_final_report(query, chain_analysis, company_analyses)
        
        return final_report

    def _get_initial_chain_analysis(self, query: str) -> str:
        """获取初始产业链分析"""
        analysis_prompt = f"""
        你是一位专业的产业链研究专家。请对以下产业链进行深入分析：
        
        分析主题：{query}
        
        请按照以下结构输出分析结果：
        
        ## 一、产业链全景图
        - 上游环节：主要原材料、核心零部件、关键技术
        - 中游环节：核心制造商、加工组装、系统集成
        - 下游环节：终端产品、应用领域、客户群体
        
        ## 二、关键环节分析
        - 卡脖子环节：技术壁垒高、依赖进口的环节
        - 高附加值环节：毛利率高、利润丰厚的环节
        - 增长潜力环节：未来增长空间大的环节
        
        ## 三、相关上市公司
        列出产业链相关的主要上市公司及其业务定位，
        请尽可能包含完整的股票代码（6位数字）。
        
        ## 四、发展趋势与展望
        - 行业发展趋势
        - 政策影响
        - 未来市场规模预测
        
        ## 五、投资机会与风险提示
        """
        
        response = self.llm.invoke(analysis_prompt)
        return response.content if hasattr(response, 'content') else response

    def _analyze_single_stock(self, stock_code: str) -> str:
        """分析单只股票"""
        logger.info(f"📊 分析股票: {stock_code}")
        
        # 获取K线数据
        kline_data = self._fetch_kline_data(stock_code)
        
        # 构建分析prompt
        analysis_prompt = f"""
        请对股票 {stock_code} 进行综合分析，包含以下内容：
        
        【K线数据】
        日线: {kline_data['daily'][:500]}
        周线: {kline_data['weekly'][:500]}
        月线: {kline_data['monthly'][:500]}
        
        请提供：
        1. 技术面分析（趋势、支撑压力位）
        2. 投资评级与风险提示
        
        请简洁专业地回答。
        """
        
        response = self.llm.invoke(analysis_prompt)
        return response.content if hasattr(response, 'content') else response

    def _generate_final_report(self, query: str, chain_analysis: str, company_analyses: list) -> str:
        """生成最终综合报告"""
        logger.info(f"📝 生成最终产业链分析报告")
        
        companies_section = ""
        if company_analyses:
            companies_section = "## 六、重点公司分析\n\n"
            for company in company_analyses:
                name = company.get('name', '未知公司')
                code = company.get('code', '')
                analysis = company.get('analysis', '')
                
                companies_section += f"### {name}({code})\n\n"
                companies_section += f"{analysis}\n\n"
        
        report_prompt = f"""
        请基于以下内容，整合生成一份专业的产业链投资分析报告：
        
        【分析主题】
        {query}
        
        【产业链全景分析】
        {chain_analysis}
        
        【公司分析】
        {companies_section if companies_section else '暂无详细公司分析'}
        
        请生成一份结构清晰、专业的完整报告，包含：
        1. 产业链概况
        2. 重点公司分析
        3. 投资建议
        4. 风险提示
        """
        
        response = self.llm.invoke(report_prompt)
        return response.content if hasattr(response, 'content') else response

    def extract_stock_code(self, query: str) -> str:
        """从问题里提取6位股票代码"""
        match = re.search(r'\d{6}', query)
        return match.group(0) if match else None

    def run(self, query: str):
        """
        大脑统一入口：接收问题 → 路由 → 检索 → 回答
        """
        # 清理无效的 Unicode 字符
        cleaned_query = self._clean_unicode(query)
        logger.info(f"🧠 中央大脑收到问题：{cleaned_query}")
        
        # 判断是否为产业链分析
        if "产业链" in cleaned_query or "产业" in cleaned_query:
            logger.info(f"🧠 检测到产业链分析请求")
            return self.analyze_industry_chain(cleaned_query)
        
        # 判断是否为股票分析
        code = self.extract_stock_code(cleaned_query)
        if code:
            # 1. 自动路由到对应知识库
            kb_id = self._route_to_knowledge_base(cleaned_query)
            logger.info(f"🧠 中央大脑路由结果：{kb_id}")
            # 2. 获取对应问答链
            qa_chain = self._get_qa_chain(kb_id)
            return self.analyze_stock(code, "日线", qa_chain)
        else:
            # 非股票分析问题，直接回答
            return self.llm.invoke(f"请回答以下问题：{cleaned_query}")
