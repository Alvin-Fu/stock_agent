"""
路由 Agent（大脑）
职责：意图识别 + 任务拆解分发
输入：用户问题
输出：更新状态中的 intent / stock_code / industry_name / next_agents（待执行 Agent 队列）

并行扇出架构：route_node 把待执行 Agent 列表写入 state["next_agents"]，
由 graph.py 的并行节点直接消费，路由层不再维护队列弹出/条件边逻辑。
"""

import json
import re
from typing import Dict, Any

from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from utils.constants import IntentType
from core.llm import get_router_llm
from .prompts import ROUTER_SYSTEM_PROMPT, ROUTER_USER_TEMPLATE
from utils.logger import logger

# 主图里可被路由的节点名（compliance 固定在 responder 之后执行，不进队列）
VALID_AGENT_NODES = {"retriever", "analyst", "researcher", "technical"}


def _resolve_stock_code(llm_code: str, company_name: str, fallback_code: str) -> str:
    """
    确定最终的股票代码：
    1. LLM 给出的代码必须能在 tushare stock_basic 里反查到公司名，否则视为幻觉丢弃
       (ETF 代码例外——它们不在 stock_basic 中，直接放行)
    2. 没有有效代码时，用公司名通过 tushare 数据查代码
    3. 都没有则回退到上游传入的代码
    """
    from tools.company_code_validator import find_stock_code, find_company_name
    from tools.stock_type import is_etf

    llm_code = (llm_code or "").strip()
    company_name = (company_name or "").strip()

    if llm_code and re.match(r'^\d{6}$', llm_code):
        # ETF 代码不在 stock_basic 表中，直接放行
        if is_etf(code=llm_code):
            logger.info(f"ETF 代码无需验证: {llm_code}")
            return llm_code
        try:
            real_name = find_company_name(llm_code)
        except Exception as e:
            logger.warning(f"验证股票代码失败（{llm_code}）: {e}")
            real_name = None
        if real_name:
            logger.info(f"股票代码验证通过: {llm_code} -> {real_name}")
            return llm_code
        logger.warning(f"LLM 给出的股票代码 {llm_code} 在股票基础表中不存在，丢弃")

    if company_name:
        try:
            found = find_stock_code(company_name)
        except Exception as e:
            logger.warning(f"按公司名查代码失败（{company_name}）: {e}")
            found = None
        if found:
            logger.info(f"公司名解析成功: {company_name} -> {found}")
            return found
        logger.warning(f"未能通过公司名「{company_name}」找到股票代码")

    return fallback_code


class RouterAgent:
    """路由 Agent，基于 LLM 的意图识别与任务拆解"""

    def __init__(self):
        self.llm = get_router_llm()  # 低温度模型，确定性路由

    def route_node(self, state: AgentState) -> Dict[str, Any]:
        """路由节点：分析意图，产出待执行 Agent 队列"""
        question = state.get("question", "")
        logger.info(f"路由分析: {question[:80]}...")

        try:
            route_result = self._llm_route(question)
        except Exception as e:
            logger.warning(f"LLM 路由失败，启用规则兜底: {e}")
            route_result = self._rule_based_route(question)

        intent = route_result.get("intent", IntentType.UNKNOWN)
        industry_name = route_result.get("industry_name") or state.get("industry_name", "")
        confidence = route_result.get("confidence", 0.5)
        reasoning = route_result.get("reasoning", "")

        # 公司名/股票代码解析（含 tushare 验证，防 LLM 幻觉代码）
        stock_code = _resolve_stock_code(
            route_result.get("stock_code", ""),
            route_result.get("company_name", ""),
            state.get("stock_code", ""),
        )

        # 判断标的类型 → 管线分流
        from tools.stock_type import classify
        stock_type, _ = classify(code=stock_code, name=route_result.get("company_name"))

        # 队列只保留合法节点名；compliance 固定在最后由图保证，不进队列
        raw_agents = route_result.get("next_agents") or []
        next_agents = [a for a in raw_agents if a in VALID_AGENT_NODES]

        # ETF 无财报数据，跳过 analyst
        if stock_type == "etf" and "analyst" in next_agents:
            logger.info("ETF 模式：跳过 analyst（无财报数据）")
            next_agents.remove("analyst")

        # 宏观分析不涉及个股技术面，跳过 technical_agent
        if intent == IntentType.MACRO and "technical" in next_agents:
            logger.info("宏观分析模式：跳过 technical（宏观分析不走技术面）")
            next_agents.remove("technical")

        # 个股分析类意图但没解析出代码时，去掉依赖代码的节点，交给 researcher 联网研究
        if not stock_code and not industry_name:
            code_dependent = {"analyst", "technical"}
            if any(a in code_dependent for a in next_agents):
                logger.warning("未解析出股票代码，跳过依赖代码的 analyst/technical 节点")
                next_agents = [a for a in next_agents if a not in code_dependent]

        logger.info(f"路由结果: intent={intent}, stock_code={stock_code}, "
                    f"stock_type={stock_type}, industry={industry_name}, "
                    f"next_agents={next_agents}, confidence={confidence}")

        # 标的属性分类（周期股/成长股/防御股/价值股）：router 统一判定后写入 state，
        # 下游 Agent（analyst/technical/responder）直接读取，避免各自重复调用 classify_stock_attribute
        stock_attr = None
        if stock_code and "," not in stock_code and stock_type != "etf":
            try:
                from tools.stock_classifier import classify_stock_attribute
                stock_attr = classify_stock_attribute(stock_code)
                if stock_attr and stock_attr.get("type", "unknown") != "unknown":
                    logger.info(f"标的属性分类(router): {stock_code} → "
                                f"{stock_attr.get('type')}({stock_attr.get('label', '')})")
            except Exception as e:
                logger.debug(f"标的属性分类失败（不影响路由）: {e}")

        return {
            "intent": intent,
            "stock_code": stock_code,
            "stock_type": stock_type,
            "industry_name": industry_name,
            "stock_attribute": stock_attr,
            "next_agents": next_agents,
            "confidence": confidence,
            "intermediate_steps": [("router", {
                "intent": intent,
                "stock_code": stock_code,
                "stock_type": stock_type,
                "industry_name": industry_name,
                "next_agents": list(next_agents),
                "reasoning": reasoning,
            })],
        }

    def _llm_route(self, question: str) -> Dict[str, Any]:
        """使用 LLM 进行路由决策"""
        messages = [
            SystemMessage(content=ROUTER_SYSTEM_PROMPT),
            HumanMessage(content=ROUTER_USER_TEMPLATE.format(question=question)),
        ]
        response = self.llm.invoke(messages, timeout=120)
        content = response.content.strip()
        logger.info(f"LLM 路由结果: {content[:200]}")

        # 处理可能被 markdown 代码块包裹的情况
        json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
        if json_match:
            content = json_match.group(1)
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"LLM 返回非 JSON 格式: {content[:200]}")
            raise ValueError("Invalid JSON response")

    def _rule_based_route(self, question: str) -> Dict[str, Any]:
        """基于规则的兜底路由（不含 stock_code 字段，保留上游传入值）"""
        question_lower = question.lower()

        # 检测 ETF 代码（6 位数字，ETF 前缀）
        from tools.stock_type import is_etf
        etf_match = re.search(r'\b(5[12568]\d{4}|1[568]\d{4})\b', question)
        if etf_match:
            return {
                "intent": IntentType.FINANCIAL_ANALYSIS,
                "stock_code": etf_match.group(1),
                "next_agents": ["researcher", "technical"],
                "confidence": 0.9,
                "reasoning": "规则匹配 ETF 代码，路由至 researcher → technical",
            }

        # 常见行业/赛道名称，即使不带"行业/产业链"后缀也应命中
        industry_keywords = [
            "行业", "产业", "产业链", "龙头", "龙一", "龙二", "景气", "赛道",
            "机器人", "人形机器人", "新能源", "光伏", "半导体", "芯片",
            "人工智能", "ai", "医药", "白酒", "消费", "汽车", "军工",
            "储能", "煤炭", "电力", "地产", "金融",
        ]
        is_industry = any(kw in question_lower for kw in industry_keywords)

        stock_keywords = ["股票", "分析", "走势", "财务", "均线", "macd", "k线"]
        is_stock_analysis = any(kw in question_lower for kw in stock_keywords)

        # 宏观关键词：降息/加息/MLF/LPR/CPI/PMI/M2/社融/美债收益率/汇率等
        macro_keywords = [
            "降息", "加息", "mlf", "lpr", "cpi", "pmi", "m2", "社融", "社融数据",
            "美债收益率", "美债", "国债收益率", "汇率", "宏观", "货币政策", "降准",
            "公开市场操作", "逆回购", "再贷款",
        ]
        has_macro = any(kw in question_lower for kw in macro_keywords)

        has_financial = any(kw in question_lower for kw in ["财务", "比率", "roe", "roa", "毛利率", "净利率", "估值", "杜邦"])
        has_technical = any(kw in question_lower for kw in ["均线", "macd", "k线", "走势", "金叉", "死叉"])
        has_realtime = any(kw in question_lower for kw in ["股价", "新闻", "最新", "实时", "今天", "公告"])

        # 宏观分析：含宏观关键词且未聚焦具体个股时识别为宏观分析
        # （宏观+个股混合问题走个股分析链路，在 researcher._analyze_stock 中注入宏观数据）
        has_stock_code = bool(re.search(r'\b\d{6}\b', question))
        if has_macro and not is_stock_analysis and not has_stock_code:
            return {
                "intent": IntentType.MACRO,
                "next_agents": ["researcher"],
                "confidence": 0.8,
                "reasoning": "宏观分析问题（降息/加息/MLF/LPR/CPI/PMI/M2/社融/美债/汇率等），路由至 researcher → responder",
            }

        # 行业/产业链关键词优先：这类问题通常也含"分析"等个股词，不能被个股分支劫走
        if is_industry:
            return {
                "intent": IntentType.INDUSTRY_ANALYSIS,
                "industry_name": question,
                "next_agents": ["researcher", "technical"],
                "confidence": 0.8,
                "reasoning": "行业/产业链分析问题，路由至 researcher → technical",
            }

        if is_stock_analysis:
            # retriever 不进默认队列：知识库是可选能力（当前未灌文档），
            # 财报/新闻/公告由 analyst 和 researcher 实时获取
            next_agents = []
            if has_financial:
                next_agents.append("analyst")
            if has_technical:
                next_agents.append("technical")
            if has_realtime:
                next_agents.append("researcher")
            if not next_agents:
                next_agents = ["analyst", "technical", "researcher"]
            return {
                "intent": IntentType.FINANCIAL_ANALYSIS if has_financial
                else IntentType.TECHNICAL_ANALYSIS if has_technical
                else IntentType.REAL_TIME_INFO,
                "next_agents": next_agents,
                "confidence": 0.8,
                "reasoning": "股票分析问题，需要多个Agent协作",
            }

        if has_financial:
            return {"intent": IntentType.FINANCIAL_ANALYSIS, "next_agents": ["analyst"],
                    "confidence": 0.7, "reasoning": "规则匹配财务分析关键词"}
        if has_technical:
            return {"intent": IntentType.TECHNICAL_ANALYSIS, "next_agents": ["technical"],
                    "confidence": 0.7, "reasoning": "规则匹配技术分析关键词"}
        if has_realtime:
            return {"intent": IntentType.REAL_TIME_INFO, "next_agents": ["researcher"],
                    "confidence": 0.7, "reasoning": "规则匹配实时信息关键词"}
        if any(kw in question_lower for kw in ["你好", "谢谢", "再见", "帮助"]):
            return {"intent": IntentType.GENERAL_CHAT, "next_agents": [],
                    "confidence": 0.7, "reasoning": "闲聊问题，直接生成回答"}

        return {
            "intent": IntentType.KNOWLEDGE_QUERY,
            "next_agents": ["retriever"],
            "confidence": 0.5,
            "reasoning": "默认路由至检索 Agent",
        }


def create_router_node():
    """返回一个 LangGraph 节点函数"""
    agent = RouterAgent()
    return agent.route_node
