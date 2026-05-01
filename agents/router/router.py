"""
路由 Agent
职责：意图识别 + 任务分发决策
输入：用户问题
输出：更新状态中的 intent 和 next_agent
"""

import json
import re
from typing import Dict, Any, Optional

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END

from agents.base import AgentState
from utils.constants import IntentType, AgentName
from core.llm import get_router_llm
from .prompts import ROUTER_SYSTEM_PROMPT, ROUTER_USER_TEMPLATE
from utils.logger import logger


class RouterAgent:
    """路由 Agent，基于 LLM 的意图识别与分发"""

    # 备用规则映射（当 LLM 解析失败时启用）
    FALLBACK_RULES = [
        (["计算", "比率", "ROE", "ROA", "毛利率", "净利率", "估值", "杜邦"], IntentType.FINANCIAL_ANALYSIS, "analyst"),
        (["股价", "新闻", "最新", "实时", "今天", "公告"], IntentType.REAL_TIME_INFO, "researcher"),
        (["均线", "MACD", "技术", "金叉", "死叉", "趋势"], IntentType.TECHNICAL_ANALYSIS, "technical"),
        (["你好", "谢谢", "再见", "帮助"], IntentType.GENERAL_CHAT, None),
    ]

    def __init__(self):
        self.llm = get_router_llm()  # 低温度模型，确定性路由
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """构建简单路由图（单节点）"""
        workflow = StateGraph(AgentState)
        workflow.add_node("route", self.route_node)
        workflow.set_entry_point("route")
        workflow.add_edge("route", END)
        return workflow.compile()

    def route_node(self, state: AgentState) -> Dict[str, Any]:
        """
        路由节点：分析意图，决定 next_agent
        """
        question = state.get("question", "")
        logger.info(f"路由分析: {question[:80]}...")

        # 调用 LLM 进行意图识别
        try:
            route_result = self._llm_route(question)
        except Exception as e:
            logger.warning(f"LLM 路由失败，启用规则兜底: {e}")
            route_result = self._rule_based_route(question)

        # 提取结果
        intent = route_result.get("intent", IntentType.UNKNOWN)
        next_agents = route_result.get("next_agents", [])
        stock_code = route_result.get("stock_code", "")

        # 兼容旧格式
        if not next_agents and route_result.get("next_agent"):
            next_agents = [route_result.get("next_agent")]
        confidence = route_result.get("confidence", 0.5)
        reasoning = route_result.get("reasoning", "")

        logger.info(f"路由结果: intent={intent}, next_agents={next_agents}, confidence={confidence}")

        # 确保 next_agents 映射到正确的节点名称
        node_map = {
            "retriever": "retriever",
            "analyst": "analyst",
            "researcher": "researcher",
            "technical": "technical",
            "compliance": "compliance",
            "responder": "responder",
        }
        next_nodes = [node_map.get(agent, "responder") for agent in next_agents]

        # 返回状态更新
        rue =  {
            "intent": intent,
            "stock_code": stock_code,
            "next_agents": next_agents,
            "next_nodes": next_nodes,
            "confidence": confidence,
            "intermediate_steps": [("router", {"intent": intent, "next_agents": next_agents, "next_nodes": next_nodes, "reasoning": reasoning})],
        }
        logger.info(f"状态更新: {rue}")
        return rue

    def _llm_route(self, question: str) -> Dict[str, Any]:
        """使用 LLM 进行路由决策"""
        messages = [
            SystemMessage(content=ROUTER_SYSTEM_PROMPT),
            HumanMessage(content=ROUTER_USER_TEMPLATE.format(question=question)),
        ]
        logger.info(f"开始调用 LLM，提示词长度：{len(str(messages))}")
        try:
            # 给 invoke 加超时，比如 60 秒，超时会抛出异常，不会一直卡
            response = self.llm.invoke(
                messages,
                timeout=120,
            )
        except Exception as e:
            logger.error(f"LLM 调用失败: {str(e)}")
            raise e
        content = response.content.strip()
        logger.info(f"LLM 路由结果: {content[:200]}")

        # 尝试提取 JSON
        try:
            # 处理可能被 markdown 代码块包裹的情况
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                content = json_match.group(1)
            return json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"LLM 返回非 JSON 格式: {content[:200]}")
            raise ValueError("Invalid JSON response")

    def _rule_based_route(self, question: str) -> Dict[str, Any]:
        """基于规则的兜底路由"""
        question_lower = question.lower()
        
        # 检查是否是股票分析相关问题
        stock_keywords = ["股票", "分析", "走势", "财务", "均线", "MACD", "K线"]
        is_stock_analysis = any(kw in question_lower for kw in stock_keywords)
        
        # 检查具体分析类型
        has_financial = any(kw in question_lower for kw in ["财务", "比率", "ROE", "ROA", "毛利率", "净利率", "估值", "杜邦"])
        has_technical = any(kw in question_lower for kw in ["均线", "MACD", "K线", "走势", "金叉", "死叉"])
        has_realtime = any(kw in question_lower for kw in ["股价", "新闻", "最新", "实时", "今天", "公告"])
        
        # 处理复杂的股票分析问题
        if is_stock_analysis:
            next_agents = ["retriever"]  # 先检索知识库
            
            if has_financial:
                next_agents.append("analyst")  # 财务分析
            if has_technical:
                next_agents.append("technical")  # 技术分析
            if has_realtime:
                next_agents.append("researcher")  # 实时信息
            
            next_agents.append("compliance")  # 最后进行合规审查
            
            return {
                "intent": IntentType.FINANCIAL_ANALYSIS if has_financial else IntentType.TECHNICAL_ANALYSIS if has_technical else IntentType.REAL_TIME_INFO,
                "next_agents": next_agents,
                "confidence": 0.8,
                "reasoning": f"股票分析问题，需要多个Agent协作"
            }
        
        # 处理其他简单问题
        for keywords, intent, agent in self.FALLBACK_RULES:
            if any(kw in question_lower for kw in keywords):
                # 转换AgentName枚举值为字符串
                agent_str = agent.value if hasattr(agent, 'value') else agent
                return {
                    "intent": intent,
                    "next_agents": [agent_str] if agent_str else [],
                    "confidence": 0.7,
                    "reasoning": f"规则匹配关键词: {keywords}"
                }
        
        # 默认走知识检索
        return {
            "intent": IntentType.KNOWLEDGE_QUERY,
            "next_agents": ["retriever"],
            "confidence": 0.5,
            "reasoning": "默认路由至检索 Agent"
        }

    def invoke(self, state: AgentState) -> AgentState:
        """执行路由"""
        return self.graph.invoke(state)


# 创建可插入主图的节点函数
def create_router_node():
    """返回一个 LangGraph 节点函数"""
    agent = RouterAgent()
    return agent.route_node


# 条件边函数：根据 next_agent 或 next_nodes 决定下一步
def route_next_agent(state: AgentState) -> str:
    """
    用于 LangGraph 条件边，返回下一个节点名称
    """
    # 打印debug信息
    logger.info(f"state keys {state.keys()}")

    # 从 intermediate_steps 中提取 next_agents 和 next_nodes
    next_agents = []
    next_nodes = []
    
    if state.get("intermediate_steps"):
        for step in state["intermediate_steps"]:
            if step[0] == "router" and isinstance(step[1], dict):
                next_agents = step[1].get("next_agents", [])
                next_nodes = step[1].get("next_nodes", [])
                logger.info(f"从 intermediate_steps 中提取 next_agents: {next_agents}")
                logger.info(f"从 intermediate_steps 中提取 next_nodes: {next_nodes}")
                break
    
    # 优先使用 next_nodes
    if next_nodes:
        logger.info(f"route_next_agent - 使用 next_nodes: {next_nodes}")
        if next_nodes:
            # 返回第一个节点
            next_node = next_nodes[0]
            # 更新 next_nodes 列表，移除已经执行过的节点
            remaining_nodes = next_nodes[1:]
            # 更新 intermediate_steps 中的 next_nodes
            if state.get("intermediate_steps"):
                for i, step in enumerate(state["intermediate_steps"]):
                    if step[0] == "router" and isinstance(step[1], dict):
                        state["intermediate_steps"][i][1]["next_nodes"] = remaining_nodes
                        logger.info(f"更新 intermediate_steps 中的 next_nodes: {remaining_nodes}")
                        break
            logger.info(f"route_next_agent - 剩余 next_nodes: {remaining_nodes}")
            return next_node
    
    # 处理 next_agents
    if next_agents:
        logger.info(f"route_next_agent - 使用 next_agents: {next_agents}")
        if next_agents:
            # 返回第一个节点
            next_agent = next_agents[0]
            # 更新 next_agents 列表，移除已经执行过的 agent
            remaining_agents = next_agents[1:]
            # 更新 intermediate_steps 中的 next_agents
            if state.get("intermediate_steps"):
                for i, step in enumerate(state["intermediate_steps"]):
                    if step[0] == "router" and isinstance(step[1], dict):
                        state["intermediate_steps"][i][1]["next_agents"] = remaining_agents
                        logger.info(f"更新 intermediate_steps 中的 next_agents: {remaining_agents}")
                        break
            logger.info(f"route_next_agent - 剩余 next_agents: {remaining_agents}")
            return next_agent

    # 尝试从顶层状态获取
    if state.get("next_nodes"):
        next_nodes = state["next_nodes"]
        logger.info(f"route_next_agent - 从顶层状态使用 next_nodes: {next_nodes}")
        if next_nodes:
            next_node = next_nodes[0]
            state["next_nodes"] = next_nodes[1:]
            return next_node
    
    if state.get("next_agents"):
        next_agents = state["next_agents"]
        logger.info(f"route_next_agent - 从顶层状态使用 next_agents: {next_agents}")
        if next_agents:
            next_agent = next_agents[0]
            state["next_agents"] = next_agents[1:]
            return next_agent

    return "responder"
    



