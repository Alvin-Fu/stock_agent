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

from agents.base import AgentState, IntentType, AgentName
from core.llm import get_router_llm
from .prompts import ROUTER_SYSTEM_PROMPT, ROUTER_USER_TEMPLATE
from utils.logger import logger


class RouterAgent:
    """路由 Agent，基于 LLM 的意图识别与分发"""

    # 备用规则映射（当 LLM 解析失败时启用）
    FALLBACK_RULES = [
        (["计算", "比率", "ROE", "ROA", "毛利率", "净利率", "估值", "杜邦"], IntentType.FINANCIAL_ANALYSIS, AgentName.ANALYST),
        (["股价", "新闻", "最新", "实时", "今天", "公告"], IntentType.REAL_TIME_INFO, AgentName.RESEARCHER),
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
        next_agent = route_result.get("next_agent")
        confidence = route_result.get("confidence", 0.5)
        reasoning = route_result.get("reasoning", "")

        logger.info(f"路由结果: intent={intent}, next_agent={next_agent}, confidence={confidence}")

        # 确保 next_agent 映射到正确的节点名称
        node_map = {
            AgentName.RETRIEVER: "retriever",
            AgentName.ANALYST: "analyst",
            AgentName.RESEARCHER: "researcher",
            AgentName.RESPONDER: "responder",
        }
        next_node = node_map.get(next_agent, "responder")

        # 返回状态更新
        return {
            "intent": intent,
            "next_agent": next_agent,
            "next_node": next_node,
            "confidence": confidence,
            "intermediate_steps": [("router", {"intent": intent, "next_agent": next_agent, "next_node": next_node, "reasoning": reasoning})],
        }

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
        for keywords, intent, agent in self.FALLBACK_RULES:
            if any(kw in question_lower for kw in keywords):
                return {
                    "intent": intent,
                    "next_agent": agent,
                    "confidence": 0.7,
                    "reasoning": f"规则匹配关键词: {keywords}"
                }
        # 默认走知识检索
        return {
            "intent": IntentType.KNOWLEDGE_QUERY,
            "next_agent": AgentName.RETRIEVER,
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


# 条件边函数：根据 next_agent 决定下一步
def route_next_agent(state: AgentState) -> str:
    """
    用于 LangGraph 条件边，返回下一个节点名称
    """
    # 优先使用 next_node
    if state.get("next_node"):
        return state["next_node"]
    
    # 兼容旧逻辑
    next_agent = state.get("next_agent")
    node_map = {
        AgentName.RETRIEVER: "retriever",
        AgentName.ANALYST: "analyst",
        AgentName.RESEARCHER: "researcher",
        AgentName.RESPONDER: "responder",
    }
    
    if next_agent in node_map:
        return node_map[next_agent]
    elif next_agent is None:
        # 无下游，直接进入 responder
        return "responder"
    else:
        logger.warning(f"未知的 next_agent: {next_agent}，默认进入 responder")
        return "responder"