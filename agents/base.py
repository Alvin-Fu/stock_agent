"""
Agent 基础定义
包含共享状态类型、消息结构、常量等
"""

from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage
import operator

from utils.constants import IntentType, AgentName


def _keep_last(a, b):
    """并行分支同时写入时的合并器：保留最新的非空值（error 等标量字段用）"""
    return b or a


class AgentState(TypedDict):
    """
    多 Agent 系统的共享状态定义

    字段说明：
        - messages: 对话消息历史（自动合并）
        - stock_code: 股票代码
        - stock_type: 标的类型（a_stock / etf / hk_stock），由路由器设置，用于管线分流
        - industry_name: 行业名称（行业/产业链分析时使用）
        - chain_leaders: 行业龙头股列表 [{code, name, rank}]（龙一龙二）
        - question: 当前用户问题
        - intent: 识别出的意图类型
        - documents: 检索到的文档列表
        - financial_data: 结构化的财务数据（从文档/工具提取）
        - analysis_result: 分析 Agent 的输出结果
        - research_result: 研究 Agent 的输出结果
        - compliance_result: 合规 Agent 的输出结果
        - technical_result: 技术分析 Agent 的输出结果
        - final_answer: 最终生成的回答
        - intermediate_steps: 中间步骤记录（用于调试）
        - next_agents: 大脑（router）排出的待执行 Agent 队列，各节点执行后弹出自己
        - confidence: 路由置信度
        - error: 错误信息（如有）
    """
    messages: Annotated[List[BaseMessage], add_messages]
    stock_code: str
    stock_type: Optional[str]  # a_stock / etf / hk_stock
    industry_name: Optional[str]
    chain_leaders: Optional[Dict[str, Any]]
    question: str
    intent: Optional[str]
    documents: List[Any]  # Document 对象列表
    financial_data: Optional[Dict[str, Any]]
    analysis_result: Optional[Dict[str, Any]]
    research_result: Optional[Dict[str, Any]]
    compliance_result: Optional[Dict[str, Any]]
    technical_result: Optional[Dict[str, Any]]
    final_answer: Optional[str]
    intermediate_steps: Annotated[List[tuple], operator.add]
    next_agents: List[str]
    confidence: Optional[float]
    error: Annotated[Optional[str], _keep_last]


