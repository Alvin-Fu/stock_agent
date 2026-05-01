"""
Agent 基础定义
包含共享状态类型、消息结构、常量等
"""

from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage
import operator

from utils.constants import IntentType, AgentName


class AgentState(TypedDict):
    """
    多 Agent 系统的共享状态定义

    字段说明：
        - messages: 对话消息历史（自动合并）
        - stock_code: 股票代码
        - question: 当前用户问题
        - intent: 识别出的意图类型
        - documents: 检索到的文档列表
        - financial_data: 结构化的财务数据（从文档/工具提取）
        - analysis_result: 分析 Agent 的输出结果
        - research_result: 研究 Agent 的输出结果
        - compliance_result: 合规 Agent 的输出结果
        - final_answer: 最终生成的回答
        - intermediate_steps: 中间步骤记录（用于调试）
        - next_agent: 下一步应调用的 Agent 名称
        - error: 错误信息（如有）
    """
    messages: Annotated[List[BaseMessage], add_messages]
    stock_code: str
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
    next_agent: Optional[str]
    error: Optional[str]


