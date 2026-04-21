"""
Agent 基础定义
包含共享状态类型、消息结构、常量等
"""

from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage
import operator


class AgentState(TypedDict):
    """
    多 Agent 系统的共享状态定义

    字段说明：
        - messages: 对话消息历史（自动合并）
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
    question: str
    intent: Optional[str]
    documents: List[Any]  # Document 对象列表
    financial_data: Optional[Dict[str, Any]]
    analysis_result: Optional[Dict[str, Any]]
    research_result: Optional[Dict[str, Any]]
    compliance_result: Optional[Dict[str, Any]]
    final_answer: Optional[str]
    intermediate_steps: Annotated[List[tuple], operator.add]
    next_agent: Optional[str]
    error: Optional[str]


# 意图类型常量
class IntentType:
    """预定义的意图分类"""
    KNOWLEDGE_QUERY = "knowledge_query"  # 简单知识问答
    FINANCIAL_ANALYSIS = "financial_analysis"  # 财务比率计算与分析
    REAL_TIME_INFO = "real_time_info"  # 需要联网查询实时信息
    COMPLIANCE_CHECK = "compliance_check"  # 合规性审查
    GENERAL_CHAT = "general_chat"  # 闲聊/问候
    UNKNOWN = "unknown"  # 无法识别


# 可用的 Agent 名称常量
class AgentName:
    ROUTER = "router"
    RETRIEVER = "retriever"
    ANALYST = "analyst"
    RESEARCHER = "researcher"
    COMPLIANCE = "compliance"
    RESPONDER = "responder"