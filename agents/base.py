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
        - ranked_candidates: 产业链候选公司排名数据（含排名/评分/基本面分析，由 researcher 产出供 technical 交叉分析）
        - stock_attribute: 标的属性分类（周期股/成长股/防御股/价值股），由 router 统一判定，下游 Agent 直接读取避免重复调用
        - current_node: 当前执行节点名（用于调试与状态追踪）
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
    ranked_candidates: Optional[List[Dict[str, Any]]]
    stock_attribute: Annotated[Optional[Dict[str, Any]], _keep_last]  # 标的属性分类（周期股/成长股/防御股/价值股），由 router 统一判定；并行分支都写入此字段时用 _keep_last 合并
    current_node: Optional[str]  # 当前执行节点名（用于调试与状态追踪）
    question: str
    intent: Optional[str]
    documents: List[Any]  # Document 对象列表
    financial_data: Optional[Dict[str, Any]]
    analysis_result: Optional[Dict[str, Any]]
    research_result: Optional[Dict[str, Any]]
    compliance_result: Optional[Dict[str, Any]]
    technical_result: Optional[Dict[str, Any]]
    quality_metrics: Optional[Dict[str, Any]]  # 质量否决权指标（ROE/扣非/商誉）
    final_answer: Optional[str]
    intermediate_steps: Annotated[List[tuple], operator.add]
    next_agents: List[str]
    confidence: Optional[float]
    error: Annotated[Optional[str], _keep_last]


