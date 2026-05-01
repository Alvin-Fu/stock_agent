"""
系统常量定义
"""

# 意图类型常量
class IntentType:
    """预定义的意图分类"""
    KNOWLEDGE_QUERY = "knowledge_query"  # 简单知识问答
    FINANCIAL_ANALYSIS = "financial_analysis"  # 财务比率计算与分析
    REAL_TIME_INFO = "real_time_info"  # 需要联网查询实时信息
    COMPLIANCE_CHECK = "compliance_check"  # 合规性审查
    TECHNICAL_ANALYSIS = "technical_analysis"  # 技术分析（均线、MACD等）
    GENERAL_CHAT = "general_chat"  # 闲聊/问候
    UNKNOWN = "unknown"  # 无法识别


# 可用的 Agent 名称常量
class AgentName:
    """预定义的 Agent 名称"""
    RETRIEVER = "retriever"
    ROUTER = "router"
    ANALYST = "analyst"
    RESEARCHER = "researcher"
    TECHNICAL = "technical"
    COMPLIANCE = "compliance"
    RESPONDER = "responder"
