"""
多 Agent 协作图构建模块
基于 LangGraph 定义节点和条件边
"""

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from agents.base import AgentState, AgentName, IntentType
from agents.router.router import create_router_node, route_next_agent
from agents.retriever.retriever_agent import create_retriever_node
from agents.financial_analyst.service import create_analyst_node
from agents.researcher.researcher_agent import create_researcher_node
from agents.compliance.compliance_agent import create_compliance_node
from agents.responder.responder_agent import create_responder_node
from utils.logger import logger


class MultiAgentGraph:
    """
    多 Agent 协作图构建器
    支持检查点持久化（用于多轮对话）
    """

    def __init__(self, enable_memory: bool = True):
        self.enable_memory = enable_memory
        self.memory = MemorySaver() if enable_memory else None
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """构建完整的协作图"""
        workflow = StateGraph(AgentState)

        # ---------- 添加所有 Agent 节点 ----------
        workflow.add_node("router", create_router_node())
        workflow.add_node("retriever", create_retriever_node())
        workflow.add_node("analyst", create_analyst_node())
        workflow.add_node("researcher", create_researcher_node())
        workflow.add_node("compliance", create_compliance_node())
        workflow.add_node("responder", create_responder_node())

        # ---------- 设置入口 ----------
        workflow.set_entry_point("router")

        # ---------- 路由分发：根据意图决定第一个执行 Agent ----------
        workflow.add_conditional_edges(
            "router",
            route_next_agent,
            {
                AgentName.RETRIEVER: "retriever",
                AgentName.ANALYST: "analyst",
                AgentName.RESEARCHER: "researcher",
                AgentName.RESPONDER: "responder",   # 闲聊直接回答
            }
        )

        # ---------- 定义下游连接 ----------
        # 检索完成后，根据原始意图决定是否需要分析或研究
        workflow.add_conditional_edges(
            "retriever",
            self._after_retriever_route,
            {
                "analyst": "analyst",
                "researcher": "researcher",
                "compliance": "compliance",
                "responder": "responder",
            }
        )

        # 分析完成后进入合规审查
        workflow.add_edge("analyst", "compliance")

        # 研究完成后进入合规审查
        workflow.add_edge("researcher", "compliance")

        # 合规审查完成后进入回答生成
        workflow.add_edge("compliance", "responder")

        # 回答生成后结束
        workflow.add_edge("responder", END)

        # 编译图
        if self.enable_memory:
            return workflow.compile(checkpointer=self.memory)
        return workflow.compile()

    def _after_retriever_route(self, state: AgentState) -> str:
        """
        检索完成后决定下一步：
        - 如果原始意图是财务分析 → analyst
        - 如果原始意图是实时信息 → researcher
        - 否则直接 → compliance → responder
        """
        intent = state.get("intent", "")
        docs_count = len(state.get("documents", []))
        logger.info(f"检索后路由，意图: {intent}, 检索到文档数: {docs_count}")

        # 确保数据完整性
        if not state.get("question"):
            logger.warning("状态中缺少question字段，使用默认值")
            state["question"] = ""

        if intent == IntentType.FINANCIAL_ANALYSIS:
            logger.info("路由到财务分析Agent")
            return "analyst"
        elif intent == IntentType.REAL_TIME_INFO:
            logger.info("路由到研究Agent")
            return "researcher"
        else:
            # 简单知识查询，直接进入合规审查（或可跳过）
            logger.info("路由到合规审查Agent")
            return "compliance"

    def get_compiled_graph(self):
        """返回编译后的图"""
        return self.graph


# 全局单例（可选）
_default_graph = None

def get_default_graph(enable_memory: bool = True):
    """获取默认的编译图实例"""
    global _default_graph
    if _default_graph is None:
        builder = MultiAgentGraph(enable_memory=enable_memory)
        _default_graph = builder.get_compiled_graph()
    return _default_graph