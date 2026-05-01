"""
多 Agent 协作图构建模块
基于 LangGraph 定义节点和条件边
"""

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from agents.base import AgentState
from utils.constants import AgentName, IntentType
from agents.router.router import create_router_node, route_next_agent
from agents.retriever.retriever_agent import create_retriever_node
from agents.financial_analyst.analyst import create_analyst_node
from agents.researcher.researcher_agent import create_researcher_node
from agents.technical_agent.technical_agent import create_technical_node
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

        # ---------- 1. 注册节点 ----------
        # 入口是路由
        workflow.add_node("router", create_router_node())
        # 下游节点
        workflow.add_node("retriever", create_retriever_node())
        workflow.add_node("analyst", create_analyst_node())
        workflow.add_node("researcher", create_researcher_node())
        workflow.add_node("technical", create_technical_node())
        workflow.add_node("compliance", create_compliance_node())
        workflow.add_node("responder", create_responder_node())

        # ---------- 设置入口 ----------
        workflow.set_entry_point("router")

        # ---------- 路由分发：根据意图决定第一个执行 Agent ----------
        # 使用route_next_agent函数来获取下一个节点
        workflow.add_conditional_edges(
            "router",
            route_next_agent,
            {
                "retriever": "retriever",
                "analyst": "analyst",
                "researcher": "researcher",
                "technical": "technical",
                "compliance": "compliance",
                "responder": "responder",
            }
        )

        # ---------- 定义下游连接 ----------
        # 检索完成后，根据next_agents决定下一步
        workflow.add_conditional_edges(
            "retriever",
            route_next_agent,
            {
                "analyst": "analyst",
                "researcher": "researcher",
                "technical": "technical",
                "compliance": "compliance",
                "responder": "responder",
            }
        )

        # 分析完成后根据next_agents决定下一步
        workflow.add_conditional_edges(
            "analyst",
            route_next_agent,
            {
                "technical": "technical",
                "researcher": "researcher",
                "compliance": "compliance",
                "responder": "responder",
            }
        )

        # 研究完成后根据next_agents决定下一步
        workflow.add_conditional_edges(
            "researcher",
            route_next_agent,
            {
                "technical": "technical",
                "analyst": "analyst",
                "compliance": "compliance",
                "responder": "responder",
            }
        )

        # 技术分析完成后根据next_agents决定下一步
        workflow.add_conditional_edges(
            "technical",
            route_next_agent,
            {
                "analyst": "analyst",
                "researcher": "researcher",
                "compliance": "compliance",
                "responder": "responder",
            }
        )

        # 合规审查完成后进入回答生成
        workflow.add_edge("compliance", "responder")

        # 回答生成后结束
        workflow.add_edge("responder", END)

        # 编译图
        if self.enable_memory:
            return workflow.compile(checkpointer=self.memory)
        return workflow.compile()

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
