"""
多 Agent 协作图构建模块
基于 LangGraph 定义节点和条件边

结构：
    router（大脑：意图识别 + 排出执行队列 next_agents）
      → 队列中的各专职节点（retriever / analyst / researcher / technical），
        每个节点执行完由包装器把自己从队首弹出，条件边读队首决定下一跳
      → responder（整合所有结果生成最终回答）
      → compliance（对最终回答做合规审查，必要时补免责声明）
      → END
"""

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from agents.base import AgentState
from agents.router.router import create_router_node, route_next_agent, with_queue_pop
from agents.retriever.retriever_agent import create_retriever_node
from agents.financial_analyst.analyst import create_analyst_node
from agents.researcher.researcher_agent import create_researcher_node
from agents.technical_agent.technical_agent import create_technical_node
from agents.compliance.compliance_agent import create_compliance_node
from agents.responder.responder_agent import create_responder_node
from utils.logger import logger


def _make_checkpointer():
    """
    对话记忆检查点：优先 SQLite 持久化（重启进程不丢多轮上下文），
    未安装 langgraph-checkpoint-sqlite 时回退内存记忆并告警。
    """
    try:
        import os
        import sqlite3
        from langgraph.checkpoint.sqlite import SqliteSaver

        path = "./data/sqlite/conversation_memory.db"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # 分析在工作线程里跑，必须放开 same_thread 限制（SqliteSaver 内部有锁）
        conn = sqlite3.connect(path, check_same_thread=False)
        logger.info(f"对话记忆使用 SQLite 持久化: {path}")
        return SqliteSaver(conn)
    except Exception as e:
        logger.warning(f"SQLite 记忆不可用（pip install langgraph-checkpoint-sqlite 可启用持久化），"
                       f"回退进程内记忆: {e}")
        return MemorySaver()


class MultiAgentGraph:
    """
    多 Agent 协作图构建器
    支持检查点持久化（用于多轮对话）
    """

    def __init__(self, enable_memory: bool = True):
        self.enable_memory = enable_memory
        self.memory = _make_checkpointer() if enable_memory else None
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """构建完整的协作图"""
        workflow = StateGraph(AgentState)

        # ---------- 注册节点 ----------
        workflow.add_node("router", create_router_node())
        workflow.add_node("retriever", with_queue_pop("retriever", create_retriever_node()))
        workflow.add_node("analyst", with_queue_pop("analyst", create_analyst_node()))
        workflow.add_node("researcher", with_queue_pop("researcher", create_researcher_node()))
        workflow.add_node("technical", with_queue_pop("technical", create_technical_node()))
        workflow.add_node("responder", create_responder_node())
        workflow.add_node("compliance", create_compliance_node())

        workflow.set_entry_point("router")

        # ---------- 队列驱动的条件边 ----------
        route_targets = {
            "retriever": "retriever",
            "analyst": "analyst",
            "researcher": "researcher",
            "technical": "technical",
            "responder": "responder",
        }
        for source in ["router", "retriever", "analyst", "researcher", "technical"]:
            workflow.add_conditional_edges(source, route_next_agent, route_targets)

        # ---------- 收尾：整合回答 → 合规审查 ----------
        workflow.add_edge("responder", "compliance")
        workflow.add_edge("compliance", END)

        if self.enable_memory:
            return workflow.compile(checkpointer=self.memory)
        return workflow.compile()

    def get_compiled_graph(self):
        """返回编译后的图"""
        return self.graph


# 全局单例（按 enable_memory 分开缓存）
_graph_cache = {}

def get_default_graph(enable_memory: bool = True):
    """获取默认的编译图实例"""
    if enable_memory not in _graph_cache:
        builder = MultiAgentGraph(enable_memory=enable_memory)
        _graph_cache[enable_memory] = builder.get_compiled_graph()
    return _graph_cache[enable_memory]
