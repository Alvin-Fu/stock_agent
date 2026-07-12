"""
多 Agent 协作图构建模块
基于 LangGraph 定义节点和条件边

结构（并行版）：
    router（大脑：意图识别 + 给出执行计划 next_agents）
      → 计划中的专职节点**并行执行**（retriever / analyst / researcher / technical
        相互独立，同一超步并发，总耗时≈最慢的一个而不是全部之和）
      → responder（等全部分支完成后整合生成最终回答）
      → compliance（对最终回答做合规审查 + 程序数字回查）
      → END
    例外：产业链模式下 technical 依赖 researcher 产出的候选代码，
    保持 researcher → technical 接力（此时首波只跑 researcher）。
"""

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from agents.base import AgentState
from agents.router.router import create_router_node
from agents.retriever.retriever_agent import create_retriever_node
from agents.financial_analyst.analyst import create_analyst_node
from agents.researcher.researcher_agent import create_researcher_node
from agents.technical_agent.technical_agent import create_technical_node
from agents.compliance.compliance_agent import create_compliance_node
from agents.responder.responder_agent import create_responder_node
from utils.logger import logger

EXEC_AGENTS = ("retriever", "analyst", "researcher", "technical")


def _is_industry_mode(state) -> bool:
    """产业链模式：有行业名（此时 technical 的输入依赖 researcher 筛出的候选代码）"""
    return bool(state.get("industry_name")) and not state.get("stock_code")


def route_fanout(state):
    """
    router 之后的并行分发（返回列表 = 同一超步并行执行）。
    产业链模式只放 researcher 首发，其余计划节点由接力边处理，
    避免 responder 在不同超步被触发两次。
    """
    plan = [a for a in (state.get("next_agents") or []) if a in EXEC_AGENTS]
    if not plan:
        return ["responder"]
    if _is_industry_mode(state):
        if "researcher" in plan:
            dropped = [a for a in plan if a not in ("researcher",)]
            if [a for a in dropped if a != "technical"]:
                logger.info(f"产业链模式：{dropped} 中除 technical 外的节点不适用多代码输入，跳过")
            return ["researcher"]
    return plan


def route_after_researcher(state):
    """researcher 之后：产业链模式且计划含 technical 时接力，否则汇合到 responder"""
    plan = state.get("next_agents") or []
    if state.get("industry_name") and "technical" in plan and "," in (state.get("stock_code") or ""):
        return "technical"
    return "responder"


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
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
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
        """构建完整的协作图（执行节点并行，产业链模式 researcher→technical 接力）"""
        workflow = StateGraph(AgentState)

        # ---------- 注册节点 ----------
        workflow.add_node("router", create_router_node())
        workflow.add_node("retriever", create_retriever_node())
        workflow.add_node("analyst", create_analyst_node())
        workflow.add_node("researcher", create_researcher_node())
        workflow.add_node("technical", create_technical_node())
        workflow.add_node("responder", create_responder_node())
        workflow.add_node("compliance", create_compliance_node())

        workflow.set_entry_point("router")

        # ---------- 并行分发：route_fanout 返回列表，同一超步并发执行 ----------
        fanout_targets = {name: name for name in EXEC_AGENTS}
        fanout_targets["responder"] = "responder"
        workflow.add_conditional_edges("router", route_fanout, fanout_targets)

        # ---------- 汇合：并行分支全部完成后 responder 才执行（多入边=等待所有激活的前驱） ----------
        workflow.add_edge("retriever", "responder")
        workflow.add_edge("analyst", "responder")
        workflow.add_edge("technical", "responder")
        # researcher 的出边有分支：产业链模式接力 technical，其余直接汇合
        workflow.add_conditional_edges("researcher", route_after_researcher,
                                       {"technical": "technical", "responder": "responder"})

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
