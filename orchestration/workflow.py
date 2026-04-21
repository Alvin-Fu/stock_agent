"""
工作流执行器
提供同步/异步、流式输出的执行接口
"""

import asyncio
from typing import Dict, Any, Optional, AsyncGenerator

from langchain_core.messages import HumanMessage

from agents.base import AgentState
from .graph import get_default_graph
from utils.logger import logger


class WorkflowExecutor:
    """
    工作流执行器，封装 LangGraph 调用细节
    """

    def __init__(self, enable_memory: bool = True):
        self.graph = get_default_graph(enable_memory=enable_memory)
        # 用于多轮对话的会话 ID（可动态传入）
        self.thread_id = "default"

    def _init_state(self, question: str, **kwargs) -> AgentState:
        """初始化状态"""
        return {
            "messages": [HumanMessage(content=question)],
            "question": question,
            "intent": None,
            "documents": [],
            "financial_data": None,
            "analysis_result": None,
            "research_result": None,
            "compliance_result": None,
            "final_answer": None,
            "intermediate_steps": [],
            "next_agent": None,
            "error": None,
            **kwargs
        }

    def run_sync(self, question: str, thread_id: Optional[str] = None) -> AgentState:
        """
        同步执行工作流
        :param question: 用户问题
        :param thread_id: 会话 ID（用于多轮对话记忆）
        :return: 最终状态
        """
        if thread_id:
            self.thread_id = thread_id

        initial_state = self._init_state(question)
        config = {"configurable": {"thread_id": self.thread_id}}

        logger.info(f"开始执行工作流，问题: {question[:50]}...")
        try:
            final_state = self.graph.invoke(initial_state, config)
            logger.info("工作流执行完成")
            return final_state
        except Exception as e:
            logger.error(f"工作流执行失败: {e}")
            initial_state["error"] = str(e)
            initial_state["final_answer"] = f"系统处理出错：{e}"
            return initial_state

    async def run_async(self, question: str, thread_id: Optional[str] = None) -> AgentState:
        """
        异步执行工作流（内部使用 run_in_executor）
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.run_sync, question, thread_id)

    async def run_stream(self, question: str, thread_id: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """
        流式执行工作流，实时返回中间状态
        :yield: 每次节点执行后的状态更新
        """
        if thread_id:
            self.thread_id = thread_id

        initial_state = self._init_state(question)
        config = {"configurable": {"thread_id": self.thread_id}}

        logger.info(f"开始流式执行，问题: {question[:50]}...")
        try:
            async for event in self.graph.astream(initial_state, config):
                yield event
        except Exception as e:
            logger.error(f"流式执行失败: {e}")
            yield {"error": str(e)}

    def get_final_answer(self, state: AgentState) -> str:
        """从最终状态提取回答文本"""
        if state.get("error"):
            return f"抱歉，处理您的问题时发生错误：{state['error']}"
        return state.get("final_answer", "未生成回答，请重试。")