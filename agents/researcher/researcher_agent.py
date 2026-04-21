"""
信息核实 Agent（Researcher）
职责：联网搜索最新信息，验证事实的时效性和准确性
"""

from typing import Dict, Any, List
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode

from agents.base import AgentState
from core.llm import get_default_llm
from .web_search_tool import web_search
from utils.logger import logger



class ResearcherAgent:
    def __init__(self):
        self.llm = get_default_llm()
        self.tools = [web_search]
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(AgentState)
        workflow.add_node("research", self.research_node)
        workflow.add_node("search_tools", ToolNode(self.tools))
        workflow.set_entry_point("research")
        workflow.add_conditional_edges(
            "research",
            self.should_continue,
            {"continue": "search_tools", "end": END}
        )
        workflow.add_edge("search_tools", "research")
        return workflow.compile()

    def research_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            question = state.get("question", "")
            documents = state.get("documents", [])
            logger.info(f"研究 Agent 开始处理: {question[:80]}...")

            # 构建上下文
            context = "\n".join([d.page_content[:500] for d in documents[:3]]) if documents else "无现有资料"

            system_prompt = """你是一个财经信息研究员，负责核实和补充最新信息。
当现有资料不足或需要实时数据时，请调用 web_search 工具搜索互联网。
搜索时请使用精确的关键词（如公司名+年份+财务指标）。
获得搜索结果后，请提炼关键信息并判断时效性。"""

            user_message = f"""用户问题：{question}

【现有资料摘要】
{context}

请判断是否需要联网搜索以补充信息。如需搜索，请调用工具。"""

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]

            response = self.llm_with_tools.invoke(messages)

            return {
                "messages": [response],
                "intermediate_steps": state.get("intermediate_steps", []) + [("researcher", response)],
            }
        except Exception as e:
            logger.error(f"研究节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"研究执行失败: {e}",
                "intermediate_steps": state.get("intermediate_steps", []) + [("researcher", {"error": str(e)})],
            }

    def should_continue(self, state: AgentState) -> str:
        last_message = state["messages"][-1]
        if hasattr(last_message, "tool_calls") and last_message.tool_calls:
            return "continue"
        # 将最终响应存入 research_result
        return "end"

    def invoke(self, state: AgentState) -> AgentState:
        try:
            result_state = self.graph.invoke(state)
            # 提取最终回答作为研究结果
            if result_state.get("messages") and len(result_state["messages"]) > 0:
                final_msg = result_state["messages"][-1]
                summary = final_msg.content if hasattr(final_msg, 'content') else str(final_msg)
            else:
                summary = "研究过程中未生成有效结果"
            
            result_state["research_result"] = {
                "summary": summary,
                "sources": self._extract_sources(result_state),
            }
            return result_state
        except Exception as e:
            logger.error(f"研究 Agent 执行失败: {e}")
            state["research_result"] = {
                "summary": f"研究执行失败: {e}",
                "sources": [],
            }
            state["error"] = f"研究执行失败: {e}"
            return state

    def _extract_sources(self, state: AgentState) -> List[str]:
        """从工具调用中提取来源 URL"""
        sources = []
        for msg in state["messages"]:
            if hasattr(msg, "tool_calls"):
                for tc in msg.tool_calls:
                    if tc.get("name") == "web_search":
                        sources.append(tc.get("args", {}).get("query", ""))
        return sources


def create_researcher_node():
    agent = ResearcherAgent()
    return agent.research_node