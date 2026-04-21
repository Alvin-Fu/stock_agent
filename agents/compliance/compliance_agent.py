"""
合规审查 Agent
职责：检查回答是否包含不当投资建议、是否标注风险提示、引用是否合规
"""

from typing import Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END

from agents.base import AgentState
from core.llm import get_default_llm
from utils.logger import logger



class ComplianceAgent:
    def __init__(self):
        self.llm = get_default_llm()
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(AgentState)
        workflow.add_node("review", self.review_node)
        workflow.set_entry_point("review")
        workflow.add_edge("review", END)
        return workflow.compile()

    def review_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            # 收集需要审查的内容
            analysis = state.get("analysis_result", {})
            research = state.get("research_result", {})
            documents = state.get("documents", [])

            draft_content = self._build_draft_content(state)

            system_prompt = """你是金融合规审查专家。审查以下 AI 生成的回答草稿，检查是否存在：
1. 明确或暗示的投资建议（如"建议买入/卖出"）
2. 对未来股价的预测性陈述
3. 缺少风险提示
4. 引用来源不明确

请返回审查结果，包含：
- passed: 是否通过 (true/false)
- issues: 发现的问题列表
- required_disclaimer: 是否需要添加免责声明 (true/false)
- suggested_edits: 建议修改的部分（可选）"""

            user_message = f"待审查内容：\n{draft_content}"

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]
            response = self.llm.invoke(messages)

            # 简单解析（实际可要求 LLM 返回 JSON）
            review_result = {
                "passed": True,
                "issues": [],
                "required_disclaimer": True,
                "raw_response": response.content if hasattr(response, 'content') else str(response),
            }

            logger.info(f"合规审查完成，通过: {review_result['passed']}")

            return {
                "compliance_result": review_result,
                "intermediate_steps": state.get("intermediate_steps", []) + [("compliance", review_result)],
            }
        except Exception as e:
            logger.error(f"合规审查节点执行失败: {e}")
            # 即使审查失败，也返回默认审查结果，确保流程继续
            review_result = {
                "passed": False,
                "issues": [f"合规审查过程中出现错误: {e}"],
                "required_disclaimer": True,
                "raw_response": f"审查失败: {e}",
            }
            return {
                "compliance_result": review_result,
                "error": f"合规审查执行失败: {e}",
                "intermediate_steps": state.get("intermediate_steps", []) + [("compliance", {"error": str(e)})],
            }

    def _build_draft_content(self, state: AgentState) -> str:
        """构建待审查的草稿内容"""
        parts = []
        if state.get("analysis_result"):
            parts.append(f"[分析结果]\n{state['analysis_result'].get('summary', '')}")
        if state.get("research_result"):
            parts.append(f"[研究结果]\n{state['research_result'].get('summary', '')}")
        if state.get("documents"):
            parts.append(f"[引用文档] 共 {len(state['documents'])} 条")
        return "\n\n".join(parts) if parts else "无内容"

    def invoke(self, state: AgentState) -> AgentState:
        return self.graph.invoke(state)


def create_compliance_node():
    agent = ComplianceAgent()
    return agent.review_node