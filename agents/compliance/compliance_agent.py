"""
合规审查 Agent
职责：检查回答是否包含不当投资建议、是否标注风险提示、引用是否合规
"""

import json
import re
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
            draft_content = self._build_draft_content(state)

            system_prompt = """你是金融合规审查专家。审查以下 AI 生成的多维度分析内容，检查是否存在：
1. 明确或暗示的投资建议（如"建议买入/卖出/加仓/减仓"）
2. 对未来股价的预测性陈述（如"股价将上涨到XX元"）
3. 缺少风险提示
4. 引用来源不明确
5. 绝对化表述（如"必定""一定""稳赚"）

请严格按照以下JSON格式输出（不要markdown包裹，不要解释）：
{
  "passed": true或false,
  "issues": ["问题1", "问题2"],
  "required_disclaimer": true或false,
  "risk_level": "low"或"medium"或"high",
  "suggested_edits": ["建议修改1"]
}

判断标准：
- passed=true：仅当无任何投资建议、无股价预测、有风险提示
- required_disclaimer=true：建议总是为true，金融分析必须配免责声明
- risk_level：low=无明显问题；medium=有少量绝对化表述；high=有投资建议或股价预测"""

            user_message = f"待审查内容：\n{draft_content}"

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]
            response = self.llm.invoke(messages)
            raw_response = response.content if hasattr(response, 'content') else str(response)

            review_result = self._parse_review_result(raw_response)

            logger.info(f"合规审查完成，通过: {review_result['passed']}，风险等级: {review_result.get('risk_level', 'unknown')}，问题数: {len(review_result.get('issues', []))}")

            return {
                "compliance_result": review_result,
                "intermediate_steps": state.get("intermediate_steps", []) + [("compliance", review_result)],
            }
        except Exception as e:
            logger.error(f"合规审查节点执行失败: {e}")
            review_result = {
                "passed": False,
                "issues": [f"合规审查过程中出现错误: {e}"],
                "required_disclaimer": True,
                "risk_level": "unknown",
                "raw_response": f"审查失败: {e}",
            }
            return {
                "compliance_result": review_result,
                "error": f"合规审查执行失败: {e}",
                "intermediate_steps": state.get("intermediate_steps", []) + [("compliance", {"error": str(e)})],
            }

    def _parse_review_result(self, raw_response: str) -> Dict[str, Any]:
        """解析 LLM 的合规审查结果"""
        default = {
            "passed": True,
            "issues": [],
            "required_disclaimer": True,
            "risk_level": "low",
            "raw_response": raw_response,
        }

        try:
            json_match = re.search(r'\{.*\}', raw_response, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(0))
                default.update({
                    "passed": bool(parsed.get("passed", True)),
                    "issues": parsed.get("issues", []) or [],
                    "required_disclaimer": bool(parsed.get("required_disclaimer", True)),
                    "risk_level": parsed.get("risk_level", "low"),
                    "suggested_edits": parsed.get("suggested_edits", []) or [],
                })
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"解析合规审查结果失败，使用默认值: {e}")

        return default

    def _build_draft_content(self, state: AgentState) -> str:
        """构建待审查的草稿内容（包含所有 Agent 的输出）"""
        parts = []

        analysis = state.get("analysis_result", {})
        if analysis and analysis.get("summary"):
            parts.append(f"[财务分析结果]\n{analysis.get('summary', '')}")

        research = state.get("research_result", {})
        if research and research.get("summary"):
            parts.append(f"[信息研究结果]\n{research.get('summary', '')}")

        technical = state.get("technical_result", {})
        if technical and technical.get("summary"):
            parts.append(f"[技术分析结果]\n{technical.get('summary', '')}")

        documents = state.get("documents", [])
        if documents:
            parts.append(f"[引用文档] 共 {len(documents)} 条")

        final_answer = state.get("final_answer")
        if final_answer:
            parts.append(f"[最终回答草稿]\n{final_answer}")

        return "\n\n".join(parts) if parts else "无内容"

    def invoke(self, state: AgentState) -> AgentState:
        return self.graph.invoke(state)


def create_compliance_node():
    agent = ComplianceAgent()
    return agent.review_node