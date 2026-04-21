"""
回答生成 Agent
职责：综合所有 Agent 的输出，生成最终用户回答
"""

from typing import Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END

from agents.base import AgentState
from core.llm import get_responder_llm
from utils.logger import logger



class ResponderAgent:
    def __init__(self):
        self.llm = get_responder_llm()
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(AgentState)
        workflow.add_node("generate", self.generate_node)
        workflow.set_entry_point("generate")
        workflow.add_edge("generate", END)
        return workflow.compile()

    def generate_node(self, state: AgentState) -> Dict[str, Any]:
        question = state.get("question", "")
        intent = state.get("intent", "unknown")
        documents = state.get("documents", [])
        analysis = state.get("analysis_result", {})
        research = state.get("research_result", {})
        compliance = state.get("compliance_result", {})

        logger.info("开始生成最终回答")

        # 构建综合上下文
        context = self._format_context(documents, analysis, research)

        system_prompt = """你是一位专业的财经顾问，请根据提供的资料回答用户问题。

【回答要求】
1. 语言专业、清晰、简洁
2. 涉及数据的必须注明来源（如文档片段编号）
3. 如资料不足，请诚实说明
4. 根据合规审查结果，必要时添加风险提示
5. 使用 Markdown 格式提升可读性

【合规提示】
{compliance_note}"""

        compliance_note = ""
        if compliance and compliance.get("required_disclaimer"):
            compliance_note = "⚠️ 请在回答末尾添加标准免责声明：*以上内容基于公开信息整理，不构成投资建议。*"
        if compliance and compliance.get("issues"):
            compliance_note += f"\n注意避免以下问题：{', '.join(compliance['issues'])}"

        user_message = f"""用户问题：{question}

【参考资料】
{context}

请生成回答。"""

        messages = [
            SystemMessage(content=system_prompt.format(compliance_note=compliance_note)),
            HumanMessage(content=user_message),
        ]

        response = self.llm.invoke(messages)
        final_answer = response.content

        # 自动追加免责声明（如果合规要求且未包含）
        if compliance.get("required_disclaimer") and "不构成投资建议" not in final_answer:
            final_answer += "\n\n---\n*免责声明：以上内容基于公开信息整理，不构成投资建议。*"

        logger.info("回答生成完成")

        return {
            "final_answer": final_answer,
            "intermediate_steps": state.get("intermediate_steps", []) + [("responder", final_answer[:200])],
        }

    def _format_context(self, documents, analysis, research) -> str:
        parts = []
        if documents:
            parts.append("【知识库检索结果】")
            for i, doc in enumerate(documents[:5], 1):
                source = doc.metadata.get("source", "未知来源")
                parts.append(f"[{i}] 来源：{source}\n{doc.page_content[:500]}...\n")
        if analysis:
            parts.append(f"【财务分析结果】\n{analysis.get('summary', '')}")
            if analysis.get("ratios"):
                parts.append(f"关键比率：{analysis['ratios']}")
        if research:
            parts.append(f"【实时信息补充】\n{research.get('summary', '')}")
        return "\n\n".join(parts) if parts else "无参考资料"

    def invoke(self, state: AgentState) -> AgentState:
        return self.graph.invoke(state)


def create_responder_node():
    agent = ResponderAgent()
    return agent.generate_node