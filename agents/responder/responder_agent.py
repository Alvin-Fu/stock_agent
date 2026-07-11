"""
回答生成 Agent
职责：综合所有 Agent 的输出，生成最终用户回答
（免责声明与合规修订由其后的 compliance 节点负责）
"""

from datetime import date
from typing import Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from core.llm import get_responder_llm
from utils.logger import logger


class ResponderAgent:
    def __init__(self):
        self.llm = get_responder_llm()

    def generate_node(self, state: AgentState) -> Dict[str, Any]:
        question = state.get("question", "")
        documents = state.get("documents", [])
        analysis = state.get("analysis_result", {})
        research = state.get("research_result", {})
        technical = state.get("technical_result", {})

        logger.info("开始生成最终回答")

        context = self._format_context(documents, analysis, research, technical)

        system_prompt = f"""你是一位专业的财经顾问，请根据提供的资料回答用户问题。
今天的日期是 {date.today().strftime('%Y-%m-%d')}，请以此为时间基准表述"最新/近期"。

【回答要求】
1. 语言专业、清晰、简洁
2. 数据必须注明出自哪个模块（"根据财务报表数据/技术分析/网络研究信息"），
   不要编造更具体的来源（如具体研报名、公告编号），参考资料里没有就不写
3. 如资料不足，请诚实说明缺失，禁止用"缺乏数据，但…"这类没有信息量的凑数表述
4. 每个定性结论必须与数据一致，禁止套用与数据矛盾的模板化说法
   （例如：均价上涨时不得写"以价换量"）
5. 不给出明确的投资建议（买入/卖出），仅做客观分析
6. 使用 Markdown 格式提升可读性，结构化输出：标题、列表、表格"""

        user_message = f"""用户问题：{question}

【参考资料】
{context}

请生成回答。"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message),
        ]

        response = self.llm.invoke(messages)
        final_answer = response.content

        logger.info("回答生成完成")

        return {
            "final_answer": final_answer,
            "intermediate_steps": [("responder", final_answer[:200])],
        }

    def _format_context(self, documents, analysis, research, technical) -> str:
        parts = []
        if documents:
            parts.append("【知识库检索结果】")
            for i, doc in enumerate(documents[:5], 1):
                source = doc.metadata.get("source", "未知来源")
                parts.append(f"[{i}] 来源：{source}\n{doc.page_content[:800]}...\n")
        if analysis:
            parts.append(f"【财务分析结果】\n{analysis.get('summary', '')}")
            if analysis.get("ratios"):
                parts.append(f"关键比率：{analysis['ratios']}")
            if analysis.get("data_source"):
                parts.append(f"数据来源：{analysis['data_source']}")
        if research:
            parts.append(f"【实时信息研究】\n{research.get('summary', '')}")
        if technical:
            parts.append(f"【技术分析结果】\n{technical.get('summary', '')}")
            if technical.get("mode"):
                parts.append(f"分析模式：{technical['mode']}")
        return "\n\n".join(parts) if parts else "无参考资料"


def create_responder_node():
    agent = ResponderAgent()
    return agent.generate_node
