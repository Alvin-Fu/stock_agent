"""
信息研究 Agent（Researcher）
职责：多维搜索全网产业/公司信息，交叉验证，输出结构化分析结论
"""

import traceback
from typing import Dict, Any, List
from datetime import date, datetime, timedelta
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from core.llm import get_default_llm
from .web_search_tool import web_search
from utils.logger import logger


class ResearcherAgent:
    """研究 Agent：程序化多维搜索 → LLM 一次性综合分析"""

    def __init__(self):
        self.llm = get_default_llm()

    def _build_search_queries(self, stock_code: str, question: str) -> List[str]:
        """
        根据股票代码和用户问题，生成覆盖全维度的搜索查询
        每个维度一条查询，确保覆盖：
          公告、产业、经营、技术、产业链地位、利好/利空
        时间范围：最近1个月为主，部分维度扩展到最近3个月
        """
        today = date.today()
        one_month_ago = today - timedelta(days=30)
        three_months_ago = today - timedelta(days=90)

        # 格式化日期范围，搜索引擎通常支持 "2025年3月" 或 "2025-03"
        recent_period = f"{today.year}年{today.month}月"
        three_month_range = f"{three_months_ago.strftime('%Y-%m')} {today.strftime('%Y-%m')}"

        return [
            f"{stock_code} 公司公告 重大事项 {one_month_ago.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",
            f"{stock_code} 所属行业 产业政策 发展趋势 {three_month_range}",
            f"{stock_code} 经营状况 营收 利润 最新业绩 {recent_period}",
            f"{stock_code} 技术实力 研发投入 核心竞争力 专利",
            f"{stock_code} 产业链 上下游 市场地位 竞争格局 {recent_period}",
            f"{stock_code} 利好 利空 机构评级 目标价 {one_month_ago.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",
        ]

    def _do_search(self, queries: List[str]) -> Dict[str, str]:
        """每个查询搜一次，返回 {query: result}"""
        results = {}
        for q in queries:
            try:
                logger.info(f"搜索: {q[:40]}...")
                results[q] = web_search.invoke({"query": q})
            except Exception as e:
                logger.error(f"搜索失败 [{q}]: {e}")
                results[q] = f"搜索失败: {e}"
        return results

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            stock_code = state.get("stock_code", "")
            question = state.get("question", "")
            logger.info(f"研究 Agent 开始，股票: {stock_code}，问题: {question[:80]}...")

            # 1. 生成多维搜索查询
            queries = self._build_search_queries(stock_code, question)

            # 2. 并行搜索（每个维度一条）
            all_results = self._do_search(queries)

            # 3. 拼接所有搜索结果
            search_text = ""
            for q, result in all_results.items():
                search_text += f"\n{'='*40}\n【{q}】\n{result}\n"

            # 4. LLM 综合分析
            system_prompt = """你是一个专业的股票信息研究员和分析师。

请基于下方搜索结果，对该公司的以下维度进行客观分析并给出核心结论：

1. **公司公告与重大事项**：
   - 近期是否有重大公告（增发/回购/并购/分红等）
   - 这些公告对公司的影响

2. **产业信息**：
   - 所属行业当前景气度
   - 行业政策是否利好或利空
   - 行业发展趋势

3. **经营状态**：
   - 最新营收/利润情况
   - 同比/环比变化
   - 经营是否稳健

4. **技术实力**：
   - 研发投入水平
   - 核心技术/专利情况
   - 技术壁垒是否明显

5. **产业链地位**：
   - 公司在产业链中的位置（上游/中游/下游）
   - 上下游议价能力
   - 竞争格局和市场份额

6. **利好与利空分析**：
   - 利好消息汇总及影响力评估（高/中/低）
   - 利空消息汇总及影响力评估（高/中/低）
   - 利好利空消息数量占比
   - 综合偏多/偏空判断

【输出要求】
- 每个维度给出明确结论，依据搜索结果
- 如果搜索结果不足，请标注「信息不足」
- 利好利空分析中必须给出占比和综合判断
- 最后给出 3 句以内的核心结论总结"""

            user_message = f"""用户问题：{question}
股票代码：{stock_code}

========== 全网搜索结果 ==========
{search_text[:12000]}

请基于以上信息进行全面分析。"""

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]

            logger.info("LLM 综合分析中...")
            response = self.llm.invoke(messages)
            summary = response.content if hasattr(response, 'content') else str(response)
            logger.info(f"研究分析完成，长度: {len(summary)}， {summary[:200]}")

            # 提取搜索来源
            sources = [q for q in queries]

            return {
                "messages": [response],
                "research_result": {
                    "summary": summary,
                    "sources": sources,
                },
                "current_node": "researcher",
                "intermediate_steps": state.get("intermediate_steps", []) + [
                    ("researcher", {"stock_code": stock_code, "queries": len(queries), "content": summary[:200]})
                ],
            }

        except Exception as e:
            logger.error(f"研究节点执行失败: {e}\n{traceback.format_exc()}")
            return {
                "messages": [],
                "research_result": {
                    "summary": f"研究执行失败: {e}",
                    "sources": [],
                },
                "error": f"研究执行失败: {e}",
                "intermediate_steps": state.get("intermediate_steps", []) + [("researcher", {"error": str(e)})],
            }

    def invoke(self, state: AgentState) -> Dict[str, Any]:
        return self.analyze_node(state)


def create_researcher_node():
    """
    创建研究节点，返回 analyze_node（直通流程，不走工具调用循环）
    """
    agent = ResearcherAgent()
    return agent.analyze_node

