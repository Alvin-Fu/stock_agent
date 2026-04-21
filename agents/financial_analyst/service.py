"""
财务分析 Agent
职责：解读财报数据，计算财务比率，生成分析结论
依赖：向量检索结果 + 财务计算工具 + LLM
"""

from typing import Dict, Any, List, Optional
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from datetime import date, datetime

from core.llm import get_analyst_llm
from agents.base import AgentState
from .tools import (
    calculate_profitability_ratios,
    calculate_liquidity_ratios,
    calculate_solvency_ratios,
    calculate_valuation_ratios,
    calculate_growth_rates,
    perform_dupont_analysis,
)
from tools.stock_tools import stock_analyst_tools
from utils.logger import logger
from storage.sqlite.stock_storage import get_db
import os


class AnalystAgent:
    """财务分析 Agent 的 LangGraph 实现"""

    def __init__(self):
        """
        初始化财务分析 Agent
        """
        self.llm = get_analyst_llm()
        # 合并财务计算工具和研报获取工具
        self.tools = [
            calculate_profitability_ratios,
            calculate_liquidity_ratios,
            calculate_solvency_ratios,
            calculate_valuation_ratios,
            calculate_growth_rates,
            perform_dupont_analysis,
        ] + stock_analyst_tools
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        self.db = get_db()
        self.graph = self._build_graph()
    
    def _identify_pdf_type(self, report_name: str, report_type: str = None) -> str:
        """
        识别PDF文件类型
        
        Args:
            report_name: 报告名称
            
        Returns:
            str: 报告类型，可能值：机构研报、年报、季报
        """
        report_name_lower = report_name.lower()
        if report_type is not None:
            return report_type
        
        # 识别年报
        if any(keyword in report_name_lower for keyword in ['年报', 'annual', 'yearly']):
            return "年报"
        # 识别季报
        elif any(keyword in report_name_lower for keyword in ['季报', 'quarterly', 'q1', 'q2', 'q3', 'q4']):
            return "季报"
        # 识别机构研报
        elif any(keyword in report_name_lower for keyword in ['研报', 'research', 'report', 'analyst']):
            return "机构研报"
        # 默认类型
        else:
            return "机构研报"
    


    def _build_graph(self) -> StateGraph:
        """构建分析工作流图"""
        workflow = StateGraph(AgentState)

        # 添加节点
        workflow.add_node("analyze", self.analyze_node)
        workflow.add_node("tools", ToolNode(self.tools))
        workflow.add_node("finalize", self.finalize_node)

        # 设置入口
        workflow.set_entry_point("analyze")

        # 条件边：是否需要调用工具
        workflow.add_conditional_edges(
            "analyze",
            self.should_continue,
            {
                "continue": "tools",
                "end": "finalize",
            }
        )

        # 工具调用后返回 analyze 继续推理
        workflow.add_edge("tools", "analyze")

        # 最终节点结束
        workflow.add_edge("finalize", END)

        return workflow.compile()

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        """
        分析节点：调用 LLM 进行财务推理
        输入 state 包含：
            - question: 用户问题
            - documents: 检索到的财报文本块
            - financial_data: 预处理的结构化财务数据（可选）
        """
        try:
            logger.info(f"开始分析，问题: {state.get('question', '')[:50]}...")

            # 构建提示消息
            system_prompt = self._build_system_prompt()
            context = self._format_context(state)
            user_message = f"""请基于以下财务资料进行分析：

【参考资料】
{context}

【用户问题】
{state['question']}

请使用可用的财务工具进行计算，并给出专业分析意见。

如果需要研报数据，请使用 stock_research_report_fetcher 工具获取。"""

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]

            # 调用 LLM（可能返回工具调用请求）
            response = self.llm_with_tools.invoke(messages)

            return {
                "messages": [response],
                "intermediate_steps": state.get("intermediate_steps", []) + [("analyze", response)],
            }
        except Exception as e:
            logger.error(f"分析节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"分析执行失败: {e}",
                "intermediate_steps": state.get("intermediate_steps", []) + [("analyze", {"error": str(e)})],
            }

    def finalize_node(self, state: AgentState) -> Dict[str, Any]:
        """
        最终节点：生成结构化分析报告
        """
        try:
            logger.info("生成最终分析报告")

            # 从消息历史中提取最终 LLM 响应
            if state.get("messages") and len(state["messages"]) > 0:
                final_message = state["messages"][-1]
                summary = final_message.content if hasattr(final_message, 'content') else str(final_message)
            else:
                summary = "分析过程中未生成有效结果"

            # 构建结构化输出
            analysis_result = {
                "summary": summary,
                "ratios": self._extract_calculated_ratios(state),
                "confidence": self._assess_confidence(state),
            }

            # 存储分析结果到数据库
            self._save_analysis_to_db(state, summary, analysis_result["ratios"])

            return {
                "analysis_result": analysis_result,
                "agent_output": analysis_result,
            }
        except Exception as e:
            logger.error(f"最终节点执行失败: {e}")
            return {
                "analysis_result": {
                    "summary": f"分析过程中出现错误: {e}",
                    "ratios": {},
                    "confidence": "低",
                },
                "agent_output": {
                    "summary": f"分析过程中出现错误: {e}",
                    "ratios": {},
                    "confidence": "低",
                },
                "error": f"最终节点执行失败: {e}",
            }
    
    def _save_analysis_to_db(self, state: AgentState, analysis_content: str, financial_data: Dict[str, Any]):
        """
        保存分析结果到数据库
        
        Args:
            state: 代理状态
            analysis_content: 分析内容
            financial_data: 财务数据
        """
        try:
            # 从状态中提取股票代码
            # 尝试从问题中提取股票代码
            question = state.get("question", "")
            code = ""
            
            # 简单的股票代码提取逻辑，实际应用中可能需要更复杂的正则表达式
            import re
            code_match = re.search(r'[0-9]{6}', question)
            if code_match:
                code = code_match.group(0)
            
            # 如果没有提取到股票代码，尝试从工具调用结果中提取
            if not code:
                for step in state.get("intermediate_steps", []):
                    if step[0] == "tools":
                        tool_output = step[1]
                        if isinstance(tool_output, str) and "股票研报" in tool_output:
                            # 从工具输出中提取股票代码
                            code_match = re.search(r'\【([0-9]{6})', tool_output)
                            if code_match:
                                code = code_match.group(1)
                                break
            
            if code:
                # 保存分析结果
                success = self.db.save_financial_analyze(
                    code=code,
                    date=date.today(),
                    pdf_name=f"analysis_{code}_{date.today().strftime('%Y%m%d')}.pdf",
                    report_type="机构研报",
                    analyze_content=analysis_content,
                    ratios=financial_data,
                    confidence="high"
                )
                if success:
                    logger.info(f"成功保存分析结果到数据库: {code}")
                else:
                    logger.warning(f"保存分析结果到数据库失败: {code}")
            else:
                logger.warning("未找到股票代码，无法保存分析结果到数据库")
        except Exception as e:
            logger.error(f"保存分析结果到数据库时发生错误: {e}")

    def should_continue(self, state: AgentState) -> str:
        """决定是否继续调用工具"""
        messages = state["messages"]
        last_message = messages[-1]

        # 如果 LLM 请求调用工具
        if hasattr(last_message, "tool_calls") and last_message.tool_calls:
            return "continue"
        return "end"

    def _build_system_prompt(self) -> str:
        """构建分析师 System Prompt"""
        return """你是一位资深财务分析师（CFA），拥有 15 年上市公司财报分析经验。

【分析原则】
1. 优先使用提供工具进行定量计算（财务比率、增长率、杜邦分析等）
2. 结合检索到的财报文本进行定性解读
3. 与行业平均水平对比时需明确说明数据来源
4. 避免给出投资建议，仅做客观分析

【可用工具】
- 盈利能力比率：毛利率、净利率、ROE、ROA
- 偿债能力比率：流动比率、速动比率、资产负债率
- 营运能力比率：存货周转率、应收账款周转率
- 估值比率：P/E、P/B、EV/EBITDA（需要市值数据）
- 增长率计算：收入、净利润的同比/复合增长率
- 杜邦分析：分解 ROE 驱动因素

【输出要求】
- 先展示计算结果，再给出解读
- 重要数据变化（>10%）需特别标注
- 最后给出总结性观点"""

    def _format_context(self, state: AgentState) -> str:
        """格式化检索到的文档上下文"""
        documents = state.get("documents", [])
        if not documents:
            return "未检索到相关财务资料，请基于通用知识谨慎分析。"

        context_parts = []
        for i, doc in enumerate(documents, 1):
            source = doc.metadata.get("source", "未知来源")
            content = doc.page_content[:1500]  # 截断过长文本
            context_parts.append(f"[片段 {i}] 来源：{source}\n{content}\n")
        return "\n".join(context_parts)

    def _extract_calculated_ratios(self, state: AgentState) -> Dict[str, float]:
        """从工具调用结果中提取计算出的比率（简化实现）"""
        ratios = {}
        for step in state.get("intermediate_steps", []):
            if step[0] == "tools":
                tool_output = step[1]
                # 实际应用中需解析工具返回的结构化数据
                if isinstance(tool_output, dict):
                    ratios.update(tool_output)
        return ratios

    def _assess_confidence(self, state: AgentState) -> str:
        """评估分析结论的可信度"""
        if not state.get("documents"):
            return "低（无参考资料）"
        return "高（有财报资料支撑）"

    def invoke(self, state: AgentState) -> AgentState:
        """执行分析工作流"""
        return self.graph.invoke(state)


# 便捷函数：创建 Analyst Agent 节点（供主图调用）
def create_analyst_node():
    """
    创建分析节点
    
    Returns:
        分析节点函数
    """
    agent = AnalystAgent()
    return agent.analyze_node