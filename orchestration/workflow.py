"""
工作流执行器
提供同步/异步、流式输出的执行接口
"""

import asyncio
from typing import Dict, Any, Optional, AsyncGenerator

from langchain_core.messages import HumanMessage

from agents.base import AgentState
from .graph import get_default_graph
from utils.common import sanitize_text
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
        """初始化状态（入口统一清洗非法 Unicode，防 surrogates not allowed）"""
        question = sanitize_text(question)
        # 数据源健康采集器清零：本次分析各信源的成败由 compliance 末尾汇总上屏
        try:
            from tools.source_health import reset_health
            reset_health()
        except Exception:
            pass
        return {
            "messages": [HumanMessage(content=question)],
            "question": question,
            "stock_code": kwargs.pop("stock_code", ""),
            "stock_type": kwargs.pop("stock_type", None),
            "industry_name": kwargs.pop("industry_name", ""),
            "chain_leaders": kwargs.pop("chain_leaders", []),
            "intent": None,
            "documents": [],
            "financial_data": None,
            "analysis_result": None,
            "research_result": None,
            "compliance_result": None,
            "technical_result": None,
            "final_answer": None,
            "intermediate_steps": [],
            "next_agents": [],
            "confidence": None,
            "error": None,
            **kwargs
        }

    def run_sync(self, question: str, thread_id: Optional[str] = None, **kwargs) -> AgentState:
        """
        同步执行工作流
        :param question: 用户问题
        :param thread_id: 会话 ID（用于多轮对话记忆）
        :param kwargs: 额外初始状态字段（如 stock_code / industry_name）
        :return: 最终状态
        """
        if thread_id:
            self.thread_id = thread_id

        initial_state = self._init_state(question, **kwargs)
        config = {"configurable": {"thread_id": self.thread_id}}

        logger.info(f"开始执行工作流，问题: {question[:50]}...")
        logger.info(f"初始状态: {initial_state}")
        try:
            final_state = self.graph.invoke(initial_state, config)
            logger.info("工作流执行完成")

            # 3. 强制兜底：如果invoke返回None，用初始状态代替
            final_state = final_state or initial_state

            # 4. 复盘留档：异步抽取判断存快照（不阻塞返回）
            #    单只个股 → 个股快照；产业链（多代码）→ 产业链快照
            try:
                snap_code = final_state.get("stock_code") or ""
                snap_answer = final_state.get("final_answer") or ""
                snap_industry = final_state.get("industry_name") or ""
                if snap_answer:
                    if snap_code and "," not in snap_code:
                        from monitoring.review import snapshot_analysis_async
                        snapshot_analysis_async(
                            snap_code, question, snap_answer,
                            trade_plan=(final_state.get("technical_result") or {}).get("trade_plan"))
                    elif snap_industry and "," in snap_code:
                        from monitoring.review import snapshot_industry_analysis_async
                        codes = [c.strip() for c in snap_code.split(",") if c.strip()]
                        # 产业链复盘要看候选与技术面全文，把两份摘要拼给抽取器
                        research_result = final_state.get("research_result") or {}
                        research = research_result.get("summary", "")
                        technical = (final_state.get("technical_result") or {}).get("summary", "")
                        full_report = f"{snap_answer}\n\n{research[-3000:]}\n\n{technical[-2000:]}"
                        snapshot_industry_analysis_async(
                            snap_industry, question, full_report, codes,
                            valuation=research_result.get("industry_valuation"),
                            excluded_codes=research_result.get("gate_excluded_codes"))
            except Exception as e:
                logger.warning(f"分析快照留档触发失败（不影响本次回答）: {e}")

            self._archive_report(question, final_state)

            logger.debug(f"最终状态: {final_state}")
            return final_state
        except Exception as e:
            logger.error(f"工作流执行失败: {type(e).__name__}: {e}", exc_info=True)
            initial_state["error"] = str(e)
            initial_state["final_answer"] = f"系统处理出错：{e}"
            return initial_state

    @staticmethod
    def _archive_report(question: str, final_state) -> None:
        """把最终报告存 data/reports/ 下的 markdown（横向对比与复盘引用用），失败不影响回答"""
        try:
            answer = final_state.get("final_answer") or ""
            if not answer.strip():
                return
            import os
            import re as _re
            from datetime import datetime as _dt
            os.makedirs("./data/reports", exist_ok=True)
            code = final_state.get("stock_code") or ""
            tag = final_state.get("industry_name") or (code.replace(",", "_") if code else "query")
            tag = _re.sub(r"[^\w一-鿿_-]", "", str(tag))[:40] or "query"
            ts = _dt.now().strftime('%Y%m%d_%H%M%S')
            path = f"./data/reports/{ts}_{tag}.md"
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"# {question}\n\n> 生成时间：{_dt.now().strftime('%Y-%m-%d %H:%M')}\n\n{answer}\n")
            logger.info(f"报告已归档: {path}")

            # 单只个股时：额外用 full_report_fetcher 生成结构化数据报告
            if code and "," not in code and len(code) == 6:
                try:
                    from tools.stock_tools import call_fetch_full_report
                    full = call_fetch_full_report(code)
                    if full and "❌" not in full[:10]:
                        report_path = f"./data/reports/{ts}_{tag}_full_report.md"
                        with open(report_path, "w", encoding="utf-8") as f:
                            f.write(full)
                        logger.info(f"结构化数据报告已归档: {report_path}")
                except Exception as e:
                    logger.warning(f"结构化数据报告生成失败（不影响回答）: {e}")
        except Exception as e:
            logger.warning(f"报告归档失败（不影响本次回答）: {e}")

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