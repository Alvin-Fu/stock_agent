"""
财务分析 Agent
职责：直接调研报 → 提取数据 → 计算比率 → LLM分析 → 保存到数据库
"""

from typing import Dict, Any, List, Optional
from langchain_core.messages import SystemMessage, HumanMessage
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
from tools.stock_tools import call_fetch_stock_research_report
from utils.logger import logger
from storage.sqlite.stock_storage import get_db
import os


class AnalystAgent:
    """财务分析 Agent：一套直通流程，不走工具调用循环"""

    def __init__(self):
        self.llm = get_analyst_llm()
        self.financial_tools = [
            ("盈利能力", calculate_profitability_ratios),
            ("短期偿债", calculate_liquidity_ratios),
            ("长期偿债", calculate_solvency_ratios),
            ("估值比率", calculate_valuation_ratios),
            ("增长率", calculate_growth_rates),
            ("杜邦分析", perform_dupont_analysis),
        ]
        self.db = get_db()

    def _fetch_report(self, stock_code: str) -> str:
        """调研报，只调一次"""
        try:
            return call_fetch_stock_research_report(stock_code)
        except Exception as e:
            logger.error(f"调研报失败 {stock_code}: {e}")
            return ""

    def _call_financial_tools(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """全部财务工具各调一次，返回{工具名: 结果}"""
        results = {}
        for name, func in self.financial_tools:
            try:
                results[name] = func.invoke(data)
                logger.info(f"财务工具 {name} 计算完成")
            except Exception as e:
                logger.error(f"财务工具 {name} 失败: {e}")
                results[name] = f"计算失败: {e}"
        return results

    def _build_system_prompt(self) -> str:
        return """你是一位资深财务分析师（CFA），拥有 15 年上市公司财报分析经验。

请基于下方提供的研报原文和已计算的财务比率，进行定性解读。

【分析原则】
1. 已计算的比率是定量依据，请在此基础上做解读
2. 重要数据变化（>10%）需特别标注
3. 结合研报原文做定性补充
4. 避免给出投资建议，仅做客观分析

【输出要求】
- 先总览，再逐项解读
- 最后给出总结性观点"""

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        """
        直通流程：调研报 → 提取数据 → 计算比率 → LLM分析 → 保存DB
        """
        try:
            stock_code = state.get("stock_code", "")
            question = state.get("question", "")
            logger.info(f"财务分析开始，股票: {stock_code}，问题: {question[:50]}...")

            # 1. 调研报
            logger.info("获取研报...")
            report_text = self._fetch_report(stock_code)

            if not report_text or "未获取到" in report_text:
                report_text = state.get("documents", [])
                if report_text:
                    report_text = "\n".join([d.page_content for d in report_text])
                else:
                    report_text = "未获取到研报数据"

            # 2. 提取财务数据
            financial_data = self._extract_financial_data(str(report_text))
            if not financial_data:
                logger.warning("未能从研报中提取到财务数据")
                financial_data = {}

            # 3. 计算所有比率
            calculated = self._call_financial_tools(financial_data)

            # 4. 构建 LLM 提示
            system_prompt = self._build_system_prompt()
            user_message = f"""请分析股票 {stock_code} 的财务状况。

【用户问题】
{question}

========== 研报数据 ==========
{str(report_text)[:5000]}

========== 提取的财务数据 ==========
{financial_data}

========== 计算出的财务比率 ==========
{calculated}

请基于以上数据给出专业分析意见。"""

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]

            # 5. LLM 一次性分析
            logger.info("LLM 财务分析中...")
            response = self.llm.invoke(messages)
            summary = response.content if hasattr(response, 'content') else str(response)
            logger.info(f"财务分析完成，长度: {len(summary)}")

            # 6. 保存到数据库
            self._save_analysis_to_db(state, summary, calculated)

            return {
                "messages": [response],
                "financial_data": financial_data,
                "analysis_result": {"summary": summary, "ratios": calculated},
                "agent_output": {"summary": summary, "ratios": calculated},
                "current_node": "analyst",
                "intermediate_steps": state.get("intermediate_steps", []) + [
                    ("analyze", {"stock_code": stock_code, "content": summary[:200]})
                ],
            }

        except Exception as e:
            logger.error(f"财务分析节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"分析执行失败: {e}",
                "intermediate_steps": state.get("intermediate_steps", []) + [("analyze", {"error": str(e)})],
            }

    def _save_analysis_to_db(self, state: AgentState, analysis_content: str, ratios: Dict[str, Any]):
        try:
            question = state.get("question", "") or ""
            stock_code = state.get("stock_code", "")
            if not stock_code:
                import re
                m = re.search(r'[0-9]{6}', question)
                stock_code = m.group(0) if m else ""
            if stock_code:
                self.db.save_financial_analyze(
                    code=stock_code,
                    date=date.today(),
                    pdf_name=f"analysis_{stock_code}_{date.today().strftime('%Y%m%d')}.pdf",
                    report_type="机构研报",
                    analyze_content=analysis_content,
                    ratios=ratios,
                    confidence="high",
                )
                logger.info(f"分析结果已保存: {stock_code}")
        except Exception as e:
            logger.error(f"保存分析结果失败: {e}")

    def _extract_financial_data(self, text: str) -> Dict[str, Any]:
        """
        从研报或年报文本中提取财务数据
        
        Args:
            text: 研报或年报文本
            
        Returns:
            包含财务数据的字典
        """
        import re
        financial_data = {}
        
        # 提取收入
        revenue_patterns = [
            r'营业收入[：:]?\s*([\d.,]+)\s*亿元',
            r'营收[：:]?\s*([\d.,]+)\s*亿元',
            r'收入[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in revenue_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['revenue'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取净利润
        net_income_patterns = [
            r'净利润[：:]?\s*([\d.,]+)\s*亿元',
            r'归母净利润[：:]?\s*([\d.,]+)\s*亿元',
            r'净利[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in net_income_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['net_income'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取总资产
        total_assets_patterns = [
            r'总资产[：:]?\s*([\d.,]+)\s*亿元',
            r'资产总额[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in total_assets_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['total_assets'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取总负债
        total_liabilities_patterns = [
            r'总负债[：:]?\s*([\d.,]+)\s*亿元',
            r'负债总额[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in total_liabilities_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['total_liabilities'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取所有者权益
        total_equity_patterns = [
            r'所有者权益[：:]?\s*([\d.,]+)\s*亿元',
            r'股东权益[：:]?\s*([\d.,]+)\s*亿元',
            r'权益总额[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in total_equity_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['total_equity'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取流动资产
        current_assets_patterns = [
            r'流动资产[：:]?\s*([\d.,]+)\s*亿元',
            r'流动总资产[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in current_assets_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['current_assets'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取流动负债
        current_liabilities_patterns = [
            r'流动负债[：:]?\s*([\d.,]+)\s*亿元',
            r'流动总负债[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in current_liabilities_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['current_liabilities'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取存货
        inventory_patterns = [
            r'存货[：:]?\s*([\d.,]+)\s*亿元',
            r'库存[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in inventory_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['inventory'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取营业成本
        cogs_patterns = [
            r'营业成本[：:]?\s*([\d.,]+)\s*亿元',
            r'成本[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in cogs_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['cost_of_goods_sold'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取EBIT
        ebit_patterns = [
            r'EBIT[：:]?\s*([\d.,]+)\s*亿元',
            r'息税前利润[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in ebit_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['ebit'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取利息费用
        interest_expense_patterns = [
            r'利息费用[：:]?\s*([\d.,]+)\s*亿元',
            r'财务费用[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in interest_expense_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['interest_expense'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取EBITDA
        ebitda_patterns = [
            r'EBITDA[：:]?\s*([\d.,]+)\s*亿元',
            r'息税折旧摊销前利润[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in ebitda_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['ebitda'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        # 提取市值
        market_cap_patterns = [
            r'市值[：:]?\s*([\d.,]+)\s*亿元',
            r'总市值[：:]?\s*([\d.,]+)\s*亿元'
        ]
        for pattern in market_cap_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    financial_data['market_cap'] = float(match.group(1).replace(',', ''))
                    break
                except:
                    pass
        
        return financial_data

    def invoke(self, state: AgentState) -> Dict[str, Any]:
        return self.analyze_node(state)


def create_analyst_node():
    agent = AnalystAgent()
    return agent.analyze_node