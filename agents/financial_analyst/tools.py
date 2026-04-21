"""
财务分析工具集
所有工具均被装饰为 LangChain Tool，供 Agent 调用
"""

from typing import Dict, Optional
from langchain_core.tools import tool
from pydantic import BaseModel, Field


# ---------- 输入模型定义 ----------
class RatioInput(BaseModel):
    """比率计算通用输入"""
    financial_statements: Dict = Field(
        description="包含利润表、资产负债表、现金流量表关键项目的字典"
    )


class GrowthInput(BaseModel):
    """增长率计算输入"""
    current_value: float = Field(description="当前期数值")
    previous_value: float = Field(description="上期数值")
    periods: Optional[int] = Field(default=1, description="期数（用于年化）")


# ---------- 工具函数 ----------
@tool(args_schema=RatioInput)
def calculate_profitability_ratios(financial_statements: Dict) -> Dict[str, float]:
    """
    计算盈利能力比率：毛利率、净利率、ROE、ROA
    输入需包含：revenue, cost_of_goods_sold, net_income,
                total_assets, total_equity (平均或期末)
    """
    try:
        revenue = financial_statements.get("revenue", 0)
        cogs = financial_statements.get("cost_of_goods_sold", 0)
        net_income = financial_statements.get("net_income", 0)
        total_assets = financial_statements.get("total_assets", 1)
        total_equity = financial_statements.get("total_equity", 1)

        gross_margin = (revenue - cogs) / revenue if revenue else 0
        net_margin = net_income / revenue if revenue else 0
        roa = net_income / total_assets if total_assets else 0
        roe = net_income / total_equity if total_equity else 0

        return {
            "毛利率": round(gross_margin * 100, 2),
            "净利率": round(net_margin * 100, 2),
            "ROA": round(roa * 100, 2),
            "ROE": round(roe * 100, 2),
        }
    except Exception as e:
        return {"错误": str(e)}


@tool(args_schema=RatioInput)
def calculate_liquidity_ratios(financial_statements: Dict) -> Dict[str, float]:
    """
    计算短期偿债能力比率：流动比率、速动比率
    输入需包含：current_assets, current_liabilities, inventory
    """
    try:
        current_assets = financial_statements.get("current_assets", 0)
        current_liabilities = financial_statements.get("current_liabilities", 1)
        inventory = financial_statements.get("inventory", 0)

        current_ratio = current_assets / current_liabilities
        quick_ratio = (current_assets - inventory) / current_liabilities

        return {
            "流动比率": round(current_ratio, 2),
            "速动比率": round(quick_ratio, 2),
        }
    except Exception as e:
        return {"错误": str(e)}


@tool(args_schema=RatioInput)
def calculate_solvency_ratios(financial_statements: Dict) -> Dict[str, float]:
    """
    计算长期偿债能力比率：资产负债率、利息保障倍数
    输入需包含：total_liabilities, total_assets, ebit, interest_expense
    """
    try:
        total_liabilities = financial_statements.get("total_liabilities", 0)
        total_assets = financial_statements.get("total_assets", 1)
        ebit = financial_statements.get("ebit", 0)
        interest_expense = financial_statements.get("interest_expense", 1)

        debt_ratio = total_liabilities / total_assets
        interest_coverage = ebit / interest_expense if interest_expense else 0

        return {
            "资产负债率": round(debt_ratio * 100, 2),
            "利息保障倍数": round(interest_coverage, 2),
        }
    except Exception as e:
        return {"错误": str(e)}


@tool(args_schema=RatioInput)
def calculate_valuation_ratios(financial_statements: Dict) -> Dict[str, float]:
    """
    计算估值比率（需要市值数据）
    输入需包含：market_cap, net_income, total_equity, ebitda
    """
    try:
        market_cap = financial_statements.get("market_cap", 0)
        net_income = financial_statements.get("net_income", 1)
        total_equity = financial_statements.get("total_equity", 1)
        ebitda = financial_statements.get("ebitda", 1)

        pe = market_cap / net_income if net_income else 0
        pb = market_cap / total_equity if total_equity else 0
        ev_ebitda = market_cap / ebitda if ebitda else 0  # 简化，未考虑净债务

        return {
            "市盈率 (P/E)": round(pe, 2),
            "市净率 (P/B)": round(pb, 2),
            "EV/EBITDA": round(ev_ebitda, 2),
        }
    except Exception as e:
        return {"错误": str(e)}


@tool(args_schema=GrowthInput)
def calculate_growth_rates(
    current_value: float, previous_value: float, periods: int = 1
) -> Dict[str, float]:
    """
    计算增长率：同比增长率、复合年增长率
    """
    try:
        if previous_value == 0:
            yoy_growth = 0
        else:
            yoy_growth = (current_value - previous_value) / abs(previous_value)

        cagr = 0
        if periods > 1 and previous_value > 0:
            cagr = (current_value / previous_value) ** (1 / periods) - 1

        return {
            "同比增长率": round(yoy_growth * 100, 2),
            f"{periods}年复合增长率": round(cagr * 100, 2),
        }
    except Exception as e:
        return {"错误": str(e)}


@tool(args_schema=RatioInput)
def perform_dupont_analysis(financial_statements: Dict) -> Dict[str, float]:
    """
    杜邦分析：分解 ROE 为 净利率 × 资产周转率 × 权益乘数
    """
    try:
        net_income = financial_statements.get("net_income", 0)
        revenue = financial_statements.get("revenue", 1)
        total_assets = financial_statements.get("total_assets", 1)
        total_equity = financial_statements.get("total_equity", 1)

        net_margin = net_income / revenue if revenue else 0
        asset_turnover = revenue / total_assets if total_assets else 0
        equity_multiplier = total_assets / total_equity if total_equity else 0
        roe = net_margin * asset_turnover * equity_multiplier

        return {
            "净利率": round(net_margin * 100, 2),
            "资产周转率": round(asset_turnover, 2),
            "权益乘数": round(equity_multiplier, 2),
            "ROE (杜邦)": round(roe * 100, 2),
        }
    except Exception as e:
        return {"错误": str(e)}