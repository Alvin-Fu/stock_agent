"""
财务分析工具集
所有工具均被装饰为 LangChain Tool，供 Agent 调用

原则：数据缺失时明确返回「缺少XX数据」，绝不使用默认值伪造比率——
喂给 LLM 一个用假数算出来的 ROE 比没有数据更糟。
"""
from typing import Dict, Optional
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from utils.logger import logger


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


def _get(fs: Dict, key: str):
    """取字段：不存在或为 None 返回 None，不做任何默认值兜底"""
    value = fs.get(key)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------- 工具函数 ----------
@tool(args_schema=RatioInput)
def calculate_profitability_ratios(financial_statements: Dict) -> Dict[str, object]:
    """
    计算盈利能力比率：毛利率、净利率、ROE、ROA
    输入需包含：revenue, cost_of_goods_sold, net_income,
                total_assets, total_equity (平均或期末)
    """
    try:
        fs = financial_statements
        revenue = _get(fs, "revenue")
        cogs = _get(fs, "cost_of_goods_sold")
        net_income = _get(fs, "net_income")
        total_assets = _get(fs, "total_assets")
        total_equity = _get(fs, "total_equity")

        result = {}
        if revenue and cogs is not None:
            result["毛利率"] = round((revenue - cogs) / revenue * 100, 2)
        else:
            result["毛利率"] = "缺少营收/成本数据"
        if revenue and net_income is not None:
            result["净利率"] = round(net_income / revenue * 100, 2)
        else:
            result["净利率"] = "缺少营收/净利润数据"
        if total_assets and net_income is not None:
            result["ROA"] = round(net_income / total_assets * 100, 2)
        else:
            result["ROA"] = "缺少总资产/净利润数据"
        if total_equity and net_income is not None:
            result["ROE"] = round(net_income / total_equity * 100, 2)
        else:
            result["ROE"] = "缺少净资产/净利润数据"
        # 利润表是累计口径：一季报时 ROE/ROA 是单季利润/期末资产，天然偏低
        result["口径说明"] = "净利润为报告期累计值，ROE/ROA 未年化（一季报时数值约为年化的1/4）"
        return result
    except Exception as e:
        logger.error(f"计算盈利能力比率失败: {e}")
        return {"错误": str(e)}


@tool(args_schema=RatioInput)
def calculate_liquidity_ratios(financial_statements: Dict) -> Dict[str, object]:
    """
    计算短期偿债能力比率：流动比率、速动比率
    输入需包含：current_assets, current_liabilities, inventory
    """
    try:
        fs = financial_statements
        current_assets = _get(fs, "current_assets")
        current_liabilities = _get(fs, "current_liabilities")
        inventory = _get(fs, "inventory")

        if not current_liabilities or current_assets is None:
            return {"说明": "缺少流动资产/流动负债数据，无法计算短期偿债比率"}

        result = {"流动比率": round(current_assets / current_liabilities, 2)}
        if inventory is not None:
            result["速动比率"] = round((current_assets - inventory) / current_liabilities, 2)
        else:
            result["速动比率"] = "缺少存货数据"
        return result
    except Exception as e:
        logger.error(f"计算短期偿债比率失败: {e}")
        return {"错误": str(e)}


@tool(args_schema=RatioInput)
def calculate_solvency_ratios(financial_statements: Dict) -> Dict[str, object]:
    """
    计算长期偿债能力比率：资产负债率、利息保障倍数
    输入需包含：total_liabilities, total_assets, ebit, interest_expense
    """
    try:
        fs = financial_statements
        total_liabilities = _get(fs, "total_liabilities")
        total_assets = _get(fs, "total_assets")
        ebit = _get(fs, "ebit")
        interest_expense = _get(fs, "interest_expense")

        result = {}
        if total_assets and total_liabilities is not None:
            result["资产负债率"] = round(total_liabilities / total_assets * 100, 2)
        else:
            result["资产负债率"] = "缺少总资产/总负债数据"
        if interest_expense and ebit is not None:
            result["利息保障倍数"] = round(ebit / interest_expense, 2)
        else:
            result["利息保障倍数"] = "缺少利息费用数据"
        return result
    except Exception as e:
        logger.error(f"计算长期偿债比率失败: {e}")
        return {"错误": str(e)}


@tool(args_schema=RatioInput)
def calculate_valuation_ratios(financial_statements: Dict) -> Dict[str, object]:
    """
    计算估值比率。
    优先使用每日指标表的 pe_ttm/pb（TTM 口径，正确）；
    禁止用"市值/最新一期累计净利润"自算 PE——利润表是累计口径，
    一季报时会把 PE 虚高约 4 倍。
    """
    try:
        fs = financial_statements
        pe_ttm = _get(fs, "pe_ttm")
        pb = _get(fs, "pb")
        market_cap = _get(fs, "market_cap")
        total_equity = _get(fs, "total_equity")

        result = {}
        if pe_ttm:
            result["市盈率 PE(TTM)"] = pe_ttm
        else:
            result["市盈率 PE(TTM)"] = "缺少TTM数据（利润表累计口径不能直接算PE）"
        if pb:
            result["市净率 P/B"] = pb
        elif market_cap and total_equity:
            # PB 用时点数计算无口径问题，可以退化自算
            result["市净率 P/B"] = round(market_cap / total_equity, 2)
        else:
            result["市净率 P/B"] = "缺少数据"
        if fs.get("pe_ttm_历史分位"):
            result["PE(TTM) 历史分位"] = fs["pe_ttm_历史分位"]
        if fs.get("pb_历史分位"):
            result["PB 历史分位"] = fs["pb_历史分位"]
        return result
    except Exception as e:
        logger.error(f"计算估值比率失败: {e}")
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
def perform_dupont_analysis(financial_statements: Dict) -> Dict[str, object]:
    """
    杜邦分析：分解 ROE 为 净利率 × 资产周转率 × 权益乘数
    """
    try:
        fs = financial_statements
        net_income = _get(fs, "net_income")
        revenue = _get(fs, "revenue")
        total_assets = _get(fs, "total_assets")
        total_equity = _get(fs, "total_equity")

        if not revenue or not total_assets or not total_equity or net_income is None:
            return {"说明": "缺少净利润/营收/总资产/净资产数据，无法做杜邦分解"}

        net_margin = net_income / revenue
        asset_turnover = revenue / total_assets
        equity_multiplier = total_assets / total_equity
        roe = net_margin * asset_turnover * equity_multiplier

        return {
            "净利率": round(net_margin * 100, 2),
            "资产周转率": round(asset_turnover, 2),
            "权益乘数": round(equity_multiplier, 2),
            "ROE (杜邦)": round(roe * 100, 2),
            "口径说明": "基于报告期累计利润，未年化",
        }
    except Exception as e:
        logger.error(f"杜邦分析失败: {e}")
        return {"错误": str(e)}
