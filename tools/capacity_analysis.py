# -*- coding: utf-8 -*-
"""
产能扩张分析
===========
从资产负债表（固定资产/在建工程）+ 现金流量表（资本开支）分析产能扩张趋势。
数据源：Tushare pro（balancesheet, cashflow）
返回 formatted text 供 LLM 报告使用。
"""

from typing import Optional, Dict, Any
from datetime import datetime

import pandas as pd
import numpy as np

from utils.logger import logger


# ========================================================================
# 工具函数
# ========================================================================

_YI = 100_000_000


def _yi(v) -> Optional[float]:
    """元转亿元，NaN/None 返回 None"""
    if v is None:
        return None
    try:
        val = float(v)
        if val != val:  # NaN check
            return None
        return round(val / _YI, 2)
    except (TypeError, ValueError):
        return None


def _pct_change(cur, prev) -> Optional[float]:
    """同比变化率（%）"""
    if cur is None or prev is None or prev == 0:
        return None
    return round((cur - prev) / abs(prev) * 100, 1)


def _chg(cur, prev) -> Optional[float]:
    """同比变动绝对值"""
    if cur is None or prev is None:
        return None
    return round(cur - prev, 2)


# ========================================================================
# 核心分析函数
# ========================================================================

def analyze_capacity(stock_code: str) -> Dict[str, Any]:
    """
    产能扩张综合分析。

    参数
    ----
    stock_code : str
        股票代码（如 "002594"）

    返回
    ----
    dict
        {
            "stock_code": str,
            "report_date": str,         # 最新报告期
            "fixed_assets_cur": float,  # 最新固定资产（亿元）
            "fixed_assets_prev": float, # 上期固定资产（亿元）
            "fixed_assets_chg_pct": float,  # 固定资产同比（%）
            "cip_cur": float,           # 最新在建工程（亿元）
            "cip_prev": float,          # 上期在建工程（亿元）
            "capex_cur": float,         # 最新资本开支（亿元）
            "capex_prev": float,        # 上期资本开支（亿元）
            "capex_chg_pct": float,     # 资本开支同比（%）
            "fixed_asset_turnover_cur": float,  # 固定资产周转率（次）
            "fixed_asset_turnover_prev": float,
            "expansion_judgment": str,  # "扩张" / "收缩" / "平稳"
            "summary": str,             # 一句话总结
            "data_quality": str,        # "完整" / "部分缺失" / "不可用"
            "details": [str],           # 逐项说明
        }
    """
    try:
        from tools.stock_tools import stock_tool_instance as sti

        details = []

        # ========== 1. 获取资产负债表原始数据（含 fixed_assets, construction_in_progress） ==========
        df_bs_raw = sti.fetch_and_save_stock_balance_sheet(stock_code)
        if df_bs_raw is None or df_bs_raw.empty:
            return {"error": f"未获取到 {stock_code} 的资产负债表数据", "data_quality": "不可用"}

        # 获取 Tushare 原始数据（含 fixed_assets, construction_in_progress）
        try:
            bs_full = sti.tushare.balancesheet(stock_code, 
                                                start_date=f"{datetime.now().year - 5}-01-01",
                                                end_date=datetime.now().strftime("%Y-%m-%d"))
        except Exception as e:
            logger.warning(f"[产能分析] 拉取原始资产负债失败，降级: {e}")
            bs_full = None

        if bs_full is None or bs_full.empty:
            return {"error": "未获取到完整的资产负债表原始数据", "data_quality": "不可用"}

        # 清洗去重
        if 'update_flag' in bs_full.columns:
            bs_full = bs_full.sort_values('update_flag', ascending=False)
        bs_full = bs_full.drop_duplicates(subset=['end_date'], keep='first')
        bs_full['end_date'] = pd.to_datetime(bs_full['end_date'])
        bs_full = bs_full.sort_values('end_date', ascending=False).reset_index(drop=True)

        # 提取固定资产和在建工程
        def _safe_get(df, col):
            if col not in df.columns:
                return None
            return _yi(pd.to_numeric(df[col], errors='coerce').iloc[0])

        def _same_period_prev(df, col, cur_date):
            """从 df 中找与 cur_date 同季度类型（Q1/Q2/Q3/全年）的上年数据"""
            if col not in df.columns or len(df) < 2:
                return None
            # 兼容 end_date 或 report_date 列名
            date_col = 'end_date' if 'end_date' in df.columns else ('report_date' if 'report_date' in df.columns else None)
            if date_col is None:
                return None
            if hasattr(cur_date, 'month'):
                month = cur_date.month
            else:
                month = pd.Timestamp(cur_date).month
            for _, r in df.iterrows():
                rd = r[date_col]
                if hasattr(rd, 'month'):
                    rd_month = rd.month
                else:
                    rd_month = pd.Timestamp(rd).month
                if rd_month == month and rd != cur_date:
                    return _yi(pd.to_numeric(r[col], errors='coerce'))
            return None

        fa_cur = _safe_get(bs_full, 'fix_assets') or _safe_get(bs_full, 'fixed_assets')
        fa_prev = _same_period_prev(bs_full, 'fix_assets', bs_full['end_date'].iloc[0]) or \
                  _same_period_prev(bs_full, 'fixed_assets', bs_full['end_date'].iloc[0])
        cip_cur = _safe_get(bs_full, 'cip') or _safe_get(bs_full, 'construction_in_progress')
        cip_prev = _same_period_prev(bs_full, 'cip', bs_full['end_date'].iloc[0]) or \
                   _same_period_prev(bs_full, 'construction_in_progress', bs_full['end_date'].iloc[0])

        # 最新报告期
        latest_date = str(bs_full['end_date'].iloc[0].date()) if not bs_full.empty else ""

        fa_chg = _chg(fa_cur, fa_prev) if fa_cur is not None else None
        fa_chg_pct = _pct_change(fa_cur, fa_prev)

        if fa_cur is not None:
            details.append(f"固定资产: {fa_cur:.1f}亿" +
                           (f"（同比{fa_chg_pct:+.1f}%，{fa_chg:+.1f}亿）" if fa_chg_pct is not None else ""))

        if cip_cur is not None:
            cip_chg = _chg(cip_cur, cip_prev) if cip_prev is not None else None
            details.append(f"在建工程: {cip_cur:.1f}亿" +
                           (f"（同比{'+' if cip_chg and cip_chg >= 0 else ''}{cip_chg}亿）" if cip_chg is not None else ""))

        # ========== 2. 获取现金流量表资本开支 ==========
        try:
            cf_raw = sti.tushare.stock_cashflow(stock_code,
                                           start_date=f"{datetime.now().year - 5}-01-01",
                                           end_date=datetime.now().strftime("%Y-%m-%d"))
        except Exception as e:
            logger.warning(f"[产能分析] 拉取现金流数据失败: {e}")
            cf_raw = None

        capex_cur = capex_prev = None
        if cf_raw is not None and not cf_raw.empty:
            if 'update_flag' in cf_raw.columns:
                cf_raw = cf_raw.sort_values('update_flag', ascending=False)
            cf_raw = cf_raw.drop_duplicates(subset=['end_date'], keep='first')
            cf_raw['end_date'] = pd.to_datetime(cf_raw['end_date'])
            cf_raw = cf_raw.sort_values('end_date', ascending=False).reset_index(drop=True)

            cf_col = 'c_pay_acq_const_fiolta' if 'c_pay_acq_const_fiolta' in cf_raw.columns else \
                     ('capex' if 'capex' in cf_raw.columns else None)
            if cf_col:
                capex_cur = _safe_get(cf_raw, cf_col)
                capex_prev = _same_period_prev(cf_raw, cf_col, cf_raw['end_date'].iloc[0])

            if capex_cur is not None:
                capex_chg = _chg(capex_cur, capex_prev) if capex_prev is not None else None
                capex_chg_pct = _pct_change(capex_cur, capex_prev)
                details.append(f"资本开支: {capex_cur:.1f}亿" +
                               (f"（同比{capex_chg_pct:+.1f}%，{capex_chg:+.1f}亿）" if capex_chg_pct is not None else ""))

        # ========== 3. 固定资产周转率（年化营收 / 固定资产） ==========
        # 营收必须取与资产负债表同口径期间：最新是 Q1 则取 Q1 上年同期
        df_income = sti.fetch_and_save_stock_income(stock_code)
        rev_cur = rev_prev = None
        if df_income is not None and not df_income.empty:
            df_income = df_income.sort_values('report_date', ascending=False).reset_index(drop=True)
            cur_date = bs_full['end_date'].iloc[0]
            if 'total_revenue' in df_income.columns:
                match = df_income[df_income['report_date'] == cur_date]
                if not match.empty:
                    rev_cur = _yi(pd.to_numeric(match['total_revenue'], errors='coerce').iloc[0])
                else:
                    rev_cur = _yi(pd.to_numeric(df_income['total_revenue'], errors='coerce').iloc[0])

                rev_prev = _same_period_prev(df_income, 'total_revenue', cur_date)

        turn_cur = turn_prev = None
        if rev_cur is not None and fa_cur is not None and fa_cur > 0:
            # 单季营收年化（Q1×4, Q2×2, Q3×4/3, Q4/全年×1）
            month = cur_date.month if hasattr(cur_date, 'month') else pd.Timestamp(cur_date).month
            annual_factor = {3: 4, 6: 2, 9: 4/3, 12: 1}.get(month, 1)
            turn_cur = round(rev_cur * annual_factor / fa_cur, 2)
        if rev_prev is not None and fa_prev is not None and fa_prev > 0:
            # 前一年同期的月度与当期一致，复用 annual_factor
            turn_prev = round(rev_prev * annual_factor / fa_prev, 2)

        if turn_cur is not None:
            turn_chg = round(turn_cur - turn_prev, 2) if turn_prev is not None else None
            details.append(f"固定资产周转率: {turn_cur}次" +
                           (f"（上年{turn_prev}次，{'+' if turn_chg and turn_chg >= 0 else ''}{turn_chg}）" if turn_chg is not None else ""))
            if turn_chg is not None:
                if turn_chg > 0.1:
                    details[-1] += " ↑ 产能利用效率提升"
                elif turn_chg < -0.1:
                    details[-1] += " ↓ 产能利用效率下降"

        # ========== 4. 在建工程/固定资产比 ==========
        cip_fa_ratio = None
        if cip_cur is not None and fa_cur is not None and fa_cur > 0:
            cip_fa_ratio = round(cip_cur / fa_cur * 100, 1)
            details.append(f"在建工程/固定资产: {cip_fa_ratio}%" +
                           ("（在建占比较高，后续转固将增加折旧压力）" if cip_fa_ratio > 20 else ""))

        # ========== 5. 扩张判定 ==========
        # 三信号：固定资产增长 + 资本开支增长 + 在建工程高位
        signals = []
        if fa_chg_pct is not None:
            if fa_chg_pct > 10:
                signals.append("固定资产大幅增长")
            elif fa_chg_pct > 3:
                signals.append("固定资产稳步增长")
            elif fa_chg_pct < -3:
                signals.append("固定资产收缩")

        capex_chg_pct = _pct_change(capex_cur, capex_prev)
        if capex_chg_pct is not None:
            if capex_chg_pct > 20:
                signals.append("资本开支大幅增加")
            elif capex_chg_pct > 5:
                signals.append("资本开支增加")
            elif capex_chg_pct < -10:
                signals.append("资本开支缩减")

        if cip_fa_ratio is not None and cip_fa_ratio > 15:
            signals.append("在建工程占比偏高（未来产能释放）")

        if signals:
            expansion_judgment = "扩张" if any("增长" in s or "增加" in s or "释放" in s for s in signals) else \
                                 "收缩" if any("收缩" in s or "缩减" in s for s in signals) else "平稳"
        else:
            expansion_judgment = "平稳"

        # ========== 6. 汇总 ==========
        if fa_cur is None and cip_cur is None and capex_cur is None:
            data_quality = "不可用"
            summary = "产能相关数据缺失（Tushare 未返回固定资产/在建工程字段）"
        elif fa_cur is None:
            data_quality = "部分缺失"
            summary = "固定资产数据缺失，仅从资本开支维度判断"
        else:
            data_quality = "完整"

        if data_quality == "完整":
            if expansion_judgment == "扩张":
                summary = f"产能处扩张期: 固定资产{fa_cur:.1f}亿" + \
                          (f"（同比+{fa_chg_pct:.1f}%），" if fa_chg_pct else "") + \
                          (f"在建工程{cip_cur:.1f}亿，" if cip_cur else "") + \
                          (f"资本开支{capex_cur:.1f}亿" if capex_cur else "")
            elif expansion_judgment == "收缩":
                summary = f"产能处收缩期: 固定资产{fa_cur:.1f}亿" + \
                          (f"（同比{fa_chg_pct:.1f}%），" if fa_chg_pct else "") + \
                          (f"资本开支缩减" if capex_chg_pct and capex_chg_pct < -10 else "")
            else:
                summary = f"产能基本平稳: 固定资产{fa_cur:.1f}亿，周转率{turn_cur}次" if turn_cur else \
                          f"产能基本平稳: 固定资产{fa_cur:.1f}亿"

        return {
            "stock_code": stock_code,
            "report_date": latest_date,
            "fixed_assets_cur": fa_cur,
            "fixed_assets_prev": fa_prev,
            "fixed_assets_chg_pct": fa_chg_pct,
            "cip_cur": cip_cur,
            "cip_prev": cip_prev,
            "capex_cur": capex_cur,
            "capex_prev": capex_prev,
            "fixed_asset_turnover_cur": turn_cur,
            "fixed_asset_turnover_prev": turn_prev,
            "expansion_judgment": expansion_judgment,
            "summary": summary,
            "data_quality": data_quality,
            "details": details,
        }

    except Exception as e:
        logger.error(f"[产能分析] 分析失败: {e}")
        return {"error": f"产能分析异常: {e}", "data_quality": "不可用"}


def format_capacity_analysis(result: Dict[str, Any]) -> str:
    """格式化为文本块"""
    if not result:
        return ""
    if "error" in result:
        return f"❌ 产能分析失败: {result['error']}"

    lines = [f"🏭 产能扩张分析: {result.get('stock_code', '')}"]
    if result.get("report_date"):
        lines[0] += f"（截至{result['report_date']}）"

    lines.append("")

    if result["data_quality"] == "不可用":
        lines.append("  ⚠️ 产能相关数据缺失（Tushare 资产负债表未返回固定资产/在建工程字段）")
        return "\n".join(lines)

    for d in result.get("details", []):
        lines.append(f"  • {d}")

    lines.append("")
    judge = result.get("expansion_judgment", "")
    if judge == "扩张":
        emoji = "📈"
    elif judge == "收缩":
        emoji = "📉"
    else:
        emoji = "➡️"
    lines.append(f"  综合判断: {emoji} {judge}")

    if result.get("summary"):
        lines.append(f"  {result['summary']}")

    return "\n".join(lines)
