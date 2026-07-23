# -*- coding: utf-8 -*-
"""
利润下降归因分析
==============
从利润表中系统化分析归母净利润变动的原因，按瀑布分解：
营收贡献、毛利率变化、费用率变化（销售/管理/研发/财务）、非经常性损益。

数据源：Tushare 利润表（income）+ 财务指标（fina_indicator）
"""

from typing import Optional, Dict, Any, List
from datetime import datetime

import pandas as pd
import numpy as np

from utils.logger import logger
from tools.stock_tools import stock_tool_instance


# ========================================================================
# 工具函数
# ========================================================================

_YI = 100_000_000  # 亿元


def _num(v, default=None):
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _to_yi(v) -> Optional[float]:
    """元转亿元"""
    if v is None:
        return None
    try:
        return round(float(v) / _YI, 2)
    except (TypeError, ValueError):
        return None


def _pct(v, decimals=1) -> Optional[float]:
    """小数转百分比"""
    if v is None:
        return None
    return round(float(v) * 100, decimals)


# ========================================================================
# 核心归因函数
# ========================================================================

def analyze_profit_decline(stock_code: str) -> Dict[str, Any]:
    """
    分析利润下降的具体原因（同比归因瀑布）。

    返回
    ----
    dict
        {
            "stock_code": str,
            "report_date": str,           # 最新报告期
            "compare_date": str,          # 对比报告期（去年同期）
            "net_profit_cur": float,      # 本期归母净利润（亿元）
            "net_profit_prev": float,     # 上期归母净利润（亿元）
            "net_profit_change": float,   # 变动额（亿元）
            "net_profit_change_pct": float,  # 变动率（%）
            "factors": [
                {
                    "factor": str,        # 因素名称
                    "label": str,         # 中英文标签
                    "impact_yi": float,   # 影响额（亿元，负数=拖累利润，正数=贡献利润）
                    "impact_pct": float,  # 占总变动的百分比
                    "detail": str,        # 可读描述
                },
            ],
            "primary_cause": str,         # 主因判定
            "summary": str,               # 一句话总结
            "confidence": str,            # 高/中/低
        }
    """
    try:
        # ========== 1. 获取利润表数据 ==========
        df_income = stock_tool_instance.fetch_and_save_stock_income(stock_code)
        if df_income is None or df_income.empty:
            return {"error": f"未获取到 {stock_code} 的利润表数据"}

        # 按 report_date 降序，最新在前
        df_income = df_income.sort_values("report_date", ascending=False).reset_index(drop=True)

        # ========== 2. 找本期与去年同期数据 ==========
        latest = df_income.iloc[0]
        cur_date = latest["report_date"]
        cur_date_ts = cur_date if isinstance(cur_date, pd.Timestamp) else pd.Timestamp(cur_date)

        # 找去年同期（同一季度）：去年同季
        cur_year = cur_date_ts.year
        cur_quarter = cur_date_ts.quarter
        prev_date_str = f"{cur_year - 1}-12-31"
        if cur_quarter == 1:
            prev_date_str = f"{cur_year - 1}-03-31"
        elif cur_quarter == 2:
            prev_date_str = f"{cur_year - 1}-06-30"
        elif cur_quarter == 3:
            prev_date_str = f"{cur_year - 1}-09-30"
        else:
            prev_date_str = f"{cur_year - 1}-12-31"

        prev_row = df_income[df_income["report_date"] == prev_date_str]
        if prev_row.empty:
            # 尝试用下一个可用报告期
            if len(df_income) >= 2:
                prev_row = df_income.iloc[1:2]
            if prev_row.empty:
                return {"error": f"无法找到对比期的利润表数据（需要 {prev_date_str}）"}

        prev = prev_row.iloc[0]
        prev_date = prev["report_date"]

        # ========== 3. 提取核心数值（亿元） ==========
        def _get(r, field):
            return _num(r.get(field))

        # 营收/利润
        rev_cur = _to_yi(_get(latest, "total_revenue"))
        rev_prev = _to_yi(_get(prev, "total_revenue"))
        np_cur = _to_yi(_get(latest, "net_profit"))
        np_prev = _to_yi(_get(prev, "net_profit"))

        if None in (rev_cur, rev_prev, np_cur, np_prev):
            return {"error": "利润表核心字段缺失（营收/归母净利润）"}

        np_change = round(np_cur - np_prev, 2)
        np_change_pct = round(((np_cur - np_prev) / abs(np_prev)) * 100, 2) if np_prev != 0 else 0.0

        # ========== 4. 计算各因素贡献（亿元） ==========
        # 营收贡献 = Δ营收 × 上期净利率
        net_margin_prev = round(np_prev / rev_prev * 100, 2) if rev_prev > 0 else 0
        revenue_impact = round((rev_cur - rev_prev) * net_margin_prev / 100, 2)

        # 费用率提取：销售/管理/研发/财务费用
        def _expense_rate(row, field, revenue) -> float:
            v = _to_yi(_get(row, field))
            if v is not None and revenue and revenue > 0:
                return round(v / revenue * 100, 2)
            return 0.0

        fields_exp = [
            ("sell_exp", "销售费用"),
            ("admin_exp", "管理费用"),
            ("rd_exp", "研发费用"),
            ("fin_exp", "财务费用"),
        ]

        # 费用贡献 = 上期费用率 - 本期费用率（费用率上升=拖累利润）
        exp_impacts = []
        total_exp_impact = 0.0
        for field, label in fields_exp:
            cur_rate = _expense_rate(latest, field, rev_cur)
            prev_rate = _expense_rate(prev, field, rev_prev)
            # 费用率上升 → 负贡献（拖累利润）
            rate_change = prev_rate - cur_rate  # 正=费用率下降（利好），负=费用率上升（利空）
            impact = round(rate_change * rev_cur / 100, 2)
            if abs(impact) >= 0.01:  # 只展示显著影响
                exp_impacts.append({
                    "factor": f"{label}费用率变化",
                    "impact_yi": impact,
                    "prev_rate": prev_rate,
                    "cur_rate": cur_rate,
                })
                total_exp_impact += impact

        # 毛利贡献（含营收 + 毛利率变化）
        # 更精确的分解：
        # 利润变化 = 营收变化贡献 + 毛利率变化贡献 + 费用率变化贡献 + 非经常性损益
        # 其中：毛利率变化贡献 = rev_cur × (gross_margin_cur - gross_margin_prev)
        gm_cur_pct = _get(latest, "gross_margin")
        gm_prev_pct = _get(prev, "gross_margin")
        gm_impact = 0.0
        has_gm = False
        if gm_cur_pct is not None and gm_prev_pct is not None:
            gm_impact = round(rev_cur * (gm_cur_pct - gm_prev_pct) / 100, 2)
            has_gm = abs(gm_impact) >= 0.01

        # 非经常性损益 = 总变动 - 营收贡献 - 毛利率贡献 - 费用贡献
        # 但营收贡献和毛利率贡献有重叠，更准确：
        # 总变动 = 营收贡献（按上期净利率） + 利润率变化贡献（含毛利率+费用率+非经常性）
        # 利润率的拆解：Δ净利率（百分点） = Δ毛利率（百分点） - Δ费用率合计（百分点） + Δ非经常性（残余）
        residual_change = np_change - revenue_impact
        # 其中可归因于费用的部分已算入 total_exp_impact
        # 可归因于毛利率的部分已算入 gm_impact
        # 剩余就是非经常性损益和其他
        non_op_impact = round(residual_change - gm_impact - total_exp_impact, 2)

        # ========== 5. 构建因素列表 ==========
        factors = []
        total_abs = 0.0

        def _add_factor(factor: str, impact_yi: float, detail: str = ""):
            nonlocal total_abs
            if abs(impact_yi) < 0.01:
                return
            total_abs += abs(impact_yi)
            factors.append({
                "factor": factor,
                "impact_yi": round(impact_yi, 2),
                "impact_pct": 0.0,  # 稍后计算
                "detail": detail,
            })

        # 营收端
        rev_detail = (f"营收{rev_cur:.1f}亿 vs 上年{rev_prev:.1f}亿（{'增长' if rev_cur >= rev_prev else '下滑'}"
                      f"{abs(rev_cur - rev_prev):.1f}亿）")
        if abs(revenue_impact) >= 0.01:
            _add_factor("营收变化", revenue_impact, rev_detail)

        # 毛利率
        if has_gm:
            gm_detail = (f"毛利率{gm_cur_pct:.1f}% vs 上年{gm_prev_pct:.1f}%"
                         f"（{'提升' if gm_cur_pct >= gm_prev_pct else '压缩'}"
                         f"{abs(gm_cur_pct - gm_prev_pct):.1f}百分点）")
            _add_factor("毛利率变化", gm_impact, gm_detail)

        # 各项费用
        for ei in exp_impacts:
            detail = (f"{ei['factor'][:3]}{ei['cur_rate']:.1f}% vs 上年{ei['prev_rate']:.1f}%"
                      f"（{'费用率下降' if ei['cur_rate'] <= ei['prev_rate'] else '费用率上升'}"
                      f"{abs(ei['cur_rate'] - ei['prev_rate']):.1f}百分点）")
            _add_factor(ei["factor"], ei["impact_yi"], detail)

        # 非经常性损益
        if abs(non_op_impact) >= 0.01:
            _add_factor("非经常性损益及其他", non_op_impact,
                        "投资收益、资产减值、公允价值变动、营业外收支等")

        # ========== 6. 计算占比 ==========
        total_abs = max(total_abs, 0.01)
        for f in factors:
            impact_abs = abs(f["impact_yi"])
            f["impact_pct"] = round(impact_abs / total_abs * 100, 1)

        # 按影响绝对值降序排列
        factors.sort(key=lambda x: abs(x["impact_yi"]), reverse=True)

        # ========== 7. 主因判定 ==========
        primary_cause = "无"
        if factors:
            top_factor = factors[0]
            if top_factor["factor"] == "营收变化":
                primary_cause = "营收端" if revenue_impact < 0 else "需求增长放缓"
            elif "毛利率" in top_factor["factor"]:
                primary_cause = "毛利率压缩（成本上升/降价压力）" if gm_impact < 0 else "毛利率改善"
            elif "费用" in top_factor["factor"]:
                primary_cause = "费用扩张" if top_factor["impact_yi"] < 0 else "费用控制"
            elif "非经常性" in top_factor["factor"]:
                primary_cause = "非经常性损益扰动"

        # ========== 8. 置信度 ==========
        # 非经常性损益残余过大 → 低置信度
        confidence = "高"
        if abs(non_op_impact) > abs(np_change) * 0.5:
            confidence = "中"
        if abs(non_op_impact) > abs(np_change) * 0.8:
            confidence = "低"

        # ========== 9. 一句话总结 ==========
        direction = "下降" if np_change < 0 else ("增长" if np_change > 0 else "持平")
        summary_parts = [
            f"归母净利润{direction}{abs(np_change):.1f}亿（同比{'+' if np_change >= 0 else ''}{np_change_pct:.1f}%）",
        ]

        # 找最大的拖累因素（只说前2）
        negative = [f for f in factors if f["impact_yi"] < 0]
        positive = [f for f in factors if f["impact_yi"] > 0]

        if negative:
            neg_str = "、".join(f"{f['factor']}拖累{f['impact_yi']:.1f}亿" for f in negative[:2])
            summary_parts.append(f"主因：{neg_str}")
        if positive:
            pos_str = "、".join(f"{f['factor']}贡献{f['impact_yi']:.1f}亿" for f in positive[:1])
            summary_parts.append(f"对冲：{pos_str}")

        summary = "；".join(summary_parts)

        # ========== 10. 格式化输出 ==========
        return {
            "stock_code": stock_code,
            "report_date": str(cur_date_ts.date()),
            "compare_date": str(prev_date.date()) if hasattr(prev_date, "date") else str(prev_date),
            "net_profit_cur": np_cur,
            "net_profit_prev": np_prev,
            "net_profit_change": np_change,
            "net_profit_change_pct": np_change_pct,
            "factors": factors,
            "primary_cause": primary_cause,
            "summary": summary,
            "confidence": confidence,
        }

    except Exception as e:
        logger.error(f"[利润归因] 分析失败: {e}")
        return {"error": f"利润归因分析异常: {e}"}


# ========================================================================
# 格式化
# ========================================================================

def format_profit_attribution(result: Dict[str, Any]) -> str:
    """
    将 analyze_profit_decline 的结果格式化为文本块。
    """
    if not result:
        return ""
    if "error" in result:
        return f"❌ 利润归因失败: {result['error']}"

    lines = [f"📉 利润归因分析：{result.get('stock_code', '')}"]
    lines.append(f"报告期: {result['report_date']} vs {result['compare_date']}")
    lines.append(f"{'-' * 40}")

    # 总览
    change = result["net_profit_change"]
    change_pct = result["net_profit_change_pct"]
    direction = "下降" if change < 0 else "增长"
    lines.append(f"归母净利润: {result['net_profit_cur']:.1f}亿 vs {result['net_profit_prev']:.1f}亿"
                 f" → {direction}{abs(change):.1f}亿（{change_pct:+.1f}%）")

    # 瀑布分解
    lines.append(f"\n瀑布分解（亿元，负=拖累利润，正=贡献利润）:")
    for f in result["factors"]:
        sign = "+" if f["impact_yi"] >= 0 else ""
        bar = "■" * max(1, int(abs(f["impact_yi"]) / 5)) if abs(f["impact_yi"]) > 3 else "□"
        lines.append(f"  {f['factor']}: {sign}{f['impact_yi']:.1f}亿（占比{f['impact_pct']:.0f}%）{bar}")

    # 主因
    lines.append(f"\n主因判定: {result['primary_cause']}")
    lines.append(f"汇总: {result['summary']}")
    lines.append(f"置信度: {result['confidence']}")

    return "\n".join(lines)
