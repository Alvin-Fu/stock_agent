# -*- coding: utf-8 -*-
"""
行业估值与位置度量（产业链分析用）：
以候选池（各环节龙头）为行业代理样本，程序计算四组硬指标 + 参考标签，
供 LLM 在"行业风险/回调风险"分析中引用——数字出自代码，LLM 只解读。

指标：
- 估值水位：池内 PE(TTM) 中位数 + 各股 PE 历史分位的中位数
- 行业位置：池内平均年内位置（pos_52w，0=年内最低 100=年内最高）
- 短期过热度：池内近20日平均涨幅
- 回调参考：池内收盘价相对 MA20 的平均乖离率
"""

import statistics
from typing import List, Dict, Any, Optional

import pandas as pd

from utils.logger import logger


def compute_industry_metrics(per_stock: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    纯计算：输入每只股票的度量行
    [{code, pe_ttm, pe_percentile, pos_52w, ret20, bias_ma20}]（缺失项为 None）
    输出汇总指标与参考标签；有效样本不足 2 只返回 None。
    """
    rows = [r for r in per_stock or [] if r.get("pos_52w") is not None]
    if len(rows) < 2:
        return None

    def _median(key):
        vals = [float(r[key]) for r in rows if r.get(key) is not None]
        return round(statistics.median(vals), 1) if vals else None

    def _mean(key):
        vals = [float(r[key]) for r in rows if r.get(key) is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    pe_median = _median("pe_ttm")
    pe_pct_median = _median("pe_percentile")
    pos_avg = _mean("pos_52w")
    ret20_avg = _mean("ret20")
    bias_avg = _mean("bias_ma20")

    # 参考标签（阈值为经验值，仅作提示，不构成预测）
    valuation_label = "估值数据不足"
    if pe_pct_median is not None:
        if pe_pct_median >= 80:
            valuation_label = "估值高分位"
        elif pe_pct_median <= 30:
            valuation_label = "估值低分位"
        else:
            valuation_label = "估值中等分位"

    position_label = "位置数据不足"
    if pos_avg is not None:
        if pos_avg >= 70:
            position_label = "年内高位"
        elif pos_avg <= 30:
            position_label = "年内低位"
        else:
            position_label = "年内中位"

    heat_label = ""
    if ret20_avg is not None and ret20_avg >= 15:
        heat_label = "短期涨幅过大"

    labels = [x for x in (valuation_label, position_label, heat_label) if x]
    if ("估值高分位" in labels and "年内高位" in labels) or heat_label:
        overall = "过热警示"
    elif "估值低分位" in labels and "年内低位" in labels:
        overall = "低位区域"
    else:
        overall = "中性"

    return {
        "sample_count": len(rows),
        "pe_median": pe_median,
        "pe_percentile_median": pe_pct_median,
        "pos_52w_avg": pos_avg,
        "ret20_avg": ret20_avg,
        "bias_ma20_avg": bias_avg,
        "labels": labels,
        "overall": overall,
    }


def collect_industry_valuation(codes: List[str]) -> Optional[Dict[str, Any]]:
    """
    取数外壳：对候选池逐只取 K 线指标与每日指标（PE），组装后交纯函数计算。
    任何一只失败只影响该只，不阻断整体；tushare 未配时 PE 相关自动缺失。
    """
    from storage.sqlite.stock_storage import get_db
    from tools.stock_tools import stock_tool_instance, _ensure_indicators

    db = get_db()
    per_stock = []
    for code in codes or []:
        row: Dict[str, Any] = {"code": code}
        try:
            df = stock_tool_instance.fetch_and_save_stock_daily_data(code)
            if df is None or df.empty:
                continue
            df = _ensure_indicators(df, "daily")
            latest = df.iloc[0]
            row["pos_52w"] = _num(latest.get("pos_52w"))
            close = _num(latest.get("close"))
            ma20 = _num(latest.get("ma20"))
            if close and ma20:
                row["bias_ma20"] = round((close / ma20 - 1) * 100, 2)
            if close is not None and len(df) > 20:
                prev = _num(df.iloc[20].get("close"))
                if prev:
                    row["ret20"] = round((close / prev - 1) * 100, 2)
        except Exception as e:
            logger.warning(f"[行业估值] {code} K线指标获取失败: {e}")
            continue

        try:
            stock_tool_instance.fetch_and_save_stock_basic_daily(code)
            basic = db.get_latest_daily_basic_data(code, 750)
            if basic is not None and not basic.empty:
                cur = _num(basic.iloc[0].get("pe_ttm"))
                if cur and cur > 0:
                    row["pe_ttm"] = round(cur, 1)
                    hist = pd.to_numeric(basic["pe_ttm"], errors="coerce").dropna()
                    if len(hist) >= 60:
                        row["pe_percentile"] = round(float((hist < cur).mean() * 100), 1)
        except Exception as e:
            logger.warning(f"[行业估值] {code} 每日指标获取失败: {e}")

        per_stock.append(row)

    return compute_industry_metrics(per_stock)


def format_industry_valuation(metrics: Optional[Dict[str, Any]]) -> str:
    """格式化为 prompt 文本块；无数据返回空串"""
    if not metrics:
        return ""
    n = metrics["sample_count"]
    # 样本不足5只时中位数没有板块代表性（薄利股的失真PE会主导结果），标题降级并明示
    if n < 5:
        lines = [f"【候选池估值参考（程序按 {n} 只样本计算——样本不足，不代表板块/行业水位，仅供参考）】"]
    else:
        lines = [f"【行业估值与位置（程序按 {n} 只龙头样本计算，仅供风险评估参考）】"]
    if metrics.get("pe_median") is not None:
        pct = f"，PE历史分位中位数 {metrics['pe_percentile_median']}%" \
            if metrics.get("pe_percentile_median") is not None else ""
        lines.append(f"  估值：池内 PE(TTM) 中位数 {metrics['pe_median']}{pct}")
    if metrics.get("pos_52w_avg") is not None:
        lines.append(f"  位置：平均年内位置 {metrics['pos_52w_avg']}%（0=年内最低,100=年内最高）")
    if metrics.get("ret20_avg") is not None:
        lines.append(f"  短期：近20日平均涨幅 {metrics['ret20_avg']}%")
    if metrics.get("bias_ma20_avg") is not None:
        lines.append(f"  乖离：收盘相对MA20平均乖离 {metrics['bias_ma20_avg']}%")
    lines.append(f"  程序参考标签：{metrics['overall']}（{'、'.join(metrics['labels'])}）")
    lines.append("  ⚠️ 使用规则：以上为历史/当前状态的量化描述，回调风险分析须基于这些数字展开，"
                 "禁止在此之外编造估值或概率数字；乖离为负=价格已回落到MA20下方（回调已发生），"
                 "禁止表述为'即将回落/存在回落压力'")
    if n < 5:
        lines.append(f"  ⚠️ 样本仅 {n} 只：禁止称'板块/行业中位数'，引用时必须写明样本数")
    return "\n".join(lines)


def _num(value) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
