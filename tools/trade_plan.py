# -*- coding: utf-8 -*-
"""
操作参考计划（程序规则计算，LLM 只负责解读表述）：
- 方向结论：多周期（日/周/月）信号一致性打分 → 可考虑介入 / 观望 / 回避
- 买入参考区：回踩支撑（MA20/BOLL中轨）区间
- 止损纪律位：支撑下沿 - 1×ATR
- 目标参考位：BOLL上轨 / 近60日高点 两档
- 参考仓位：信号强度定基础档，波动率与追高风险降档（上限3成，工具定位保守）

定位说明：这是"若决定参与时的纪律参考"，不是行情预测；所有数字可复算可回测。
"""

from typing import Dict, Any, Optional

import pandas as pd


def _num(row, key) -> Optional[float]:
    if row is None:
        return None
    v = row.get(key)
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def build_trade_plan(daily_row: Dict[str, Any],
                     weekly_row: Optional[Dict[str, Any]] = None,
                     monthly_row: Optional[Dict[str, Any]] = None,
                     recent_low20: Optional[float] = None,
                     recent_high60: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """
    输入：三周期最新指标行（dict，含 close/ma20/ma_pattern/DIF/DEA/rsi6/pos_52w/
         boll_upper/boll_mid/boll_lower/atr14）+ 近20日最低价、近60日最高价
    输出：操作参考计划 dict；日线数据不足时返回 None
    """
    close = _num(daily_row, "close")
    if not close:
        return None

    # ---------- 1. 方向打分（多周期一致性） ----------
    score, reasons = 0, []

    def _pattern_score(row, weight, label):
        nonlocal score
        pattern = (row or {}).get("ma_pattern")
        if pattern == "多头排列":
            score += weight
            reasons.append(f"{label}多头排列(+{weight})")
        elif pattern == "空头排列":
            score -= weight
            reasons.append(f"{label}空头排列(-{weight})")

    _pattern_score(daily_row, 2, "日线")
    _pattern_score(weekly_row, 2, "周线")
    _pattern_score(monthly_row, 1, "月线")

    dif, dea = _num(daily_row, "DIF"), _num(daily_row, "DEA")
    if dif is not None and dea is not None:
        if dif > dea:
            score += 1
            reasons.append("日线MACD在零轴关系上偏多(+1)")
        else:
            score -= 1
            reasons.append("日线MACD偏空(-1)")

    rsi6 = _num(daily_row, "rsi6")
    if rsi6 is not None:
        if rsi6 >= 80:
            score -= 1
            reasons.append(f"RSI6={rsi6:.0f}超买(-1)")
        elif rsi6 <= 20:
            score += 1
            reasons.append(f"RSI6={rsi6:.0f}超跌(+1)")

    if score >= 3:
        direction = "可考虑介入"
    elif score <= -2:
        direction = "回避"
    else:
        direction = "观望"

    # ---------- 2. 关键价位（全部来自已计算指标） ----------
    ma20 = _num(daily_row, "ma20")
    boll_mid = _num(daily_row, "boll_mid")
    boll_upper = _num(daily_row, "boll_upper")
    boll_lower = _num(daily_row, "boll_lower")
    atr = _num(daily_row, "atr14") or close * 0.02

    support = max(x for x in (ma20, boll_mid, boll_lower) if x is not None) \
        if any(x is not None for x in (ma20, boll_mid, boll_lower)) else None
    if recent_low20 is not None:
        support_floor = min(recent_low20, support) if support else recent_low20
    else:
        support_floor = support

    entry_zone = None
    entry_note = ""
    if support:
        if close >= support:
            entry_zone = (round(support, 2), round(min(close, support * 1.03), 2))
            entry_note = f"回踩支撑区（MA20/BOLL中轨 {round(support, 2)} 附近）"
        else:
            entry_zone = None
            entry_note = f"现价已跌破 MA20({round(support, 2)})，站回其上方再考虑介入"

    stop_loss = round(support_floor - atr, 2) if support_floor else round(close - 2 * atr, 2)
    stop_pct = round((stop_loss / close - 1) * 100, 1)

    targets = []
    if boll_upper and boll_upper > close:
        targets.append(round(boll_upper, 2))
    if recent_high60 and recent_high60 > close and (not targets or abs(recent_high60 - targets[0]) / close > 0.01):
        targets.append(round(recent_high60, 2))
    targets = sorted(targets)[:2]

    # ---------- 3. 参考仓位（0~3成，规则降档） ----------
    position, pos_notes = 0, []
    if direction == "可考虑介入":
        position = 3 if score >= 5 else 2
        pos_notes.append(f"信号强度{score}分定基础{position}成")
        atr_pct = atr / close * 100
        if atr_pct > 4:
            position -= 1
            pos_notes.append(f"波动偏大(ATR {atr_pct:.1f}%)降1成")
        pos_52w = _num(daily_row, "pos_52w")
        if pos_52w is not None and pos_52w >= 85:
            position -= 1
            pos_notes.append(f"年内位置{pos_52w:.0f}%属追高降1成")
        position = max(position, 1)  # 结论为可介入时至少给1成试探档
    elif direction == "观望":
        pos_notes.append("信号不一致，建议空仓等待")
    else:
        pos_notes.append("多周期偏空，不建议参与")

    return {
        "direction": direction,
        "score": score,
        "score_reasons": reasons,
        "close": round(close, 2),
        "entry_zone": entry_zone,
        "entry_note": entry_note,
        "stop_loss": stop_loss,
        "stop_pct": stop_pct,
        "targets": targets,
        "position": position,
        "position_notes": pos_notes,
    }


def format_trade_plan(plan: Optional[Dict[str, Any]]) -> str:
    """格式化为 prompt/报告文本块"""
    if not plan:
        return ""
    lines = ["【操作参考（程序规则计算：仅为若参与时的纪律参考，不构成预测）】"]
    lines.append(f"  方向结论：{plan['direction']}（信号分 {plan['score']}：{'；'.join(plan['score_reasons'])}）")
    lines.append(f"  现价：{plan['close']}")
    if plan["entry_zone"]:
        lines.append(f"  买入参考区：{plan['entry_zone'][0]} ~ {plan['entry_zone'][1]}（{plan['entry_note']}）")
    elif plan["entry_note"]:
        lines.append(f"  买入参考：{plan['entry_note']}")
    lines.append(f"  止损纪律位：{plan['stop_loss']}（距现价 {plan['stop_pct']}%，跌破无条件离场）")
    if plan["targets"]:
        lines.append(f"  目标参考位：{' / '.join(str(t) for t in plan['targets'])}（到达可分批了结）")
    lines.append(f"  参考仓位：{plan['position']}成（{'；'.join(plan['position_notes'])}）")
    lines.append("  ⚠️ 以上价位与仓位为规则化纪律参考，LLM 不得修改数字，只可解释依据与风险")
    return "\n".join(lines)
