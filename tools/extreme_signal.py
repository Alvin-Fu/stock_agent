# -*- coding: utf-8 -*-
"""
极端信号汇总：超买回调风险 + 超跌反弹信号
=========================================
从 K 线技术指标中提取多个维度信号，统一打分判断。
支持日线/周线/月线，自动根据频率调整阈值。

数据源：经 _ensure_indicators 补齐指标列的 DataFrame。
"""

from typing import Optional, Dict, Any

from utils.logger import logger


def _num(v, default=None):
    """安全取数值"""
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _get_thresholds(freq: str) -> dict:
    """
    根据 K 线频率返回不同阈值。
    周/月线单根K线波幅更大，周期更长，阈值相应放宽。
    """
    if freq == "month":
        return {
            "label_bar": "月",
            "bars_short": 2, "bars_mid": 3, "bars_long": 3,
            "gain_short_strong": 25, "gain_short_mod": 18,
            "gain_mid_strong": 40, "gain_mid_mod": 30,
            "loss_long_strong": 30, "loss_long_mod": 20,
            "rsi_overbought": 70, "rsi_oversold": 30,
            "kdj_overbought": 80, "kdj_oversold": 20,
            "ma_dev_strong": 20, "ma_dev_mod": 15,
            "consec_strong": 3, "consec_mod": 2,
        }
    elif freq == "week":
        return {
            "label_bar": "周",
            "bars_short": 3, "bars_mid": 6, "bars_long": 12,
            "gain_short_strong": 20, "gain_short_mod": 15,
            "gain_mid_strong": 35, "gain_mid_mod": 25,
            "loss_long_strong": 25, "loss_long_mod": 20,
            "rsi_overbought": 75, "rsi_oversold": 25,
            "kdj_overbought": 90, "kdj_oversold": 10,
            "ma_dev_strong": 15, "ma_dev_mod": 10,
            "consec_strong": 5, "consec_mod": 3,
        }
    else:  # daily
        return {
            "label_bar": "日",
            "bars_short": 5, "bars_mid": 10, "bars_long": 20,
            "gain_short_strong": 15, "gain_short_mod": 10,
            "gain_mid_strong": 25, "gain_mid_mod": 18,
            "loss_long_strong": 20, "loss_long_mod": 15,
            "rsi_overbought": 80, "rsi_oversold": 20,
            "kdj_overbought": 100, "kdj_oversold": 0,
            "ma_dev_strong": 12, "ma_dev_mod": 8,
            "consec_strong": 7, "consec_mod": 5,
        }


def assess_extreme_signals(df, freq: str = "daily") -> Dict[str, Any]:
    """
    汇总超买回调风险和超跌反弹信号。

    参数
    ----
    df : pd.DataFrame
        K 线数据（降序，最新在前），必须含以下列：
        close, pct_chg, rsi6, kdj_j, ma20, boll_mid, boll_lower, boll_upper, pos_52w
    freq : str
        "daily" / "week" / "month"

    返回
    ----
    dict
        {
            "pullback_risk": {"score": int, "level": str, "signals": [str], "summary": str},
            "oversold_bounce": {"score": int, "level": str, "signals": [str], "summary": str},
        }
        score 范围 0-5，level 为 "高"/"中"/"低"/"无"
    """
    if df is None or df.empty:
        return {"pullback_risk": {"score": 0, "level": "无", "signals": [], "summary": ""},
                "oversold_bounce": {"score": 0, "level": "无", "signals": [], "summary": ""}}

    try:
        th = _get_thresholds(freq)
        bar = th["label_bar"]

        # 最新一行
        row = df.iloc[0]
        close = _num(row.get("close"))
        if close is None:
            return {"pullback_risk": {"score": 0, "level": "无", "signals": [], "summary": ""},
                    "oversold_bounce": {"score": 0, "level": "无", "signals": [], "summary": ""}}

        # ========== 数据提取 ==========
        n = len(df)
        pct_short = _num(df.iloc[:th["bars_short"]]["pct_chg"].sum()) if n >= th["bars_short"] else None
        pct_mid = _num(df.iloc[:th["bars_mid"]]["pct_chg"].sum()) if n >= th["bars_mid"] else None
        pct_long = _num(df.iloc[:th["bars_long"]]["pct_chg"].sum()) if n >= th["bars_long"] else None

        rsi6 = _num(row.get("rsi6"))
        kdj_j = _num(row.get("kdj_j"))
        ma20 = _num(row.get("ma20"))
        boll_upper = _num(row.get("boll_upper"))
        boll_lower = _num(row.get("boll_lower"))

        # 连续同向 K 线（从最新往前数）
        def _count_consecutive(series, direction: str) -> int:
            n = 0
            for v in series:
                v = _num(v)
                if v is None:
                    break
                if direction == "up" and v > 0:
                    n += 1
                elif direction == "down" and v < 0:
                    n += 1
                else:
                    break
            return n

        up_bars = _count_consecutive(df["pct_chg"], "up")
        down_bars = _count_consecutive(df["pct_chg"], "down")

        # ========== 超买回调风险 Pullback Risk ==========
        pull_signals = []
        pull_score = 0

        # 1. 短期涨幅过大
        if pct_short is not None:
            if pct_short >= th["gain_short_strong"]:
                pull_signals.append(f"近{th['bars_short']}{bar}累计涨幅{pct_short:.1f}%（超{th['gain_short_strong']}%阀值）")
                pull_score += 1
            elif pct_short >= th["gain_short_mod"]:
                pull_signals.append(f"近{th['bars_short']}{bar}累计涨幅{pct_short:.1f}%（超{th['gain_short_mod']}%）")
                pull_score += 1

        # 中期涨幅过大
        if pct_mid is not None:
            if pct_mid >= th["gain_mid_strong"]:
                pull_signals.append(f"近{th['bars_mid']}{bar}累计涨幅{pct_mid:.1f}%（超{th['gain_mid_strong']}%阀值）")
                pull_score += 1
            elif pct_mid >= th["gain_mid_mod"]:
                pull_signals.append(f"近{th['bars_mid']}{bar}累计涨幅{pct_mid:.1f}%（超{th['gain_mid_mod']}%）")
                pull_score += 1

        # 2. RSI 超买
        if rsi6 is not None and rsi6 >= th["rsi_overbought"]:
            pull_signals.append(f"RSI(6)={rsi6:.0f}进入超买区")
            pull_score += 1

        # 3. KDJ-J 超买
        if kdj_j is not None and kdj_j >= th["kdj_overbought"]:
            pull_signals.append(f"KDJ-J={kdj_j:.0f}进入超买区")
            pull_score += 1

        # 4. 价格偏离 MA20 过远
        if close and ma20:
            dev_pct = (close / ma20 - 1) * 100
            if dev_pct >= th["ma_dev_strong"]:
                pull_signals.append(f"现价偏离MA20 +{dev_pct:.1f}%（超{th['ma_dev_strong']}%阀值）")
                pull_score += 1
            elif dev_pct >= th["ma_dev_mod"]:
                pull_signals.append(f"现价偏离MA20 +{dev_pct:.1f}%（超{th['ma_dev_mod']}%）")
                pull_score += 1

        # 5. pos_52w（仅日线有意义）
        if freq == "daily":
            pos_52w = _num(row.get("pos_52w"))
            if pos_52w is not None and pos_52w >= 90:
                pull_signals.append(f"年内价格分位{pos_52w:.0f}%（偏高）")
                pull_score += 1

        # 6. 连续同向 K 线
        if up_bars >= th["consec_strong"]:
            pull_signals.append(f"连续{up_bars}{bar}上涨")
            pull_score += 1
        elif up_bars >= th["consec_mod"]:
            pull_signals.append(f"连续{up_bars}{bar}上涨")
            pull_score += 1

        # 7. 价格接近/触及 BOLL 上轨
        if close and boll_upper and close >= boll_upper * 0.995:
            pull_signals.append(f"现价{close:.2f}已接近BOLL上轨{boll_upper:.2f}")
            pull_score += 1

        pull_score = min(pull_score, 5)

        # ========== 超跌反弹信号 Oversold Bounce ==========
        bounce_signals = []
        bounce_score = 0

        # 1. 中期跌幅较大（超跌看中期更可靠）
        if pct_long is not None:
            if pct_long <= -th["loss_long_strong"]:
                bounce_signals.append(f"近{th['bars_long']}{bar}累计跌幅{pct_long:.1f}%（超{th['loss_long_strong']}%阀值）")
                bounce_score += 1
            elif pct_long <= -th["loss_long_mod"]:
                bounce_signals.append(f"近{th['bars_long']}{bar}累计跌幅{pct_long:.1f}%（超{th['loss_long_mod']}%）")
                bounce_score += 1

        # 短期跌幅
        if pct_short is not None and pct_short <= -th["gain_short_strong"]:
            bounce_signals.append(f"近{th['bars_short']}{bar}累计跌幅{pct_short:.1f}%（超{th['gain_short_strong']}%阀值）")
            bounce_score += 1

        # 2. RSI 超卖
        if rsi6 is not None and rsi6 <= th["rsi_oversold"]:
            bounce_signals.append(f"RSI(6)={rsi6:.0f}进入超卖区")
            bounce_score += 1

        # 3. KDJ-J 超卖
        if kdj_j is not None and kdj_j <= th["kdj_oversold"]:
            bounce_signals.append(f"KDJ-J={kdj_j:.0f}进入超卖区")
            bounce_score += 1

        # 4. 价格接近/跌破 BOLL 下轨
        if close and boll_lower:
            if close <= boll_lower:
                bounce_signals.append(f"现价{close:.2f}已触及BOLL下轨{boll_lower:.2f}")
                bounce_score += 1
            elif close <= boll_lower * 1.01:
                bounce_signals.append(f"现价{close:.2f}接近BOLL下轨{boll_lower:.2f}（距下轨1%以内）")
                bounce_score += 1

        # 5. pos_52w（仅日线）
        if freq == "daily":
            pos_52w = _num(row.get("pos_52w"))
            if pos_52w is not None and pos_52w <= 10:
                bounce_signals.append(f"年内价格分位{pos_52w:.0f}%（低位）")
                bounce_score += 1

        # 6. 连续同向 K 线
        if down_bars >= th["consec_strong"]:
            bounce_signals.append(f"连续{down_bars}{bar}下跌")
            bounce_score += 1
        elif down_bars >= th["consec_mod"]:
            bounce_signals.append(f"连续{down_bars}{bar}下跌")
            bounce_score += 1

        # 7. 中期长线跌幅巨大
        if pct_long is not None and pct_long <= -th["loss_long_strong"] * 1.2:
            bounce_signals.append(f"近{th['bars_long']}{bar}深度跌幅{pct_long:.1f}%")
            bounce_score += 1

        bounce_score = min(bounce_score, 5)

        # ========== 等级判定 ==========
        def _level(score: int) -> str:
            if score >= 4:
                return "高"
            elif score >= 2:
                return "中"
            elif score >= 1:
                return "低"
            return "无"

        def _summary(signals: list, level: str, score: int, is_pullback: bool) -> str:
            if not signals:
                return ""
            label = "超买回调" if is_pullback else "超跌反弹"
            return f"{label}信号{level}（{score}项触发）: {'、'.join(signals[:3])}" + (
                f"等{len(signals)}项" if len(signals) > 3 else "")

        return {
            "pullback_risk": {
                "score": pull_score,
                "level": _level(pull_score),
                "signals": pull_signals,
                "summary": _summary(pull_signals, _level(pull_score), pull_score, True),
            },
            "oversold_bounce": {
                "score": bounce_score,
                "level": _level(bounce_score),
                "signals": bounce_signals,
                "summary": _summary(bounce_signals, _level(bounce_score), bounce_score, False),
            },
        }

    except Exception as e:
        logger.warning(f"[极端信号] 计算异常: {e}")
        return {"pullback_risk": {"score": 0, "level": "无", "signals": [], "summary": ""},
                "oversold_bounce": {"score": 0, "level": "无", "signals": [], "summary": ""}}


def format_extreme_signals(signal_result: dict, freq_label: str = "日线") -> str:
    """
    将 assess_extreme_signals 的结果格式化为文本块。
    """
    if not signal_result:
        return ""

    parts = []
    for key, label in [("pullback_risk", "超买回调风险"), ("oversold_bounce", "超跌反弹信号")]:
        data = signal_result.get(key, {})
        level = data.get("level", "无")
        score = data.get("score", 0)
        signals = data.get("signals", [])

        if level == "无" or not signals:
            continue

        lines = [f"【{freq_label} {label}】{level}（{score}/5）"]
        for s in signals:
            lines.append(f"  • {s}")
        parts.append("\n".join(lines))

    return "\n\n".join(parts) if parts else ""
