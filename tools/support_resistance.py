# -*- coding: utf-8 -*-
"""
支撑/压力位程序化计算（纯规则，可复算）：
1. 摆动点：分形法找局部高低点（比左右各 k 根K线都高/低），是市场真实博弈过的价位
2. 聚类：相邻价位（容差 1.5%）归并成一个关键位，触碰次数越多越硬
3. 成交密集区：按价格分箱累计成交量，量最大的几个箱是筹码密集区，
   与摆动点重合时该价位强度加成
输出：现价下方的支撑（近的在前）与上方的压力（近的在前），各带强度依据。
均线/BOLL 是"跟随价格移动的参考线"，这里算的是"固定价位的历史博弈位"，两者互补。
"""

from typing import Dict, List, Optional

from utils.logger import logger


def _col(df, name) -> List[Optional[float]]:
    if name not in df.columns:
        return []
    out = []
    for v in df[name].tolist():
        try:
            f = float(v)
            out.append(None if f != f else f)
        except (TypeError, ValueError):
            out.append(None)
    return out


def _find_swings(values: List[Optional[float]], k: int, is_high: bool) -> List[float]:
    """分形摆动点：比左右各 k 个都高（低）。values 为时间升序"""
    swings = []
    n = len(values)
    for i in range(k, n - k):
        v = values[i]
        if v is None:
            continue
        window = values[i - k:i] + values[i + 1:i + k + 1]
        if any(w is None for w in window):
            continue
        if is_high and all(v > w for w in window):
            swings.append(v)
        elif not is_high and all(v < w for w in window):
            swings.append(v)
    return swings


def _cluster(prices: List[float], tol_pct: float) -> List[Dict]:
    """相邻价位归并：返回 [{price(加权中心), touches}]，按价格升序"""
    if not prices:
        return []
    prices = sorted(prices)
    clusters = []
    cur = [prices[0]]
    for p in prices[1:]:
        if (p - cur[-1]) / cur[-1] * 100 <= tol_pct:
            cur.append(p)
        else:
            clusters.append(cur)
            cur = [p]
    clusters.append(cur)
    return [{"price": sum(c) / len(c), "touches": len(c)} for c in clusters]


def _volume_nodes(closes: List[Optional[float]], volumes: List[Optional[float]],
                  bins: int, top_n: int = 3) -> List[float]:
    """价格分箱累计成交量，取量最大的 top_n 个箱中心（成交密集区）"""
    pairs = [(c, v) for c, v in zip(closes, volumes) if c is not None and v is not None and v > 0]
    if len(pairs) < bins:
        return []
    lo = min(c for c, _ in pairs)
    hi = max(c for c, _ in pairs)
    if hi <= lo:
        return []
    width = (hi - lo) / bins
    acc = [0.0] * bins
    for c, v in pairs:
        idx = min(int((c - lo) / width), bins - 1)
        acc[idx] += v
    ranked = sorted(range(bins), key=lambda i: acc[i], reverse=True)[:top_n]
    return [lo + (i + 0.5) * width for i in ranked]


def compute_sr_levels(df, lookback: int = 120, swing_k: int = 2,
                      cluster_tol_pct: float = 1.5, volume_bins: int = 24) -> Optional[Dict]:
    """
    输入日线 DataFrame（项目约定：降序，最新在前，含 high/low/close/volume 列）。
    返回 {"close", "supports": [...], "resistances": [...]}；数据不足返回 None。
    supports/resistances 每项 {"price", "touches", "vol_node", "strength"}，均按"离现价近"排前。
    """
    try:
        if df is None or df.empty or len(df) < swing_k * 2 + 5:
            return None
        window = df.head(lookback)
        # 转时间升序做摆动点判定
        highs = _col(window, "high")[::-1]
        lows = _col(window, "low")[::-1]
        closes = _col(window, "close")[::-1]
        volumes = _col(window, "volume")[::-1]
        close = next((c for c in reversed(closes) if c is not None), None)
        if close is None:
            return None

        swing_prices = _find_swings(highs, swing_k, True) + _find_swings(lows, swing_k, False)
        clusters = _cluster(swing_prices, cluster_tol_pct)

        # 成交密集区：与摆动位重合则加强度，不重合则作为独立关键位
        for node in _volume_nodes(closes, volumes, volume_bins):
            matched = False
            for c in clusters:
                if abs(node - c["price"]) / c["price"] * 100 <= cluster_tol_pct:
                    c["vol_node"] = True
                    matched = True
                    break
            if not matched:
                clusters.append({"price": node, "touches": 0, "vol_node": True})

        for c in clusters:
            c.setdefault("vol_node", False)
            c["strength"] = c["touches"] + (2 if c["vol_node"] else 0)
            c["price"] = round(c["price"], 2)

        # 距现价 0.5% 以内的位当作"就在脚下"，不作为支撑/压力输出
        supports = sorted([c for c in clusters if c["price"] < close * 0.995],
                          key=lambda c: -c["price"])[:4]
        resistances = sorted([c for c in clusters if c["price"] > close * 1.005],
                             key=lambda c: c["price"])[:4]
        # 各保留强度>=1 的前3个（纯噪音位丢弃）
        supports = [c for c in supports if c["strength"] >= 1][:3]
        resistances = [c for c in resistances if c["strength"] >= 1][:3]
        if not supports and not resistances:
            return None
        return {"close": round(close, 2), "supports": supports, "resistances": resistances}
    except Exception as e:
        logger.warning(f"支撑压力位计算失败（不影响其余分析）: {e}")
        return None


def _fmt_level(c: Dict) -> str:
    tags = []
    if c["touches"]:
        tags.append(f"触碰{c['touches']}次")
    if c["vol_node"]:
        tags.append("成交密集")
    return f"{c['price']}（{'·'.join(tags)}）"


def format_sr_levels(sr: Optional[Dict]) -> str:
    if not sr:
        return ""
    lines = ["【程序计算关键位（近120根K线：摆动点聚类+成交密集区，历史博弈价位）】"]
    if sr["supports"]:
        lines.append("  支撑（由近及远）：" + " ｜ ".join(_fmt_level(c) for c in sr["supports"]))
    if sr["resistances"]:
        lines.append("  压力（由近及远）：" + " ｜ ".join(_fmt_level(c) for c in sr["resistances"]))
    lines.append("  说明：触碰次数越多、带成交密集标记的价位越硬；与均线/BOLL位互补使用")
    return "\n".join(lines)
