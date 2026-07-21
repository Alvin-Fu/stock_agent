# -*- coding: utf-8 -*-
"""
大盘环境（沪深300）：个股操作不能不看大盘脸色。
程序判定三档：顺风（收盘>MA20>MA60）/ 逆风（收盘<MA20<MA60）/ 中性（其余）。
逆风时操作参考的仓位自动降一档（trade_plan 里执行）。当日缓存。

含估值维度：上证50 PE/PB + 中证500 PE/PB（通过 Tushare index_dailybasic），
以及沪深300日线/周线/月线 K 线均线数据（与个股技术面 MA 口径一致），
帮助 LLM 判断大盘趋势与估值水位。
"""

import threading
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd

from utils.logger import logger

_ENV_CACHE = {"day": None, "env": None}
_lock = threading.Lock()
_VALUATION_CACHE = {"day": None, "text": None}  # 估值文本独立缓存


def judge_market_env(closes: List[float]) -> Optional[Dict]:
    """纯函数：closes 为时间升序收盘序列（至少60根）；返回 {label, close, ma20, ma60, chg20}"""
    closes = [c for c in closes if c is not None]
    if len(closes) < 60:
        return None
    close = closes[-1]
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60
    chg20 = (close / closes[-21] - 1) * 100 if len(closes) >= 21 and closes[-21] else None
    if close > ma20 > ma60:
        label = "顺风"
    elif close < ma20 < ma60:
        label = "逆风"
    else:
        label = "中性"
    return {"label": label, "close": round(close, 2), "ma20": round(ma20, 2),
            "ma60": round(ma60, 2), "chg20": round(chg20, 2) if chg20 is not None else None}


# 均线周期与个股技术面保持一致
_MA_PERIODS = [5, 10, 20, 50, 120, 200]


def _ma_pattern(ma5, ma10, ma20, ma50):
    """与 stock/base.py 一致的均线形态判定"""
    vals = [ma5, ma10, ma20, ma50]
    if any(v is None or pd.isna(v) for v in vals):
        return "数据不足"
    if vals[0] > vals[1] > vals[2] > vals[3]:
        return "多头排列"
    if vals[0] < vals[1] < vals[2] < vals[3]:
        return "空头排列"
    return "缠绕"


def _fmt_ma(v: Optional[float]) -> str:
    if v is None or pd.isna(v):
        return "-"
    return f"{v:.2f}"


def _compute_ma_str(closes: List[float], freq_label: str) -> str:
    """从收盘价序列计算均线（closes 按日期升序排列），输出格式与个股 K 线摘要一致"""
    if len(closes) < 5:
        return ""
    s = pd.Series(closes)
    mas = {}
    for p in _MA_PERIODS:
        if len(s) >= p:
            mas[p] = s.rolling(p, min_periods=p).mean().iloc[-1]
        else:
            mas[p] = None
    close = closes[-1]
    pattern = _ma_pattern(mas[5], mas[10], mas[20], mas[50])
    parts = [
        f"收盘={close:.2f}",
        f"MA5={_fmt_ma(mas[5])}",
        f"MA10={_fmt_ma(mas[10])}",
        f"MA20={_fmt_ma(mas[20])}",
    ]
    # 长周期均线（MA50+）只在周期内数据够时才展示，避免注水
    if mas[50] is not None and len(s) >= 50:
        parts.append(f"MA50={_fmt_ma(mas[50])}")
    if mas[120] is not None and len(s) >= 120:
        parts.append(f"MA120={_fmt_ma(mas[120])}")
    if mas[200] is not None and len(s) >= 200:
        parts.append(f"MA200={_fmt_ma(mas[200])}")
    parts.append(f"形态: {pattern}")
    segments = " ".join(parts)
    return f"{freq_label}: {segments}"


_INDEX_KLINE_CACHE = {"day": None, "text": None}  # K 线文本独立缓存


def _fetch_index_kline_text() -> str:
    """
    通过 Tushare 获取沪深300日线/周线/月线收盘价，计算均线。
    当日缓存。
    """
    today = date.today()
    if _INDEX_KLINE_CACHE["text"] is not None and _INDEX_KLINE_CACHE["day"] == today:
        return _INDEX_KLINE_CACHE["text"]

    try:
        import tushare as ts
        pro = ts.pro_api()
    except Exception:
        return ""

    lines = []
    # 日线：拉最近1年（约250个交易日），确保MA200有数据
    try:
        start = (today - timedelta(days=400)).strftime("%Y%m%d")
        df = pro.index_daily(ts_code="000300.SH", start_date=start,
                             end_date=today.strftime("%Y%m%d"), fields="trade_date,close")
        if df is not None and not df.empty:
            closes = [float(v) for v in df.sort_values("trade_date")["close"].values]
            s = _compute_ma_str(closes, "日线")
            if s:
                lines.append(s)
    except Exception as e:
        logger.debug(f"[大盘K线] 沪深300日线失败: {e}")

    # 周线：拉近3年（约156根周线）
    try:
        start = (today - timedelta(days=1100)).strftime("%Y%m%d")
        df = pro.index_weekly(ts_code="000300.SH", start_date=start,
                              end_date=today.strftime("%Y%m%d"), fields="trade_date,close")
        if df is not None and not df.empty:
            closes = [float(v) for v in df.sort_values("trade_date")["close"].values]
            s = _compute_ma_str(closes, "周线")
            if s:
                lines.append(s)
    except Exception as e:
        logger.debug(f"[大盘K线] 沪深300周线失败: {e}")

    # 月线：拉近10年（约120根月线）
    try:
        start = (today - timedelta(days=3700)).strftime("%Y%m%d")
        df = pro.index_monthly(ts_code="000300.SH", start_date=start,
                               end_date=today.strftime("%Y%m%d"), fields="trade_date,close")
        if df is not None and not df.empty:
            closes = [float(v) for v in df.sort_values("trade_date")["close"].values]
            s = _compute_ma_str(closes, "月线")
            if s:
                lines.append(s)
    except Exception as e:
        logger.debug(f"[大盘K线] 沪深300月线失败: {e}")

    result = "\n".join(lines) if lines else ""
    if result:
        _INDEX_KLINE_CACHE.update(day=today, text=result)
    return result


def _fetch_valuation_text() -> str:
    """
    通过 Tushare index_dailybasic 获取上证50和中证500的PE/PB。
    当日缓存，失败返回空串。
    """
    today = date.today()
    if _VALUATION_CACHE["text"] is not None and _VALUATION_CACHE["day"] == today:
        return _VALUATION_CACHE["text"]

    try:
        import tushare as ts
        pro = ts.pro_api()
    except Exception as e:
        logger.debug(f"[大盘估值] Tushare 初始化失败: {e}")
        return ""

    today_str = today.strftime("%Y%m%d")
    codes = [
        ("000016.SH", "上证50"),
        ("000905.SH", "中证500"),
    ]
    parts = []
    for ts_code, name in codes:
        try:
            df = pro.index_dailybasic(ts_code=ts_code, start_date=today_str,
                                      end_date=today_str, fields="pe,pb")
            if df is not None and not df.empty:
                pe = df.iloc[0].get("pe")
                pb = df.iloc[0].get("pb")
                pe_str = f"PE {pe}" if pe is not None else "PE --"
                pb_str = f"PB {pb}" if pb is not None else "PB --"
                parts.append(f"{name} {pe_str}/{pb_str}")
            else:
                parts.append(f"{name} 暂无数据")
        except Exception as e:
            logger.debug(f"[大盘估值] {name} 获取失败: {e}")

    result = "；".join(parts) if parts else ""
    if result:
        _VALUATION_CACHE.update(day=today, text=result)
    return result


def format_market_env(env: Optional[Dict]) -> str:
    if not env:
        return ""
    seg = (f"【大盘环境（沪深300，程序判定）】{env['label']}："
           f"收盘 {env['close']}，MA20 {env['ma20']}，MA60 {env['ma60']}")
    if env.get("chg20") is not None:
        seg += f"，近20日 {'+' if env['chg20'] >= 0 else ''}{env['chg20']}%"
    notes = {"顺风": "指数多头结构，个股信号可按常规执行",
             "逆风": "指数空头结构，个股多头信号胜率打折，参考仓位已自动降一档",
             "中性": "指数方向不明，仓位保持常规偏保守"}
    seg += f"。{notes[env['label']]}"

    # 追加估值维度
    val_text = _fetch_valuation_text()
    if val_text:
        seg += f"\n【大盘估值（程序）】{val_text}"

    # 追加沪深300 K线均线
    kline_text = _fetch_index_kline_text()
    if kline_text:
        seg += f"\n【沪深300K线（程序）】\n{kline_text}"
    return seg


def get_market_env() -> Optional[Dict]:
    """取当日大盘环境（当日缓存；失败返回 None 不阻断）"""
    today = date.today()
    if _ENV_CACHE["env"] is not None and _ENV_CACHE["day"] == today:
        return _ENV_CACHE["env"]
    with _lock:
        if _ENV_CACHE["env"] is not None and _ENV_CACHE["day"] == today:
            return _ENV_CACHE["env"]
        try:
            import akshare as ak
            df = ak.stock_zh_index_daily(symbol="sh000300")
            closes = [float(c) for c in df["close"].tolist()[-120:]]
            env = judge_market_env(closes)
            if env:
                _ENV_CACHE.update(day=today, env=env)
            return env
        except Exception as e:
            logger.warning(f"[大盘环境] 沪深300获取失败（不影响分析）: {e}")
            return _ENV_CACHE["env"]
