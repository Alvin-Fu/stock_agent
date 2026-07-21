# -*- coding: utf-8 -*-
"""
神奇九转（TD Sequential）指标计算工具
======================================
基于 Tom DeMark 的 TD Sequential 理论，识别股价连续9根K线的趋势消耗信号：
- 顶部九转（sell setup）：连续9根收盘价 > 前第4根收盘价 → 潜在反转信号
- 底部九转（buy setup）：连续9根收盘价 < 前第4根收盘价 → 潜在反转信号

数据源：akshare 日线 / 60min K线
依赖：akshare, pandas
"""

from typing import List, Tuple, Optional
from datetime import date, timedelta

import pandas as pd

from utils.logger import logger
from utils.retry_utils import retry_with_multiple_sources


def _fetch_kline(stock_code: str, period: str = "daily", months: int = 6) -> pd.DataFrame:
    """从 akshare 获取 K 线数据（支持 daily / 60min），多数据源重试"""
    import akshare as ak
    end = date.today()
    start = end - timedelta(days=30 * months)
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    rename_map = {"日期": "date", "收盘": "close", "开盘": "open",
                  "最高": "high", "最低": "low", "时间": "date"}

    def _parse_df(df):
        if df is None or getattr(df, "empty", True):
            return pd.DataFrame()
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df

    def _fetch_em():
        if period == "daily":
            df = ak.stock_zh_a_hist(
                symbol=stock_code, period="daily",
                start_date=start_str, end_date=end_str, adjust="qfq",
            )
        elif period == "60min":
            start_dt = start.strftime("%Y-%m-%d 09:00:00")
            end_dt = end.strftime("%Y-%m-%d 15:00:00")
            df = ak.stock_zh_a_hist_min_em(
                symbol=stock_code, period="60",
                start_date=start_dt, end_date=end_dt, adjust="qfq",
            )
        else:
            return pd.DataFrame()
        return _parse_df(df)

    def _fetch_tencent():
        if period != "daily":
            return pd.DataFrame()
        df = ak.stock_zh_a_hist_tx(
            symbol=stock_code, period="daily",
            start_date=start_str, end_date=end_str, adjust="qfq",
        )
        return _parse_df(df)

    def _fetch_sina():
        if period != "daily":
            return pd.DataFrame()
        df = ak.stock_zh_a_hist_sina(
            symbol=stock_code, period="daily",
            start_date=start_str, end_date=end_str, adjust="qfq",
        )
        return _parse_df(df)

    sources = [("东财em", _fetch_em)]
    if period == "daily":
        sources.append(("腾讯", _fetch_tencent))
        sources.append(("新浪", _fetch_sina))

    df = retry_with_multiple_sources(sources)
    if df is not None and not df.empty:
        logger.info(f"[九转] {stock_code} {period} K线获取成功，{len(df)}条")
        return df
    else:
        logger.warning(f"[九转] {stock_code} {period} K线获取失败，所有数据源均不可用")
        return pd.DataFrame()


def _calc_td_setup(closes: List[float], direction: str) -> List[Tuple[int, int]]:
    """
    计算 TD Sequential setup 阶段的连续计数。
    direction="buy"：close[i] < close[i-4]
    direction="sell"：close[i] > close[i-4]
    返回 [(index, count)]，count 为 1-9，0 表示未计数。
    """
    n = len(closes)
    counts = [0] * n
    current = 0
    for i in range(4, n):
        if direction == "buy" and closes[i] < closes[i - 4]:
            current += 1
        elif direction == "sell" and closes[i] > closes[i - 4]:
            current += 1
        else:
            current = 0
        if 1 <= current <= 9:
            counts[i] = current
        else:
            # 超过9后继续跟踪，但只显示9之后的状态
            counts[i] = 0
    return [(i, c) for i, c in enumerate(counts) if c > 0]


def _describe_signal(stock_code: str, df: pd.DataFrame,
                     sell_signals: List[Tuple[int, int]],
                     buy_signals: List[Tuple[int, int]]) -> str:
    """生成可读的九转信号描述"""
    lines = [f"【神奇九转（TD Sequential）】代码: {stock_code}"]
    lines.append("数据源: akshare K线（前复权）\n")

    # ---- 顶部九转 ----
    lines.append("── 顶部九转（sell setup）──")
    top_count = 0
    top_at9 = []
    for idx, cnt in sell_signals:
        dt = df.iloc[idx]["date"].strftime("%Y-%m-%d")
        close = df.iloc[idx]["close"]
        lines.append(f"  #{cnt} {dt} 收盘{close:.2f}")
        top_count = cnt
        if cnt == 9:
            top_at9.append(dt)
    if not sell_signals:
        lines.append("  （无连续信号）")
    elif top_count == 9:
        lines.append(f"\n  ⚠️ 顶部九转完成于: {', '.join(top_at9)} — 潜在回调风险")
    elif top_count >= 7:
        lines.append(f"\n  🔺 当前计数 {top_count}/9，接近完成，注意压力")
    else:
        lines.append(f"\n  当前计数 {top_count}/9（未完成）")

    # ---- 底部九转 ----
    lines.append("\n── 底部九转（buy setup）──")
    bot_count = 0
    bot_at9 = []
    for idx, cnt in buy_signals:
        dt = df.iloc[idx]["date"].strftime("%Y-%m-%d")
        close = df.iloc[idx]["close"]
        lines.append(f"  #{cnt} {dt} 收盘{close:.2f}")
        bot_count = cnt
        if cnt == 9:
            bot_at9.append(dt)
    if not buy_signals:
        lines.append("  （无连续信号）")
    elif bot_count == 9:
        lines.append(f"\n  ⚠️ 底部九转完成于: {', '.join(bot_at9)} — 潜在反弹机会")
    elif bot_count >= 7:
        lines.append(f"\n  🔻 当前计数 {bot_count}/9，接近完成，关注支撑")
    else:
        lines.append(f"\n  当前计数 {bot_count}/9（未完成）")

    return "\n".join(lines)


def fetch_magic_nine_turn(stock_code: str, months: int = 6) -> str:
    """
    主入口：获取个股的神奇九转（TD Sequential）信号。

    Parameters
    ----------
    stock_code : str
        6位股票代码
    months : int
        回溯月数，默认6个月

    Returns
    -------
    str
        格式化信号文本，包含日线+60min（如有）的九转计数
    """
    if not stock_code or len(stock_code) != 6:
        logger.warning(f"[九转] 无效股票代码: {stock_code}")
        return ""

    blocks = []

    # ---- 日线九转 ----
    df_daily = _fetch_kline(stock_code, "daily", months)
    if not df_daily.empty:
        closes = df_daily["close"].tolist()
        signals_sell = _calc_td_setup(closes, "sell")
        signals_buy = _calc_td_setup(closes, "buy")
        blocks.append(_describe_signal(stock_code, df_daily, signals_sell, signals_buy))

    # ---- 60min 九转（配合日线辅助判断） ----
    df_60 = _fetch_kline(stock_code, "60min", 2)  # 60min 只看近2个月
    if not df_60.empty:
        closes_60 = df_60["close"].tolist()
        sig_sell_60 = _calc_td_setup(closes_60, "sell")
        sig_buy_60 = _calc_td_setup(closes_60, "buy")
        if sig_sell_60 or sig_buy_60:
            blocks.append("")
            blocks.append("【60分钟九转（辅助判断，数据近2个月）】")
            for idx, cnt in sig_sell_60:
                dt = df_60.iloc[idx]["date"].strftime("%Y-%m-%d %H:%M")
                close = df_60.iloc[idx]["close"]
                blocks.append(f"  顶部#{cnt} {dt} 收盘{close:.2f}")
            for idx, cnt in sig_buy_60:
                dt = df_60.iloc[idx]["date"].strftime("%Y-%m-%d %H:%M")
                close = df_60.iloc[idx]["close"]
                blocks.append(f"  底部#{cnt} {dt} 收盘{close:.2f}")

    return "\n".join(blocks)
