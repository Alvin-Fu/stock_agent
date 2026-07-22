# -*- coding: utf-8 -*-
"""
神奇九转（TD Sequential）指标计算工具
======================================
基于 Tom DeMark 的 TD Sequential 理论，识别股价连续9根K线的趋势消耗信号：
- 顶部九转（sell setup）：连续9根收盘价 > 前第4根收盘价 → 潜在反转信号
- 底部九转（buy setup）：连续9根收盘价 < 前第4根收盘价 → 潜在反转信号

数据源：日线/周线/月线来自数据库（Tushare），60min 来自 AkShare
依赖：pandas
"""

from typing import List, Tuple, Optional
from datetime import date, timedelta

import pandas as pd

from utils.logger import logger


def _fetch_kline_from_db(stock_code: str, period: str = "daily", months: int = 6) -> pd.DataFrame:
    """从数据库读取日线数据，周线/月线通过日线聚合生成，避免重复从 AkShare 拉取。"""
    try:
        from storage.sqlite.stock_storage import get_db
        df = get_db().get_all_daily_data(stock_code)
    except Exception as e:
        logger.warning(f"[九转] 数据库读取失败: {e}")
        return pd.DataFrame()

    if df.empty:
        logger.warning(f"[九转] {stock_code} 数据库无日线数据")
        return pd.DataFrame()

    # 确保必备列和日期排序
    for col in ("date", "close", "open", "high", "low"):
        if col not in df.columns:
            logger.warning(f"[九转] 数据库日线缺列 {col}")
            return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 回溯时间窗口
    end = date.today()
    start = end - timedelta(days=30 * months)
    df = df[df["date"] >= pd.Timestamp(start)].copy()

    if df.empty:
        return df

    if period == "weekly":
        # 周线：按周取最后一个交易日的行情
        df = df.resample("W-FRI", on="date").agg({
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum",
        }).dropna(subset=["close"]).reset_index()
    elif period == "monthly":
        # 月线：按月取最后一个交易日的行情
        df = df.resample("ME", on="date").agg({
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum",
        }).dropna(subset=["close"]).reset_index()

    logger.info(f"[九转] {stock_code} {period} K线从数据库获取成功，{len(df)}条")
    return df


def _fetch_kline_60min(stock_code: str, months: int = 2) -> pd.DataFrame:
    """60分钟 K 线通过 AkShare 拉取（数据库无分钟线）"""
    import akshare as ak
    end = date.today()
    start = end - timedelta(days=30 * months)
    start_dt = start.strftime("%Y-%m-%d 09:00:00")
    end_dt = end.strftime("%Y-%m-%d 15:00:00")
    try:
        df = ak.stock_zh_a_hist_min_em(
            symbol=stock_code, period="60",
            start_date=start_dt, end_date=end_dt, adjust="qfq",
        )
        if df is not None and not getattr(df, "empty", True):
            rename_map = {"时间": "date", "收盘": "close"}
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            logger.info(f"[九转] {stock_code} 60min K线获取成功，{len(df)}条")
            return df
    except Exception as e:
        logger.warning(f"[九转] {stock_code} 60min K线获取失败: {e}")
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
    lines.append("数据源: 日线来自数据库，周线/月线由日线聚合\n")

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
        格式化信号文本，包含日线+周线+月线+60min（如有）的九转计数
    """
    if not stock_code or len(stock_code) != 6:
        logger.warning(f"[九转] 无效股票代码: {stock_code}")
        return ""

    blocks = []

    # ---- 月线九转（最优先：从数据库日线聚合） ----
    df_monthly = _fetch_kline_from_db(stock_code, "monthly", 36)  # 月线看近3年
    if not df_monthly.empty:
        closes_m = df_monthly["close"].tolist()
        sig_sell = _calc_td_setup(closes_m, "sell")
        sig_buy = _calc_td_setup(closes_m, "buy")
        if sig_sell or sig_buy:
            text = "【月线九转（大周期，由日线聚合）】\n"
            for idx, cnt in sig_sell:
                dt = df_monthly.iloc[idx]["date"].strftime("%Y-%m")
                text += f"  顶部#{cnt} {dt} 收盘{df_monthly.iloc[idx]['close']:.2f}\n"
            for idx, cnt in sig_buy:
                dt = df_monthly.iloc[idx]["date"].strftime("%Y-%m")
                text += f"  底部#{cnt} {dt} 收盘{df_monthly.iloc[idx]['close']:.2f}\n"
            blocks.append(text.rstrip())

    # ---- 周线九转（中周期：从数据库日线聚合） ----
    df_weekly = _fetch_kline_from_db(stock_code, "weekly", 24)  # 周线看近6个月
    if not df_weekly.empty:
        closes_w = df_weekly["close"].tolist()
        sig_sell = _calc_td_setup(closes_w, "sell")
        sig_buy = _calc_td_setup(closes_w, "buy")
        if sig_sell or sig_buy:
            text = "【周线九转（中周期，由日线聚合）】\n"
            for idx, cnt in sig_sell:
                dt = df_weekly.iloc[idx]["date"].strftime("%Y-%m-%d")
                text += f"  顶部#{cnt} {dt} 收盘{df_weekly.iloc[idx]['close']:.2f}\n"
            for idx, cnt in sig_buy:
                dt = df_weekly.iloc[idx]["date"].strftime("%Y-%m-%d")
                text += f"  底部#{cnt} {dt} 收盘{df_weekly.iloc[idx]['close']:.2f}\n"
            blocks.append(text.rstrip())

    # ---- 日线九转（短周期：从数据库直接读取） ----
    df_daily = _fetch_kline_from_db(stock_code, "daily", months)
    if not df_daily.empty:
        closes = df_daily["close"].tolist()
        signals_sell = _calc_td_setup(closes, "sell")
        signals_buy = _calc_td_setup(closes, "buy")
        blocks.append(_describe_signal(stock_code, df_daily, signals_sell, signals_buy))

    # ---- 60min 九转（超短周期，辅助判断：AkShare 拉取） ----
    df_60 = _fetch_kline_60min(stock_code, 2)  # 60min 只看近2个月
    if not df_60.empty:
        closes_60 = df_60["close"].tolist()
        sig_sell_60 = _calc_td_setup(closes_60, "sell")
        sig_buy_60 = _calc_td_setup(closes_60, "buy")
        if sig_sell_60 or sig_buy_60:
            blocks.append("")
            blocks.append("【60分钟九转（超短周期，辅助判断，数据近2个月）】")
            for idx, cnt in sig_sell_60:
                dt = df_60.iloc[idx]["date"].strftime("%Y-%m-%d %H:%M")
                close = df_60.iloc[idx]["close"]
                blocks.append(f"  顶部#{cnt} {dt} 收盘{close:.2f}")
            for idx, cnt in sig_buy_60:
                dt = df_60.iloc[idx]["date"].strftime("%Y-%m-%d %H:%M")
                close = df_60.iloc[idx]["close"]
                blocks.append(f"  底部#{cnt} {dt} 收盘{close:.2f}")

    return "\n".join(blocks)
