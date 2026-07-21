# -*- coding: utf-8 -*-
"""
信号历史胜率权重调整模块

基于信号历史统计，生成权重调整建议文本，
供技术分析 prompt 动态调整各信号的打分权重。
"""

from utils.logger import logger


def build_weight_adjustment_text(stats: dict, min_samples: int = 10) -> str:
    """
    分析信号历史胜率统计，生成权重调整建议。

    对每个信号：
    - 20日后胜率 < 40% → 参考价值有限，建议降低权重
    - 20日后胜率 > 65% → 可作为正向参考信号

    Args:
        stats: calc_signal_history_stats 返回的统计字典
        min_samples: 最小样本数，低于此阈值不生成建议

    Returns:
        格式化的建议文本块；若无统计数据或无可标记信号则返回空字符串
    """
    if not stats:
        return ""

    adjustments = []
    for name, horizons in stats.items():
        if 20 not in horizons:
            continue
        h20 = horizons[20]
        if h20["n"] < min_samples:
            continue
        wr = h20["win_rate"]
        n = h20["n"]
        if wr < 40:
            adjustments.append(
                f"⚠ {name} 20日后胜率仅{wr}%（共{n}次），参考价值有限，打分权重酌情降低"
            )
        elif wr > 65:
            adjustments.append(
                f"✅ {name} 20日后胜率{wr}%（共{n}次），可作为正向参考信号"
            )

    if not adjustments:
        return ""

    logger.debug(f"[信号权重] 生成 {len(adjustments)} 条调整建议")
    return "【信号历史胜率调整建议（程序自动生成）】\n" + "\n".join(adjustments)


def get_weight_adjustment_for_code(stock_code: str) -> str:
    """
    高级接口：获取股票日线数据 → 计算信号历史胜率 → 返回权重调整建议文本。

    Args:
        stock_code: 股票代码，如 "600519"

    Returns:
        权重调整建议文本；任何异常情况下返回空字符串
    """
    try:
        from tools.stock_tools import stock_tool_instance, _ensure_indicators
        from tools.stock.base import calc_signal_history_stats

        logger.debug(f"[信号权重] 开始计算 {stock_code}")

        df = stock_tool_instance.fetch_and_save_stock_daily_data(stock_code=stock_code)
        if df is None or df.empty:
            logger.warning(f"[信号权重] {stock_code} 未获取到数据")
            return ""

        df = _ensure_indicators(df, "daily")
        stats = calc_signal_history_stats(df)
        result = build_weight_adjustment_text(stats)

        if result:
            logger.debug(f"[信号权重] {stock_code} 权重调整建议生成成功")
        else:
            logger.debug(f"[信号权重] {stock_code} 无有效信号建议")
        return result

    except Exception as e:
        logger.debug(f"[信号权重] {stock_code} 计算失败: {e}")
        return ""
