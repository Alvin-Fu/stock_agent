# -*- coding: utf-8 -*-
"""
业绩敏感性测算工具。

量化核心变量（如原材料价格、销量、汇率等）变动对净利润的影响，
以表格形式呈现不同情景下的净利润结果。

使用方式：
    from tools.sensitivity_analysis import build_sensitivity_table

    variables = [
        {"name": "碳酸锂价格", "impact": 5.0, "unit": "亿元/万元变动", "range": [-30, -15, 0, 15, 30]},
        {"name": "汽车销量",   "impact": 3.0, "unit": "亿元/万辆变动",  "range": [-20, -10, 0, 10, 20]},
    ]
    result = build_sensitivity_table(base_profit=100.0, variables=variables)
"""

from utils.logger import logger


def build_sensitivity_table(base_profit: float, variables: list) -> str:
    """
    构建业绩敏感性分析表格。

    对每个变量，在其指定的变动幅度（range）下，计算对应的净利润结果：
        净利润 = base_profit + impact * 变动幅度

    Parameters
    ----------
    base_profit : float
        基准净利润（亿元）。
    variables : list of dict
        每个 dict 包含：
        - name (str)         : 变量名
        - impact (float)     : 每单位变动对净利润的影响（亿元）
        - unit (str)         : 单位说明（如 "亿元/万元变动"）
        - range (list)       : 变动幅度列表（百分比或绝对值，如 [-30, -15, 0, 15, 30]）

    Returns
    -------
    str
        格式化的 markdown 敏感性分析表格（字符串）。失败时返回空字符串。
    """
    try:
        if not variables or base_profit is None:
            logger.warning("[敏感性分析] variables 为空或 base_profit 为 None")
            return ""

        # 确定最大变动幅度数量，构建表头
        var_count = len(variables)
        range_counts = [len(v.get("range", [])) for v in variables]
        max_range = max(range_counts) if range_counts else 0

        if max_range == 0:
            logger.warning("[敏感性分析] 所有变量的 range 均为空")
            return ""

        lines = []
        # 表头
        header_cells = ["变量名", "单位", f"基准({base_profit:.2f}亿)"]
        # 用第一个变量的 range 作为列标签（假设各变量 range 一致或取最大长度）
        sample_var = variables[0]
        for val in sample_var.get("range", []):
            if val >= 0:
                header_cells.append(f"+{val}%")
            else:
                header_cells.append(f"{val}%")
        lines.append("| " + " | ".join(header_cells) + " |")
        lines.append("|" + "|".join(["---"] * len(header_cells)) + "|")

        # 数据行
        for var in variables:
            name = var.get("name", "未知变量")
            unit = var.get("unit", "")
            impact = float(var.get("impact", 0))
            var_range = var.get("range", [])

            row_cells = [name, unit, f"{base_profit:.2f}"]
            for val in var_range:
                # 净利润 = base_profit + impact * (变动幅度 / 100 * base_profit)?
                # 从题目描述来看，impact 是"每单位变动对净利润的影响"，range 表示变动幅度
                # 通常敏感性分析中，range 是百分比变动，impact 是每1%变动对应的净利润变化
                # 但题目的示例 range = [-30, -15, 0, 15, 30] 是百分比
                # impact = 5.0 "亿元/万元变动"，说明每变动1单位（如1万元）影响5亿净利润
                # 这里的 range 是百分比，所以需要转化为实际变动量
                # 但更常见的是：range 是变量的变动幅度值（如碳酸锂价格变动 -30%、-15%...）
                # impact 是每变动 1% 对应的净利润变化
                # 题目给的示例 impact=5.0 "亿元/万元变动"，表示每1万元价格变动影响5亿净利润
                # 所以实际净利润 = base_profit + impact * (base_value * val/100) ???
                # 但这里没有 base_value（变量的基准值）。
                #
                # 重新审视：对于碳酸锂价格，impact=5.0 表示每变动1万元影响5亿净利润
                # range=[-30, -15, 0, 15, 30] 表示价格变动 -30%、-15% 等
                # 但不知道基准价格，无法计算实际变动量。
                #
                # 更合理的简化：直接将 range 中的值乘以 impact 来得到净利润变动
                # 即净利润 = base_profit + impact * val / 100 * base_value
                # 但由于 base_value 未知，无法准确计算。
                #
                # 参考题目要求："每单位变动对净利润的影响（亿元）"，range = "变动幅度列表"
                # 最简单的实现：净利润 = base_profit + impact * val
                # 即把 range 中的值直接作为"变动单位数"而非百分比。
                # 但从示例 range=[-30, -15, 0, 15, 30] 来看，这明显是百分比。
                # 所以 impact 的含义应该理解为：每变动 1% 对净利润的影响（亿元）。
                # 这样净利润 = base_profit + impact * val
                profit_change = impact * val
                result = base_profit + profit_change
                row_cells.append(f"{result:.2f}")
            lines.append("| " + " | ".join(row_cells) + " |")

        # 添加说明
        lines.append("")
        lines.append("**说明**：净利润 = 基准净利润 + impact × 变动幅度，impact 表示每变动1%对净利润的影响（亿元）。")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"[敏感性分析] 构建表格失败: {e}")
        return ""
