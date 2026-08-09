# -*- coding: utf-8 -*-
"""
业绩敏感性测算工具。

量化核心变量（如原材料价格、销量、汇率等）变动对净利润的影响，
以表格形式呈现不同情景下的净利润结果。

使用方式：
    from tools.sensitivity_analysis import build_sensitivity_table

    variables = [
        {"name": "碳酸锂价格", "elasticity": 0.3, "unit": "亿元/万元变动", "range": [-30, -15, 0, 15, 30]},
        {"name": "汽车销量",   "elasticity": 0.2, "unit": "亿元/万辆变动",  "range": [-20, -10, 0, 10, 20]},
    ]
    result = build_sensitivity_table(base_profit=100.0, variables=variables)
"""

from utils.logger import logger


def build_sensitivity_table(base_profit: float, variables: list) -> str:
    """
    构建业绩敏感性分析表格。

    对每个变量，在其指定的变动幅度（range）下，计算对应的净利润结果：
        impact = base_profit * elasticity / 100      （每1%变动对净利润的影响，亿元）
        profit_change = impact * val                  （val 为变动幅度，如 -20 代表 -20%）
        净利润 = base_profit + profit_change

    Parameters
    ----------
    base_profit : float
        基准净利润（亿元）。
    variables : list of dict
        每个 dict 包含：
        - name (str)         : 变量名
        - elasticity (float) : 弹性系数（默认0.3），变量每变动1%引起净利润变动的百分比；
                               impact = base_profit * elasticity / 100
        - impact (float)     : 可选，显式指定每1%变动对净利润的影响（亿元），
                               提供时优先于 elasticity 使用（向后兼容）
        - unit (str)         : 单位说明（如 "亿元/万元变动"）
        - range (list)       : 变动幅度列表（百分比，如 [-30, -15, 0, 15, 30]）

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
            var_range = var.get("range", [])

            # impact = 每1%变动对净利润的影响（亿元）。
            # 计算公式：impact = base_profit * elasticity / 100
            #   - elasticity（弹性系数，默认0.3）：变量每变动1%引起净利润变动的百分比
            #   - 例如 base_profit=100亿、elasticity=0.3，则每1%变动影响 100*0.3/100=0.3亿
            # 若变量显式提供 impact（亿元/1%变动），则直接使用，否则按弹性系数推算（向后兼容）
            if var.get("impact") is not None:
                impact = float(var["impact"])
            else:
                elasticity = float(var.get("elasticity", 0.3))
                impact = base_profit * elasticity / 100

            row_cells = [name, unit, f"{base_profit:.2f}"]
            for val in var_range:
                # val 为变量变动幅度（百分比，如 -20 代表 -20%）
                # profit_change = impact * val，即 每1%影响 × 变动百分比数
                profit_change = impact * val
                result = base_profit + profit_change
                row_cells.append(f"{result:.2f}")
            lines.append("| " + " | ".join(row_cells) + " |")

        # 添加说明
        lines.append("")
        lines.append("**说明**：净利润 = 基准净利润 + impact × 变动幅度。"
                     "impact 为每1%变动对净利润的影响（亿元），"
                     "由 impact = 基准净利润 × elasticity ÷ 100 计算（elasticity 默认0.3）；"
                     "变动幅度为百分比（如 -20 代表 -20%）。")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"[敏感性分析] 构建表格失败: {e}")
        return ""
