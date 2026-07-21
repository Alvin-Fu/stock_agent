# -*- coding: utf-8 -*-
"""
SOTP（Sum-of-The-Parts）分部估值模型工具。

将公司各业务板块分开估值，加总得到整体价值，用于判断当前市值是否合理。

使用方式：
    from tools.sotp_valuation import build_sotp_valuation, sotp_implied_price

    segments = [
        {"name": "整车业务", "profit": 80.0, "pe_assumed": 15, "weight": 0.6},
        {"name": "电池业务", "profit": 30.0, "pe_assumed": 25, "weight": 0.3},
        {"name": "其他业务", "profit": 10.0, "pe_assumed": 10, "weight": 0.1},
    ]
    result = build_sotp_valuation(segments, total_mv=1500.0)
"""

from utils.logger import logger


def sotp_implied_price(total_sotp_value: float, total_shares: float) -> float:
    """
    计算 SOTP 隐含每股价格。

    Parameters
    ----------
    total_sotp_value : float
        SOTP 合计估值（亿元）。
    total_shares : float
        总股本（亿股）。

    Returns
    -------
    float
        隐含每股价格（元）。若 total_shares <= 0 则返回 0.0。
    """
    if total_shares <= 0:
        logger.warning("[SOTP] 总股本 <= 0，无法计算隐含每股价格")
        return 0.0
    return round(total_sotp_value / total_shares, 2)


def build_sotp_valuation(segments: list, total_mv: float) -> str:
    """
    构建 SOTP 分部估值表格。

    对每个业务板块计算 估值贡献 = profit * pe_assumed，
    加总得到 SOTP 合计估值，并与当前总市值对比计算折溢价率。

    Parameters
    ----------
    segments : list of dict
        每个 dict 包含：
        - name (str)      : 业务名称
        - profit (float)  : 该业务净利润（亿元）
        - pe_assumed (int): 给予该业务的假设 PE 倍数
        - weight (float)  : 该业务利润占比（0~1）
    total_mv : float
        公司当前总市值（亿元）。

    Returns
    -------
    str
        格式化的 markdown 分部估值表格。失败时返回空字符串。
    """
    try:
        if not segments or total_mv <= 0:
            logger.warning("[SOTP] segments 为空或 total_mv 不合法")
            return ""

        total_sotp = 0.0
        rows = []

        for i, seg in enumerate(segments):
            name = seg.get("name", f"业务{i+1}")
            profit = float(seg.get("profit", 0))
            pe = int(seg.get("pe_assumed", 10))
            weight = float(seg.get("weight", 0))

            valuation = profit * pe
            total_sotp += valuation

            rows.append({
                "name": name,
                "profit": profit,
                "weight": weight,
                "pe": pe,
                "valuation": valuation,
            })

        # 计算隐含每股价值（假设每股贡献 = 各板块估值占比 * 总市值 … 这里用估值贡献直接算）
        # 每股价值列：对每个板块，每股价值 = 估值贡献 / total_shares_equivalent
        # 但题目要求输出"每股价值"，这里简单用板块估值贡献除以总股本（由 total_mv 反推的逻辑不通用）
        # 更合理的做法：显示板块估值贡献占总 SOTP 比例隐含的每股价值
        # 由于 total_shares 未知，我们用"每股价值"表示该板块对每股的内在价值贡献百分比很难量化。
        # 实际业务中每股价值 = 板块估值贡献 / 总股本，但这里没有总股本参数。
        # 我们采用"板块每股价值（元）"= 板块估值贡献 / (total_mv / 隐含每股价格)... 这陷入了循环。
        #
        # 更务实的做法：假设 total_mv 对应每股价格已知（但未传入），因此
        # 在缺少总股本的情况下，"每股价值"列可以展示为板块估值贡献占 SOTP 总值的比例 * 假设的每股价格。
        # 但为避免过度假设，此处"每股价值"列填充"——"，或按板块估值贡献占比 * (total_mv / total_shares)
        # 由于没有 total_shares，无法计算真实每股价值。
        #
        # 参考题目要求："每股价值" 列保留但简单处理为 占 SOTP 估值比例
        # 这里实现为：该板块估值贡献 / total_sotp（仅显示比例），标记为"占比"而非"每股价值"
        # 但题目明确要求列名为"每股价值"，我们用占比来填充比较合理
        # 另一种做法：如果用户想计算每股价值，可以使用 sotp_implied_price 辅助函数。
        #
        # 我们采用：每股价值 = 板块估值贡献 / (total_sotp / implied_price)
        # 但 implied_price 也需要 total_shares，这里无法自动计算。
        # 因此直接显示 "——" 或 留空。
        # 但题目说明要求包含"每股价值"列，我们用占比来填充并标注清楚。

        lines = [
            "| 业务板块 | 净利润(亿) | 占比 | 假设PE | 估值贡献(亿) | 每股价值占比 |",
            "|---------|----------|-----|-------|------------|------------|",
        ]

        for r in rows:
            pct = f"{r['weight'] * 100:.1f}%"
            val_pct = f"{r['valuation'] / total_sotp * 100:.1f}%" if total_sotp > 0 else "0.0%"
            lines.append(
                f"| {r['name']} "
                f"| {r['profit']:.2f} "
                f"| {pct} "
                f"| {r['pe']} "
                f"| {r['valuation']:.2f} "
                f"| {val_pct} |"
            )

        # SOTP 合计
        premium = (total_mv - total_sotp) / total_sotp * 100 if total_sotp > 0 else 0
        premium_str = f"{premium:+.2f}%" if abs(premium) > 0 else "0.00%"
        lines.append("")
        lines.append(f"**SOTP 合计估值**: {total_sotp:.2f} 亿元")
        lines.append(f"**当前总市值**: {total_mv:.2f} 亿元")
        lines.append(f"**折溢价率**: {premium_str} （正值为溢价，负值为折价）")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"[SOTP] 构建分部估值失败: {e}")
        return ""
