# -*- coding: utf-8 -*-
"""
量化打分规则工具
提供护城河评分、边际变化评分和PE拥挤区判定功能。
纯规则计算，不依赖外部数据源。
"""

from typing import Dict, Optional


def score_moat(
    market_share_rank: int = None,
    total_ranked: int = None,
    rd_ratio: float = None,
    gross_margin: float = None,
    top5_customer_ratio: float = None,
    has_certification: bool = False,
    patent_count: int = None,
) -> Dict:
    """
    护城河评分（1-10分），基于5个客观维度加权计算。

    Args:
        market_share_rank: 行业排名（如第1、第3等）
        total_ranked: 参与排名的企业总数
        rd_ratio: 研发费用率（%），如 18.5 表示 18.5%
        gross_margin: 毛利率（%），如 52.3 表示 52.3%
        top5_customer_ratio: 前5大客户集中度（%），如 28.0 表示 28%
        has_certification: 是否持有行业认证/许可证（军工四证等）
        patent_count: 专利数量（作为辅助参考）

    Returns:
        dict: {"score": int(1-10), "details": {...}, "evidence": [...]}
    """
    details = {}
    evidence = []
    total = 0.0

    # ---------- 1. 市占率评分（0-3分） ----------
    mkt_score = 0
    if market_share_rank is not None and total_ranked is not None and total_ranked > 0:
        if market_share_rank <= 3:
            mkt_score = 3
            evidence.append(f"行业排名第{market_share_rank}（前3），市占率得分3分")
        elif market_share_rank <= 10:
            mkt_score = 2
            evidence.append(f"行业排名第{market_share_rank}（前10），市占率得分2分")
        else:
            mkt_score = 1
            evidence.append(f"行业排名第{market_share_rank}（10名开外），市占率得分1分")
    details["market_share"] = mkt_score
    total += mkt_score

    # ---------- 2. 技术壁垒评分（0-3分） ----------
    tech_score = 0
    if rd_ratio is not None:
        if rd_ratio > 15:
            tech_score = 3
            evidence.append(f"研发费用率{rd_ratio:.1f}%（>15%），技术壁垒得分3分")
        elif rd_ratio >= 10:
            tech_score = 2
            evidence.append(f"研发费用率{rd_ratio:.1f}%（10-15%），技术壁垒得分2分")
        else:
            tech_score = 1
            evidence.append(f"研发费用率{rd_ratio:.1f}%（<10%），技术壁垒得分1分")
    if patent_count is not None:
        evidence.append(f"专利数量{patent_count}项")
    details["technology"] = tech_score
    total += tech_score

    # ---------- 3. 客户壁垒评分（0-4分） ----------
    customer_score = 0
    if top5_customer_ratio is not None:
        if top5_customer_ratio < 30:
            customer_score += 2
            evidence.append(
                f"前5客户集中度{top5_customer_ratio:.1f}%（<30%），客户分散度得分2分"
            )
        else:
            evidence.append(
                f"前5客户集中度{top5_customer_ratio:.1f}%（≥30%），集中度偏高"
            )
    if has_certification:
        customer_score += 2
        evidence.append("持有行业认证/许可证，客户壁垒得分2分")
    details["customer"] = customer_score
    total += customer_score

    # ---------- 4. 毛利率评分（0-2分） ----------
    gm_score = 0
    if gross_margin is not None:
        if gross_margin > 50:
            gm_score = 2
            evidence.append(f"毛利率{gross_margin:.1f}%（>50%），毛利率得分2分")
        elif gross_margin >= 30:
            gm_score = 1
            evidence.append(f"毛利率{gross_margin:.1f}%（30-50%），毛利率得分1分")
        else:
            evidence.append(f"毛利率{gross_margin:.1f}%（<30%），毛利率得分0分")
    details["gross_margin"] = gm_score
    total += gm_score

    # ---------- 5. 品牌/准入评分（0-1分） ----------
    # 许可证、军工四证等复用 has_certification 参数
    brand_score = 1 if has_certification else 0
    if brand_score:
        evidence.append("持有品牌/准入资质（许可证、军工四证等），得分1分")
    details["brand_access"] = brand_score
    total += brand_score

    # ---------- 总分映射到 1-10 ----------
    # 理论最高分 = 3 + 3 + 4 + 2 + 1 = 13
    if total <= 0:
        final_score = 1
    else:
        final_score = max(1, min(10, round(total / 13 * 10)))

    details["raw_total"] = total
    return {
        "score": final_score,
        "details": details,
        "evidence": evidence,
    }


def score_momentum(
    revenue_growth: float = None,
    profit_growth: float = None,
    gross_margin_trend: str = None,
    has_new_orders: bool = False,
    has_capacity_expansion: bool = False,
) -> Dict:
    """
    边际变化评分（1-10分），基于营收、利润增速及趋势判断。

    Args:
        revenue_growth: 营业收入同比增速（%），如 25.3 表示 25.3%
        profit_growth: 净利润同比增速（%），如 35.0 表示 35.0%
        gross_margin_trend: 毛利率趋势，"up" / "stable" / "down"
        has_new_orders: 是否有明确披露的新增订单
        has_capacity_expansion: 是否有产能扩张计划

    Returns:
        dict: {"score": int(1-10), "details": {...}, "evidence": [...]}
    """
    details = {}
    evidence = []
    total = 0.0

    # ---------- 1. 营收增速（0-3分） ----------
    rev_score = 0
    if revenue_growth is not None:
        if revenue_growth > 30:
            rev_score = 3
            evidence.append(f"营收增速{revenue_growth:.1f}%（>30%），得分3分")
        elif revenue_growth >= 15:
            rev_score = 2
            evidence.append(f"营收增速{revenue_growth:.1f}%（15-30%），得分2分")
        elif revenue_growth >= 0:
            rev_score = 1
            evidence.append(f"营收增速{revenue_growth:.1f}%（0-15%），得分1分")
        else:
            evidence.append(f"营收增速{revenue_growth:.1f}%（负增长），得分0分")
    details["revenue_growth"] = rev_score
    total += rev_score

    # ---------- 2. 净利润增速（0-3分） ----------
    profit_score = 0
    if profit_growth is not None:
        if profit_growth > 30:
            profit_score = 3
            evidence.append(f"净利润增速{profit_growth:.1f}%（>30%），得分3分")
        elif profit_growth >= 15:
            profit_score = 2
            evidence.append(f"净利润增速{profit_growth:.1f}%（15-30%），得分2分")
        elif profit_growth >= 0:
            profit_score = 1
            evidence.append(f"净利润增速{profit_growth:.1f}%（0-15%），得分1分")
        else:
            evidence.append(f"净利润增速{profit_growth:.1f}%（负增长），得分0分")
    details["profit_growth"] = profit_score
    total += profit_score

    # ---------- 3. 新订单/产能扩张（0-2分） ----------
    expansion_score = 0
    if has_new_orders:
        expansion_score += 1
        evidence.append("明确披露新增订单，得分1分")
    if has_capacity_expansion:
        expansion_score += 1
        evidence.append("有产能扩张计划，得分1分")
    details["expansion"] = expansion_score
    total += expansion_score

    # ---------- 4. 毛利率趋势（0-2分） ----------
    gm_trend_score = 0
    if gross_margin_trend == "up":
        gm_trend_score = 2
        evidence.append("毛利率趋势提升，得分2分")
    elif gross_margin_trend == "stable":
        gm_trend_score = 1
        evidence.append("毛利率趋势稳定，得分1分")
    elif gross_margin_trend == "down":
        evidence.append("毛利率趋势下降，得分0分")
    details["gross_margin_trend"] = gm_trend_score
    total += gm_trend_score

    # ---------- 总分映射到 1-10 ----------
    # 理论最高分 = 3 + 3 + 2 + 2 = 10，可直接映射
    final_score = max(1, min(10, int(total) if total >= 1 else 1))

    details["raw_total"] = total
    return {
        "score": final_score,
        "details": details,
        "evidence": evidence,
    }


def calc_congestion(
    pe_percentile: float,
    pe_current: float = None,
    industry_pe_median: float = None,
) -> Dict:
    """
    判定PE拥挤区（5级标签体系），并计算惩罚系数。

    绝对PE优先于分位判定，按以下顺序匹配（命中即停）：
      🔴极端泡沫   : PE > 200倍
      🔴极度拥挤   : PE > 100倍 且 PE分位 >= 70%
      🟠拥挤       : PE分位 >= 80% 或 PE > 100倍
      🟡中性       : 其他
      🟢机会区·低估: PE分位 <= 30% 且 PE <= 50倍

    Args:
        pe_percentile: PE历史分位（%），如 85.0 表示 85% 分位
        pe_current: 当前PE值
        industry_pe_median: 行业中位数PE（保留参数向后兼容，5级体系不再使用）

    Returns:
        dict: {"zone": str, "penalty": float, "label_str": str, "adjusted_formula": str}
            - zone: 拥挤等级（极端泡沫/极度拥挤/拥挤/中性/机会区）
            - penalty: 惩罚系数（0.0~0.8）
            - label_str: 带emoji的中文标签及说明
            - adjusted_formula: 调整公式字符串
    """
    penalty = 0.0
    zone = ""
    label_str = ""
    pe_str = f"{pe_current:.2f}" if pe_current is not None else "N/A"

    # 5级体系，绝对PE优先于分位判定，命中即停
    if pe_current is not None and pe_current > 200:
        # 🔴极端泡沫：PE > 200倍
        zone = "极端泡沫"
        penalty = 0.8
        label_str = (f"🔴极端泡沫：PE({pe_str})>200倍，估值严重脱离基本面，"
                     f"惩罚系数{penalty}")
    elif pe_current is not None and pe_current > 100 and pe_percentile >= 70:
        # 🔴极度拥挤：PE > 100倍 且 PE分位 >= 70%
        zone = "极度拥挤"
        penalty = 0.6
        label_str = (f"🔴极度拥挤：PE({pe_str})>100倍且历史分位{pe_percentile:.1f}%≥70%，"
                     f"惩罚系数{penalty}")
    elif pe_percentile >= 80 or (pe_current is not None and pe_current > 100):
        # 🟠拥挤：PE分位 >= 80% 或 PE > 100倍
        zone = "拥挤"
        penalty = 0.5
        label_str = (f"🟠拥挤：PE历史分位{pe_percentile:.1f}%≥80%或PE({pe_str})>100倍，"
                     f"惩罚系数{penalty}")
    elif pe_percentile <= 30 and (pe_current is None or pe_current <= 50):
        # 🟢机会区·低估：PE分位 <= 30% 且 PE <= 50倍
        zone = "机会区"
        penalty = 0.0
        label_str = (f"🟢机会区·低估：PE历史分位{pe_percentile:.1f}%≤30%且PE({pe_str})≤50倍，"
                     f"无惩罚")
    else:
        # 🟡中性：其他
        zone = "中性"
        penalty = 0.0
        label_str = (f"🟡中性：PE历史分位{pe_percentile:.1f}%，PE({pe_str})，"
                     f"无惩罚")

    adjusted_formula = (
        f"adjusted_score = raw_score × (1 - {penalty})"
    )

    return {
        "zone": zone,
        "penalty": penalty,
        "label_str": label_str,
        "adjusted_formula": adjusted_formula,
    }
