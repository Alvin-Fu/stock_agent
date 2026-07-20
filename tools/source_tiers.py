# -*- coding: utf-8 -*-
"""
信源分级体系（T1-T4）：
决定各信息源在分析中的可信权重，LLM 在冲突时以高等级为准。

等级定义：
  T1 — 权威信源：交易所公告、财报原始数据、监管信息、**经认证的官方社交媒体账号**。
        不可绕过，分析必须引用。
  T2 — 结构化信源：财经媒体 API、行业专用数据源。有明确出处和结构化字段。
  T3 — 未验证社交：非官方的社交媒体内容、股吧、论坛。仅供参考。
  T4 — 网络搜索：全网搜索兜底。仅供参考，不能作为分析的主要依据。

用法：
    from tools.source_tiers import TIER, tier_tag
    print(tier_tag(TIER.T1))  # → "【T1·权威】"
"""

from enum import IntEnum


class TIER(IntEnum):
    T1 = 1  # 权威信源（含公告/财报/认证官方社交）
    T2 = 2  # 结构化信源
    T3 = 3  # 未验证社交
    T4 = 4  # 网络搜索


_TIER_LABELS = {
    TIER.T1: "T1·权威",
    TIER.T2: "T2·结构化",
    TIER.T3: "T3·未验证",
    TIER.T4: "T4·网络搜索",
}

_TIER_COLORS = {
    TIER.T1: "🟢",
    TIER.T2: "🔵",
    TIER.T3: "🟡",
    TIER.T4: "⚪",
}


def tier_tag(tier: TIER) -> str:
    """返回带颜色的等级标签，如「🟢T1·权威」"""
    color = _TIER_COLORS.get(tier, "⚪")
    label = _TIER_LABELS.get(tier, "未知")
    return f"【{color}{label}】"


def tier_priority_instruction() -> str:
    """返回 Prompt 中信源优先级规则段"""
    return """【信源优先级规则】
信息冲突时，按以下等级决定采信顺序：
🟢 T1 权威（公告/财报/认证官方社交）> 🔵 T2 结构化（财经媒体）> 🟡 T3 未验证社交 > ⚪ T4 网络搜索
- 高等级信源与低等级信源数据不一致时，以高等级为准并标注差异
- 同一等级内信源冲突时，引用更具体的数据（有数字>无数字，有出处>无出处）
- 所有引用的数据点必须标注来源等级"""
