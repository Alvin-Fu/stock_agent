# -*- coding: utf-8 -*-
"""
报告格式后处理：对 LLM 生成的最终报告做维度完整性检查、缺失补位、顺序重排。
在 ResponderAgent 生成 final_answer 之后、返回之前调用。

三种模式：
- etf：6 个维度 + 分析局限性说明，超过 60% 缺失时标记"数据不足"
- stock：9 个维度，缺失标注 ⚠️ 并重排
- industry：9 个维度，缺失标注 ⚠️ 并重排
"""

import re
from typing import List, Tuple

from utils.logger import logger

# ======================== ETF 预期维度（顺序固定） ========================

_ETF_SECTIONS = [
    ("ETF 基本信息", ["基本信息", "etf.*基本", "基金信息", "基金概况"]),
    ("行情与折溢价", ["行情.*折溢价", "折溢价", "行情.*价格", "市价.*净值", "iopv", "最新价"]),
    ("行情关键位置", ["行情关键位置", "关键位", "支撑压力", "关键位置", "多空分水岭"]),
    ("份额与资金流向", ["份额.*资金", "资金流向", "份额变动", "份额.*流向", "资金.*占比"]),
    ("行业配置", ["行业配置", "行业分布", "行业集中", "行业.*占比", "持仓行业"]),
    ("前 5 大重仓股穿透", ["重仓股", "持仓穿透", "前.*大重仓", "前.*大持仓", "重仓.*分析"]),
    ("持仓组合评估", ["持仓组合", "组合评估", "持仓质量", "持仓.*整体", "集中度风险", "组合风险"]),
    ("分析局限性说明", ["分析局限性", "局限", "免责", "风险提示"]),
]

# ======================== 个股预期维度 ========================

_STOCK_SECTIONS = [
    ("结论", ["📌", "结论", "核心结论", "投资结论"]),
    ("公司概况与业务拆解", ["公司概况", "业务拆解", "主营业务", "业务构成", "公司业务", "业务分析"]),
    ("财务分析", ["财务分析", "财务数据", "财务状况", "业绩分析", "财报分析"]),
    ("护城河", ["护城河", "竞争优势", "竞争壁垒", "核心优势"]),
    ("利润驱动与飞轮", ["利润驱动", "飞轮", "盈利驱动", "增长驱动"]),
    ("大盘与筹码", ["大盘", "筹码", "资金面", "股东结构", "主力资金"]),
    ("关键支撑压力位", ["关键支撑压力", "关键位", "支撑压力", "支撑.*压力", "多空分水岭", "行情关键位置"]),
    ("估值", ["估值", "估值分析", "估值分位", "pe.*pb", "sotp"]),
    ("操作参考与情景推演", ["操作参考", "情景推演", "交易计划", "操作建议", "买卖建议"]),
    ("分析局限性说明", ["分析局限性", "局限", "风险提示", "免责"]),
]

# ======================== 产业链预期维度 ========================

_INDUSTRY_SECTIONS = [
    ("结论", ["📌", "结论", "核心结论", "行业结论"]),
    ("产业链全景图", ["产业链全景", "全景图", "产业链概览", "产业链结构", "全产业链"]),
    ("关键环节", ["关键环节", "核心环节", "重要环节", "价值环节"]),
    ("候选公司", ["候选公司", "最值得投资", "⭐", "重点标的", "推荐标的", "投资标的"]),
    ("行业趋势", ["行业趋势", "发展趋势", "行业前景", "未来趋势", "行业方向"]),
    ("环节利润迁移判断", ["利润迁移", "环节利润", "利润判断", "利润变动"]),
    ("投资建议", ["投资建议", "投资策略", "配置建议", "行业配置"]),
    ("行业风险", ["行业风险", "风险因素", "风险提示", "潜在风险"]),
    ("分析局限性说明", ["分析局限性", "局限", "免责", "风险提示"]),
]

# ======================== 核心逻辑 ========================


def _find_section(heading: str, expected: List[Tuple[str, List[str]]]) -> int:
    """判断标题属于预期维度中的哪一个，返回索引，-1 表示未匹配"""
    hl = heading.lower().strip()
    for idx, (name, patterns) in enumerate(expected):
        for p in patterns:
            if re.search(p, hl):
                return idx
    return -1


def _split_text(text: str) -> List[Tuple[str, str]]:
    """将 markdown 文本按 ##/### 标题切分，返回 [(标题, 内容), ...]"""
    # 按 ## 或 ### 分割（保留标题行），但不匹配 ######
    pattern = r"^(#{2,3})\s+(.+)$"
    lines = text.split("\n")
    sections = []
    current_heading = ""
    current_content = []

    for line in lines:
        m = re.match(pattern, line.strip())
        if m:
            if current_heading or current_content:
                sections.append((current_heading, "\n".join(current_content).strip()))
            current_heading = m.group(2).strip()
            current_content = []
        else:
            current_content.append(line)

    if current_heading or current_content:
        sections.append((current_heading, "\n".join(current_content).strip()))

    # 如果没按标题切分（纯文本），当成一个整体
    if not sections:
        sections = [("", text.strip())]

    return sections


def _format_section(name: str, content: str) -> str:
    """格式化为统一标题 + 内容"""
    # 如果内容包含 📌 标记，保留标记
    if "📌" in content[:10]:
        return f"## 📌 {name}\n\n{content}"
    return f"## {name}\n\n{content}"


def _is_low_quality_text(text: str, min_chars: int = 30) -> bool:
    """判断一段文本是否有效（够长、不全是 placeholder）"""
    return len(text.strip()) > min_chars


MSG_NO_DATA = "（数据不足，未生成分析）"

# 一段内容最少要有多少字符才算有效
_MIN_CONTENT_CHARS = 15
MSG_MISSING_SECTION = "（该维度在当前分析中未覆盖）"

_MISSING_THRESHOLD = 0.6  # 超过 60% 维度缺失时标记数据不足


def format_etf_report(text: str) -> str:
    """ETF 报告后处理：查缺、重排、补占位"""
    if not text or len(text.strip()) < 20:
        return "## 数据不足\n\nETF 行情数据获取失败，无法生成有效分析。"

    sections = _split_text(text)
    assignments = {}  # idx → (title, content)

    for heading, content in sections:
        idx = _find_section(heading, _ETF_SECTIONS)
        if idx >= 0:
            if idx not in assignments or len(content) > len(assignments[idx][1]):
                assignments[idx] = (heading, content)

    # 查缺
    present_count = 0
    output = []
    for idx, (expected_name, _) in enumerate(_ETF_SECTIONS):
        if idx in assignments:
            _, content = assignments[idx]
            if _is_low_quality_text(content, _MIN_CONTENT_CHARS):
                present_count += 1
                output.append(_format_section(expected_name, content))
            else:
                output.append(f"## {expected_name}\n\n{MSG_NO_DATA}")
        else:
            output.append(f"## {expected_name}\n\n{MSG_NO_DATA}")

    # 数据足量标记
    coverage = present_count / len(_ETF_SECTIONS)
    if coverage < _MISSING_THRESHOLD:
        note = (
            f"\n\n> ⚠️ **数据不足提示**：仅获取到 {present_count}/{len(_ETF_SECTIONS)} "
            f"个维度的数据（覆盖率 {coverage:.0%}），分析结论参考价值有限。"
        )
        output.append(note)

    return "\n\n".join(output)


def format_stock_report(text: str) -> str:
    """个股报告后处理：查缺、重排、补占位"""
    if not text or len(text.strip()) < 20:
        return text

    sections = _split_text(text)
    assignments = {}

    for heading, content in sections:
        idx = _find_section(heading, _STOCK_SECTIONS)
        if idx >= 0:
            if idx not in assignments or len(content) > len(assignments[idx][1]):
                assignments[idx] = (heading, content)

    output = []
    for idx, (expected_name, _) in enumerate(_STOCK_SECTIONS):
        if idx in assignments:
            _, content = assignments[idx]
            output.append(_format_section(expected_name, content))
        else:
            output.append(f"## {expected_name}\n\n{MSG_MISSING_SECTION}")

    return "\n\n".join(output)


def format_industry_report(text: str) -> str:
    """产业链报告后处理：查缺、重排、补占位"""
    if not text or len(text.strip()) < 20:
        return text

    sections = _split_text(text)
    assignments = {}

    for heading, content in sections:
        idx = _find_section(heading, _INDUSTRY_SECTIONS)
        if idx >= 0:
            if idx not in assignments or len(content) > len(assignments[idx][1]):
                assignments[idx] = (heading, content)

    output = []
    for idx, (expected_name, _) in enumerate(_INDUSTRY_SECTIONS):
        if idx in assignments:
            _, content = assignments[idx]
            output.append(_format_section(expected_name, content))
        else:
            output.append(f"## {expected_name}\n\n{MSG_MISSING_SECTION}")

    return "\n\n".join(output)


# ======================== 统一入口 ========================


def format_report(text: str, mode: str) -> str:
    """
    报告格式后处理统一入口。

    Args:
        text: LLM 生成的原始报告
        mode: "etf" / "stock" / "industry"

    Returns:
        格式化后的报告（固定顺序 + 补缺）
    """
    if not text or not text.strip():
        return text

    try:
        if mode == "etf":
            return format_etf_report(text)
        elif mode == "stock":
            return format_stock_report(text)
        elif mode == "industry":
            return format_industry_report(text)
        return text
    except Exception as e:
        logger.warning(f"[格式化] 报告后处理失败（返回原文）: {e}")
        return text
