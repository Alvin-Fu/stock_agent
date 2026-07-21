# -*- coding: utf-8 -*-
"""
自动报告质量评分器
对 LLM 生成的 final_answer 进行基于规则的质量评估，不依赖 LLM 调用。

评分维度:
  - 缺失章节数（"未覆盖"出现次数）
  - 数据引用密度（亿元、%、万等数字出现次数）
  - 风险警告完整性（是否包含风险/免责章节）
  - 根据上述维度计算 0-100 分，并提供改进建议和重试辅助。
"""

import re
from typing import Dict, List

from utils.logger import logger

# ======================== 正则常量 ========================

# 数据引用匹配模式：数字 + 常见财务/统计单位
_DATA_CITATION_RE = re.compile(
    r"\d+(?:\.\d+)?\s*(?:亿元|亿|万元|万|%|百分点|倍|倍率|元|美元|港元|亿港元|亿日元|亿欧元|亿美元|港元|欧元|日元|千万|百万|千亿|万亿|bps|BP)"
)

# 风险/免责章节关键词
_RISK_KEYWORDS = ["风险", "分析局限", "局限性", "免责"]

# ======================== 容差阈值 ========================

_PASS_THRESHOLD = 70
_MAX_MISSING_SCORE_PENALTY = 100  # 每个"未覆盖"扣10分，最多扣到0
_PENALTY_PER_MISSING_SECTION = 10
_DATA_CITATION_BONUS_PER = 5      # 每多一条数据引用加5分
_DATA_CITATION_BONUS_MAX = 15     # 数据引用加分上限
_DATA_CITATION_BASELINE = 3       # 前3条不计入加分


def assess_report_quality(final_answer: str, formatter_mode: str = "stock") -> Dict:
    """
    对报告进行质量评分。

    Args:
        final_answer: 报告的完整文本内容。
        formatter_mode: 报告模式，可选 "stock" / "etf" / "industry"。
                       用于构建更精确的建议，不影响评分逻辑。

    Returns:
        dict: 包含以下字段的质量评估结果:
            - score: int        0-100 综合质量评分
            - missing_sections: int   "未覆盖"占位文本出现次数
            - data_citations: int     数据引用（数字+单位）出现次数
            - has_risk_warning: bool  是否包含风险/免责章节
            - pass: bool              是否达到及格线（score >= 70）
            - suggestions: List[str]  改进建议列表
    """
    if not final_answer or not final_answer.strip():
        logger.warning(f"[质量评分] 收到空报告（mode={formatter_mode}），评分为 0")
        return {
            "score": 0,
            "missing_sections": 0,
            "data_citations": 0,
            "has_risk_warning": False,
            "pass": False,
            "suggestions": ["报告内容为空，需要重新生成"],
        }

    # ---------- 1. 统计缺失章节 ----------
    missing_sections = final_answer.count("未覆盖")
    missing_sections = max(missing_sections, 0)

    # ---------- 2. 统计数据引用 ----------
    data_citations = len(_DATA_CITATION_RE.findall(final_answer))

    # ---------- 3. 检查风险警告 ----------
    has_risk_warning = any(kw in final_answer for kw in _RISK_KEYWORDS)

    # ---------- 4. 计算基础分 ----------
    score = 100 - (missing_sections * _PENALTY_PER_MISSING_SECTION)
    score = max(score, 0)

    # ---------- 5. 数据引用加分 ----------
    if data_citations > _DATA_CITATION_BASELINE:
        extra = (data_citations - _DATA_CITATION_BASELINE) * _DATA_CITATION_BONUS_PER
        score += min(extra, _DATA_CITATION_BONUS_MAX)

    # ---------- 6. 风险警告加分 ----------
    if has_risk_warning:
        score += 10

    # 封顶 100
    score = min(score, 100)

    # ---------- 7. 构建建议 ----------
    suggestions = _build_suggestions(missing_sections, data_citations, has_risk_warning, score)

    # ---------- 8. 判定 ----------
    passed = score >= _PASS_THRESHOLD

    result = {
        "score": score,
        "missing_sections": missing_sections,
        "data_citations": data_citations,
        "has_risk_warning": has_risk_warning,
        "pass": passed,
        "suggestions": suggestions,
    }

    logger.info(
        f"[质量评分] mode={formatter_mode} score={score} "
        f"missing={missing_sections} citations={data_citations} "
        f"risk={has_risk_warning} pass={passed}"
    )

    return result


def _build_suggestions(
    missing_sections: int,
    data_citations: int,
    has_risk_warning: bool,
    score: int,
) -> List[str]:
    """生成改进建议列表。"""
    suggestions: List[str] = []

    if missing_sections > 0:
        suggestions.append(
            f"报告存在 {missing_sections} 处维度未覆盖（「未覆盖」占位），"
            f"请补充对应章节的分析内容"
        )

    if data_citations < 3:
        suggestions.append(
            f"数据引用偏少（仅 {data_citations} 处），建议引用更多具体财务数字和行业数据"
        )

    if not has_risk_warning:
        suggestions.append("报告缺少风险提示或分析局限性说明章节，建议补充")

    if score < _PASS_THRESHOLD:
        suggestions.append(
            f"综合质量评分 {score} 分（及格线 {_PASS_THRESHOLD} 分），建议全面充实报告内容"
        )

    return suggestions


def should_retry(assessment: Dict) -> bool:
    """
    根据质量评估判断是否需要重新生成报告。

    触发条件：
      - 总分低于 70 分；或
      - 缺失章节数超过 3 个

    Args:
        assessment: assess_report_quality() 的返回结果。

    Returns:
        bool: True 表示需要重试，False 表示可接受。
    """
    score = assessment.get("score", 0)
    missing = assessment.get("missing_sections", 0)
    if score < _PASS_THRESHOLD:
        logger.info(f"[质量评分] 触发重试：评分 {score} < {_PASS_THRESHOLD}")
        return True
    if missing > 3:
        logger.info(f"[质量评分] 触发重试：缺失章节 {missing} > 3")
        return True
    return False


def build_retry_hint(assessment: Dict) -> str:
    """
    根据质量评估生成面向 LLM 的重试提示指令。

    根据评分短板生成有针对性的改进要求，优先定位最严重的缺失项。

    Args:
        assessment: assess_report_quality() 的返回结果。
        formatter_mode: 报告模式，可选 "stock" / "etf" / "industry"。

    Returns:
        str: 中文重试指令文本，可直接拼入 LLM prompt。
    """
    data_citations = assessment.get("data_citations", 0)
    has_risk_warning = assessment.get("has_risk_warning", False)
    score = assessment.get("score", 0)
    missing = assessment.get("missing_sections", 0)

    # 优先级 1：数据引用过少
    if data_citations < 3:
        return (
            f"上一版数据引用太少（仅{data_citations}处数字），"
            f"请更多引用材料中的具体财务数字"
        )

    # 优先级 2：缺少风险提示
    if not has_risk_warning:
        return "报告缺少风险提示章节，必须包含风险分析"

    # 优先级 3：缺失章节过多
    if missing > 0:
        return (
            f"报告存在 {missing} 处未覆盖章节，请确保每个章节都有实质性分析，"
            f"不要留「未覆盖」占位"
        )

    # 通用兜底
    return f"报告质量评分偏低（{score}分），请确保所有章节内容完整充实"
