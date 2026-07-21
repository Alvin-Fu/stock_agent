# -*- coding: utf-8 -*-
"""
用户反馈聚合与 Prompt 自动优化模块

纯规则驱动，不依赖 LLM 调用。
从 SQLite 中读取用户纠错记录，按类别聚合关键词，
自动生成 prompt 补丁文本，供 responder_agent.py 注入到分析 prompt 中。
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import select, desc, func

from storage.sqlite.stock_storage import get_db, UserFeedback
from utils.logger import logger


def get_feedback_summary(days: int = 30) -> List[Dict[str, Any]]:
    """
    获取最近 N 天内的所有用户反馈，按目标（code/name）分组统计。

    Returns:
        按反馈数量降序排列的列表，每项包含：
        - target: 目标名称
        - code: 股票代码（可能为空）
        - count: 反馈次数
        - recent_samples: 最近 3 条反馈原文
    """
    cutoff = datetime.now() - timedelta(days=days)
    db = get_db()

    with db.get_session() as session:
        # 1. 查出最近 days 天的所有反馈
        results = session.execute(
            select(UserFeedback)
            .where(UserFeedback.created_at >= cutoff)
            .order_by(desc(UserFeedback.created_at))
        ).scalars().all()

        if not results:
            logger.info("get_feedback_summary: no feedback found in last %d days", days)
            return []

        # 2. 按 (code, target_name) 分组聚合
        groups: Dict[tuple, Dict[str, Any]] = {}
        for r in results:
            key = (r.code, r.target_name)
            if key not in groups:
                groups[key] = {
                    "target": r.target_name,
                    "code": r.code,
                    "count": 0,
                    "samples": [],
                }
            groups[key]["count"] += 1
            if len(groups[key]["samples"]) < 3:
                groups[key]["samples"].append(r.content)

        # 3. 排序取 top 10
        sorted_groups = sorted(groups.values(), key=lambda x: x["count"], reverse=True)[:10]

        summary = [
            {
                "target": g["target"],
                "code": g["code"],
                "count": g["count"],
                "recent_samples": g["samples"],
            }
            for g in sorted_groups
        ]

        logger.info(
            "get_feedback_summary: %d targets with feedback in last %d days",
            len(summary), days,
        )
        return summary


def categorize_feedback(feedback_list: List[Dict]) -> Dict[str, List[str]]:
    """
    通过关键词匹配将反馈文本分类。

    分类规则：
        - data_accuracy: 口径 / 数据来源 / 数字错误
        - missing_section: 缺 / 漏 / 未覆盖 / 没有
        - contradiction: 矛盾 / 不一致 / 不对
        - judgment: 方向 / 判断 / 结论
        - other: 其他

    Args:
        feedback_list: get_feedback_for_target() 返回的 dict 列表

    Returns:
        {category: [feedback_content, ...]}
    """
    # 关键词规则，优先级从高到低（第一个命中的类别为准）
    RULES = [
        ("data_accuracy", ["口径", "数据来源", "数字错误"]),
        ("missing_section", ["缺", "漏", "未覆盖", "没有"]),
        ("contradiction", ["矛盾", "不一致", "不对"]),
        ("judgment", ["方向", "判断", "结论"]),
    ]

    categorized: Dict[str, List[str]] = {
        "data_accuracy": [],
        "missing_section": [],
        "contradiction": [],
        "judgment": [],
        "other": [],
    }

    for fb in feedback_list:
        content = fb.get("content", "")
        if not content:
            continue
        matched = False
        for category, keywords in RULES:
            if any(kw in content for kw in keywords):
                categorized[category].append(content)
                matched = True
                break
        if not matched:
            categorized["other"].append(content)

    return categorized


def build_prompt_patch(categorized: Dict[str, List[str]]) -> str:
    """
    根据分类后的反馈生成 prompt 改进补丁文本。

    仅对样本数 >= 2 的类别生成具体补丁。
    若所有类别样本数均为 0，返回空字符串。

    Args:
        categorized: categorize_feedback() 的返回结果

    Returns:
        拼接后的 prompt 补丁文本
    """
    PATCH_TEMPLATES = {
        "data_accuracy": (
            "【自动优化-数据精度】近期用户多次指出数据口径/数字错误，"
            "请特别注意核对数据来源和计算口径，确保所有数字准确无误。"
        ),
        "missing_section": (
            "【自动优化-章节完整度】用户反馈频繁出现章节缺失，"
            "请确保基本面分析、技术面分析、行业对比、风险提示等所有必需章节都已完整生成。"
        ),
        "contradiction": (
            "【自动优化-一致性】请特别注意跨材料数据的交叉验证，"
            "确保全文逻辑一致，避免出现自相矛盾的分析结论。"
        ),
        "judgment": (
            "【自动优化-判断依据】请确保每个方向/判断都有具体数据支撑，"
            "避免空泛的主观断言，必要时补充量化数据或行业基准。"
        ),
    }

    patches = []
    for category, samples in categorized.items():
        if category == "other":
            continue
        if len(samples) >= 2:
            patch = PATCH_TEMPLATES.get(category)
            if patch:
                patches.append(patch)

    if not patches:
        total = sum(len(v) for v in categorized.values())
        if total == 0:
            return ""
        # 只有 other 分类有内容，或每个类别样本数不足 2 条
        return ""

    return "\n".join(patches)


def get_prompt_patch_for_target(
    code: Optional[str] = None,
    name: Optional[str] = None,
    days: int = 30,
) -> str:
    """
    高阶函数：获取指定目标的反馈 → 分类 → 生成 prompt 补丁。

    这是供 responder_agent.py 调用的入口函数。

    Args:
        code: 股票代码（可选）
        name: 目标名称（可选）
        days: 回溯天数，默认 30 天

    Returns:
        prompt 补丁文本；若无相关反馈则返回空字符串
    """
    if not code and not name:
        logger.warning("get_prompt_patch_for_target: neither code nor name provided")
        return ""

    db = get_db()
    feedback_list = db.get_feedback_for_target(code=code, name=name, limit=20)

    if not feedback_list:
        logger.info(
            "get_prompt_patch_for_target: no feedback for code=%s name=%s", code, name
        )
        return ""

    categorized = categorize_feedback(feedback_list)
    patch = build_prompt_patch(categorized)

    if patch:
        logger.info(
            "get_prompt_patch_for_target: generated patch for code=%s name=%s (%d chars)",
            code, name, len(patch),
        )
    else:
        logger.info(
            "get_prompt_patch_for_target: no significant pattern for code=%s name=%s",
            code, name,
        )

    return patch
