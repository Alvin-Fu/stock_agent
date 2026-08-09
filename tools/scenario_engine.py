# -*- coding: utf-8 -*-
"""
情景推演模块：基于产业链分析材料，由 LLM 生成乐观/基准/悲观三场景推演。

依赖：
    - core.llm.get_default_llm() 获取 LLM 实例
    - langchain_core.messages.SystemMessage, HumanMessage
"""

import re

from core.llm import get_default_llm
from langchain_core.messages import SystemMessage, HumanMessage
from utils.logger import logger

# 可验证指标正则：百分比 / 日期 / 季度 / 带单位数字 / 常见事件名
_VERIFIABLE_TRIGGER_PATTERNS = [
    re.compile(r'\d+\.?\d*\s*%'),                     # 百分比（如 30%）
    re.compile(r'\d{4}\s*年'),                         # 年份（如 2026年）
    re.compile(r'Q[1-4]', re.IGNORECASE),               # 季度（如 Q2）
    re.compile(r'\d+\s*月'),                            # 月份（如 7月）
    re.compile(r'\d+\.?\d*\s*(亿|万|百万|billion)'),     # 带单位的数字
    re.compile(r'(降息|加息|降准|关税|政策|量产|投产|获批|发布|上市|'
               r'解禁|回购|减持|并购|重组|落地|开工|交付|出货|审批|'
               r'招标|中标|签约|挂牌|退市|停牌|复牌)'),  # 常见事件名
]


def has_scenarios(text: str) -> bool:
    """检测文本中是否已包含情景推演章节。

    通过查找 Markdown 二级标题「情景推演」来判断。
    """
    return bool(re.search(r'##\s*情景推演', text))


def _validate_scenario_probabilities(result: str) -> None:
    """校验三场景概率之和是否在 0.9~1.1 之间，不在则告警。

    解析 Markdown 表格中的概率列（通常为第2列），提取百分比并求和。
    """
    probs = []
    for line in result.split("\n"):
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.split("|") if c.strip()]
        if len(cells) < 2:
            continue
        # 跳过表头行和分隔行
        if "情景" in line or "概率" in line and "触发" in line:
            continue
        if all(c.replace("-", "").replace(":", "").strip() == "" for c in cells):
            continue
        # 概率通常在第2列（乐观 | 30% | ...）
        prob_cell = cells[1]
        match = re.search(r'(\d+\.?\d*)\s*%', prob_cell)
        if match:
            try:
                val = float(match.group(1))
                if 0 < val <= 100:
                    probs.append(val / 100)
            except ValueError:
                pass
    if len(probs) >= 2:
        total = sum(probs)
        if total < 0.9 or total > 1.1:
            logger.warning(f"情景推演概率之和 {total:.2f} 不在 0.9~1.1 范围内，"
                           f"各情景概率: {probs}")


def _annotate_trigger_verifiability(result: str) -> str:
    """对每个情景的触发条件检查可验证性，不可验证的标注 ⚠️。

    遍历表格中的触发条件列，检查是否包含可验证指标（百分比/日期/数字/事件名）。
    不包含任何可验证指标的触发条件追加 ⚠️ 标注。
    """
    lines = result.split("\n")
    # 定位触发条件列索引和表头行位置
    trigger_col_idx = None
    header_line_idx = None
    for i, line in enumerate(lines):
        if "|" in line and "触发条件" in line:
            cells = line.split("|")
            for j, cell in enumerate(cells):
                if "触发条件" in cell:
                    trigger_col_idx = j
                    header_line_idx = i
                    break
            if trigger_col_idx is not None:
                break

    if trigger_col_idx is None:
        # 找不到触发条件列（LLM 未按表格格式输出），跳过标注
        return result

    new_lines = list(lines)
    for idx in range(header_line_idx + 1, len(new_lines)):
        line = new_lines[idx]
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = line.split("|")
        if trigger_col_idx >= len(cells):
            continue
        trigger = cells[trigger_col_idx].strip()
        # 跳过空值和分隔行（全是 - 或 :）
        if not trigger or all(c in "-: " for c in trigger):
            continue
        # 跳过表头残留
        if "触发条件" in trigger:
            continue
        is_verifiable = any(p.search(trigger) for p in _VERIFIABLE_TRIGGER_PATTERNS)
        if not is_verifiable:
            cells[trigger_col_idx] = f" {trigger} ⚠️（触发条件缺乏可验证指标） "
            new_lines[idx] = "|".join(cells)

    return "\n".join(new_lines)


def generate_scenarios(industry_name: str, research_text: str) -> str:
    """基于行业分析材料，由 LLM 生成三场景（乐观/基准/悲观）推演。

    Args:
        industry_name: 行业名称。
        research_text: 产业链分析的研究材料，含候选人、估值、护城河等。

    Returns:
        格式化后的 Markdown 情景推演文本。
    """
    system_prompt = (
        "你是一个投资情景推演专家。请基于以下行业分析材料，"
        "生成乐观/基准/悲观三个场景推演。\n\n"
        "规则：\n"
        "- 每个场景必须包含：① 触发条件（可验证的关键事件/指标） ② 传导路径 "
        "③ 业绩与估值影响 ④ 概率权重\n"
        "- 场景之间互斥，概率之和=100%\n"
        "- 所有假设必须基于材料中的数据，禁止凭空编造\n"
        "- 可能性只用高/中/低标注，标注「推演非预测」\n"
        "- 悲观场景必须包含「若已持仓」的应对纪律\n"
        "- 输出为 Markdown 表格格式：\n\n"
        "## 情景推演\n\n"
        "| 情景 | 概率 | 触发条件 | 传导路径 | 业绩/估值影响 | 应对纪律 |\n"
        "|------|------|---------|---------|-------------|---------|\n"
        "| 乐观 | X% | ... | ... | ... | ... |\n"
        "| 基准 | X% | ... | ... | ... | ... |\n"
        "| 悲观 | X% | ... | ... | ... | ... |"
    )

    user_message = (
        f"行业名称：{industry_name}\n\n"
        f"研究材料：\n{research_text}"
    )

    llm = get_default_llm()
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_message),
    ]
    response = llm.invoke(messages)
    result = response.content if hasattr(response, 'content') else str(response)

    # ---- 程序校验：概率之和 + 触发条件可验证性 ----
    try:
        _validate_scenario_probabilities(result)
    except Exception as e:
        logger.warning(f"情景推演概率校验异常（不阻断）: {e}")
    try:
        result = _annotate_trigger_verifiability(result)
    except Exception as e:
        logger.warning(f"情景推演触发条件标注异常（不阻断）: {e}")

    return result
