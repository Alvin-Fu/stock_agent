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


def has_scenarios(text: str) -> bool:
    """检测文本中是否已包含情景推演章节。

    通过查找 Markdown 二级标题「情景推演」来判断。
    """
    return bool(re.search(r'##\s*情景推演', text))


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

    return result
