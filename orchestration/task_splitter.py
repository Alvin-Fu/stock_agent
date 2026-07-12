# -*- coding: utf-8 -*-
"""
入口级任务拆解（大脑的第一步）：
用户一条消息里可能含多个分析对象（"分析比亚迪和宁德时代"/"比亚迪，再看看半导体产业链"），
在进工作流之前先拆成独立子任务，逐个跑完整分析。
原则：拆解失败/单对象时原样单任务处理，绝不因拆解引入新故障；最多拆 4 个防跑飞。
"""

import json
import re
from typing import Dict, List

from utils.logger import logger

MAX_TASKS = 4

_SPLIT_PROMPT = """判断用户的分析请求里包含几个**独立的分析对象**（每家公司、每个行业各算一个）。

只输出JSON数组（不要markdown包裹，不要解释）：
[{{"kind": "company"或"industry", "target": "对象名", "sub_question": "针对该对象的独立完整问题"}}]

规则：
- 只有一个对象时，输出只含一个元素的数组
- sub_question 必须保留用户的原始诉求（如"能不能介入""有哪些机会"），只把对象换成单个
- 公司名/6位股票代码 → kind=company；行业/板块/产业链 → kind=industry
- 最多 {max_tasks} 个对象，超出只取前 {max_tasks} 个
- 无法判断时输出一个元素：target 给空串，sub_question 给原问题

用户请求：{question}"""


def _parse_split_result(raw: str, question: str) -> List[Dict[str, str]]:
    """解析 LLM 拆解输出（纯函数，便于测试）；任何异常回落单任务"""
    fallback = [{"kind": "", "target": "", "question": question}]
    try:
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if not match:
            return fallback
        items = json.loads(match.group(0))
        tasks, seen = [], set()
        for it in items[:MAX_TASKS]:
            if not isinstance(it, dict):
                continue
            target = str(it.get("target") or "").strip()
            sub_q = str(it.get("sub_question") or "").strip() or question
            kind = it.get("kind") if it.get("kind") in ("company", "industry") else ""
            if target and target in seen:
                continue
            if target:
                seen.add(target)
            tasks.append({"kind": kind, "target": target, "question": sub_q})
        if not tasks:
            return fallback
        if len(tasks) == 1:
            # 单对象：用原问题进工作流，避免改写失真
            return [{"kind": tasks[0]["kind"], "target": tasks[0]["target"], "question": question}]
        return tasks
    except (json.JSONDecodeError, TypeError, ValueError):
        return fallback


def split_tasks(question: str) -> List[Dict[str, str]]:
    """
    拆解用户请求为分析子任务列表。
    返回 [{"kind": company/industry/"", "target": 对象名, "question": 子问题}]；
    单对象/拆解失败时返回单元素列表（question=原问题）。
    """
    fallback = [{"kind": "", "target": "", "question": question}]
    if not question or len(question.strip()) < 4:
        return fallback
    try:
        from core.llm import get_agent_llm
        resp = get_agent_llm("router").invoke(
            _SPLIT_PROMPT.format(question=question[:500], max_tasks=MAX_TASKS))
        raw = resp.content if hasattr(resp, "content") else str(resp)
        tasks = _parse_split_result(raw, question)
        if len(tasks) > 1:
            logger.info(f"任务拆解: {len(tasks)} 个对象 -> {[t['target'] for t in tasks]}")
        return tasks
    except Exception as e:
        logger.warning(f"任务拆解失败，按单任务处理: {e}")
        return fallback
