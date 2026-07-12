# -*- coding: utf-8 -*-
"""
数据源健康采集器：解决"静默降级"问题——某个信源挂了，报告照出、质量悄悄下降，
没人知道是数据断了（产销快报链路曾静默失败很久才被发现）。

用法：workflow 入口 reset_health()；各 fetch 函数成功/失败都 report_source()；
compliance 末尾把 format_health() 程序化附加到最终回答（不经 LLM，不会被改写）。
进程内全局状态：飞书/调度入口都是串行跑分析，可接受；并发场景下摘要可能混串但不崩。
"""

import threading
from typing import Dict, List

_LOCK = threading.Lock()
# name -> {"ok": int, "fail": int, "note": str}（note 记最后一次失败原因）
_ENTRIES: Dict[str, Dict] = {}
_ORDER: List[str] = []


def reset_health() -> None:
    """新一次分析开始时清零"""
    with _LOCK:
        _ENTRIES.clear()
        _ORDER.clear()


def report_source(name: str, ok: bool, note: str = "") -> None:
    """上报一次信源结果；同名多次上报（如逐股拉K线）自动聚合计数"""
    with _LOCK:
        if name not in _ENTRIES:
            _ENTRIES[name] = {"ok": 0, "fail": 0, "note": ""}
            _ORDER.append(name)
        e = _ENTRIES[name]
        if ok:
            e["ok"] += 1
        else:
            e["fail"] += 1
            if note:
                e["note"] = str(note)[:60]


def format_health() -> str:
    """一行式健康摘要；没有任何上报返回空串"""
    with _LOCK:
        if not _ORDER:
            return ""
        parts = []
        for name in _ORDER:
            e = _ENTRIES[name]
            total = e["ok"] + e["fail"]
            if e["fail"] == 0:
                mark = "✓" if total == 1 else f"✓{e['ok']}/{total}"
            elif e["ok"] == 0:
                mark = "✗" + (f"({e['note']})" if e["note"] else "")
            else:
                mark = f"△{e['ok']}/{total}" + (f"({e['note']})" if e["note"] else "")
            parts.append(f"{name}{mark}")
        return "【数据源健康】" + "｜".join(parts)
