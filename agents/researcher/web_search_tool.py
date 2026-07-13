"""
联网搜索工具封装：多引擎链 Tavily → Brave → DuckDuckGo，逐级兜底。
- Tavily key 从配置读取并注入环境变量（langchain_tavily 只认 TAVILY_API_KEY）
- Tavily 配额耗尽/鉴权失败（432/401/quota）后本进程内直接跳过它，
  不再每条查询都撞一次墙（曾把错误字典当结果喂给 LLM，且健康摘要显示假"✓"）
- Brave Search API 可选（search.brave_api_key，免费档每月 2000 次），直接 HTTP 调用不加依赖
- DuckDuckGo 免费无 key 兜底，加最小间隔降低批量搜索被限流的概率
- 延迟初始化：import 阶段不实例化搜索引擎，避免没配 key 时整个包 import 失败
"""

import os
import time
import threading
from langchain_core.tools import tool
from utils.config import get_search_config
from utils.logger import logger

_init_lock = threading.Lock()
_last_ddg_ts = 0.0
# 免费搜索源的最小调用间隔（秒），产业链模式会串行发大量查询
_MIN_INTERVAL_SECONDS = 1.0

_tavily_tool = None
_tavily_dead = False  # 配额/鉴权失败后本进程内不再尝试
_ddg_tool = None

# Tavily 错误信息里出现这些词 → 配额/鉴权类问题，重试无意义
_TAVILY_FATAL_MARKS = ("432", "401", "quota", "exceeded", "unauthorized", "invalid api key")


def _get_tavily():
    """有 key 且未标记死亡时返回 Tavily 工具，否则 None"""
    global _tavily_tool
    if _tavily_dead:
        return None
    if _tavily_tool is not None:
        return _tavily_tool
    with _init_lock:
        if _tavily_tool is not None or _tavily_dead:
            return _tavily_tool
        key = (get_search_config().get("tavily_api_key") or "").strip() \
            or (os.environ.get("TAVILY_API_KEY") or "").strip()
        if not key:
            return None
        os.environ["TAVILY_API_KEY"] = key
        from langchain_tavily import TavilySearch
        _tavily_tool = TavilySearch(max_results=3)
        logger.info("联网搜索主引擎：Tavily")
        return _tavily_tool


def _mark_tavily_dead(err: str) -> None:
    global _tavily_dead
    if not _tavily_dead:
        _tavily_dead = True
        logger.warning(f"Tavily 已标记不可用（本进程内不再尝试）: {err[:120]}")


def _search_tavily(query: str) -> str:
    """Tavily 搜索；配额类失败标记死亡；失败返回空串"""
    tool_ = _get_tavily()
    if tool_ is None:
        return ""
    try:
        result = tool_.invoke(query)
    except Exception as e:
        err = str(e)
        if any(m in err.lower() for m in _TAVILY_FATAL_MARKS):
            _mark_tavily_dead(err)
        logger.warning(f"Tavily 搜索失败 [{query[:50]}]: {err[:150]}")
        return ""
    # 配额耗尽/出错时 Tavily 返回 {'error': ...} 而不是抛异常
    if isinstance(result, dict) and result.get("error"):
        err = str(result["error"])
        if any(m in err.lower() for m in _TAVILY_FATAL_MARKS):
            _mark_tavily_dead(err)
        logger.warning(f"Tavily 返回错误 [{query[:50]}]: {err[:150]}")
        return ""
    if isinstance(result, str):
        return result[:2000]
    if isinstance(result, list):
        return "\n".join(str(r) for r in result[:3])[:2000]
    if isinstance(result, dict):
        items = result.get("results", [])
        return "\n".join(str(r) for r in items[:3])[:2000] or str(result)[:2000]
    return str(result)[:2000]


def _search_brave(query: str) -> str:
    """Brave Search API（可选 key）；未配 key 或失败返回空串"""
    key = (get_search_config().get("brave_api_key") or "").strip()
    if not key:
        return ""
    try:
        import requests
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            params={"q": query, "count": 5},
            headers={"X-Subscription-Token": key, "Accept": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        results = ((resp.json().get("web") or {}).get("results")) or []
        lines = []
        for r in results[:5]:
            title = (r.get("title") or "").strip()
            desc = (r.get("description") or "").strip()
            if title or desc:
                lines.append(f"{title}: {desc}")
        return "\n".join(lines)[:2000]
    except Exception as e:
        logger.warning(f"Brave 搜索失败 [{query[:50]}]: {str(e)[:150]}")
        return ""


def _search_ddg(query: str) -> str:
    """DuckDuckGo 免费兜底；带最小间隔限速；失败返回空串"""
    global _ddg_tool, _last_ddg_ts
    try:
        if _ddg_tool is None:
            from langchain_community.tools import DuckDuckGoSearchRun
            _ddg_tool = DuckDuckGoSearchRun()
        elapsed = time.time() - _last_ddg_ts
        if elapsed < _MIN_INTERVAL_SECONDS:
            time.sleep(_MIN_INTERVAL_SECONDS - elapsed)
        _last_ddg_ts = time.time()
        text = _ddg_tool.invoke(query)
        return str(text)[:2000] if text else ""
    except Exception as e:
        logger.warning(f"DuckDuckGo 搜索失败 [{query[:50]}]: {str(e)[:150]}")
        return ""


# 引擎链：按顺序尝试，先出结果者胜（存函数名运行时查表，便于测试替换单个引擎）
_PROVIDERS = (("Tavily", "_search_tavily"), ("Brave", "_search_brave"), ("DuckDuckGo", "_search_ddg"))


@tool
def web_search(query: str) -> str:
    """
    搜索互联网获取最新信息。适用于查询实时股价、新闻、公告等。
    参数 query: 搜索关键词
    """
    try:
        for name, fn_name in _PROVIDERS:
            text = globals()[fn_name](query)
            if text:
                if name != "Tavily":
                    logger.info(f"搜索由 {name} 兜底完成 [{query[:40]}]")
                return text
        return "搜索失败: 所有搜索引擎（Tavily/Brave/DuckDuckGo）均无结果或不可用"
    except Exception as e:
        logger.warning(f"联网搜索失败 [{query[:50]}]: {e}")
        return f"搜索失败: {str(e)}"
