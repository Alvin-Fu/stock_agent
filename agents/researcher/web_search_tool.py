"""
联网搜索工具封装（使用 Tavily 或 DuckDuckGo）
- Tavily key 从配置读取并注入环境变量（langchain_tavily 只认 TAVILY_API_KEY）
- 延迟初始化：import 阶段不实例化搜索引擎，避免没配 key 时整个包 import 失败
- DuckDuckGo 分支加最小间隔，降低批量搜索被限流的概率
"""

import os
import time
import threading
from langchain_core.tools import tool
from utils.config import get_search_config
from utils.logger import logger

_search_tool = None
_init_lock = threading.Lock()
_last_call_ts = 0.0
# 免费搜索源的最小调用间隔（秒），产业链模式会串行发大量查询
_MIN_INTERVAL_SECONDS = 1.0


def _get_search_tool():
    """延迟创建搜索工具：优先 Tavily（配置了非空 key），否则 DuckDuckGo"""
    global _search_tool
    if _search_tool is not None:
        return _search_tool

    with _init_lock:
        if _search_tool is not None:
            return _search_tool

        tavily_key = (get_search_config().get("tavily_api_key") or "").strip() \
            or (os.environ.get("TAVILY_API_KEY") or "").strip()
        if tavily_key:
            os.environ["TAVILY_API_KEY"] = tavily_key
            from langchain_tavily import TavilySearch
            _search_tool = TavilySearch(max_results=3)
            logger.info("联网搜索使用 Tavily")
        else:
            from langchain_community.tools import DuckDuckGoSearchRun
            _search_tool = DuckDuckGoSearchRun()
            logger.info("未配置 Tavily key，联网搜索使用 DuckDuckGo（有限流风险）")
        return _search_tool


@tool
def web_search(query: str) -> str:
    """
    搜索互联网获取最新信息。适用于查询实时股价、新闻、公告等。
    参数 query: 搜索关键词
    """
    global _last_call_ts
    try:
        search = _get_search_tool()

        # 最小间隔限速
        elapsed = time.time() - _last_call_ts
        if elapsed < _MIN_INTERVAL_SECONDS:
            time.sleep(_MIN_INTERVAL_SECONDS - elapsed)
        _last_call_ts = time.time()

        result = search.invoke(query)
        if isinstance(result, str):
            return result[:2000]
        elif isinstance(result, list):
            return "\n".join([str(r) for r in result[:3]])[:2000]
        elif isinstance(result, dict):
            # TavilySearch 返回 dict，取 results 列表
            items = result.get("results", [])
            return "\n".join([str(r) for r in items[:3]])[:2000] or str(result)[:2000]
        return str(result)[:2000]
    except Exception as e:
        logger.warning(f"联网搜索失败 [{query[:50]}]: {e}")
        return f"搜索失败: {str(e)}"
