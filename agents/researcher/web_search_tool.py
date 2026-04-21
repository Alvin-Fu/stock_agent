"""
联网搜索工具封装（使用 Tavily 或 DuckDuckGo）
"""

import os
from langchain_core.tools import tool
from langchain_tavily import TavilySearch
from langchain_community.tools import TavilySearchResults, DuckDuckGoSearchRun

# 根据配置选择搜索引擎
USE_TAVILY = os.getenv("TAVILY_API_KEY") is not None

if USE_TAVILY:
    _search_tool = TavilySearch(max_results=3)
else:
    _search_tool = DuckDuckGoSearchRun()


@tool
def web_search(query: str) -> str:
    """
    搜索互联网获取最新信息。适用于查询实时股价、新闻、公告等。
    参数 query: 搜索关键词
    """
    try:
        result = _search_tool.invoke(query)
        # 限制返回长度
        if isinstance(result, str):
            return result[:2000]
        elif isinstance(result, list):
            return "\n".join([str(r) for r in result[:3]])[:2000]
        return str(result)[:2000]
    except Exception as e:
        return f"搜索失败: {str(e)}"