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
import json
import threading
from datetime import datetime
from langchain_core.tools import tool
from utils.config import get_search_config
from utils.logger import logger

_init_lock = threading.Lock()
_last_ddg_ts = 0.0
# 搜索日志目录
_SEARCH_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "search_logs")
_SEARCH_LOG_LOCK = threading.Lock()
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


def _search_serpapi(query: str) -> str:
    """SerpAPI 搜索（需配置 serpapi_api_key）；每月 100 次免费；失败返回空串"""
    key = (get_search_config().get("serpapi_api_key") or "").strip()
    if not key:
        return ""
    try:
        import requests
        resp = requests.get(
            "https://serpapi.com/search",
            params={"api_key": key, "q": query, "engine": "google", "num": 5},
            timeout=30,
        )
        data = resp.json()
        if "organic_results" not in data:
            if "error" in data:
                logger.warning(f"SerpAPI 返回错误 [{query[:50]}]: {data.get('error')}")
            return ""
        lines = []
        for r in data["organic_results"][:5]:
            title = (r.get("title") or "").strip()
            snippet = (r.get("snippet") or "").strip()
            link = (r.get("link") or "").strip()
            if title or snippet:
                lines.append(f"{title}: {snippet} ({link})" if link else f"{title}: {snippet}")
        return "\n".join(lines)[:2000]
    except Exception as e:
        logger.warning(f"SerpAPI 搜索失败 [{query[:50]}]: {str(e)[:150]}")
        return ""


def _search_thenewsapi(query: str) -> str:
    """TheNewsAPI 搜索（需配置 newsapi_token）；免费 100 次/天；覆盖 40000+ 新闻源"""
    token = (get_search_config().get("newsapi_token") or "").strip()
    if not token:
        return ""
    try:
        import requests
        resp = requests.get(
            "https://api.thenewsapi.com/v1/news/top",
            params={
                "api_token": token,
                "search": query,
                "language": "zh,en",
                "limit": 5,
                "sort": "relevance_score",
            },
            timeout=20,
        )
        data = resp.json()
        articles = data.get("data") or []
        if not articles:
            return ""
        lines = []
        for a in articles[:5]:
            title = (a.get("title") or "").strip()
            snippet = (a.get("snippet") or "").strip()
            source = (a.get("source") or "").strip()
            url = (a.get("url") or "").strip()
            pub = (a.get("published_at") or "")[:10]
            parts = [title] if title else []
            if snippet:
                parts.append(snippet)
            if source or url:
                parts.append(f"({source} {pub} {url})" if source else f"({url})")
            if parts:
                lines.append(": ".join(parts))
        return "\n".join(lines)[:2000]
    except Exception as e:
        logger.warning(f"TheNewsAPI 搜索失败 [{query[:50]}]: {str(e)[:150]}")
        return ""


def _search_google_free(query: str) -> str:
    """Google 免费搜索（googlesearch-python）；失败返回空串"""
    try:
        from googlesearch import search
        results = list(search(query, num_results=5))
        if results:
            return "\n".join(results[:5])[:2000]
        return ""
    except Exception as e:
        logger.warning(f"Google免费搜索失败 [{query[:50]}]: {str(e)[:150]}")
        return ""


def _search_google_api(query: str) -> str:
    """Google Custom Search API（需配置 google_api_key + google_cx）；失败返回空串"""
    key = (get_search_config().get("google_api_key") or "").strip()
    cx = (get_search_config().get("google_cx") or "").strip()
    if not key or not cx:
        return ""
    try:
        import requests
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": key, "cx": cx, "q": query, "num": 5},
            timeout=15,
        )
        data = resp.json()
        if "items" not in data:
            if "error" in data:
                logger.warning(f"Google API 返回错误 [{query[:50]}]: {data['error'].get('message','')}")
            return ""
        lines = []
        for item in data["items"][:5]:
            title = (item.get("title") or "").strip()
            snippet = (item.get("snippet") or "").strip()
            link = (item.get("link") or "").strip()
            if title or snippet:
                lines.append(f"{title}: {snippet} ({link})" if link else f"{title}: {snippet}")
        return "\n".join(lines)[:2000]
    except Exception as e:
        logger.warning(f"Google API 搜索失败 [{query[:50]}]: {str(e)[:150]}")
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


def _search_ddg_fallback(query: str) -> str:
    """DuckDuckGo SSL 备用方案：限制 TLS 1.2 + verify=False，绕过 macOS LibreSSL TLS 版本限制"""
    global _last_ddg_ts
    try:
        elapsed = time.time() - _last_ddg_ts
        if elapsed < _MIN_INTERVAL_SECONDS:
            time.sleep(_MIN_INTERVAL_SECONDS - elapsed)
        _last_ddg_ts = time.time()

        import re
        import ssl
        import requests
        from requests.adapters import HTTPAdapter

        # 创建自定义适配器，限制 TLS 版本为 1.2（LibreSSL 2.8.3 不支持 TLS 1.3）
        class _TLS12Adapter(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = ssl.create_default_context()
                ctx.maximum_version = ssl.TLSVersion.TLSv1_2
                kwargs["ssl_context"] = ctx
                return super().init_poolmanager(*args, **kwargs)

        session = requests.Session()
        session.mount("https://", _TLS12Adapter())

        resp = session.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
            timeout=15,
        )
        resp.raise_for_status()

        # 解析 HTML 提取标题+摘要
        html = resp.text
        results = []
        # DuckDuckGo HTML 版每个结果包含 class="result__body"
        blocks = re.split(r'<div[^>]*class="[^"]*?\bresult__body\b[^"]*?"[^>]*>', html)[1:6]
        for block in blocks:
            title_m = re.search(
                r'<a[^>]*class="result__a"[^>]*>(.*?)</a>', block, re.DOTALL
            )
            snippet_m = re.search(
                r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>', block, re.DOTALL
            )
            title = re.sub(r'<[^>]+>', '', title_m.group(1)).strip() if title_m else ""
            snippet = (
                re.sub(r'<[^>]+>', '', snippet_m.group(1)).strip() if snippet_m else ""
            )
            if title or snippet:
                line = f"{title}: {snippet}" if title and snippet else (title or snippet)
                results.append(line)

        return "\n".join(results)[:2000] if results else ""
    except Exception as e:
        logger.warning(
            f"DuckDuckGo 备用搜索失败 [{query[:50]}]: {str(e)[:150]}"
        )
        return ""


# 引擎链：按顺序尝试，先出结果者胜（存函数名运行时查表，便于测试替换单个引擎）
_PROVIDERS = (
    ("Tavily", "_search_tavily"),
    ("Brave", "_search_brave"),
    ("SerpAPI(Google)", "_search_serpapi"),
    ("TheNewsAPI", "_search_thenewsapi"),
    ("Google(免费)", "_search_google_free"),
    ("Google(CustomSearch)", "_search_google_api"),
    ("DuckDuckGo", "_search_ddg"),
    ("DuckDuckGo(SSL备用)", "_search_ddg_fallback"),
)

# ────────────────────────────── 搜索日志持久化 ──────────────────────────────
def _save_search_log(query: str, engine: str, content: str) -> None:
    """每次搜索成功后，将结果快照追加到 data/search_logs/ 下的 JSONL 文件"""
    try:
        os.makedirs(_SEARCH_LOG_DIR, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        path = os.path.join(_SEARCH_LOG_DIR, f"search_{today}.jsonl")
        record = {
            "ts": datetime.now().isoformat(),
            "query": query[:80],
            "engine": engine,
            "char_len": len(content),
            "snippet": content[:500],
        }
        with _SEARCH_LOG_LOCK:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug(f"搜索日志存储异常: {e}")


@tool
def web_search(query: str) -> str:
    """
    搜索互联网获取最新信息。适用于查询实时股价、新闻、公告等。
    参数 query: 搜索关键词
    """
    errors = []
    for name, fn_name in _PROVIDERS:
        try:
            text = globals()[fn_name](query)
            if text:
                if name != "Tavily":
                    logger.info(f"搜索由 {name} 兜底完成 [{query[:40]}]")
                _save_search_log(query, name, text)
                return text
        except Exception as e:
            err = str(e)[:100]
            errors.append(f"{name}: {err}")
            logger.debug(f"搜索 {name} 失败 [{query[:40]}]: {err}")
            continue
    if errors:
        logger.warning(f"联网搜索全部失败 [{query[:50]}]: {'; '.join(errors[:3])}")
        return f"搜索失败: {'; '.join(errors[:3])}"
    return "搜索失败: 所有搜索引擎（Tavily/Brave/DuckDuckGo）均无结果或不可用"
