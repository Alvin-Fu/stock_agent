# -*- coding: utf-8 -*-
"""
增量财经信息源：通过 web_search（site: 限定域）补充专业财经信息。

新增 4 个来源（均为 T2/T3 等级，排序靠前者质量更高）：
1. 雪球（xueqiu.com）— T3·未验证社交：个股讨论帖、用户深度分析、市场情绪
2. 新浪财经（finance.sina.com.cn）— T2·结构化：财务指标速览、新闻聚合
3. 华尔街见闻（wallstreetcn.com）— T2·结构化：宏观/产业深度分析
4. 证券时报/中证报（stcn.com / cs.com.cn）— T2·结构化：证监指定披露媒体

使用方式（被 researcher agent 调用）：
    from tools.financial_sources import fetch_financial_sources_text
    text = fetch_financial_sources_text(stock_code, company_name)

限流策略：全局队列 + 源级最小间隔（2 秒），避免批量搜索时触发搜索引擎限流。
"""

import time
import threading

from utils.logger import logger
from tools.source_tiers import TIER, tier_tag

# ===== 全局限流 =====
# 每个搜索源一个锁 + 最后调用时间戳，确保源级串行 + 最小间隔
_FINANCE_LOCK = threading.Lock()
_LAST_CALL_TS = 0.0
_MIN_INTERVAL = 2.0  # 秒，源与源之间的最小间隔


def _throttle():
    """全局节流：每次调用前等待 _MIN_INTERVAL 秒"""
    global _LAST_CALL_TS
    with _FINANCE_LOCK:
        elapsed = time.time() - _LAST_CALL_TS
        if elapsed < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - elapsed)
        _LAST_CALL_TS = time.time()


# ===== 各来源搜索函数 =====

def _search_xueqiu(company_name: str) -> str:
    """雪球：个股讨论帖与深度分析"""
    try:
        from agents.researcher.web_search_tool import web_search
        _throttle()
        raw = web_search.invoke({"query": f"site:xueqiu.com {company_name} 股票 分析 2026"})
        return raw or ""
    except Exception as e:
        logger.debug(f"[财经来源] 雪球搜索失败: {e}")
        return ""


def _search_sina_finance(company_name: str, stock_code: str) -> str:
    """新浪财经：财务指标速览与个股新闻"""
    try:
        from agents.researcher.web_search_tool import web_search
        _throttle()
        query = f"site:finance.sina.com.cn {company_name} {stock_code}" if stock_code else f"site:finance.sina.com.cn {company_name}"
        raw = web_search.invoke({"query": query})
        return raw or ""
    except Exception as e:
        logger.debug(f"[财经来源] 新浪财经搜索失败: {e}")
        return ""


def _search_wallstreetcn(company_name: str) -> str:
    """华尔街见闻：宏观与产业深度分析"""
    try:
        from agents.researcher.web_search_tool import web_search
        _throttle()
        raw = web_search.invoke({"query": f"site:wallstreetcn.com {company_name} 2026"})
        return raw or ""
    except Exception as e:
        logger.debug(f"[财经来源] 华尔街见闻搜索失败: {e}")
        return ""


def _search_stcn(company_name: str) -> str:
    """证券时报/中证报：证监会指定披露媒体"""
    try:
        from agents.researcher.web_search_tool import web_search
        _throttle()
        raw = web_search.invoke({"query": f"site:stcn.com OR site:cs.com.cn {company_name} 2026"})
        return raw or ""
    except Exception as e:
        logger.debug(f"[财经来源] 证券时报/中证报搜索失败: {e}")
        return ""


def _search_zhihu(company_name: str) -> str:
    """知乎：行业深度分析、个股讨论、专家观点（web search 兜底）"""
    try:
        from agents.researcher.web_search_tool import web_search
        _throttle()
        raw = web_search.invoke({"query": f"site:zhihu.com {company_name} 股票 分析 2026"})
        return raw or ""
    except Exception as e:
        logger.debug(f"[财经来源] 知乎搜索失败: {e}")
        return ""


def _search_zhihu_api(company_name: str) -> str:
    """知乎官方 API（多 key 轮换）；配额耗尽自动切下一个 key；失败返回空串"""
    from utils.keys import get_zhihu_secrets, mark_zhihu_key_dead

    secrets = get_zhihu_secrets()
    if not secrets:
        return ""
    import requests
    import time as t
    errors = []
    for secret in secrets:
        try:
            ts = str(int(t.time()))
            resp = requests.get(
                "https://developer.zhihu.com/api/v1/content/zhihu_search",
                params={"Query": company_name},
                headers={
                    "Authorization": f"Bearer {secret}",
                    "X-Request-Timestamp": ts,
                    "Content-Type": "application/json",
                },
                timeout=15,
            )
            if resp.status_code in (429, 403, 401):
                mark_zhihu_key_dead(secret, f"HTTP {resp.status_code}")
                errors.append(f"key_{secret[:8]}: {resp.status_code}")
                continue
            resp.raise_for_status()
            data = resp.json()
            items = ((data.get("Data") or {}).get("Items")) or []
            if not items:
                return ""
            lines = []
            for item in items[:5]:
                title = (item.get("Title") or "").strip()
                content = (item.get("ContentText") or "").strip()
                url = (item.get("Url") or "").strip()
                vote = item.get("VoteUpCount") or 0
                author = (item.get("AuthorName") or "").strip()
                if title:
                    parts = [title]
                    if content:
                        parts.append(content[:150])
                    if author:
                        parts.append(f"@{author}")
                    if url:
                        parts.append(f"({url})")
                    line = ": ".join(parts)
                    if vote:
                        line = f"[赞{vote}] {line}"
                    lines.append(line)
            return "\n".join(lines)[:2000] if lines else ""
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else 0
            if status in (429, 403, 401):
                mark_zhihu_key_dead(secret, f"HTTP {status}")
                errors.append(f"key_{secret[:8]}: {status}")
                continue
            errors.append(f"key_{secret[:8]}: {str(e)[:60]}")
            continue
        except Exception as e:
            errors.append(f"key_{secret[:8]}: {str(e)[:60]}")
            continue
    if errors:
        logger.debug(f"[财经来源] 知乎API搜索失败，所有key均失败: {'; '.join(errors)}")
    return ""


# ===== 格式化 =====

def _format_source_block(source_label: str, tier: TIER, content: str, max_lines: int = 8) -> str:
    """统一格式化单来源输出，截取有效行"""
    if not content or len(content.strip()) < 20:
        return ""
    tag = tier_tag(tier)
    lines = [f"{tag}【{source_label}】"]
    count = 0
    for line in content.split("\n"):
        line = line.strip()
        if not line or len(line) < 10:
            continue
        if line.startswith("搜索失败") or line.startswith("!"):
            continue
        lines.append(f"· {line[:200]}")
        count += 1
        if count >= max_lines:
            break
    if count == 0:
        return ""
    return "\n".join(lines)


# ===== 统一入口 =====

def fetch_financial_sources_text(stock_code: str, company_name: str) -> str:
    """
    统一获取增量财经信息源文本。
    4 个来源依次串行搜索（全局 2 秒间隔限流）。

    返回格式化的文本块，可直接注入到 researcher 的结构化信源中。
    """
    if not company_name:
        return ""

    logger.info(f"[财经来源] 实时获取 [{stock_code} {company_name}]")
    blocks = []

    content = _search_xueqiu(company_name)
    block = _format_source_block("雪球", TIER.T3, content)
    if block:
        blocks.append(block)

    content = _search_sina_finance(company_name, stock_code)
    block = _format_source_block("新浪财经", TIER.T2, content)
    if block:
        blocks.append(block)

    content = _search_wallstreetcn(company_name)
    block = _format_source_block("华尔街见闻", TIER.T2, content)
    if block:
        blocks.append(block)

    content = _search_stcn(company_name)
    block = _format_source_block("证券时报/中证报", TIER.T2, content)
    if block:
        blocks.append(block)

    content = _search_zhihu(company_name)
    block = _format_source_block("知乎", TIER.T3, content)
    if block:
        blocks.append(block)

    # 知乎官方 API（有 key 时优先，结果更精准）
    content = _search_zhihu_api(company_name)
    if content:
        blocks.append(f"[T3·知乎API]\n{content}")

    result = "\n\n".join(blocks) if blocks else ""
    logger.info(f"[财经来源] 获取完成 [{stock_code} {company_name}]: {len(blocks)} 个数据块")
    return result
