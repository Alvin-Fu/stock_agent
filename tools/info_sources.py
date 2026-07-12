# -*- coding: utf-8 -*-
"""
结构化联网信源统一模块（researcher 分析与监控共用）：
- 东财个股新闻   fetch_stock_news        —— 按代码，带时间戳
- 巨潮个股公告   fetch_stock_announcements —— 最硬的信源，重大事项直接来自公告
- 财联社电报快讯 fetch_cls_telegraph      —— 宏观/行业快讯流，可按关键词过滤

原则：全部 lazy import akshare、失败返回空列表并记 warning，绝不阻断分析主流程；
结构化信源可信度高于网页搜索，prompt 中应说明冲突时以结构化信源为准。
"""

import threading
import time
from datetime import date, timedelta
from typing import List, Dict, Optional

from utils.logger import logger

# 财联社电报当日内存缓存（快讯流全市场共用，避免每个标的重复拉）
_CLS_CACHE = {"ts": 0.0, "items": None}
_CLS_CACHE_TTL = 600  # 秒
_cls_lock = threading.Lock()


def fetch_stock_news(code: str, limit: int = 15) -> List[Dict[str, str]]:
    """东财个股新闻（结构化，带时间戳与来源）"""
    try:
        import akshare as ak
        df = ak.stock_news_em(symbol=code)
        items = []
        for _, row in df.head(limit).iterrows():
            items.append({
                "title": str(row.get("新闻标题", "")).strip(),
                "content": str(row.get("新闻内容", "")).strip()[:300],
                "time": str(row.get("发布时间", "")).strip(),
                "source": str(row.get("文章来源", "")).strip(),
                "url": str(row.get("新闻链接", "")).strip(),
            })
        return [it for it in items if it["title"]]
    except Exception as e:
        logger.warning(f"[信源] 东财个股新闻获取失败 {code}: {e}")
        return []


def fetch_stock_announcements(code: str, days: int = 30, limit: int = 15) -> List[Dict[str, str]]:
    """巨潮个股公告（标题+时间），重大事项的第一手来源"""
    try:
        import akshare as ak
        end = date.today()
        start = end - timedelta(days=days)
        df = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=code, market="沪深京",
            start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"),
        )
        items = []
        for _, row in df.head(limit).iterrows():
            items.append({
                "title": str(row.get("公告标题", "")).strip(),
                "time": str(row.get("公告时间", "")).strip(),
                "url": str(row.get("公告链接", "")).strip(),
            })
        return [it for it in items if it["title"]]
    except Exception as e:
        logger.warning(f"[信源] 巨潮公告获取失败 {code}: {e}")
        return []


def _load_cls_telegraph() -> List[Dict[str, str]]:
    """拉财联社电报全量快讯（10分钟内存缓存）"""
    now = time.time()
    if _CLS_CACHE["items"] is not None and now - _CLS_CACHE["ts"] < _CLS_CACHE_TTL:
        return _CLS_CACHE["items"]
    with _cls_lock:
        if _CLS_CACHE["items"] is not None and time.time() - _CLS_CACHE["ts"] < _CLS_CACHE_TTL:
            return _CLS_CACHE["items"]
        items = _try_flash_sources()
        if items:
            _CLS_CACHE.update(ts=time.time(), items=items)
            return items
        return _CLS_CACHE["items"] or []


def _try_flash_sources() -> List[Dict[str, str]]:
    """
    快讯源多级回退（实测 2026-07：财联社端点在部分 akshare 版本 404，
    东财全球快讯最稳定，作为第二源；新浪兜底）
    """
    import akshare as ak

    # 1. 财联社电报（新旧接口名兼容）
    fetch_cls = getattr(ak, "stock_info_global_cls", None) or getattr(ak, "stock_telegraph_cls", None)
    if fetch_cls is not None:
        try:
            df = fetch_cls(symbol="全部")
            items = [{
                "title": str(r.get("标题", "")).strip(),
                "content": str(r.get("内容", "")).strip()[:300],
                "time": f"{r.get('发布日期', '')} {r.get('发布时间', '')}".strip(),
            } for _, r in df.iterrows()]
            items = [it for it in items if it["title"] or it["content"]]
            if items:
                return items
        except Exception as e:
            logger.warning(f"[信源] 财联社电报获取失败，切换东财快讯: {e}")

    # 2. 东财全球财经快讯（标题/摘要/发布时间/链接）
    fetch_em = getattr(ak, "stock_info_global_em", None)
    if fetch_em is not None:
        try:
            df = fetch_em()
            items = [{
                "title": str(r.get("标题", "")).strip(),
                "content": str(r.get("摘要", "")).strip()[:300],
                "time": str(r.get("发布时间", "")).strip(),
            } for _, r in df.iterrows()]
            items = [it for it in items if it["title"] or it["content"]]
            if items:
                return items
        except Exception as e:
            logger.warning(f"[信源] 东财快讯获取失败，切换新浪: {e}")

    # 3. 新浪全球快讯（时间/内容）
    fetch_sina = getattr(ak, "stock_info_global_sina", None)
    if fetch_sina is not None:
        try:
            df = fetch_sina()
            items = [{
                "title": str(r.get("内容", "")).strip()[:60],
                "content": str(r.get("内容", "")).strip()[:300],
                "time": str(r.get("时间", "")).strip(),
            } for _, r in df.iterrows()]
            return [it for it in items if it["content"]]
        except Exception as e:
            logger.warning(f"[信源] 新浪快讯获取失败: {e}")

    return []


def fetch_cls_telegraph(keywords: Optional[List[str]] = None, limit: int = 20) -> List[Dict[str, str]]:
    """
    财联社电报快讯。keywords 非空时只保留标题/内容命中任一关键词的条目
    （用于按公司名/行业名过滤宏观快讯流）。
    """
    items = _load_cls_telegraph()
    if keywords:
        kws = [k for k in keywords if k]
        items = [it for it in items
                 if any(k in it["title"] or k in it["content"] for k in kws)]
    return items[:limit]


def format_info_block(title: str, items: List[Dict[str, str]], with_content: bool = True) -> str:
    """把信源条目格式化为 prompt 文本块；空列表返回空串"""
    if not items:
        return ""
    lines = [f"【{title}】"]
    for it in items:
        line = f"- [{it.get('time', '')}] {it.get('title', '')}"
        src = it.get("source")
        if src:
            line += f"（{src}）"
        lines.append(line)
        if with_content and it.get("content"):
            lines.append(f"  {it['content'][:200]}")
    return "\n".join(lines)
