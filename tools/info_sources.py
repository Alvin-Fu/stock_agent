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
from tools.source_health import report_source
from tools.source_tiers import TIER, tier_tag

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
        items = [it for it in items if it["title"]]
        report_source("东财新闻", bool(items), "接口返回为空")
        return items
    except Exception as e:
        logger.warning(f"[信源] 东财个股新闻获取失败 {code}: {e}")
        report_source("东财新闻", False, str(e))
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
        items = [it for it in items if it["title"]]
        report_source("巨潮公告", bool(items), "接口返回为空")
        return items
    except Exception as e:
        logger.warning(f"[信源] 巨潮公告获取失败 {code}: {e}")
        report_source("巨潮公告", False, str(e))
        return []


# 产销快报类公告标题特征（排除取消/更正前的旧版）
_SALES_TITLE = None  # 延迟编译


def _pick_sales_flash(announcements: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """从公告列表挑最新一份产销快报（纯函数，便于测试）"""
    import re
    global _SALES_TITLE
    if _SALES_TITLE is None:
        _SALES_TITLE = (re.compile(r"(产销快报|产销数据|销量快报|产销情况|产销.*自愿性信息披露)"),
                        re.compile(r"(取消|更正前|英文)"))
    want, block = _SALES_TITLE
    for it in announcements:  # 上游已按时间倒序
        title = it.get("title", "")
        if want.search(title) and not block.search(title):
            return it
    return None


def _to_pdf_urls(url: str) -> List[str]:
    """
    把巨潮公告链接换算成 PDF 直链候选（纯函数）。
    akshare 巨潮接口给的"公告链接"是网页详情页（/new/disclosure/detail?...，返回 HTML），
    直接喂 PDF 解析器必报 "EOF marker not found"；真正的 PDF 在
    https://static.cninfo.com.cn/finalpage/{公告日期}/{announcementId}.PDF（扩展名大小写都存在）。
    无法解析出参数时原样返回，兼容本来就是直链的 URL。
    """
    from urllib.parse import urlparse, parse_qs
    try:
        q = parse_qs(urlparse(url).query)
        ann_id = (q.get("announcementId") or [""])[0].strip()
        ann_time = (q.get("announcementTime") or [""])[0].strip()[:10]
        if ann_id and ann_time:
            base = f"https://static.cninfo.com.cn/finalpage/{ann_time}/{ann_id}"
            return [f"{base}.PDF", f"{base}.pdf"]
    except Exception:
        pass
    return [url]


def _download_pdf_text(url: str, max_pages: int = 4) -> str:
    """下载公告 PDF 并抽取文本；任何失败返回空串"""
    try:
        import io
        import requests
        from PyPDF2 import PdfReader
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        last_err = "无候选URL"
        for cand in _to_pdf_urls(url):
            try:
                resp = requests.get(cand, timeout=15, headers=headers)
                resp.raise_for_status()
                # 内容嗅探：详情页/错误页是 HTML，喂给 PdfReader 只会报晦涩的 EOF 错误
                if not resp.content.lstrip().startswith(b"%PDF"):
                    last_err = f"响应不是PDF（HTML详情页或错误页）: {cand}"
                    continue
                reader = PdfReader(io.BytesIO(resp.content))
                pages = [p.extract_text() or "" for p in reader.pages[:max_pages]]
                text = "\n".join(pages).strip()
                if text:
                    return text
                last_err = f"PDF无文本层（扫描版？）: {cand}"
            except Exception as e:
                last_err = f"{cand}: {e}"
        logger.warning(f"[信源] 公告 PDF 抽取失败 {url}: {last_err}")
        return ""
    except Exception as e:
        logger.warning(f"[信源] 公告 PDF 抽取失败 {url}: {e}")
        return ""


def fetch_sales_flash_text(code: str, days: int = 40) -> str:
    """
    最近一期产销快报公告原文（权威销量口径）。
    搜索引擎转述的销量数字口径混乱（含不含商用车/媒体笔误），
    产销快报是上市公司公告，数字必须以此为准。失败返回空串不阻断。
    正文按 (code, 标题) 落库缓存：同一份公告的 PDF 只从巨潮下载一次，
    公告列表仍每次拉取（轻量，用于发现新一期）。
    """
    try:
        ann = _pick_sales_flash(fetch_stock_announcements(code, days=days, limit=30))
        if not ann or not ann.get("url"):
            report_source("产销快报", False, f"近{days}天无产销快报公告（部分公司不发布，属正常）")
            return ""

        db = None
        text = None
        try:
            from storage.sqlite.stock_storage import get_db
            db = get_db()
            text = db.get_announcement_text(code, ann["title"])
        except Exception as e:
            logger.warning(f"[信源] 公告缓存读取失败（改为直接下载）: {e}")

        if not text:
            text = _download_pdf_text(ann["url"])
            if not text:
                report_source("产销快报", False, "PDF下载或抽取失败")
                return ""
            if db is not None:
                db.save_announcement_text(code, ann["title"], ann_time=ann.get("time"),
                                          url=ann.get("url"), content=text)
                logger.info(f"[信源] 产销快报正文已缓存: {code} {ann['title']}")

        report_source("产销快报", True)
        return (f"【产销快报公告原文（{ann['title']}，{ann.get('time', '')}，权威口径，"
                f"销量数字以此为准）】\n{text[:2500]}")
    except Exception as e:
        logger.warning(f"[信源] 产销快报获取失败 {code}: {e}")
        report_source("产销快报", False, str(e))
        return ""


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
    report_source("快讯流", bool(items), "三个快讯源均无数据")
    if keywords:
        kws = [k for k in keywords if k]
        items = [it for it in items
                 if any(k in it["title"] or k in it["content"] for k in kws)]
    return items[:limit]


def format_info_block(title: str, items: List[Dict[str, str]],
                      with_content: bool = True, tier: TIER = TIER.T2) -> str:
    """把信源条目格式化为 prompt 文本块；空列表返回空串。tier 标注信源等级。"""
    if not items:
        return ""
    tag = tier_tag(tier)
    lines = [f"{tag}【{title}】"]
    for it in items:
        line = f"- [{it.get('time', '')}] {it.get('title', '')}"
        src = it.get("source")
        if src:
            line += f"（{src}）"
        lines.append(line)
        if with_content and it.get("content"):
            lines.append(f"  {it['content'][:200]}")
    return "\n".join(lines)
