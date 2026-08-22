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
        _SALES_TITLE = (re.compile(r"(产销快报|产销数据|销量快报|产销情况|产销月报|月度产销|产销简报|销售简报|月度销售|销量月报|产销.*自愿性信息披露)"),
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
        # ==== 程序提取关键字段摘要（放正文前，LLM 直接引用不用读 2500 字符找）====
        import re as _re
        summary_parts = []
        t_for_parse = text[:3000]

        # 1) 连续正增月数（匹配"连续X月正增长/环比正增长"等）
        for m in _re.finditer(r"连续\s*(\d+)\s*个月?\s*(?:销量|产量|正增长|同环比增长|保持正增)", t_for_parse):
            summary_parts.append(f"连续正增月数:{m.group(1)}个月")
            break

        # 2) 出口量 / 海外销量 + 同比%
        exp_pat = _re.compile(
            r"(?:出口\s*量?|海外\s*销量?|境外\s*销量?)\s*(?:约|:|为)?\s*([\d.]+\s*[万辆台艘辆]*)"
            r"(?:.*?同比\s*([+\-]?\s*[\d.]+%\s*(?:[上下]?降|[增减]长)?))?", _re.S)
        for m in exp_pat.finditer(t_for_parse):
            amt = m.group(1).strip()
            yoy = (m.group(2) or "").strip()
            summary_parts.append(f"出口量:{amt}" + (f" | 出口同比:{yoy}" if yoy else ""))
            break

        # 3) 累计同比（1-7月 / 1-X月，必须标"-10.54%（较H1收窄）"这种）
        cum_pat = _re.compile(
            r"(1[-\u4e00\-至]\d+\s*月|本年[前到]\d+\s*个月?)\s*(?:累计销量?|合计销量?)?"
            r".*?(同比\s*[+\-]?\s*[\d.]+%\s*(?:[上下]?降|[增减]长)?(?:\s*收窄)?)", _re.S)
        for m in cum_pat.finditer(t_for_parse):
            period = m.group(1).strip()
            yoy = m.group(2).strip()
            note = ""
            # 判断是否较前一期（H1）收窄：文中若有"较上半年""较H1""较 1-6 月"+"收窄"关键字
            narrow_pat = _re.compile(r"(?:较\s*(?:上半年|H1|1[-\-]6\s*月))[^，。]*?收窄", _re.S)
            if narrow_pat.search(t_for_parse):
                note = "（较H1收窄）"
            summary_parts.append(f"累计[{period}]同比:{yoy}{note}")
            break

        # 4) 高端占比（高端车型/高端系列/高端品牌 占比 15%）
        hratio_pat = _re.compile(
            r"(高端(?:车型|系列|品牌|产品)?|豪华|旗舰)\s*[的之]?\s*销量?\s*占比\s*(?:约|为|:)?\s*([\d.]+%)", _re.S)
        for m in hratio_pat.finditer(t_for_parse):
            summary_parts.append(f"高端占比:{m.group(2)}（{m.group(1)}口径）")
            break
        alt_hratio = _re.compile(
            r"(?:腾势|仰望|方程豹)\s*(?:合计|系列)?\s*销量?\s*(?:占比|占\s*整体销量)\s*([\d.]+%)", _re.S)
        for m in alt_hratio.finditer(t_for_parse):
            summary_parts.append(f"高端品牌占比:{m.group(1)}（腾势/仰望/方程豹合计）")
            break

        # 5) 技术/产品关键词（二代刀片、第X代电池、CTB、iTAC、新平台等）
        tech_keywords = ["二代刀片", "第二代刀片电池", "刀片电池 升级", "CTB",
                         "iTAC", "新平台", "全新平台", "e平台", "e3.1", "e3.2",
                         "鲲鹏动力", "DM 5.0", "DM-i 5.0", "第五代 DM"]
        tech_hits = [kw for kw in tech_keywords if kw in t_for_parse]
        # 扩展：找 XX切换 或 切换至 XX
        switch_pat = _re.compile(r"(\S{2,8})(?:切换至|切换为|切换为第|全面切换)(\S{2,12})")
        sw_hits = [f"{m.group(1)}→{m.group(2)}" for m in switch_pat.finditer(t_for_parse)][:3]
        if tech_hits or sw_hits:
            items = []
            if tech_hits: items.append(f"关键词:{'/'.join(tech_hits)}")
            if sw_hits: items.append(f"技术切换:{'、'.join(sw_hits)}")
            summary_parts.append(" | ".join(items))

        # 6) 当月产量/销量 + 同比
        mo_pat = _re.compile(
            r"(?:本月\s*销量|当月\s*销量|本期\s*销量)\s*(?:约|:|为)?\s*([\d.]+\s*[万辆台艘辆]*)"
            r"(?:.*?同比\s*([+\-]?\s*[\d.]+%\s*(?:[上下]?降|[增减]长)?))?", _re.S)
        for m in mo_pat.finditer(t_for_parse):
            amt = m.group(1).strip()
            yoy = (m.group(2) or "").strip()
            s = f"当月销量:{amt}"
            if yoy: s += f" | 同比:{yoy}"
            summary_parts.insert(0, s)  # 当月销量放最前
            break

        summary_text = ""
        if summary_parts:
            summary_text = ("【摘要★（程序直接提取，必须嵌入「公司概况/核心逻辑」段首2句，不得只放运营数据段）】\n  "
                            + "\n  ".join(summary_parts)
                            + "\n--- 公告原文 ---")
        return (f"【产销快报公告原文（{ann['title']}，{ann.get('time', '')}，权威口径，"
                f"销量数字以此为准）】\n{summary_text}{text[:2500]}")
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
