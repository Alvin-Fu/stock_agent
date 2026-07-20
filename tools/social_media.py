# -*- coding: utf-8 -*-
"""
社交媒体信息抓取工具：
- 微博：通过 web search 精确搜索公司官方认证微博内容（site:weibo.com + 关键词过滤）
- 微信公众号：先通过 wechatsogou 定位官方认证公众号，再只保留该号的文章
- 两级缓存：数据库 → 实时搜索，24 小时刷新一次

使用方式（被 researcher agent 调用）：
    from tools.social_media import fetch_social_media_text
    text = fetch_social_media_text(stock_code, company_name)
"""

import json
import re
from datetime import datetime
from typing import Optional

from storage.sqlite.stock_storage import get_db
from utils.logger import logger
from tools.source_tiers import TIER, tier_tag


# ========== 微博：用 web search 精确搜索认证账号 ==========

def _is_official_weibo_snippet(text: str) -> bool:
    """判断搜索结果片段是否来自官方微博账号"""
    signals = ["官方微博", "蓝V", "黄V", "认证", "weibo.com/u/",
               "weibo.com/", "新浪微博", "verified"]
    return any(s in text for s in signals)


def _search_weibo_posts(company_name: str) -> list:
    """通过 web search 搜索公司官方微博近期动态，只保留带认证标识的结果"""
    try:
        from agents.researcher.web_search_tool import web_search

        # 用 site:weibo.com 限定搜索域，搜索带"官方""认证"标识的微博
        raw = web_search.invoke({"query": f"{company_name} site:weibo.com 官方 认证 2026"})
        text = raw or ""

        raw2 = web_search.invoke({"query": f"{company_name} 官方微博 蓝V 最新"})
        text += "\n" + (raw2 or "")

        if not text.strip():
            return []

        lines = [l.strip() for l in text.split("\n") if l.strip()]
        posts = []
        for line in lines:
            if len(line) < 15:
                continue
            if line.startswith("搜索失败") or line.startswith("!"):
                continue
            # 只保留带官方认证标识的行
            if _is_official_weibo_snippet(line):
                posts.append({"text": line[:300], "source": "weibo"})
            elif len(posts) == 0:
                # 首轮没有认证结果时宽松兜底但不标注"官方"
                posts.append({"text": f"[未确认来源] {line[:280]}", "source": "unknown"})
            if len(posts) >= 6:
                break
        # 如果混入了非官方来源，给日志警告
        n_unknown = sum(1 for p in posts if p.get("source") == "unknown")
        if n_unknown:
            logger.warning(f"[社交媒体] {company_name} 微博结果中 {n_unknown} 条未确认是否官方账号")
        return posts
    except Exception as e:
        logger.debug(f"[社交媒体] 搜索微博动态失败: {e}")
        return []


def _format_weibo_text(posts: list) -> str:
    """将微博搜索结果格式化为文本块"""
    if not posts:
        return ""
    tag = tier_tag(TIER.T3)
    lines = [f"{tag}【官方微博动态】"]
    for p in posts[:6]:
        prefix = "" if p.get("source") == "weibo" else "⚠️ "
        lines.append(f"· {prefix}{p.get('text', '')}")
    return "\n".join(lines)


# ========== 微信公众号：官方号定位 + 文章过滤 ==========

def _search_wechat_account(company_name: str) -> Optional[dict]:
    """通过搜狗微信搜索查找公司官方公众号（搜狗返回的是经过微信认证的账号）"""
    try:
        import wechatsogou
        ws_api = wechatsogou.WechatSogouAPI(timeout=10)
        info = ws_api.get_gzh_info(company_name)
        if info:
            wechat_name = info.get("wechat_name") or info.get("name") or ""
            wechat_id = info.get("wechat_id") or info.get("id") or ""
            logger.info(f"[社交媒体] 搜索到 {company_name} 认证公众号: {wechat_name}({wechat_id})")
            return {"name": wechat_name, "id": wechat_id}
    except ImportError:
        logger.debug("[社交媒体] wechatsogou 未安装，跳过公众号搜索")
    except Exception as e:
        logger.debug(f"[社交媒体] 搜索公众号失败: {e}")
    return None


def _search_wechat_articles(company_name: str, official_name: str = None, limit: int = 5) -> list:
    """通过搜狗微信搜索公众号文章。若已知官方公众号名，只保留该号的文章。"""
    try:
        import wechatsogou
        ws_api = wechatsogou.WechatSogouAPI(timeout=10)
        results = ws_api.search_article(company_name, page=1)
        articles = []
        for art in (results or [])[:limit * 3]:  # 多取一些以便过滤
            source = (art.get("source") or "").strip()
            # 已知官方号名时，只保留来源匹配的文章
            if official_name and official_name.lower() not in source.lower():
                continue
            articles.append({
                "title": (art.get("title") or "")[:100],
                "abstract": (art.get("abstract") or "")[:200],
                "source": source,
                "time": art.get("time") or "",
            })
            if len(articles) >= limit:
                break
        if official_name and not articles:
            logger.info(f"[社交媒体] 未找到 {official_name} 的公众号文章（搜狗微信搜索结果中无该号文章）")
        return articles
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"[社交媒体] 搜索公众号文章失败: {e}")
    return []


def _format_wechat_text(articles: list, account: Optional[dict] = None) -> str:
    """将公众号信息格式化为文本块"""
    if not articles and not account:
        return ""
    tag = tier_tag(TIER.T3)
    lines = [f"{tag}【微信公众号】"]
    if account:
        lines.append(f"认证公众号：{account.get('name', '')}")
        if account.get("id"):
            lines.append(f"微信号：{account['id']}")
    if articles:
        lines.append(f"该号近期文章（{tag.strip()}）：")
        for a in articles[:5]:
            time_str = (a.get("time") or "")[:10]
            lines.append(f"· {time_str} | {a.get('title', '')}")
            if a.get("abstract"):
                lines.append(f"  {a['abstract'][:100]}")
    if not account:
        lines.append("（未找到认证公众号，以下为含公司名的微信文章，来源未验证）")
    return "\n".join(lines)


# ========== 统一入口 ==========

def fetch_social_media_text(stock_code: str, company_name: str) -> str:
    """
    统一获取公司社交媒体信息（微博+公众号），返回格式化文本。
    内部实现两级缓存：数据库 → 实时搜索（存库），24 小时刷新一次。
    被 researcher agent 调用，融入结构化信源块。
    """
    if not company_name:
        return ""

    db = get_db()
    cached = db.get_social_account(stock_code)

    need_refresh = True
    if cached:
        updated = cached.get("updated_at")
        if updated:
            age = (datetime.now() - updated).total_seconds()
            need_refresh = age > 86400

    if not need_refresh and cached:
        parts = []
        if cached.get("weibo_posts"):
            posts = json.loads(cached["weibo_posts"])
            parts.append(_format_weibo_text(posts))
        if cached.get("wechat_articles"):
            articles = json.loads(cached["wechat_articles"])
            acc = {}
            if cached.get("wechat_name"):
                acc["name"] = cached["wechat_name"]
            if cached.get("wechat_id"):
                acc["id"] = cached["wechat_id"]
            parts.append(_format_wechat_text(articles, acc or None))
        result = "\n\n".join(t for t in parts if t)
        logger.info(f"[社交媒体] 使用缓存数据 [{stock_code} {company_name}]")
        return result

    # 实时搜索
    weibo_posts = _search_weibo_posts(company_name)
    wechat_account = _search_wechat_account(company_name)
    # 公众号文章搜索时传入官方号名过滤
    wechat_articles = _search_wechat_articles(
        company_name,
        official_name=(wechat_account or {}).get("name"),
    )

    # 格式化输出
    parts = []
    weibo_text = _format_weibo_text(weibo_posts)
    if weibo_text:
        parts.append(weibo_text)
    wechat_text = _format_wechat_text(wechat_articles, wechat_account)
    if wechat_text:
        parts.append(wechat_text)

    # 存入数据库
    try:
        db.save_social_account(
            code=stock_code, company_name=company_name,
            weibo_posts=json.dumps(weibo_posts, ensure_ascii=False) if weibo_posts else None,
            wechat_name=(wechat_account or {}).get("name") if wechat_account else None,
            wechat_id=(wechat_account or {}).get("id") if wechat_account else None,
            wechat_articles=json.dumps(wechat_articles, ensure_ascii=False) if wechat_articles else None,
        )
    except Exception as e:
        logger.warning(f"[社交媒体] 缓存写入失败: {e}")

    result = "\n\n".join(parts)
    logger.info(f"[社交媒体] 实时获取完成 [{stock_code} {company_name}]: {len(parts)} 个数据块")
    return result
