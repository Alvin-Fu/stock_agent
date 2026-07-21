# -*- coding: utf-8 -*-
"""
社交媒体信息抓取工具：
- 微博：通过 web search 精确搜索公司官方认证微博内容（site:weibo.com + 关键词过滤）
- 微信公众号：通过 web search 搜索 site:mp.weixin.qq.com 定位官方公众号及文章
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

# ===== 微信公众号搜索：用 web search 替代 wechatsogou =====
# wechatsogou（搜狗微信搜索）因依赖旧版 werkzeug 已不可用，
# 改为通过 web_search 搜索 site:mp.weixin.qq.com 来找公众号文章，
# 与微博的处理方式一致。


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
    tag = tier_tag(TIER.T1)
    lines = [f"{tag}【官方微博动态】"]
    for p in posts[:6]:
        prefix = "" if p.get("source") == "weibo" else "⚠️ "
        lines.append(f"· {prefix}{p.get('text', '')}")
    return "\n".join(lines)


# ========== 微信公众号：官方号定位 + 文章过滤 ==========

def _search_wechat_account(company_name: str) -> Optional[dict]:
    """
    通过 web search 查找公司官方认证公众号信息
    （搜索 site:mp.weixin.qq.com + 公众号认证关键词）
    """
    try:
        from agents.researcher.web_search_tool import web_search

        raw = web_search.invoke({"query": f"{company_name} site:mp.weixin.qq.com 公众号 认证 2026"})
        text = raw or ""

        if not text.strip():
            return None

        # 尝试从搜索结果中提取公众号名称
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        name, wechat_id = None, None
        for line in lines:
            if len(line) < 8:
                continue
            # 常见公众号标识模式
            if not name:
                m = re.search(r'微信号[：:]\s*(\S+)', line)
                if m:
                    wechat_id = m.group(1).strip()
                    continue
            if not name:
                m = re.search(r'公众号[：:]\s*(\S+)', line)
                if m:
                    name = m.group(1).strip()
                    continue
            # 行中包含公司名 + "公众号" → 尝试提取公众号名
            if company_name in line and "公众号" in line:
                # 格式通常是 "公司名 公众号名" 或 "公司名 - 公众号名"
                m = re.search(r'[：:]\s*(\S{2,20})', line)
                if m:
                    name = m.group(1).strip()

        if name:
            logger.info(f"[社交媒体] 搜索到 {company_name} 认证公众号: {name}（{'微信号: ' + wechat_id if wechat_id else '微信号未知'}）")
            return {"name": name, "id": wechat_id or ""}
        return None
    except Exception as e:
        logger.debug(f"[社交媒体] 搜索公众号失败: {e}")
        return None


def _search_wechat_articles(company_name: str, official_name: str = None, limit: int = 5) -> list:
    """
    通过 web search 搜索公司公众号文章。
    若已知官方公众号名，只保留该号的文章。
    """
    try:
        from agents.researcher.web_search_tool import web_search

        raw = web_search.invoke({"query": f"{company_name} site:mp.weixin.qq.com 2026"})
        text = raw or ""

        if not text.strip():
            return []

        lines = [l.strip() for l in text.split("\n") if l.strip()]
        articles = []
        for line in lines:
            if len(line) < 20:
                continue
            if line.startswith("搜索失败") or line.startswith("!"):
                continue
            # 尝试识别文章的公众号来源
            source = ""
            # 行末或行中的「来源：xxx」或「- xxx」模式
            m = re.search(r'[-–—]+\s*(\S{2,20})\s*$', line)
            if m:
                source = m.group(1).strip()
            # 如果已知官方号名，过滤非官方来源
            if official_name and source and official_name.lower() not in source.lower():
                continue

            articles.append({
                "title": line[:100],
                "abstract": line[:200],
                "source": source,
                "time": "",
            })
            if len(articles) >= limit:
                break

        if official_name and not articles:
            logger.info(f"[社交媒体] 未找到 {official_name} 的公众号文章（搜索结果中无该号文章）")
        return articles
    except Exception as e:
        logger.debug(f"[社交媒体] 搜索公众号文章失败: {e}")
        return []


def _format_wechat_text(articles: list, account: Optional[dict] = None) -> str:
    """将公众号信息格式化为文本块"""
    if not articles and not account:
        return ""
    tag = tier_tag(TIER.T1)
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
