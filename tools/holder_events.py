# -*- coding: utf-8 -*-
"""
股东筹码与事件日历（个股分析的盲区补齐）：
1. 公告事件：减持/增持/解禁/回购/质押（巨潮公告标题识别，最硬信源）
2. 股东户数变化：筹码集中/分散（东财接口）
3. 限售解禁队列：未来的已知抛压（东财接口）
4. 分红送配：临近的除权除息日（东财接口）
全部 guarded：接口名随 akshare 版本漂移时记 warning 跳过，绝不阻断分析。
"""

import re
from typing import List, Optional

from utils.logger import logger

# 股东/筹码类公告标题特征
_HOLDER_EVENT_RE = re.compile(r"(减持|增持|解除限售|限售.*上市流通|解禁|回购|质押|司法冻结)")
_HOLDER_EXCLUDE_RE = re.compile(r"(取消|更正前|英文)")


def pick_holder_announcements(announcements: List[dict], limit: int = 8) -> List[dict]:
    """从公告列表挑股东/筹码类事件（纯函数）"""
    out = []
    for it in announcements:
        title = it.get("title", "")
        if _HOLDER_EVENT_RE.search(title) and not _HOLDER_EXCLUDE_RE.search(title):
            out.append(it)
        if len(out) >= limit:
            break
    return out


def _df_block(title: str, df, rows: int = 5, max_len: int = 700) -> str:
    """把接口 DataFrame 的前几行渲染成文本块（列名随版本变也能展示真实数据）"""
    try:
        if df is None or df.empty:
            return ""
        text = df.head(rows).to_string(index=False)
        return f"◇ {title}\n{text[:max_len]}"
    except Exception:
        return ""


def _try_call(fn_names: List[str], **kwargs):
    """按名字列表尝试 akshare 接口（新旧版本名兼容），全失败返回 None"""
    import akshare as ak
    for fname in fn_names:
        fn = getattr(ak, fname, None)
        if fn is None:
            continue
        try:
            return fn(**kwargs)
        except TypeError:
            try:
                return fn(*kwargs.values())
            except Exception:
                continue
        except Exception as e:
            logger.warning(f"[股东筹码] {fname} 调用失败: {e}")
    return None


def fetch_holder_events_text(code: str, name: str = "") -> str:
    """股东筹码与事件日历文本块；每个子源独立容错"""
    blocks = []

    # 1. 公告事件（减持/解禁/回购…）——权威信源
    try:
        from tools.info_sources import fetch_stock_announcements
        events = pick_holder_announcements(fetch_stock_announcements(code, days=90, limit=40))
        if events:
            lines = ["◇ 股东/筹码类公告（近90天，权威口径）"]
            lines += [f"  - [{e.get('time', '')}] {e.get('title', '')}" for e in events]
            blocks.append("\n".join(lines))
    except Exception as e:
        logger.warning(f"[股东筹码] 公告事件获取失败 {code}: {e}")

    # 2. 分红送配（留意临近的除权除息日）
    # 注意：股东户数和限售解禁已在 stock_capital_fetcher.fetch_all_capital_data() 中统一提供，
    # 此处不再重复获取，避免LLM输入冗余。
    try:
        block = _df_block("分红送配（留意临近的除权除息日）",
                          _try_call(["stock_fhps_detail_em"], symbol=code))
        if block:
            blocks.append(block)
    except Exception as e:
        logger.warning(f"[股东筹码] 分红送配获取失败 {code}: {e}")

    if not blocks:
        return ""
    return "【股东筹码与事件日历（程序拉取；减持/解禁是已知的确定性抛压，必须纳入风险）】\n" \
           + "\n\n".join(blocks)
