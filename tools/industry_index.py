# -*- coding: utf-8 -*-
"""
行业/概念板块指数表现（产业链分析用）：
候选池样本太小时（如只筛出2家），拿它当行业代理会失真——东财概念/行业板块指数
才是行业 beta 的权威事实。程序算近5/20/60日涨幅与年内位置，LLM 只解读。

匹配策略：先概念板块后行业板块，板块名先精确匹配再包含匹配（"商业航天"是东财概念板块名，
但用户输入可能是"商业航天产业链"这类变体）。全程容错：akshare 不可用/匹配不到返回 None。
"""

from datetime import date, timedelta
from typing import Dict, Optional

import pandas as pd

from utils.logger import logger


def _match_board_name(names, industry: str) -> Optional[str]:
    """板块名匹配：精确 → 双向包含；都不中返回 None"""
    names = [str(n) for n in names if n]
    for n in names:
        if n == industry:
            return n
    for n in names:
        if industry in n or n in industry:
            return n
    return None


def _compute_metrics(closes) -> Optional[Dict]:
    """closes 为时间升序收盘序列；算近5/20/60日涨幅与近一年位置"""
    closes = [float(c) for c in closes if c is not None]
    if len(closes) < 21:
        return None
    latest = closes[-1]

    def _ret(n):
        if len(closes) > n and closes[-1 - n]:
            return round((latest / closes[-1 - n] - 1) * 100, 1)
        return None

    year = closes[-244:]
    lo, hi = min(year), max(year)
    pos = round((latest - lo) / (hi - lo) * 100, 1) if hi > lo else None
    return {"ret5": _ret(5), "ret20": _ret(20), "ret60": _ret(60), "pos_52w": pos}


def _closes_from_hist(hist) -> Optional[list]:
    """从行情表提取收盘序列（东财列名「收盘」，同花顺「收盘价」）"""
    if hist is None or getattr(hist, "empty", True):
        return None
    col = next((c for c in ("收盘", "收盘价", "close") if c in hist.columns), None)
    if not col:
        return None
    return pd.to_numeric(hist[col], errors="coerce").dropna().tolist()


def fetch_industry_index_metrics(industry: str) -> Optional[Dict]:
    """
    取行业对应的板块指数表现：东财概念/行业板块优先，同花顺兜底
    （东财 push2 行情域名在部分网络被按 SNI 掐断，曾致健康行常年"行业指数✗"）。
    返回 {"board": 板块名, "kind": 来源+概念/行业, "ret5", "ret20", "ret60", "pos_52w"}；失败返回 None。
    """
    try:
        import akshare as ak
    except ImportError:
        logger.warning("[行业指数] akshare 未安装，跳过")
        return None

    start = (date.today() - timedelta(days=400)).strftime("%Y%m%d")
    end = date.today().strftime("%Y%m%d")
    sources = (
        ("东财概念", "stock_board_concept_name_em", "stock_board_concept_hist_em",
         "板块名称", {"period": "daily", "adjust": ""}),
        ("东财行业", "stock_board_industry_name_em", "stock_board_industry_hist_em",
         "板块名称", {"period": "daily", "adjust": ""}),
        ("同花顺行业", "stock_board_industry_name_ths", "stock_board_industry_index_ths",
         "name", {}),
        ("同花顺概念", "stock_board_concept_name_ths", "stock_board_concept_index_ths",
         "name", {}),
    )
    for kind, names_fname, hist_fname, name_col, hist_kwargs in sources:
        fetch_names = getattr(ak, names_fname, None)
        fetch_hist = getattr(ak, hist_fname, None)
        if fetch_names is None or fetch_hist is None:
            continue
        try:
            names_df = fetch_names()
            if names_df is None or names_df.empty or name_col not in names_df.columns:
                continue
            board = _match_board_name(names_df[name_col].tolist(), industry)
            if not board:
                continue
            hist = fetch_hist(symbol=board, start_date=start, end_date=end, **hist_kwargs)
            closes = _closes_from_hist(hist)
            metrics = _compute_metrics(closes) if closes else None
            if metrics:
                metrics.update(board=board, kind=kind)
                logger.info(f"[行业指数] {industry} → {kind}板块「{board}」: {metrics}")
                from tools.source_health import report_source
                report_source("行业指数", True)
                return metrics
        except Exception as e:
            logger.warning(f"[行业指数] {kind}板块获取失败（{industry}）: {e}")
    logger.info(f"[行业指数] 各来源均未匹配到「{industry}」对应的板块指数")
    from tools.source_health import report_source
    report_source("行业指数", False, "未匹配到对应板块")
    return None


def format_industry_index(metrics: Optional[Dict]) -> str:
    """格式化为 prompt 文本块；无数据返回空串"""
    if not metrics:
        return ""

    def _pct(v):
        return f"{v:+.1f}%" if v is not None else "-"

    lines = [f"【行业指数表现（{metrics['kind']}板块「{metrics['board']}」，程序计算）】",
             f"  近5日 {_pct(metrics.get('ret5'))}｜近20日 {_pct(metrics.get('ret20'))}"
             f"｜近60日 {_pct(metrics.get('ret60'))}"
             + (f"｜近一年位置 {metrics['pos_52w']}%（0=最低,100=最高）"
                if metrics.get("pos_52w") is not None else "")]
    lines.append("  ⚠️ 使用规则：这是行业 beta 的事实描述，不构成预测；近20日涨幅超过15%"
                 "要在风险里提示短期过热与追高风险，指数与个股结论必须互相印证"
                 "（指数在涨但候选个股不涨=个股问题，反之=行业 beta 拉动）")
    return "\n".join(lines)
