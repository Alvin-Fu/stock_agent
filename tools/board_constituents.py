# -*- coding: utf-8 -*-
"""
东财板块成分股（产业链候选发现的主源）：
候选公司靠"搜索→LLM抽取"发现，搜索引擎降级时候选池直接塌缩（白酒在 DDG 兜底下
只筛出3家，五粮液/泸州老窖都没进池）。板块成分股是权威全集且免 key，
把"找到谁"从搜索依赖里解放出来——搜索只负责定性证据，不再决定候选覆盖。

匹配策略与 industry_index 一致：先概念板块后行业板块，精确匹配优先。
全程容错：akshare 不可用/匹配不到返回 None，不阻断产业链分析。
"""

from typing import Dict, List, Optional

import pandas as pd

from utils.logger import logger
from tools.industry_index import _match_board_name


def fetch_board_constituents(industry: str, top_n: int = 30) -> Optional[Dict]:
    """
    取行业对应板块的成分股（按总市值降序，最多 top_n 家）：东财优先，新浪兜底
    （东财 push2 行情域名在部分网络被按 SNI 掐断，新浪板块接口走的是另一族域名）。
    返回 {"board": 板块名, "kind": 来源+概念/行业, "total": N, "stocks": [{"name","code","mv_yi"}...]}；
    失败/未匹配返回 None。
    """
    try:
        import akshare as ak
    except ImportError:
        logger.warning("[板块成分股] akshare 未安装，跳过")
        return None

    data = _fetch_em(ak, industry, top_n) or _fetch_sina(ak, industry, top_n)
    from tools.source_health import report_source
    if data:
        report_source("板块成分股", True)
    else:
        report_source("板块成分股", False, "各来源均未匹配到对应板块")
        logger.info(f"[板块成分股] 各来源均未匹配到「{industry}」对应板块的成分股")
    return data


def _fetch_em(ak, industry: str, top_n: int) -> Optional[Dict]:
    """东财概念/行业板块成分股"""
    sources = (
        ("东财概念", getattr(ak, "stock_board_concept_name_em", None),
         getattr(ak, "stock_board_concept_cons_em", None)),
        ("东财行业", getattr(ak, "stock_board_industry_name_em", None),
         getattr(ak, "stock_board_industry_cons_em", None)),
    )
    for kind, fetch_names, fetch_cons in sources:
        if fetch_names is None or fetch_cons is None:
            continue
        try:
            names_df = fetch_names()
            if names_df is None or names_df.empty or "板块名称" not in names_df.columns:
                continue
            board = _match_board_name(names_df["板块名称"].tolist(), industry)
            if not board:
                continue
            cons = fetch_cons(symbol=board)
            if cons is None or cons.empty or "代码" not in cons.columns or "名称" not in cons.columns:
                continue
            stocks = _extract_stocks(cons, top_n)
            if stocks:
                logger.info(f"[板块成分股] {industry} → {kind}「{board}」共{len(cons)}家，取市值前{len(stocks)}家")
                return {"board": board, "kind": kind, "total": len(cons), "stocks": stocks}
        except Exception as e:
            logger.warning(f"[板块成分股] {kind}获取失败（{industry}）: {e}")
    return None


def _fetch_sina(ak, industry: str, top_n: int) -> Optional[Dict]:
    """新浪概念/行业板块成分股（东财不通时的兜底；概念覆盖比东财粗但域名族不同）"""
    for indicator in ("概念", "行业"):
        try:
            spot = ak.stock_sector_spot(indicator=indicator)
            if spot is None or spot.empty or "板块" not in spot.columns or "label" not in spot.columns:
                continue
            board = _match_board_name(spot["板块"].tolist(), industry)
            if not board:
                continue
            label = str(spot.loc[spot["板块"] == board, "label"].iloc[0])
            det = ak.stock_sector_detail(sector=label)
            if det is None or det.empty or "code" not in det.columns or "name" not in det.columns:
                continue
            stocks = _extract_stocks_sina(det, top_n)
            if stocks:
                kind = f"新浪{indicator}"
                logger.info(f"[板块成分股] {industry} → {kind}「{board}」共{len(det)}家，取市值前{len(stocks)}家")
                return {"board": board, "kind": kind, "total": len(det), "stocks": stocks}
        except Exception as e:
            logger.warning(f"[板块成分股] 新浪{indicator}获取失败（{industry}）: {e}")
    return None


def _extract_stocks_sina(det: pd.DataFrame, top_n: int) -> List[Dict]:
    """新浪成分明细：code/name/mktcap（万元）→ 统一结构；名称里的全角空格要清掉（"五 粮 液"）"""
    df = det.copy()
    if "mktcap" in df.columns:
        df["_mv"] = pd.to_numeric(df["mktcap"], errors="coerce")
        df = df.sort_values("_mv", ascending=False)
    stocks = []
    for _, row in df.head(top_n).iterrows():
        code = str(row.get("code") or "").strip().zfill(6)
        name = str(row.get("name") or "").replace(" ", "").strip()
        if not code.isdigit() or not name:
            continue
        mv_yi = None
        if "_mv" in df.columns and pd.notna(row.get("_mv")):
            mv_yi = round(float(row["_mv"]) / 1e4, 1)  # 万元 → 亿元
        stocks.append({"name": name, "code": code, "mv_yi": mv_yi})
    return stocks


def _extract_stocks(cons: pd.DataFrame, top_n: int) -> List[Dict]:
    """从成分股表提取 名称/代码/总市值(亿)，按市值降序取前 top_n；市值列缺失时按原顺序"""
    df = cons.copy()
    mv_col = next((c for c in ("总市值", "流通市值") if c in df.columns), None)
    if mv_col:
        df["_mv"] = pd.to_numeric(df[mv_col], errors="coerce")
        df = df.sort_values("_mv", ascending=False)
    stocks = []
    for _, row in df.head(top_n).iterrows():
        code = str(row.get("代码") or "").strip().zfill(6)
        name = str(row.get("名称") or "").strip()
        if not code.isdigit() or not name:
            continue
        mv_yi = None
        if mv_col and pd.notna(row.get("_mv")):
            mv_yi = round(float(row["_mv"]) / 1e8, 1)
        stocks.append({"name": name, "code": code, "mv_yi": mv_yi})
    return stocks


def format_board_constituents(data: Optional[Dict]) -> str:
    """格式化为 prompt 文本块（供候选抽取用的权威名单）；无数据返回空串"""
    if not data or not data.get("stocks"):
        return ""
    lines = [f"【{data['kind']}板块「{data['board']}」成分股（权威候选池，共{data['total']}家，"
             f"按总市值取前{len(data['stocks'])}家）】"]
    for s in data["stocks"]:
        mv = f" 市值{s['mv_yi']}亿" if s.get("mv_yi") is not None else ""
        lines.append(f"  {s['name']}({s['code']}){mv}")
    return "\n".join(lines)
