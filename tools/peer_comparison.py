# -*- coding: utf-8 -*-
"""
整车同业横向对标工具
====================
用于个股分析中自动拉取同业公司的估值、财务数据并生成对标表格。

主要函数 `fetch_peer_comparison()` 会被 researcher_agent.py 的个股模式调用，
注入到 LLM prompt 中作为同业对标数据源。

数据源与指标说明：
  - 成分股 list：akshare 东财行业板块成分股（stock_board_industry_cons_em）
  - PE/PB/市值：东财全市场快照（stock_zh_a_spot_em），与板块成分股交叉匹配
  - 毛利率/营收增速/净利润增速：akshare 财务摘要（stock_financial_abstract）
  - 部分财务指标可能缺失，表中统一显示"数据缺失"而非报错

依赖：akshare, pandas， 从 utils.logger 获取 logger
"""

from typing import List, Optional

import pandas as pd

from utils.logger import logger
from utils.retry_utils import retry_with_multiple_sources


def _fmt_num(val) -> str:
    """格式化大数字为易读的字符串表示。"""
    if val is None:
        return "数据缺失"
    try:
        if pd.isna(val):
            return "数据缺失"
        num = float(val)
        if abs(num) >= 1e8:
            return f"{num / 1e8:.2f}亿"
        elif abs(num) >= 1e4:
            return f"{num / 1e4:.2f}万"
        elif num == int(num):
            return str(int(num))
        return f"{num:.2f}"
    except (TypeError, ValueError):
        return "数据缺失"


def _safe_float(val) -> Optional[float]:
    """安全转 float，NaN/None/异常均返回 None。"""
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _build_spot_map() -> dict:
    """
    拉取全市场快照，构建 code -> {pe, pb, mv, name} 映射。
    支持多数据源重试：东财em -> 腾讯 -> 新浪，失败时返回空 dict。
    """
    def _parse_spot_df(df) -> dict:
        spot = {}
        if df is None or df.empty:
            return spot
        for _, row in df.iterrows():
            code = str(row.get("代码", row.get("股票代码", ""))).strip()
            if len(code) != 6:
                continue
            mv_raw = _safe_float(row.get("总市值", row.get("市值")))
            spot[code] = {
                "pe": _safe_float(row.get("市盈率-动态", row.get("市盈率"))),
                "pb": _safe_float(row.get("市净率")),
                "mv": round(mv_raw / 1e8, 2) if mv_raw else None,
                "name": str(row.get("名称", row.get("股票名称", ""))).strip(),
            }
        return spot

    import akshare as ak

    sources = []

    def _fetch_em():
        df = ak.stock_zh_a_spot_em()
        return _parse_spot_df(df)
    sources.append(("东财em", _fetch_em))

    if hasattr(ak, "stock_zh_a_spot_tx"):
        def _fetch_tencent():
            df = ak.stock_zh_a_spot_tx()
            return _parse_spot_df(df)
        sources.append(("腾讯", _fetch_tencent))

    if hasattr(ak, "stock_zh_a_spot_sina"):
        def _fetch_sina():
            df = ak.stock_zh_a_spot_sina()
            return _parse_spot_df(df)
        sources.append(("新浪", _fetch_sina))

    spot = retry_with_multiple_sources(sources)
    if spot:
        logger.info(f"[同业对标] 全市场快照加载完成，共 {len(spot)} 只股票")
    else:
        logger.warning("[同业对标] 全市场快照获取失败，所有数据源均不可用")
    return spot or {}


def _fetch_financial_indicators(code: str) -> dict:
    """
    通过 akshare 财务摘要获取毛利率、营收增速、净利润增速。
    返回 {"gross_margin": 毛利率(%), "rev_growth": 营收增速(%), "np_growth": 净利润增速(%)}。
    任一指标获取失败该字段为 None。
    """
    result = {"gross_margin": None, "rev_growth": None, "np_growth": None}
    try:
        import akshare as ak
        # stock_financial_abstract 返回多级 DataFrame，包含盈利能力、成长能力等分类
        df = ak.stock_financial_abstract(stock=code)
        if df is None or df.empty:
            return result

        # 尝试从 "盈利能力" 分类中获取毛利率
        try:
            gm_row = df[df["分类"] == "盈利能力"]
            if not gm_row.empty and "毛利率" in gm_row.columns:
                result["gross_margin"] = _safe_float(gm_row["毛利率"].iloc[0])
        except (KeyError, IndexError, AttributeError):
            pass

        # 尝试从 "成长能力" 分类中获取营收增速和净利润增速
        try:
            growth_row = df[df["分类"] == "成长能力"]
            if not growth_row.empty:
                if "营业收入增长率" in growth_row.columns:
                    result["rev_growth"] = _safe_float(growth_row["营业收入增长率"].iloc[0])
                if "净利润增长率" in growth_row.columns:
                    result["np_growth"] = _safe_float(growth_row["净利润增长率"].iloc[0])
        except (KeyError, IndexError, AttributeError):
            pass

        return result
    except ImportError:
        logger.warning("[同业对标] akshare 未安装，无法获取财务指标")
        return result
    except Exception as e:
        logger.debug(f"[同业对标] {code} 财务摘要获取失败: {e}")
        return result


def _try_board_cons_akshare(industry: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """带指数退避重试的 akshare 板块成分股获取，返回 None 表示失败"""
    import time
    for attempt in range(1, max_retries + 1):
        try:
            import akshare as ak
            df = ak.stock_board_industry_cons_em(symbol=industry)
            if df is not None and not df.empty:
                logger.info(f"[同业对标] akshare 板块成分股获取成功（attempt {attempt}）")
                return df
        except Exception as e:
            logger.warning(f"[同业对标] akshare 获取失败（attempt {attempt}/{max_retries}）: {e}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)  # 2s, 4s, 8s
    return None


def _try_board_cons_tushare(industry: str) -> Optional[pd.DataFrame]:
    """
    Tushare 兜底：通过申万行业分类（index_classify + index_member_all）获取同行成分股。
    三级精度（L3 > L2 > L1），支持模糊匹配，token 缺失时静默返回 None。
    """
    try:
        import tushare as ts
        from utils.config import load_config
        cfg = load_config().get("tools", {}).get("stock", {})
        token = cfg.get("tushare_token", "")
        if not token:
            return None
        pro = ts.pro_api(token)
    except Exception as e:
        logger.debug(f"[同业对标] Tushare 初始化失败: {e}")
        return None

    # ---- Step 1: 从最细粒度（L3）逐级向上匹配行业分类 ----
    matched_index_code = None
    matched_name = None
    for level in ('L3', 'L2', 'L1'):
        try:
            df_cls = pro.index_classify(level=level)
            if df_cls is None or df_cls.empty:
                continue
            if 'industry_name' not in df_cls.columns:
                continue
            # 精确匹配
            exact = df_cls[df_cls['industry_name'].str.strip() == industry]
            if not exact.empty:
                matched_index_code = exact.iloc[0]['index_code']
                matched_name = exact.iloc[0]['industry_name']
                logger.info(f"[同业对标] Tushare 行业精确匹配: {matched_name} ({matched_index_code}, {level})")
                break
            # 包含匹配（行业名含输入关键词）
            fuzzy = df_cls[df_cls['industry_name'].str.contains(industry, na=False)]
            if not fuzzy.empty:
                matched_index_code = fuzzy.iloc[0]['index_code']
                matched_name = fuzzy.iloc[0]['industry_name']
                logger.info(f"[同业对标] Tushare 行业模糊匹配: {matched_name} ({matched_index_code}, {level})")
                break
        except Exception as e:
            logger.debug(f"[同业对标] Tushare index_classify({level}) 获取失败: {e}")
            continue

    if not matched_index_code:
        logger.debug(f"[同业对标] Tushare 未匹配到行业「{industry}」的分类代码")
        return None

    # ---- Step 2: 获取该行业的所有成分股 ----
    try:
        df_member = pro.index_member_all(index_code=matched_index_code)
        if df_member is not None and not df_member.empty:
            # index_member_all 返回字段: index_code, stock_code, stock_name, in_date, out_date, is_new
            col_map = {"stock_code": "代码", "stock_name": "名称"}
            df = df_member.rename(columns={k: v for k, v in col_map.items() if k in df_member.columns})
            # 只保留在列的字段
            keep = [v for v in col_map.values() if v in df.columns]
            if keep:
                df = df[keep]
            logger.info(f"[同业对标] Tushare 申万行业成分股获取成功: {matched_name}（{level}），共 {len(df)} 只")
            return df
        else:
            logger.debug(f"[同业对标] Tushare index_member_all({matched_index_code}) 返回空")
            return None
    except Exception as e:
        logger.debug(f"[同业对标] Tushare 获取行业成分失败: {e}")
        return None


def _try_board_cons_cached(stock_code: str, industry: str) -> Optional[pd.DataFrame]:
    """从 DB 缓存读取最近一次成功的成分股数据（24小时内有效）"""
    try:
        from storage.sqlite.stock_storage import get_db
        from datetime import datetime, timedelta
        db = get_db()
        cached = db.get_cached_peer_cons(industry)
        if cached is not None and not cached.empty:
            cached_time = db.get_peer_cons_cache_time(industry)
            if cached_time and (datetime.now() - cached_time) < timedelta(hours=24):
                logger.info(f"[同业对标] 使用缓存成分股（{industry}，{len(cached)}只）")
                return cached
    except Exception as e:
        logger.debug(f"[同业对标] 缓存读取失败: {e}")
    return None


def fetch_peer_comparison(stock_code: str, industry: str = "汽车整车") -> str:
    """
    拉取同行对比对标表格。

    说明：本函数已统一委托至 `tools.peer_compare.fetch_peer_table` 实现，
    后者通过 DB 的 stock_basic 表获取同行（tushare 口径行业），具备线程安全的
    当日快照缓存与 MAX_PEERS 限制，与 `agents/financial_analyst/analyst.py`
    调用的同一实现保持一致，消除项目中两套重复的同行对比逻辑。
    `industry` 参数仅为接口兼容保留，统一实现从 DB 获取行业信息，不再使用。

    Parameters
    ----------
    stock_code : str
        目标个股代码（6 位数字字符串）
    industry : str
        历史遗留参数（东财行业板块名称，默认"汽车整车"）；统一实现从 DB
        stock_basic 表读取行业，此处保留仅为接口兼容，不再参与逻辑。

    Returns
    -------
    str
        格式化的 markdown 对标表格字符串；任一环节失败返回空字符串。
    """
    # 统一实现：委托至 peer_compare.fetch_peer_table，取返回元组的文本部分
    try:
        from tools.peer_compare import fetch_peer_table
        text, _rows = fetch_peer_table(stock_code)
        if text:
            return text
        # fetch_peer_table 返回空字符串（无行业信息/无同行/快照失败等），
        # 保持原有降级行为：返回空字符串
        logger.info(f"[同业对标] fetch_peer_table 未返回内容，{stock_code} 同行对比为空")
        return ""
    except Exception as e:
        logger.warning(f"[同业对标] 委托 fetch_peer_table 失败 {stock_code}: {e}")
        return ""


def _format_table_row(entry: dict, is_target: bool = False) -> str:
    """
    格式化单行表格数据。

    Parameters
    ----------
    entry : dict
        包含 code, name, mv, pe, pb, gross_margin, rev_growth, np_growth 的字典
    is_target : bool
        是否为对标目标公司（添加 ★ 标记）

    Returns
    -------
    str
        格式化后的 markdown 表格行
    """
    prefix = "★ " if is_target else "  "
    name = entry.get("name", "未知")
    code = entry.get("code", "未知")

    mv_val = entry.get("mv")
    mv_str = f"{mv_val:.2f}" if mv_val is not None else "数据缺失"

    pe_val = entry.get("pe")
    pe_str = f"{pe_val:.2f}" if pe_val is not None else "数据缺失"

    pb_val = entry.get("pb")
    pb_str = f"{pb_val:.2f}" if pb_val is not None else "数据缺失"

    gm_val = entry.get("gross_margin")
    gm_str = f"{gm_val:.2f}" if gm_val is not None else "数据缺失"

    rg_val = entry.get("rev_growth")
    rg_str = f"{rg_val:.2f}" if rg_val is not None else "数据缺失"

    ng_val = entry.get("np_growth")
    ng_str = f"{ng_val:.2f}" if ng_val is not None else "数据缺失"

    return f"| {prefix}{name} | {code} | {mv_str} | {pe_str} | {pb_str} | {gm_str} | {rg_str} | {ng_str} |"
