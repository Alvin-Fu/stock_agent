# -*- coding: utf-8 -*-
"""
资金筹码数据获取工具（Akshare 实现）：
1. 北向资金持仓 - 看外资态度
2. 两融余额 - 看杠杆资金情绪
3. 股东户数 - 看筹码集中/分散
4. 限售解禁 - 看未来已知抛压
5. 机构持仓 - 看机构配置方向
全部 guarded：每个 API 独立 try/except，失败返回空字典/空列表，绝不阻断分析。
"""

import time as _time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Callable

from utils.logger import logger

# ---- 进程级内存缓存（同一次分析 + 30分钟短缓存） ----
_capital_cache: Dict[str, tuple] = {}  # key → (data, expiry_ts)
_CACHE_TTL_MINUTES = 30


def _cache_key(func_name: str, codes_str: str) -> str:
    return f"{func_name}:{codes_str}"


def _with_retry(fn: Callable, max_retries: int = 3, retry_delay: float = 1.0) -> Any:
    """带指数退避的重试装饰器"""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                delay = retry_delay * (2 ** (attempt - 1))
                logger.debug(f"调用失败，{delay:.0f}s后重试（{attempt}/{max_retries}）: {e}")
                _time.sleep(delay)
    raise last_exc


def _cached_fetch(func_name: str, codes: List[str],
                  fetch_fn: Callable[[], Dict]) -> Dict:
    """缓存优先的获取函数"""
    cache_key = _cache_key(func_name, str(codes))
    now = _time.time()
    if cache_key in _capital_cache:
        data, expiry = _capital_cache[cache_key]
        if now < expiry:
            return data
    try:
        data = _with_retry(fetch_fn)
        _capital_cache[cache_key] = (data, now + _CACHE_TTL_MINUTES * 60)
        return data
    except Exception as e:
        logger.debug(f"[缓存] {func_name} 获取失败（返回空）: {e}")
        return {}


def _cache_key_str(func_name: str, codes: List[str]) -> str:
    return f"{func_name}:{','.join(sorted(codes))}"


def _em_symbol(code: str) -> str:
    """6位代码转东财带市场前缀写法（部分接口需要）"""
    if code.startswith("6"):
        return f"SH{code}"
    if code.startswith(("0", "3")):
        return f"SZ{code}"
    return f"BJ{code}"


def _is_sh(code: str) -> bool:
    """判断是否为沪市代码（6 开头，含科创板 688）"""
    return code.startswith(("6", "9"))


def _is_sz(code: str) -> bool:
    """判断是否为深市代码（0、3 开头）"""
    return code.startswith(("0", "3"))


def _safe_float(val, default=None):
    """安全转 float"""
    if val is None:
        return default
    try:
        v = float(val)
        if v != v:  # NaN
            return default
        return v
    except (TypeError, ValueError):
        return default


def fetch_north_bound_holdings(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    北向资金持仓数据（沪深港通个股持股）。
    数据来源：东方财富（通过 akshare stock_hsgt_individual_em）。
    含缓存 + 重试，返回 {code: {"shares": 持股量, "ratio": 持股占流通A股比%, "value": 持股市值}}。
    """
    if not codes:
        return {}

    def _do_fetch():
        import akshare as ak
        f = getattr(ak, "stock_hsgt_individual_em", None)
        if f is None:
            return {}
        df = f()
        if df is None or df.empty:
            return {}

        col_map = {}
        for col in df.columns:
            cl = str(col).strip()
            if "代码" in cl:
                col_map["code"] = col
            elif "持股量" in cl:
                col_map["shares"] = col
            elif "占流通" in cl:
                col_map["ratio"] = col
            elif "持股市值" in cl or cl.endswith("市值"):
                col_map["value"] = col
        if "code" not in col_map:
            return {}

        result = {}
        for code in codes:
            try:
                row = df[df[col_map["code"]].astype(str).str.strip() == code]
                if row.empty:
                    continue
                latest = row.iloc[-1]
                result[code] = {
                    "shares": _safe_float(latest.get(col_map.get("shares")), 0),
                    "ratio": _safe_float(latest.get(col_map.get("ratio")), 0),
                    "value": _safe_float(latest.get(col_map.get("value")), 0),
                }
            except Exception:
                continue
        return result

    return _cached_fetch("north_bound", codes, _do_fetch)


def fetch_margin_data(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    两融余额数据（融资融券）。
    优先使用 Tushare margin_detail API，失败时降级到 Akshare SSE/SZSE。
    含缓存 + 重试，返回 {code: {"margin_buy": 融资余额, "short_sell": 融券余额, "net": 融资净买入}}。
    """
    if not codes:
        return {}

    def _try_tushare() -> Dict[str, Dict[str, Any]]:
        """通过 Tushare margin_detail 获取两融数据"""
        try:
            from tools.stock.tushare_fetcher import TushareFetcher
            tf = TushareFetcher()
            result = {}
            for code in codes:
                try:
                    df = tf.margin_detail(code, trade_date='',
                                          start_date=(datetime.now()-timedelta(days=7)).strftime("%Y%m%d"),
                                          end_date=datetime.now().strftime("%Y%m%d"))
                    if df is not None and not df.empty:
                        latest = df.iloc[-1]
                        result[code] = {
                            "margin_buy": _safe_float(latest.get("rzye"), 0),
                            "short_sell": _safe_float(latest.get("rqye"), 0),
                            "net": _safe_float(latest.get("rzmre"), 0),
                        }
                except Exception:
                    continue
            if result:
                logger.info(f"[两融] Tushare 获取成功，{len(result)}只")
            return result
        except Exception as e:
            logger.debug(f"[两融] Tushare获取失败，降级到Akshare: {e}")
            return {}

    def _do_fetch():
        # 优先 Tushare
        ts_result = _try_tushare()
        if ts_result:
            return ts_result

        # 降级到 Akshare
        import akshare as ak
        today = datetime.now()
        date_str = today.strftime("%Y%m%d")
        result: Dict[str, Dict[str, Any]] = {}

        sse_fn = getattr(ak, "stock_margin_detail_sse", None)
        if sse_fn is not None:
            try:
                df_sh = sse_fn(date=date_str)
                if df_sh is not None and not df_sh.empty:
                    code_col = rzye_col = rqye_col = rzmre_col = None
                    for col in df_sh.columns:
                        cl = str(col).strip()
                        if "证券代码" in cl or "标的证券代码" in cl or "代码" in cl:
                            code_col = col
                        elif "融资余额" in cl: rzye_col = col
                        elif "融券余额" in cl: rqye_col = col
                        elif "融资买入" in cl: rzmre_col = col
                    if code_col:
                        for code in codes:
                            if not _is_sh(code): continue
                            try:
                                row = df_sh[df_sh[code_col].astype(str).str.strip() == code]
                                if row.empty: continue
                                r = row.iloc[-1]
                                result[code] = {"margin_buy": _safe_float(r.get(rzye_col),0),
                                                "short_sell": _safe_float(r.get(rqye_col),0),
                                                "net": _safe_float(r.get(rzmre_col),0)}
                            except Exception: continue
            except Exception as e:
                logger.debug("[两融] 沪市获取失败: %s", e)

        szse_fn = getattr(ak, "stock_margin_detail_szse", None)
        if szse_fn is not None:
            try:
                df_sz = szse_fn(date=date_str)
                if df_sz is not None and not df_sz.empty:
                    code_col = rzye_col = rqye_col = rzmre_col = None
                    for col in df_sz.columns:
                        cl = str(col).strip()
                        if "证券代码" in cl or "标的证券代码" in cl or "代码" in cl:
                            code_col = col
                        elif "融资余额" in cl: rzye_col = col
                        elif "融券余额" in cl: rqye_col = col
                        elif "融资买入" in cl: rzmre_col = col
                    if code_col:
                        for code in codes:
                            if not _is_sz(code) or code in result: continue
                            try:
                                row = df_sz[df_sz[code_col].astype(str).str.strip() == code]
                                if row.empty: continue
                                r = row.iloc[-1]
                                result[code] = {"margin_buy": _safe_float(r.get(rzye_col),0),
                                                "short_sell": _safe_float(r.get(rqye_col),0),
                                                "net": _safe_float(r.get(rzmre_col),0)}
                            except Exception: continue
            except Exception as e:
                logger.debug("[两融] 深市获取失败: %s", e)
        return result

    return _cached_fetch("margin", codes, _do_fetch)


def fetch_shareholder_count(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    股东户数数据。
    数据来源：ak.stock_zh_a_gdhs()
    含缓存 + 重试，返回 {code: {"holders": 最新股东户数, "change_pct": 变化幅度%, "trend": 变化方向}}。
    """
    if not codes:
        return {}

    def _do_fetch():
        import akshare as ak
        f = getattr(ak, "stock_zh_a_gdhs", None)
        if f is None:
            return {}
        result = {}
        for code in codes:
            try:
                df = f(symbol=code)
                if df is None or df.empty:
                    continue
                holders_col = change_pct_col = trend_col = None
                for col in df.columns:
                    cl = str(col).strip()
                    if "股东户数" in cl or "股东人数" in cl:
                        holders_col = col
                    elif "变化" in cl and ("%" in cl or "幅度" in cl or "比例" in cl):
                        change_pct_col = col
                    elif "变化" in cl and ("方向" in cl or "趋势" in cl):
                        trend_col = col
                latest = df.iloc[-1]
                result[code] = {
                    "holders": _safe_float(latest.get(holders_col), 0) if holders_col else 0,
                    "change_pct": _safe_float(latest.get(change_pct_col), 0) if change_pct_col else 0,
                    "trend": str(latest.get(trend_col, "")).strip() if trend_col else "未知",
                }
            except Exception:
                continue
        return result

    return _cached_fetch("shareholder", codes, _do_fetch)


def fetch_lockup_calendar(codes: List[str], months: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    """
    限售解禁日历。
    数据来源：ak.stock_ggtj_jj_sjll()
    筛选未来 months 个月内，含缓存 + 重试。
    """
    if not codes:
        return {}

    def _do_fetch():
        import akshare as ak
        f = getattr(ak, "stock_ggtj_jj_sjll", None)
        if f is None: return {}
        df = f()
        if df is None or df.empty: return {}

        code_col = date_col = shares_col = value_col = None
        for col in df.columns:
            cl = str(col).strip()
            if "代码" in cl or "股票代码" in cl: code_col = col
            elif "日期" in cl or "解禁时间" in cl or "解禁日" in cl: date_col = col
            elif "解禁数量" in cl or "解禁股数" in cl: shares_col = col
            elif "市值" in cl or "解禁市值" in cl: value_col = col
        if code_col is None or date_col is None:
            return {}

        now = datetime.now()
        end_date = now + timedelta(days=30 * months)
        code_set = set(codes)
        result: Dict[str, List[Dict[str, Any]]] = {}
        for _, row in df.iterrows():
            try:
                code = str(row.get(code_col, "")).strip()
                if code not in code_set: continue
                dv = row.get(date_col)
                if dv is None: continue
                dt = dv if isinstance(dv, datetime) else datetime.strptime(str(dv)[:10], "%Y-%m-%d")
                if dt < now or dt > end_date: continue
                if code not in result: result[code] = []
                result[code].append({"date": dt.strftime("%Y-%m-%d"),
                                     "shares": _safe_float(row.get(shares_col),0) if shares_col else 0,
                                     "value": _safe_float(row.get(value_col),0) if value_col else 0})
            except Exception: continue
        for code in result: result[code].sort(key=lambda x: x["date"])
        return result

    return _cached_fetch("lockup", codes, _do_fetch)


def fetch_institution_holdings(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    机构持仓数据。
    数据来源：ak.stock_institute_hold()（如果可用）
    含缓存 + 重试，返回 {code: {"fund_count": 基金家数, "fund_ratio": 基金持股比例%,
                                 "institution_count": 机构家数, "institution_ratio": 机构持股比例%}}。
    """
    if not codes:
        return {}

    def _do_fetch():
        import akshare as ak
        f = getattr(ak, "stock_institute_hold", None)
        if f is None: return {}
        df = f()
        if df is None or df.empty: return {}
        code_col = fund_count_col = fund_ratio_col = inst_count_col = inst_ratio_col = None
        for col in df.columns:
            cl = str(col).strip()
            if "代码" in cl or "股票代码" in cl: code_col = col
            elif "基金" in cl and ("家数" in cl or "数量" in cl or "持有" in cl): fund_count_col = col
            elif "基金" in cl and ("比例" in cl or "占比" in cl): fund_ratio_col = col
            elif "机构" in cl and ("家数" in cl or "数量" in cl or "持有" in cl): inst_count_col = col
            elif "机构" in cl and ("比例" in cl or "占比" in cl): inst_ratio_col = col
        if code_col is None: return {}
        result = {}
        for code in codes:
            try:
                row = df[df[code_col].astype(str).str.strip() == code]
                if row.empty: continue
                r = row.iloc[-1]
                result[code] = {"fund_count": _safe_float(r.get(fund_count_col),0) if fund_count_col else 0,
                                "fund_ratio": _safe_float(r.get(fund_ratio_col),0) if fund_ratio_col else 0,
                                "institution_count": _safe_float(r.get(inst_count_col),0) if inst_count_col else 0,
                                "institution_ratio": _safe_float(r.get(inst_ratio_col),0) if inst_ratio_col else 0}
            except Exception: continue
        return result

    return _cached_fetch("institution", codes, _do_fetch)


def fetch_all_capital_data(codes: List[str]) -> str:
    """
    统一入口：调用以上所有函数，组装成格式化的文本块。
    每节标注"根据Akshare"，适用于注入 LLM prompt。
    任何子模块失败不影响整体输出。
    """
    if not codes:
        return ""

    blocks = []

    # 1. 北向资金
    try:
        nb = fetch_north_bound_holdings(codes)
        if nb:
            lines = ["【北向资金持仓（根据Akshare）】"]
            for code in codes:
                if code in nb:
                    d = nb[code]
                    lines.append(
                        f"  - {code}: 持股量{_fmt_num(d['shares'])}股, "
                        f"占流通A股{d['ratio']:.2f}%, "
                        f"持股市值{_fmt_num(d['value'])}元"
                    )
            if len(lines) > 1:
                blocks.append("\n".join(lines))
    except Exception as e:
        logger.debug("[统一入口] 北向资金组装失败: %s", e)

    # 2. 两融余额
    try:
        mg = fetch_margin_data(codes)
        if mg:
            lines = ["【两融余额（根据Akshare）】"]
            for code in codes:
                if code in mg:
                    d = mg[code]
                    lines.append(
                        f"  - {code}: 融资余额{_fmt_num(d['margin_buy'])}元, "
                        f"融券余额{_fmt_num(d['short_sell'])}元, "
                        f"融资净买入{_fmt_num(d['net'])}元"
                    )
            if len(lines) > 1:
                blocks.append("\n".join(lines))
    except Exception as e:
        logger.debug("[统一入口] 两融组装失败: %s", e)

    # 3. 股东户数
    try:
        sh = fetch_shareholder_count(codes)
        if sh:
            lines = ["【股东户数（根据Akshare）】"]
            for code in codes:
                if code in sh:
                    d = sh[code]
                    lines.append(
                        f"  - {code}: 最新户数{_fmt_num(d['holders'])}户, "
                        f"变化幅度{d['change_pct']:.2f}%, "
                        f"趋势{d['trend']}"
                    )
            if len(lines) > 1:
                blocks.append("\n".join(lines))
    except Exception as e:
        logger.debug("[统一入口] 股东户数组装失败: %s", e)

    # 4. 限售解禁
    try:
        lk = fetch_lockup_calendar(codes)
        if lk:
            lines = ["【限售解禁日历（根据Akshare）】"]
            for code in codes:
                if code in lk:
                    for item in lk[code]:
                        lines.append(
                            f"  - {code}: 解禁日{item['date']}, "
                            f"解禁数量{_fmt_num(item['shares'])}股, "
                            f"解禁市值{_fmt_num(item['value'])}元"
                        )
            if len(lines) > 1:
                blocks.append("\n".join(lines))
    except Exception as e:
        logger.debug("[统一入口] 限售解禁组装失败: %s", e)

    # 5. 机构持仓
    try:
        ih = fetch_institution_holdings(codes)
        if ih:
            lines = ["【机构持仓（根据Akshare）】"]
            for code in codes:
                if code in ih:
                    d = ih[code]
                    lines.append(
                        f"  - {code}: 基金{d['fund_count']}家持有{d['fund_ratio']:.2f}%, "
                        f"机构{d['institution_count']}家持有{d['institution_ratio']:.2f}%"
                    )
            if len(lines) > 1:
                blocks.append("\n".join(lines))
    except Exception as e:
        logger.debug("[统一入口] 机构持仓组装失败: %s", e)

    return "\n\n".join(blocks) if blocks else ""


def _fmt_num(val) -> str:
    """格式化大数字，eg 1234567 -> 123.46万"""
    if val is None or val == 0:
        return "0"
    abs_val = abs(val)
    if abs_val >= 1e8:
        return f"{val / 1e8:.2f}亿"
    if abs_val >= 1e4:
        return f"{val / 1e4:.2f}万"
    return f"{val:.2f}"
