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
_FLOAT_SHARE_CACHE: Dict[str, float] | None = None  # code → 流通股本(万股)
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


def _load_float_shares() -> Dict[str, float]:
    """从 Tushare stock_basic 获取流通股本（万股），缓存到模块变量"""
    if _FLOAT_SHARE_CACHE is not None:
        return _FLOAT_SHARE_CACHE
    try:
        import tushare as ts
        df = ts.pro_api().stock_basic()
        m = {}
        for _, r in df.iterrows():
            code = str(r.get("ts_code", ""))[:6]
            fs = _safe_float(r.get("float_share"))
            if code and fs:
                m[code] = fs  # 万股
        _FLOAT_SHARE_CACHE = m if m else {}
        logger.info(f"[流通股本] 已加载 {len(_FLOAT_SHARE_CACHE)} 只")
    except Exception as e:
        logger.debug(f"[流通股本] 加载失败: {e}")
        _FLOAT_SHARE_CACHE = {}
    return _FLOAT_SHARE_CACHE


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
    数据来源：优先 Tushare hk_hold，降级到 akshare stock_hsgt_individual_em。
    含缓存 + 重试，返回 {code: {"shares": 持股量, "ratio": 持股占流通A股比%, "value": 持股市值}}。
    """
    if not codes:
        return {}

    def _try_tushare() -> Dict[str, Dict[str, Any]]:
        """通过 Tushare hk_hold 获取北向资金持仓（ratio 重算为占流通A股比）"""
        try:
            from tools.stock.tushare_fetcher import TushareFetcher
            from datetime import datetime, timedelta
            tf = TushareFetcher()
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
            float_shares = _load_float_shares()  # code → 万股
            result = {}
            for code in codes:
                try:
                    ts_code = (f"{code}.SZ" if code.startswith(("0", "3"))
                               else (f"{code}.BJ" if code.startswith(("8", "4", "92"))
                                     else f"{code}.SH"))
                    df = tf.hk_hold(ts_code, start, end)
                    if df is not None and not df.empty:
                        latest = df.iloc[-1]
                        vol = _safe_float(latest.get("vol"), 0)
                        # Tushare ratio 是按A股总股本算的，重算为占流通A股比
                        fs = float_shares.get(code)  # 万股
                        if fs and vol:
                            calc_ratio = vol / (fs * 10000) * 100
                        else:
                            calc_ratio = _safe_float(latest.get("ratio"), 0)
                        result[code] = {
                            "shares": vol,
                            "ratio": round(calc_ratio, 2),
                            "value": _safe_float(latest.get("vol", 0), 0) * 0,
                        }
                except Exception:
                    continue
            if result:
                logger.info(f"[北向] Tushare 获取成功，{len(result)}只")
            return result
        except Exception as e:
            logger.debug(f"[北向] Tushare获取失败，降级到Akshare: {e}")
            return {}

    def _try_akshare() -> Dict[str, Dict[str, Any]]:
        """通过 akshare stock_hsgt_individual_em 降级获取"""
        try:
            import akshare as ak
            f = getattr(ak, "stock_hsgt_individual_em", None)
            if f is None:
                return {}
            df = f()
            if df is None or df.empty:
                return {}
            # 该接口的列名通常为 ['持股日期','当日收盘价','当日涨跌幅','持股数量','持股市值','持股数量占A股百分比','今日增持股数','今日增持资金','今日持股市值变化']
            # 无代码列，但可以用名称模糊匹配；仅作为最后的降级方案
            name_col = None
            shares_col = None
            ratio_col = None
            value_col = None
            for col in df.columns:
                cl = str(col).strip()
                if "名称" in cl or "name" in cl.lower():
                    name_col = col
                elif "持股数量" == cl or cl == "持股数量":
                    shares_col = col
                elif "占A股" in cl:
                    ratio_col = col
                elif cl == "持股市值":
                    value_col = col
            if name_col is None:
                return {}
            from tools.company_code_validator import find_company_name
            result = {}
            for code in codes:
                try:
                    cname = find_company_name(code)
                    if not cname:
                        continue
                    rows = df[df[name_col].astype(str).str.contains(cname[:4], na=False)]
                    if rows.empty:
                        continue
                    latest = rows.iloc[-1]
                    result[code] = {
                        "shares": _safe_float(latest.get(shares_col), 0) if shares_col else 0,
                        "ratio": _safe_float(latest.get(ratio_col), 0) if ratio_col else 0,
                        "value": _safe_float(latest.get(value_col), 0) if value_col else 0,
                    }
                except Exception:
                    continue
            return result
        except Exception as e:
            logger.debug(f"[北向] Akshare降级失败: {e}")
            return {}

    def _do_fetch():
        result = _try_tushare()
        if result:
            return result
        return _try_akshare()

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
    数据来源：优先 Tushare holdernumber，降级到 ak.stock_zh_a_gdhs()。
    含缓存 + 重试，返回 {code: {"holders": 最新股东户数, "change_pct": 变化幅度%, "trend": 变化方向}}。
    """
    if not codes:
        return {}

    def _try_tushare() -> Dict[str, Dict[str, Any]]:
        """通过 Tushare holdernumber 获取股东户数"""
        try:
            import pandas as pd
            from tools.stock.tushare_fetcher import TushareFetcher
            from datetime import datetime, timedelta
            tf = TushareFetcher()
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
            result = {}
            for code in codes:
                try:
                    ts_code = (f"{code}.SZ" if code.startswith(("0", "3"))
                               else (f"{code}.BJ" if code.startswith(("8", "4", "92"))
                                     else f"{code}.SH"))
                    df = tf.holdernumber(ts_code, start, end)
                    if df is not None and not df.empty:
                        # 按 end_date 降序排列，最新的在前
                        if 'end_date' in df.columns:
                            df = df.sort_values('end_date', ascending=False)
                        # 过滤出有实际数据的行（holder_num 不为 NaN 且 > 0）
                        valid = df[pd.to_numeric(df['holder_num'], errors='coerce') > 0]
                        if not valid.empty:
                            latest = valid.iloc[0]  # 最新的有效行
                            holders = _safe_float(latest.get("holder_num"), 0)
                            # 取下一期（倒数第二新）计算变化
                            prev = valid.iloc[1] if len(valid) >= 2 else None
                            if prev is not None and holders > 0:
                                prev_holders = _safe_float(prev.get("holder_num"), 0)
                                if prev_holders and prev_holders > 0:
                                    change_pct = (holders - prev_holders) / prev_holders * 100
                                else:
                                    change_pct = 0.0
                            else:
                                change_pct = 0.0
                            trend = "集中" if change_pct < -3 else ("分散" if change_pct > 3 else "平稳")
                            result[code] = {
                                "holders": holders,
                                "change_pct": round(change_pct, 2),
                                "trend": trend,
                            }
                except Exception:
                    continue
            if result:
                logger.info(f"[股东户数] Tushare 获取成功，{len(result)}只")
            return result
        except Exception as e:
            logger.debug(f"[股东户数] Tushare获取失败，降级到Akshare: {e}")
            return {}

    def _try_akshare() -> Dict[str, Dict[str, Any]]:
        """通过 akshare stock_zh_a_gdhs 降级获取"""
        try:
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
        except Exception as e:
            logger.debug(f"[股东户数] Akshare降级失败: {e}")
            return {}

    def _do_fetch():
        result = _try_tushare()
        if result:
            return result
        return _try_akshare()

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
    优先从数据库 StockTop10Holder 读取（按机构类型统计基金/一般法人/其他），
    不足时降级到 akshare。
    含缓存 + 重试，返回 {code: {"fund_count": 基金家数, "fund_ratio": 基金持股比例%,
                                 "institution_count": 机构家数, "institution_ratio": 机构持股比例%}}。
    """
    if not codes:
        return {}

    def _try_db() -> Dict[str, Dict[str, Any]]:
        """从数据库 StockTop10Holder 统计机构持仓"""
        try:
            from storage.sqlite.stock_storage import get_db
            db = get_db()
            result = {}
            for code in codes:
                top10 = db.get_stock_top10_holder(code, limit=30)
                if not top10:
                    continue
                fund_count, fund_ratio = 0, 0.0
                inst_count, inst_ratio = 0, 0.0
                for row in top10:
                    holder_type = str(row.get("holder_type") or "").strip()
                    hold_ratio = _safe_float(row.get("hold_ratio"), 0)
                    if "基金" in holder_type:
                        fund_count += 1
                        fund_ratio += hold_ratio
                    elif "机构" in holder_type or "一般法人" in holder_type:
                        inst_count += 1
                        inst_ratio += hold_ratio
                if fund_count > 0 or inst_count > 0:
                    result[code] = {
                        "fund_count": fund_count,
                        "fund_ratio": round(fund_ratio, 2),
                        "institution_count": inst_count,
                        "institution_ratio": round(inst_ratio, 2),
                    }
            if result:
                logger.info(f"[机构持仓] 数据库获取成功，{len(result)}只")
            return result
        except Exception as e:
            logger.debug(f"[机构持仓] 数据库获取失败，降级: {e}")
            return {}

    def _do_fetch():
        # 优先数据库
        db_result = _try_db()
        if db_result:
            return db_result

        # 降级到 akshare
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
    每节标注来源（优先数据库/Tushare，AkShare 降级）。
    任何子模块失败不影响整体输出。
    """
    if not codes:
        return ""

    blocks = []

    # 1. 北向资金（含趋势）
    try:
        nb = fetch_north_bound_holdings(codes)
        if nb:
            lines = ["【北向资金持仓（数据库+Tushare）】"]
            for code in codes:
                if code in nb:
                    d = nb[code]
                    lines.append(
                        f"  - {code}: 持股量{_fmt_num(d['shares'])}股, "
                        f"占流通A股{d['ratio']:.2f}%, "
                        f"持股市值{_fmt_num(d['value'])}元"
                    )
                    # 尝试加趋势：查库内历史北向数据
                    try:
                        from storage.sqlite.stock_storage import get_db
                        db = get_db()
                        hist = db.get_stock_northbound_hold(code)
                        if hist is not None and len(hist) >= 10:
                            hist = hist.sort_values("trade_date")
                            first_ratio = _safe_float(hist.iloc[0].get("ratio"))
                            last_ratio = _safe_float(hist.iloc[-1].get("ratio"))
                            # 用流通股本重算历史 ratio
                            fs = _load_float_shares().get(code)
                            if fs:
                                first_vol = _safe_float(hist.iloc[0].get("vol"))
                                last_vol = _safe_float(hist.iloc[-1].get("vol"))
                                if first_vol and last_vol:
                                    first_ratio_calc = first_vol / (fs * 10000) * 100
                                    last_ratio_calc = last_vol / (fs * 10000) * 100
                                    first_date = str(hist.iloc[0].get("trade_date"))[:10]
                                    last_date = str(hist.iloc[-1].get("trade_date"))[:10]
                                    trend = last_ratio_calc - first_ratio_calc
                                    if trend < -0.5:
                                        lines.append(f"    🔴 趋势判定：外资持续减持（{first_date} {first_ratio_calc:.2f}%→{last_date} {last_ratio_calc:.2f}%，累计变化{trend:.2f}个百分点）")
                                    elif trend > 0.5:
                                        lines.append(f"    🟢 趋势判定：外资持续增持（{first_date} {first_ratio_calc:.2f}%→{last_date} {last_ratio_calc:.2f}%，累计变化{trend:.2f}个百分点）")
                    except Exception as e:
                        logger.debug(f"[北向趋势] {code} 历史查询失败: {e}")
            if len(lines) > 1:
                blocks.append("\n".join(lines))
    except Exception as e:
        logger.debug("[统一入口] 北向资金组装失败: %s", e)

    # 2. 两融余额
    try:
        mg = fetch_margin_data(codes)
        if mg:
            lines = ["【两融余额（数据库+Tushare）】"]
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
            lines = ["【股东户数（数据库+Tushare）】"]
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
            lines = ["【机构持仓（数据库+Tushare）】"]
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
