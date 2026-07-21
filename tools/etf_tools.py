# -*- coding: utf-8 -*-
"""
ETF 数据工具集：基于 Akshare + 天天基金 API 获取 ETF 分析所需数据。

稳定性设计：
1. 进程级 TTL 缓存（减少请求频率，防反爬）
2. 双数据源：持仓数据天天基金主源 + Sina 备选
3. 失败友好降级：逐层退化，不报错

数据维度：
- 实时行情 → fund_etf_spot_em (+ Sina 兜底)
- 行业配置 → fund_portfolio_industry_allocation_em
- 持仓穿透 → 天天基金持仓API + Sina 备选
- K线数据 → 复用 akshare_fetcher._fetch_etf_data
"""

import re
import time
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
import requests

from utils.logger import logger

# =========================== TTL 缓存 ===========================

_CACHE: Dict[str, tuple] = {}  # key → (timestamp, data)


def _cache_get(key: str, ttl_seconds: int) -> Optional:
    """获取缓存，过期返回 None"""
    entry = _CACHE.get(key)
    if entry and time.time() - entry[0] < ttl_seconds:
        return entry[1]
    return None


def _cache_set(key: str, data, ttl_seconds: int) -> None:
    """设置缓存"""
    _CACHE[key] = (time.time(), data)


# 缓存 TTL（秒）
_CACHE_SPOT = 3600      # 行情缓存 1 小时
_CACHE_HOLDINGS = 21600  # 持仓缓存 6 小时（季报数据变动慢）
_CACHE_INDUSTRY = 21600  # 行业配置 6 小时

# =========================== 数据源配置 ===========================

# 天天基金持仓 API
_FUND_HOLD_URL = ("https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
                  "?type=jjcc&code={code}&topline=10&year={year}")
_FUND_HOLD_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Referer": "https://fundf10.eastmoney.com/",
}

# Sina 基金持仓备选 API（JSONP）
_SINA_HOLD_URL = (
    "https://vip.stock.finance.sina.com.cn/fund_center/"
    "api/jsonp.php/IO.XSRV.FundJJDX"
    "?callback=&fundcode={code}&page=1"
)


# =========================== 行情数据 ===========================


def fetch_etf_spot(code: str) -> Optional[Dict[str, str]]:
    """获取 ETF 实时行情，带缓存 + 双源兜底"""
    cached = _cache_get(f"spot:{code}", _CACHE_SPOT)
    if cached is not None:
        return cached

    result = _fetch_spot_akshare(code)
    if result is not None:
        _cache_set(f"spot:{code}", result, _CACHE_SPOT)
        return result

    result = _fetch_spot_sina_fallback(code)
    if result is not None:
        _cache_set(f"spot:{code}", result, _CACHE_SPOT)
        return result

    logger.warning(f"[ETF] 行情所有数据源均失败 {code}")
    return None


def _match_etf_code(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """在 fund_etf_spot_em 返回的 DataFrame 中多策略匹配 ETF 代码。"""
    if df is None or df.empty:
        return pd.DataFrame()

    # 策略1：精确字符串匹配（尝试可能存在的代码列名）
    for col in ("代码", "symbol", "fund_code"):
        if col in df.columns:
            match = df[df[col].astype(str) == code]
            if not match.empty:
                return match

    # 策略2：去前导零后匹配
    stripped = code.lstrip("0")
    for col in ("代码", "symbol", "fund_code"):
        if col in df.columns:
            match = df[df[col].astype(str) == stripped]
            if not match.empty:
                return match

    # 策略3：数值匹配（列可能是 int 类型）
    try:
        code_int = int(code)
        for col in ("代码", "symbol", "fund_code"):
            if col in df.columns:
                match = df[df[col] == code_int]
                if not match.empty:
                    return match
    except (ValueError, TypeError):
        pass

    # 策略4：列值包含代码（某些源列带前缀，如 '159740.SZ'）
    for col in ("代码", "symbol", "fund_code"):
        if col in df.columns:
            match = df[df[col].astype(str).str.contains(code, na=False)]
            if not match.empty:
                return match

    return pd.DataFrame()


def _fetch_spot_akshare(code: str) -> Optional[Dict[str, str]]:
    """主源：Akshare fund_etf_spot_em"""
    for attempt in range(3):
        try:
            import akshare as ak
            df = ak.fund_etf_spot_em()
            match = _match_etf_code(df, code)
            if match.empty:
                return None
            row = match.iloc[0]
            return {
                "代码": str(row.get("代码", "")),
                "名称": str(row.get("名称", "")),
                "最新价": str(row.get("最新价", "")),
                "IOPV实时估值": str(row.get("IOPV实时估值", "")),
                "基金折价率": str(row.get("基金折价率", "")),
                "涨跌幅": str(row.get("涨跌幅", "")),
                "成交额": str(row.get("成交额", "")),
                "换手率": str(row.get("换手率", "")),
                "最新份额": str(row.get("最新份额", "")),
                "流通市值": str(row.get("流通市值", "")),
                "振幅": str(row.get("振幅", "")),
                "成交量": str(row.get("成交量", "")),
            }
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
                continue
            logger.debug(f"[ETF] spot EM 源失败 {code}: {e}")
    return None


def _fetch_spot_sina_fallback(code: str) -> Optional[Dict[str, str]]:
    """备选源：Akshare fund_etf_category_sina（只含名称和价格）"""
    try:
        import akshare as ak
        df = ak.fund_etf_category_sina()
        match = df[df["代码"] == code]
        if not match.empty:
            row = match.iloc[0]
            return {
                "代码": code,
                "名称": str(row.get("名称", "")),
                "最新价": str(row.get("最新价", "")),
            }
    except Exception as e:
        logger.debug(f"[ETF] spot Sina 源失败 {code}: {e}")
    return None


def fetch_etf_name(code: str) -> str:
    """快速获取 ETF 名称（短路径，尽量不触发完整行情）"""
    # 先查缓存
    cached = _cache_get(f"name:{code}", _CACHE_SPOT)
    if cached:
        return cached
    # 备选：Sina 分类
    try:
        import akshare as ak
        df = ak.fund_etf_category_sina()
        match = df[df["代码"] == code]
        if not match.empty:
            name = str(match.iloc[0].get("名称", ""))
            _cache_set(f"name:{code}", name, _CACHE_SPOT)
            return name
    except Exception:
        pass
    # 最后：走完整行情
    spot = _fetch_spot_akshare(code)
    if spot and spot.get("名称"):
        _cache_set(f"name:{code}", spot["名称"], _CACHE_SPOT)
        return spot["名称"]
    return ""


# =========================== 持仓穿透 ===========================


def fetch_etf_holdings(code: str, year: Optional[str] = None) -> List[Dict[str, str]]:
    """
    获取 ETF 前十大重仓股，带缓存 + 双数据源。
    每个条目: {"code": "600519", "name": "贵州茅台", "ratio": "9.06%"}
    """
    year = year or str(date.today().year)
    cache_key = f"holdings:{code}:{year}"

    cached = _cache_get(cache_key, _CACHE_HOLDINGS)
    if cached is not None:
        return cached

    # 主源：天天基金 HTML
    result = _fetch_holdings_eastmoney(code, year)
    if result:
        _cache_set(cache_key, result, _CACHE_HOLDINGS)
        return result

    # 备选源：Sina
    result = _fetch_holdings_sina(code)
    if result:
        _cache_set(cache_key, result, _CACHE_HOLDINGS)
        return result

    logger.warning(f"[ETF] 持仓两个数据源均失败 {code}")
    return []


def _fetch_holdings_eastmoney(code: str, year: str) -> List[Dict[str, str]]:
    """主源：天天基金持仓API"""
    try:
        url = _FUND_HOLD_URL.format(code=code, year=year)
        resp = requests.get(url, headers=_FUND_HOLD_HEADERS, timeout=15)
        resp.raise_for_status()

        # 从 var apidata={ content:"...",arryear:[...] } 提取
        start_marker = 'content:"'
        end_marker = '",arryear:'
        start = resp.text.find(start_marker)
        end = resp.text.find(end_marker)
        if start < 0 or end < start:
            logger.debug(f"[ETF] EM 持仓格式异常 {code}")
            return []
        start += len(start_marker)
        html = resp.text[start:end].replace("\\r\\n", "").replace("\\n", "")

        holdings = []
        rows = re.findall(r"<tr>(.*?)</tr>", html)
        for row in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row)
            if len(cells) < 6:
                continue
            code_m = re.search(r"'>(\d{6})", cells[1])
            name_m = re.search(r"'>([^<]+)", cells[2])
            ratio_raw = re.sub(r"<[^>]+>", "", cells[4]).strip().replace("%", "")
            if code_m and name_m:
                holdings.append({
                    "code": code_m.group(1),
                    "name": str(name_m.group(1)).strip(),
                    "ratio": f"{ratio_raw}%",
                })
        logger.info(f"[ETF] EM {code} 前十大重仓股: {len(holdings)} 只")
        return holdings
    except Exception as e:
        logger.debug(f"[ETF] EM 持仓失败 {code}: {e}")
        return []


def _fetch_holdings_sina(code: str) -> List[Dict[str, str]]:
    """备选源：新浪基金持仓API（JSONP）"""
    try:
        url = _SINA_HOLD_URL.format(code=code)
        resp = requests.get(url, headers=_FUND_HOLD_HEADERS, timeout=10)
        resp.raise_for_status()
        text = resp.text.strip()

        # JSONP 返回格式: IO.XSRV.FundJJDX([...]);
        json_str = re.sub(r"^[\w.]+\(|\);?$", "", text)
        import json
        data = json.loads(json_str)
        if not isinstance(data, list):
            return []

        holdings = []
        for item in data:
            stock_code = str(item.get("stockcode", "")).strip()
            stock_name = str(item.get("stockname", "")).strip()
            ratio = item.get("pct", "")
            if stock_code and stock_name:
                # Sina 的 stockcode 可能是 6 位数字
                if re.match(r"^\d{6}$", stock_code):
                    holdings.append({
                        "code": stock_code,
                        "name": stock_name,
                        "ratio": f"{ratio}%" if ratio else "",
                    })
        if holdings:
            logger.info(f"[ETF] Sina {code} 持仓: {len(holdings)} 只（备选源）")
        return holdings
    except Exception as e:
        logger.debug(f"[ETF] Sina 持仓失败 {code}: {e}")
        return []


# =========================== 行业配置 ===========================


def fetch_etf_industry_allocation(code: str, year: Optional[str] = None) -> List[Dict[str, str]]:
    """获取 ETF 行业配置，带缓存"""
    year = year or str(date.today().year)
    cache_key = f"industry:{code}:{year}"

    cached = _cache_get(cache_key, _CACHE_INDUSTRY)
    if cached is not None:
        return cached

    try:
        import akshare as ak
        df = ak.fund_portfolio_industry_allocation_em(symbol=code, date=year)
        items = []
        for _, row in df.iterrows():
            items.append({
                "industry": str(row.get("行业类别", "")),
                "ratio": f'{row.get("占净值比例", 0):.2f}%',
            })
        _cache_set(cache_key, items, _CACHE_INDUSTRY)
        return items
    except Exception as e:
        logger.debug(f"[ETF] 行业配置获取失败 {code}: {e}")
        return []


# =========================== 估值计算（成分股加权 PE/PB） ===========================

# 单只股票 PE/PB 缓存 { code: {pe, pb} }，TTL 5 分钟
_STOCK_PE_CACHE: Dict[str, tuple] = {}  # code → (expire_ts, {"pe": ..., "pb": ...})

# 东方财富单股行情 API（轻量，只拉需要的字段）
_EM_QUOTE_URL = "https://push2.eastmoney.com/api/qt/stock/get"
_EM_QUOTE_FIELDS = "f162,f167"  # f162=动态市盈率, f167=市净率
_EM_QUOTE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Referer": "https://quote.eastmoney.com/",
}


def _is_hk_code(code: str) -> bool:
    """判断股票代码是否为港股"""
    clean = code.lower().replace("hk", "").strip()
    return clean.isdigit() and len(clean) <= 5


def _a_market(code: str) -> int:
    """A 股市场代码：1=上海, 0=深圳"""
    return 1 if code.startswith(("6", "9")) else 0


def _normalize_hk_code(code: str) -> str:
    """港股代码统一格式：5位数字"""
    return code.lower().replace("hk", "").zfill(5)


def _format_pe(p) -> str:
    """格式化 PE，亏损公司返回 '亏损'"""
    try:
        p = float(p)
    except (TypeError, ValueError):
        return "—"
    if p == 0 or abs(p) < 0.01:
        return "—"
    if p < 0:
        return "亏损"
    return f"{p:.1f}"


def _format_pb(p) -> str:
    """格式化 PB"""
    try:
        p = float(p)
    except (TypeError, ValueError):
        return "—"
    if p <= 0:
        return "—"
    return f"{p:.2f}"


def _fetch_single_pe_pb(code: str) -> Dict[str, Optional[float]]:
    """
    单只股票查 PE/PB（东方财富 API），带进程级缓存。

    Args:
        code: 6 位 A 股代码 / 5 位港股代码

    Returns:
        {"pe": float|None, "pb": float|None}
    """
    # 缓存命中
    now = time.time()
    cached = _STOCK_PE_CACHE.get(code)
    if cached and cached[0] > now:
        return cached[1]

    code_clean = code.strip().upper()
    try:
        if _is_hk_code(code_clean):
            ncode = _normalize_hk_code(code_clean)
            secid = f"4.{ncode}"
        else:
            market = _a_market(code_clean)
            secid = f"{market}.{code_clean}"

        resp = requests.get(
            _EM_QUOTE_URL,
            params={"secid": secid, "fields": _EM_QUOTE_FIELDS},
            headers=_EM_QUOTE_HEADERS,
            timeout=8,
        )
        resp.raise_for_status()
        body = resp.json()
        d = body.get("data") or {}
        pe = d.get("f162")
        pb = d.get("f167")
        result = {
            "pe": float(pe) if pe is not None and float(pe) > 0 else None,
            "pb": float(pb) if pb is not None and float(pb) > 0 else None,
        }
        _STOCK_PE_CACHE[code] = (now + 300, result)  # 缓存 5 分钟
        return result
    except Exception as e:
        logger.debug(f"[ETF估值] 查 {code} PE/PB 失败: {e}")
        return {"pe": None, "pb": None}


def calculate_etf_valuation(holdings: List[Dict[str, str]]) -> str:
    """
    基于持仓成分股加权计算 ETF 的 PE/PB。
    按需单只查询，不拉全市场数据。

    Args:
        holdings: fetch_etf_holdings 返回的持仓列表，每项含 code/name/ratio

    Returns:
        格式化的估值文本块，失败返回空串
    """
    if not holdings:
        return ""

    pe_vals = []   # (weight, pe)
    pb_vals = []   # (weight, pb)
    details = []

    for h in holdings:
        code = h.get("code", "").strip().upper()
        ratio_str = h.get("ratio", "0").replace("%", "").strip()
        name = h.get("name", "")
        try:
            weight = float(ratio_str)
        except (ValueError, TypeError):
            weight = 0.0
        if weight <= 0 or not code:
            continue

        v = _fetch_single_pe_pb(code)
        pe, pb = v["pe"], v["pb"]
        source = "港股" if _is_hk_code(code) else "A股"

        if pe is not None:
            pe_vals.append((weight, pe))
        if pb is not None:
            pb_vals.append((weight, pb))

        pe_str = _format_pe(pe)
        pb_str = _format_pb(pb)
        details.append(f"· {name}({code}) [{source}] 占比{ratio_str}%    PE={pe_str}    PB={pb_str}")

    if not details:
        return ""

    # 加权 PE（调和平均）
    wpe = "—"
    if pe_vals:
        w_pe_sum = sum(w for w, _ in pe_vals)
        inv_sum = sum(w / p for w, p in pe_vals if p > 0)
        wpe_val = w_pe_sum / inv_sum if inv_sum > 0 else 0
        wpe = f"{wpe_val:.1f}" if wpe_val > 0 else "亏损"

    # 加权 PB（算术平均）
    wpb = "—"
    if pb_vals:
        pb_sum = sum(w * p for w, p in pb_vals)
        w_pb_sum = sum(w for w, _ in pb_vals)
        wpb_val = pb_sum / w_pb_sum if w_pb_sum > 0 else 0
        wpb = f"{wpb_val:.2f}" if wpb_val > 0 else "—"

    covered = sum(1 for d in details if "PE=" in d and "—" not in d.split("PE=")[1].split("    ")[0])
    rate = f"{covered}/{len(details)}"

    lines = [
        f"【持仓加权估值（成分股实时行情）】",
        f"覆盖: {rate} 只成分股有 PE 数据",
        f"加权 PE: {wpe}（调和平均，亏损股权重不计入分子）",
        f"加权 PB: {wpb}（算术平均）",
        f"成分股估值明细:",
    ] + details

    return "\n".join(lines)


# =========================== 报告格式化 ===========================


def format_etf_report(spot: Optional[Dict[str, str]],
                      holdings: List[Dict[str, str]],
                      industry: List[Dict[str, str]],
                      valuation_text: str = "") -> str:
    """将 ETF 数据格式化为报告文本块（数据缺失时友好降级）"""
    today = date.today().strftime("%Y-%m-%d")
    blocks = []

    if spot:
        name = spot.get("名称", "")
        price = spot.get("最新价", "")
        iopv = spot.get("IOPV实时估值", "")
        premium = spot.get("基金折价率", "")
        change = spot.get("涨跌幅", "")
        turnover = spot.get("成交额", "")
        volume = spot.get("成交量", "")
        shares = spot.get("最新份额", "")
        mcap = spot.get("流通市值", "")
        lines = [f"【ETF 实时行情】{name}  采集日期: {today}"]
        if price:
            lines.append(f"最新价: {price}")
        if iopv:
            lines.append(f"IOPV: {iopv}")
        if premium and premium not in ("", "None", "nan"):
            lines.append(f"折溢价: {premium}%")
        if change:
            lines.append(f"涨跌幅: {change}%")
        if turnover:
            lines.append(f"成交额: {turnover}")
        if volume:
            lines.append(f"成交量: {volume}")
        if shares:
            lines.append(f"最新份额: {shares}")
        if mcap:
            lines.append(f"流通市值: {mcap}")
        blocks.append(" | ".join(lines))
    else:
        blocks.append(f"【ETF 行情】程序采集失败（{today}），需依赖网络研究信息补充")

    if industry:
        ind_lines = [f"【行业配置（前5）】采集日期: {today}"]
        for ind in industry[:5]:
            ind_lines.append(f"· {ind['industry']}: {ind['ratio']}")
        blocks.append("\n".join(ind_lines))

    if holdings:
        hd_lines = [f"【前十大重仓股】采集日期: {today}"]
        for h in holdings:
            hd_lines.append(f"· {h['name']}({h['code']}): 占比{h['ratio']}")
        blocks.append("\n".join(hd_lines))
    else:
        blocks.append("【持仓穿透】暂无数据（分析将聚焦行情与行业配置）")

    if valuation_text:
        blocks.append(valuation_text)

    return "\n\n".join(blocks)
