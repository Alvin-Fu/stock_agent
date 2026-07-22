# -*- coding: utf-8 -*-
"""
机构盈利预测（forward 估值的锚）：
trailing PEG 用的是已披露增速，真正有用的是预期增速。东财有机构一致预测数据。

forward PE 由**程序计算**（现价÷预测EPS），LLM 只允许引用——实测让 LLM 拿预测表
心算会出"净利润422亿 vs EPS4.54元"这类自相矛盾的数字（EPS×股本≈138亿）。
接口列名随版本漂移，两种表形都做了解析，失败只降级不阻断。
"""

import re
from typing import List, Optional

from utils.logger import logger
from utils.retry_utils import retry_with_backoff


def _latest_close(code: str) -> Optional[float]:
    """库内最新收盘价（供 forward PE 计算；拿不到返回 None，只出原始预测表）"""
    try:
        from storage.sqlite.stock_storage import get_db
        df = get_db().get_all_daily_data(code)
        if df is not None and not df.empty and df.iloc[0].get("close") is not None:
            return float(df.iloc[0]["close"])
    except Exception as e:
        logger.warning(f"[盈利预测] 取收盘价失败 {code}: {e}")
    return None


def _get_total_shares(code: str) -> Optional[float]:
    """
    通过 Tushare daily_basic 获取总股本（亿股），用于 EPS × 总股本 → 净利润。
    total_mv（万元）÷ close（元）= 总股本（万股）。
    缓存总股本和 close 到模块变量，`_ts_close_cache` 供 `_latest_close` 兜底。
    """
    if _TS_SHARE_CACHE.get(code) is not None:
        return _TS_SHARE_CACHE[code]  # type: ignore[return-value]
    try:
        import tushare as ts
        ts_code = f"{code}.SZ" if code.startswith(('000', '002', '300')) else f"{code}.SH"
        from datetime import date, timedelta
        for days_back in range(0, 5):
            d = (date.today() - timedelta(days=days_back)).strftime("%Y%m%d")
            df = ts.pro_api().daily_basic(ts_code=ts_code, trade_date=d,
                                          fields="close,total_mv")
            if df is not None and not df.empty:
                close = float(df.iloc[0].get("close") or 0)
                total_mv = float(df.iloc[0].get("total_mv") or 0)
                if close > 0 and total_mv > 0:
                    shares = total_mv / close / 10000  # 万元÷元 → 万股 → 亿股
                    _TS_SHARE_CACHE[code] = shares
                    _TS_CLOSE_CACHE[code] = close  # 供 forward PE 兜底
                    return shares
        return None
    except Exception as e:
        logger.debug(f"[盈利预测] 取总股本失败 {code}: {e}")
        return None

# Tushare 缓存（模块级，进程内复用）
_TS_SHARE_CACHE: dict = {}  # code → 总股本(亿股)
_TS_CLOSE_CACHE: dict = {}  # code → close(元，供 forward PE 兜底)


def _fallback_close(code: str) -> Optional[float]:
    """兜底收盘价：优先 DB，其次 Tushare daily_basic 缓存"""
    c = _latest_close(code)
    if c is not None:
        return c
    return _TS_CLOSE_CACHE.get(code)


def forward_pe_lines(df, close: Optional[float], total_shares: Optional[float] = None) -> List[str]:
    """
    程序计算 forward PE（纯函数）：兼容两种表形——
    A. 东财单行表：列名形如「2026预测每股收益」
    B. 同花顺多行表：列「年度」+「均值」（每股收益一致预期）
    返回文本行列表；算不出返回 []。
    """
    lines = []
    if df is None or getattr(df, "empty", True) or not close or close <= 0:
        return lines

    def _num(v):
        try:
            f = float(v)
            return f if f > 0 else None
        except (TypeError, ValueError):
            return None

    pairs = []  # [(年度, eps)]
    # 表形 A：年度藏在列名里
    for col in df.columns:
        m = re.match(r"(20\d{2}).*预测每股收益", str(col))
        if m:
            eps = _num(df.iloc[0].get(col))
            if eps:
                pairs.append((m.group(1), eps))
    # 表形 B：年度是数据列
    if not pairs and "年度" in df.columns and "均值" in df.columns:
        for _, row in df.iterrows():
            ym = re.search(r"20\d{2}", str(row.get("年度")))
            eps = _num(row.get("均值"))
            if ym and eps:
                pairs.append((ym.group(0), eps))

    pairs = sorted(set(pairs))[:3]
    for year, eps in pairs:
        line = (f"  {year}年：预测EPS {eps:.2f}元 → forward PE {close / eps:.1f}倍"
                f"（现价{close:.2f}÷{eps:.2f}，程序计算）")
        if total_shares:
            net_profit = round(eps * total_shares, 1)  # EPS(元) × 总股本(亿股) = 净利润(亿元)
            line += (f" → 隐含净利润约{net_profit:.1f}亿"
                     f"（EPS{eps:.2f}×总股本{total_shares:.1f}亿，程序计算）")
        lines.append(line)
    # 预测增速也程序算：LLM 拿预测表自行推"3年净利润CAGR约9%"属于同一类心算病
    if len(pairs) >= 2:
        (y0, e0), (y1, e1) = pairs[0], pairs[-1]
        span = int(y1) - int(y0)
        if span > 0 and e0 > 0:
            cagr = ((e1 / e0) ** (1 / span) - 1) * 100
            lines.append(f"  预测EPS年均增速（{y0}→{y1}）：{cagr:+.1f}%/年（程序计算，机构预期口径）")
    return lines


def fetch_profit_forecast_text(code: str, name: str = "") -> str:
    """机构盈利预测文本块（含程序算好的 forward PE）；失败返回空串"""
    from tools.source_health import report_source
    try:
        import akshare as ak
        df = None

        def _fetch_one(fname, kwargs):
            fn = getattr(ak, fname, None)
            if fn is None:
                raise ValueError(f"函数 {fname} 不存在")
            result = fn(**kwargs)
            if result is None or getattr(result, "empty", True):
                raise ValueError("返回空数据")
            return result

        for fname, kwargs in (
                ("stock_profit_forecast_em", {"symbol": code}),
                ("stock_profit_forecast_ths", {"symbol": code}),
                ("stock_profit_forecast", {})):
            try:
                raw_df = retry_with_backoff(_fetch_one, max_retries=2, fname=fname, kwargs=kwargs)
                if raw_df is not None and not getattr(raw_df, "empty", True):
                    df = raw_df.copy()
                    for col in ("代码", "股票代码"):
                        if col in getattr(df, "columns", []):
                            df = df[df[col].astype(str).str.contains(code)]
                            break
                    if not getattr(df, "empty", True):
                        break
            except Exception as e:
                logger.warning(f"[盈利预测] {fname} 失败: {e}")
                df = None
        if df is None or getattr(df, "empty", True):
            report_source("机构盈利预测", False, "各接口均无数据")
            return ""
        report_source("机构盈利预测", True)
        text = df.head(6).to_string(index=False)[:900]

        total_shares = _get_total_shares(code)
        fpe_lines = forward_pe_lines(df, _fallback_close(code), total_shares=total_shares)
        fpe_block = ""
        if fpe_lines:
            fpe_block = ("\n【forward PE（程序计算，引用时必须原样使用并标注\"基于机构预测\"）】\n"
                         + "\n".join(fpe_lines))

        shares_note = ""
        if total_shares:
            shares_note = ("总股本{:.1f}亿股（Tushare daily_basic，程序获取）".format(total_shares)
                           + "——净利润=EPS×总股本，由程序计算，引用时直接使用上方数值。")
        return ("【机构盈利预测（东财汇总，预测值仅供参考，不是事实）】\n" + text + fpe_block
                + "\n（使用规则：净利润已由程序在上方forward PE段直接算好，**禁止自行用预测表心算**"
                  "forward PE、净利润或增速/CAGR；"
                  "程序未给出 forward PE 时只说明有机构预测覆盖、不做换算；"
                  + ("\n{}".format(shares_note) if shares_note else "")
                  + "预测与已披露实际数矛盾时以实际数为准）")
    except Exception as e:
        logger.warning(f"[盈利预测] 获取失败 {code}: {e}")
        report_source("机构盈利预测", False, str(e))
        return ""
