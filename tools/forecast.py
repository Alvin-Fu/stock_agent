# -*- coding: utf-8 -*-
"""
机构盈利预测（forward 估值的锚）：
trailing PEG 用的是已披露增速，真正有用的是预期增速。东财有机构一致预测数据。

forward PE 由**程序计算**（现价÷预测EPS），LLM 只允许引用——实测让 LLM 拿预测表
心算会出"净利润422亿 vs EPS4.54元"这类自相矛盾的数字（EPS×股本≈138亿）。
接口列名随版本漂移，两种表形都做了解析，失败只降级不阻断。
"""

import re
import json
import os
import threading
from datetime import datetime
from typing import Any, List, Optional

from utils.logger import logger
from utils.retry_utils import retry_with_backoff

# Tushare 缓存（模块级，进程内复用）+ 线程锁保护
_TS_SHARE_CACHE: dict = {}  # code → 总股本(亿股)
_TS_CLOSE_CACHE: dict = {}  # code → close(元，供 forward PE 兜底)
_TS_CACHE_LOCK = threading.Lock()

# 机构预测文本的文件缓存目录（主数据源失败时降级读取）
_FORECAST_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache")


def _save_forecast_cache(code: str, text: str) -> None:
    """将成功的预测文本缓存到文件，供下次主数据源失败时降级使用"""
    try:
        os.makedirs(_FORECAST_CACHE_DIR, exist_ok=True)
        cache_path = os.path.join(_FORECAST_CACHE_DIR, f"forecast_{code}.json")
        cache_data = {
            "code": code,
            "text": text,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False)
    except Exception as e:
        logger.debug(f"[盈利预测] 缓存写入失败 {code}: {e}")


def _load_forecast_cache(code: str) -> Optional[str]:
    """从文件缓存读取上次的预测文本；缓存不存在或损坏返回 None"""
    try:
        cache_path = os.path.join(_FORECAST_CACHE_DIR, f"forecast_{code}.json")
        if not os.path.exists(cache_path):
            return None
        with open(cache_path, "r", encoding="utf-8") as f:
            cache_data = json.load(f)
        text = cache_data.get("text", "")
        timestamp = cache_data.get("timestamp", "未知时间")
        if text:
            logger.info(f"[盈利预测] {code} 使用缓存数据（缓存于 {timestamp}）")
            return f"【以下为缓存数据（{timestamp} 缓存），实时获取失败，仅供参考】\n{text}"
        return None
    except Exception as e:
        logger.debug(f"[盈利预测] 缓存读取失败 {code}: {e}")
        return None


def _forecast_fallback(code: str) -> str:
    """主数据源失败时的降级方案：文件缓存 → 不可用标注"""
    cached = _load_forecast_cache(code)
    if cached:
        return cached
    return ("【机构预测数据暂不可用】实时获取失败且无缓存数据，"
            "请稍后重试或手动查询机构盈利预测。")


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
    通过 TushareFetcher.daily_basic 获取总股本（亿股），用于 EPS × 总股本 → 净利润。
    total_mv（万元）÷ close（元）= 总股本（万股）。
    缓存总股本和 close 到模块变量，`_TS_CLOSE_CACHE` 供 `_latest_close` 兜底。
    通过 TushareFetcher 调用以受其限流保护，不绕过 _check_rate_limit。
    """
    with _TS_CACHE_LOCK:
        if _TS_SHARE_CACHE.get(code) is not None:
            return _TS_SHARE_CACHE[code]  # type: ignore[return-value]
    try:
        from datetime import date, timedelta
        from tools.stock.tushare_fetcher import TushareFetcher
        fetcher = TushareFetcher()
        if fetcher._api is None:
            return None
        # 一次查近7天范围（覆盖5个交易日），避免循环多次 API 调用
        end_date = date.today().strftime("%Y-%m-%d")
        start_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        df = fetcher.daily_basic(code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            row = df.iloc[0]
            close = float(row.get("close") or 0)
            total_mv = float(row.get("total_mv") or 0)
            if close > 0 and total_mv > 0:
                shares = total_mv / close / 10000  # 万元÷元 → 万股 → 亿股
                with _TS_CACHE_LOCK:
                    _TS_SHARE_CACHE[code] = shares
                    _TS_CLOSE_CACHE[code] = close  # 供 forward PE 兜底
                return shares
        return None
    except Exception as e:
        logger.debug(f"[盈利预测] 取总股本失败 {code}: {e}")
        return None


def _fallback_close(code: str) -> Optional[float]:
    """兜底收盘价：优先 DB，其次 Tushare daily_basic 缓存"""
    c = _latest_close(code)
    if c is not None:
        return c
    return _TS_CLOSE_CACHE.get(code)


def _count_institutions(df) -> Optional[int]:
    """从预测表提取机构个数（东财单行表有"机构个数"列）"""
    if df is None or getattr(df, "empty", True):
        return None
    for col in df.columns:
        if "机构" in str(col) and ("个数" in str(col) or "家数" in str(col)):
            val = df.iloc[0].get(col)
            try:
                return int(float(val))
            except (TypeError, ValueError):
                pass
    return None


def _institution_summary_lines(df, total_shares: Optional[float] = None) -> List[str]:
    """
    生成机构预测摘要行（含机构数量和具体 EPS/净利润）。
    东财表形 A：列名含「预测每股收益」和「机构个数」。
    同花顺表形 B：列含「年度」+「均值」+「机构家数」。
    """
    lines = []
    if df is None or getattr(df, "empty", True):
        return lines

    pairs = []  # [(年度, eps, 机构数)]
    # 表形 A：年度藏在列名里
    for col in df.columns:
        m = re.match(r"(20\d{2}).*预测每股收益", str(col))
        if m:
            eps = _num_val(df.iloc[0].get(col))
            if eps:
                n_inst = None
                # 找同行的机构个数/家数列
                for ic in df.columns:
                    if "机构" in str(ic) and ("个数" in str(ic) or "家数" in str(ic)):
                        try:
                            n_inst = int(float(df.iloc[0].get(ic)))
                        except (TypeError, ValueError):
                            pass
                        break
                pairs.append((m.group(1), eps, n_inst))
    # 表形 B：年度是数据列
    if not pairs and "年度" in df.columns and "均值" in df.columns:
        for _, row in df.iterrows():
            ym = re.search(r"20\d{2}", str(row.get("年度")))
            eps = _num_val(row.get("均值"))
            if ym and eps:
                n_inst = None
                if "机构家数" in df.columns:
                    try:
                        n_inst = int(float(row.get("机构家数")))
                    except (TypeError, ValueError):
                        pass
                pairs.append((ym.group(0), eps, n_inst))

    pairs = sorted(set(pairs))[:3]
    for year, eps, n_inst in pairs:
        inst_str = f"{n_inst}家机构一致预期 " if n_inst else ""
        line = f"  {year}年：{inst_str}EPS {eps:.4f}元"
        if total_shares:
            net_profit = round(eps * total_shares, 3)
            line += f"（净利约{net_profit:.3f}亿）"
        lines.append(line)
    if lines:
        lines.insert(0, "【机构预测摘要（机构家数 + EPS + 推算净利）】")
    return lines


def _num_val(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


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


def forward_peg_lines(df, pe_ttm: float, close: Optional[float] = None,
                      total_shares: Optional[float] = None) -> List[str]:
    """
    程序计算 forward PEG。
    forward PEG = PE(TTM) / 预测净利CAGR（机构预期口径）
    使用预测期内首末年度的EPS计算CAGR，而非相邻年度增速——
    相邻年度增速波动大、不反映长期增长趋势，CAGR更稳健。

    Args:
        df: 预测 DataFrame（同 forward_pe_lines 的输入）
        pe_ttm: PE(TTM) 值（来自程序计算的财务数据）
        close: 收盘价（仅用于对齐格式，可省略）
        total_shares: 总股本（仅用于对齐格式，可省略）

    Returns:
        文本行列表；算不出返回 []
    """
    lines = []
    if df is None or getattr(df, "empty", True) or not pe_ttm or pe_ttm <= 0:
        return lines

    def _num(v):
        try:
            f = float(v)
            return f if f > 0 else None
        except (TypeError, ValueError):
            return None

    pairs = []  # [(year, eps)]
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

    pairs = sorted(set(pairs))
    if len(pairs) < 2:
        return lines

    # 计算 CAGR（复合年均增长率）——使用首末年度EPS
    (y0, e0), (y1, e1) = pairs[0], pairs[-1]
    span = int(y1) - int(y0)
    if span <= 0 or e0 <= 0:
        return lines

    cagr = ((e1 / e0) ** (1 / span) - 1) * 100

    lines.append(
        f"【forward PEG（程序计算，PE(TTM){pe_ttm:.1f}÷预测CAGR，机构预期口径）】"
    )
    lines.append(f"  预测EPS CAGR（{y0}→{y1}，{span}年）：{cagr:+.1f}%/年")

    if cagr <= 0:
        lines.append("  CAGR≤0，无法计算有意义的PEG")
        lines.append("（PEG<1≈相对低估，1~2≈匹配，>2≈偏贵；基于机构预测CAGR，预测≠事实）")
        return lines

    peg = round(pe_ttm / cagr, 2)
    lines.append(f"  forward PEG = {pe_ttm:.1f} / {cagr:.1f} = {peg:.2f}")

    # 展示各年度EPS供交叉验证
    if len(pairs) >= 3:
        eps_detail = " → ".join(f"{y}年EPS {e:.2f}" for y, e in pairs)
        lines.append(f"  EPS序列：{eps_detail}")

    lines.append("（PEG<1≈相对低估，1~2≈匹配，>2≈偏贵；基于机构预测CAGR，预测≠事实）")
    return lines


def _fetch_forecast_multi_source(code: str):
    """同时从东财和同花顺获取机构盈利预测，返回 [(source_name, df, n_inst), ...]。
    多源并取而非 fallback——不同源机构数和 EPS 可能不同，需同时展示供交叉验证。"""
    import akshare as ak
    sources = []

    def _fetch_one(fname, kwargs):
        fn = getattr(ak, fname, None)
        if fn is None:
            raise ValueError(f"函数 {fname} 不存在")
        result = fn(**kwargs)
        if result is None or getattr(result, "empty", True):
            raise ValueError("返回空数据")
        return result

    # 源1：东财
    try:
        raw_df = retry_with_backoff(_fetch_one, max_retries=2,
                                    fname="stock_profit_forecast_em",
                                    kwargs={"symbol": code})
        if raw_df is not None and not getattr(raw_df, "empty", True):
            df = raw_df.copy()
            for col in ("代码", "股票代码"):
                if col in getattr(df, "columns", []):
                    df = df[df[col].astype(str).str.contains(code)]
                    break
            if not getattr(df, "empty", True):
                n_inst = _count_institutions(df)
                sources.append(("东财", df, n_inst))
    except Exception as e:
        logger.warning(f"[盈利预测] 东财源失败: {e}")

    # 源2：同花顺
    try:
        raw_df = retry_with_backoff(_fetch_one, max_retries=2,
                                    fname="stock_profit_forecast_ths",
                                    kwargs={"symbol": code})
        if raw_df is not None and not getattr(raw_df, "empty", True):
            df = raw_df.copy()
            for col in ("代码", "股票代码"):
                if col in getattr(df, "columns", []):
                    df = df[df[col].astype(str).str.contains(code)]
                    break
            if not getattr(df, "empty", True):
                n_inst = _count_institutions(df)
                sources.append(("同花顺", df, n_inst))
    except Exception as e:
        logger.warning(f"[盈利预测] 同花顺源失败: {e}")

    return sources


def _pick_primary_source(sources):
    """从多源中选机构数最多的作为主源（用于 forward PE/PEG 计算）。
    机构数相同时优先东财（覆盖面更广）。返回 (source_name, df, n_inst) 或 None。"""
    if not sources:
        return None

    def _sort_key(s):
        name, _df, n = s
        return (n or 0, 1 if name == "东财" else 0)

    return sorted(sources, key=_sort_key, reverse=True)[0]


def fetch_profit_forecast_text(code: str, name: str = "",
                               pe_ttm: Optional[float] = None) -> str:
    """机构盈利预测文本块（多源并取 + 程序算好的 forward PE + forward PEG）；
    主数据源失败时降级到文件缓存，缓存也没有则返回不可用标注"""
    from tools.source_health import report_source
    try:
        sources = _fetch_forecast_multi_source(code)
        if not sources:
            report_source("机构盈利预测", False, "各接口均无数据")
            return _forecast_fallback(code)
        report_source("机构盈利预测", True)

        total_shares = _get_total_shares(code)
        close = _fallback_close(code)

        # 选主源（机构数最多）用于 forward PE/PEG 计算
        primary = _pick_primary_source(sources)
        if primary:
            primary_name, primary_df, primary_n = primary
        else:
            primary_name, primary_df, primary_n = sources[0]

        # 各源摘要行（含家数 + EPS + 净利）
        all_inst_blocks = []
        for src_name, src_df, src_n in sources:
            inst_lines = _institution_summary_lines(src_df, total_shares=total_shares)
            if inst_lines:
                header = f"【{src_name}机构预测摘要"
                if src_n:
                    header += f"（{src_n}家机构）"
                header += "】"
                inst_lines[0] = header
                all_inst_blocks.append("\n".join(inst_lines))

        inst_block = "\n\n".join(all_inst_blocks) if all_inst_blocks else ""

        # 原始表：仅主源
        raw_text = primary_df.head(6).to_string(index=False)[:900]

        # forward PE（基于主源）
        fpe_lines = forward_pe_lines(primary_df, close, total_shares=total_shares)
        fpe_block = ""
        if fpe_lines:
            src_tag = f"{primary_n}家机构" if primary_n else primary_name
            fpe_block = (f"\n【forward PE（程序计算，基于{primary_name}主源{src_tag}，"
                         "引用时必须原样使用并标注\"基于机构预测\"）】\n"
                         + "\n".join(fpe_lines))

        # forward PEG（基于主源，需 PE(TTM)）
        fpeg_block = ""
        if pe_ttm is not None and pe_ttm > 0:
            fpeg_lines = forward_peg_lines(primary_df, pe_ttm,
                                           close=close,
                                           total_shares=total_shares)
            if fpeg_lines:
                fpeg_block = "\n" + "\n".join(fpeg_lines)

        # 多源对比提示
        multi_source_note = ""
        if len(sources) >= 2:
            multi_source_note = (
                f"\n⚠️ 多源对比：共获取 {len(sources)} 个数据源"
                f"（{'、'.join(s[0] for s in sources)}），"
                f"主源为{primary_name}（机构数最多）。"
                "不同源机构覆盖范围和预测口径可能不同，"
                "引用时需标注来源和样本量；差异>3%时需说明原因。"
            )

        shares_note = ""
        if total_shares:
            shares_note = (
                "总股本{:.1f}亿股（Tushare daily_basic，程序获取）".format(total_shares)
                + "——净利润=EPS×总股本，由程序计算，引用时**必须**使用上方精确值，"
                  "不得从原始表中另行取数。"
            )

        header = "【机构盈利预测（多源并取，预测值仅供参考，不是事实）】\n"

        result_text = (header
                + (inst_block + "\n" if inst_block else "")
                + f"【原始预测表（{primary_name}主源，仅供对比参考，禁止从中取数计算）】\n"
                + raw_text
                + fpe_block + fpeg_block
                + multi_source_note
                + "\n（使用规则：净利润已由程序在各源【机构预测摘要】段按EPS×总股本直接算好，"
                  "**禁止自行用原始预测表心算**forward PE、净利润或增速/CAGR；"
                  "若各源数据不一致，以主源（机构数最多）为准；"
                  "程序未给出 forward PE 时只说明有机构预测覆盖、不做换算；"
                  + ("\n{}".format(shares_note) if shares_note else "")
                  + "预测与已披露实际数矛盾时以实际数为准）")
        # 成功获取后写入文件缓存，供下次主数据源失败时降级
        _save_forecast_cache(code, result_text)
        return result_text
    except Exception as e:
        logger.warning(f"[盈利预测] 获取失败 {code}: {e}")
        report_source("机构盈利预测", False, str(e))
        return _forecast_fallback(code)
