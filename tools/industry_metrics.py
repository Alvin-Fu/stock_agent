# -*- coding: utf-8 -*-
"""
行业估值与位置度量（产业链分析用）：
以候选池（各环节龙头）为行业代理样本，程序计算四组硬指标 + 参考标签，
供 LLM 在"行业风险/回调风险"分析中引用——数字出自代码，LLM 只解读。

指标：
- 估值水位：池内 PE(TTM) 中位数 + 各股 PE 历史分位的中位数
- 行业位置：池内平均年内位置（pos_52w，0=年内最低 100=年内最高）
- 短期过热度：池内近20日平均涨幅
- 回调参考：池内收盘价相对 MA20 的平均乖离率
"""

import statistics
from typing import List, Dict, Any, Optional

import pandas as pd

from utils.logger import logger


def compute_industry_metrics(per_stock: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    纯计算：输入每只股票的度量行
    [{code, pe_ttm, pe_percentile, pos_52w, ret20, bias_ma20}]（缺失项为 None）
    输出汇总指标与参考标签；有效样本不足 2 只返回 None。
    """
    rows = [r for r in per_stock or [] if r.get("pos_52w") is not None]
    if len(rows) < 2:
        return None

    def _median(key):
        vals = [float(r[key]) for r in rows if r.get(key) is not None]
        return round(statistics.median(vals), 1) if vals else None

    def _mean(key):
        vals = [float(r[key]) for r in rows if r.get(key) is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    pe_median = _median("pe_ttm")
    pe_pct_median = _median("pe_percentile")
    pos_avg = _mean("pos_52w")
    ret20_avg = _mean("ret20")
    bias_avg = _mean("bias_ma20")

    # 参考标签（阈值为经验值，仅作提示，不构成预测）
    valuation_label = "估值数据不足"
    if pe_pct_median is not None:
        if pe_pct_median >= 80:
            valuation_label = "估值高分位"
        elif pe_pct_median <= 30:
            valuation_label = "估值低分位"
        else:
            valuation_label = "估值中等分位"

    position_label = "位置数据不足"
    if pos_avg is not None:
        if pos_avg >= 70:
            position_label = "年内高位"
        elif pos_avg <= 30:
            position_label = "年内低位"
        else:
            position_label = "年内中位"

    heat_label = ""
    if ret20_avg is not None and ret20_avg >= 15:
        heat_label = "短期涨幅过大"

    labels = [x for x in (valuation_label, position_label, heat_label) if x]
    if ("估值高分位" in labels and "年内高位" in labels) or heat_label:
        overall = "过热警示"
    elif "估值低分位" in labels and "年内低位" in labels:
        overall = "低位区域"
    else:
        overall = "中性"

    return {
        "sample_count": len(rows),
        "pe_median": pe_median,
        "pe_percentile_median": pe_pct_median,
        "pos_52w_avg": pos_avg,
        "ret20_avg": ret20_avg,
        "bias_ma20_avg": bias_avg,
        "labels": labels,
        "overall": overall,
        # 逐股原始行（含无K线指标的股票）：预期差调整/排名表需要每只的分位/市值/资金流，
        # 只给中位数等于把最有用的横截面信息扔掉
        "per_stock": list(per_stock or []),
    }


def collect_industry_valuation(codes: List[str]) -> Optional[Dict[str, Any]]:
    """
    取数外壳：对候选池逐只取 K 线指标与每日指标（PE），组装后交纯函数计算。
    任何一只失败只影响该只，不阻断整体；tushare 未配时 PE 相关自动缺失。
    """
    from storage.sqlite.stock_storage import get_db
    from tools.stock_tools import stock_tool_instance, _ensure_indicators

    db = get_db()
    per_stock = []
    for code in codes or []:
        row: Dict[str, Any] = {"code": code}
        try:
            df = stock_tool_instance.fetch_and_save_stock_daily_data(code)
            if df is None or df.empty:
                continue
            df = _ensure_indicators(df, "daily")
            latest = df.iloc[0]
            row["pos_52w"] = _num(latest.get("pos_52w"))
            close = _num(latest.get("close"))
            ma20 = _num(latest.get("ma20"))
            if close and ma20:
                row["bias_ma20"] = round((close / ma20 - 1) * 100, 2)
            if close is not None and len(df) > 20:
                prev = _num(df.iloc[20].get("close"))
                if prev:
                    row["ret20"] = round((close / prev - 1) * 100, 2)
        except Exception as e:
            logger.warning(f"[行业估值] {code} K线指标获取失败: {e}")
            continue

        try:
            stock_tool_instance.fetch_and_save_stock_basic_daily(code)
            # 拉取10年数据（约2400个交易日），计算多窗口分位
            basic = db.get_latest_daily_basic_data(code, 2500)
            if basic is not None and not basic.empty:
                cur = _num(basic.iloc[0].get("pe_ttm"))
                if cur and cur > 0:
                    row["pe_ttm"] = round(cur, 1)
                    # hist_all 包含当前值，分位计算需排除自身（iloc[1:] 跳过最新一行）
                    hist_all = pd.to_numeric(basic["pe_ttm"], errors="coerce").dropna()
                    hist_excl = hist_all.iloc[1:]  # 排除当前值
                    if len(hist_excl) >= 60:
                        # 主分位：排除当前值后计算（ hist_excl < cur 的比例）
                        row["pe_percentile"] = round(float((hist_excl < cur).mean() * 100), 1)
                        # 多窗口分位（供报告展示，避免口径混乱）
                        # 3年≈750交易日，5年≈1250交易日
                        for label, days in [("pe_pct_3y", 750), ("pe_pct_5y", 1250)]:
                            if len(hist_excl) >= days:
                                hist_window = hist_excl[:days]
                                row[label] = round(float((hist_window < cur).mean() * 100), 1)
                            else:
                                row[label] = round(float((hist_excl < cur).mean() * 100), 1)
                        # 口径标签：标注实际数据长度对应的近似年份
                        actual_len = len(hist_excl)
                        if actual_len >= 2200:
                            row["pe_pct_window"] = "10年"
                        elif actual_len >= 1100:
                            row["pe_pct_window"] = "5年"
                        elif actual_len >= 600:
                            row["pe_pct_window"] = "3年"
                        else:
                            # 数据不足3年，标注实际月数
                            months = actual_len // 21
                            row["pe_pct_window"] = f"{months}个月"
                # 市值（total_mv 单位万元→亿元）：弹性/资金偏好判断都要它
                mv = _num(basic.iloc[0].get("total_mv"))
                if mv:
                    row["total_mv"] = round(mv / 1e4, 1)
        except Exception as e:
            logger.warning(f"[行业估值] {code} 每日指标获取失败: {e}")

        # 近20日主力净流入（单位：亿元，由大单+超大单净计算，万元→亿元）：
        # 用程序数字替代"搜索文本猜主力/游资"；无 token/失败时静默缺失
        try:
            row["mf_net20"] = _moneyflow_net20(code)
        except Exception as e:
            logger.debug(f"[行业估值] {code} 资金流获取失败: {e}")

        # 质量指标：ROE + 扣非占比（来自财务指标，最新年报）
        try:
            fina_df = db.get_stock_fina_indicator(code)
            if fina_df is not None and not fina_df.empty:
                fina_df = fina_df.copy()
                fina_df['_rd'] = pd.to_datetime(fina_df['report_date'], errors='coerce')
                # 找最新年报期的ROE
                ann_fina = fina_df[fina_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False)
                if not ann_fina.empty:
                    latest = ann_fina.iloc[0]
                    roe_val = latest.get('roe')
                    if roe_val is not None and pd.notna(roe_val):
                        roe_float = float(roe_val)
                        # 统一为百分数（如0.15 → 15%）
                        row["roe"] = round(roe_float * 100, 2) if roe_float <= 1.0 else round(roe_float, 2)
                    # 质量指标：扣非净利润/归母净利润（<30%触发否决权）
                    # 用 dt_eps/eps 近似扣非占比（两者在同一报告期的比率≈扣非/归母）
                    dt_eps = latest.get('dt_eps')
                    eps_val = latest.get('eps')
                    if dt_eps is not None and eps_val is not None and pd.notna(dt_eps) and pd.notna(eps_val):
                        eps_f = float(eps_val)
                        if abs(eps_f) > 1e-6:
                            row["deduct_net_ratio"] = round(float(dt_eps) / eps_f * 100, 1)
        except Exception as e:
            logger.debug(f"[行业估值] {code} ROE/扣非获取失败: {e}")

        # 质量指标：商誉/净资产（>25%触发否决权）
        try:
            from tools.stock.tushare_fetcher import TushareFetcher
            from datetime import date as _date, timedelta as _td
            tf = TushareFetcher()
            if tf._api is not None:
                end_d = _date.today().strftime("%Y-%m-%d")
                start_d = (_date.today() - _td(days=400)).strftime("%Y-%m-%d")
                bs_df = tf.balancesheet(code, start_date=start_d, end_date=end_d)
                if bs_df is not None and not bs_df.empty:
                    # 筛选年报（end_date 月份为12）
                    bs_df = bs_df.copy()
                    if 'end_date' in bs_df.columns:
                        bs_df['_rd'] = pd.to_datetime(bs_df['end_date'], errors='coerce')
                        ann_bs = bs_df[bs_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False)
                    else:
                        ann_bs = bs_df
                    if not ann_bs.empty:
                        gw = ann_bs.iloc[0].get('goodwill')
                        equity = ann_bs.iloc[0].get('total_hldr_eqy_inc_min_int')
                        if gw is not None and equity is not None and pd.notna(gw) and pd.notna(equity):
                            eq_float = float(equity)
                            if abs(eq_float) > 1e-6:
                                row["goodwill_ratio"] = round(float(gw) / eq_float * 100, 1)
        except Exception as e:
            logger.debug(f"[行业估值] {code} 商誉获取失败: {e}")

        per_stock.append(row)

    metrics = compute_industry_metrics(per_stock)
    from tools.source_health import report_source
    report_source("行业估值样本", metrics is not None,
                  f"有效样本不足（{len(per_stock)}只入样）" if metrics is None else "")
    return metrics


def compute_main_force_net(mf_df: pd.DataFrame) -> Optional[pd.Series]:
    """按 主力=大单+超大单 的标准口径重新计算主力净流入。

    Tushare 的 net_mf_amount 字段值与 buy/sell 列对不上（差约12倍），
    统一用 大单净额(买-卖) + 超大单净额(买-卖) 重新计算。返回值单位与
    输入列一致（Tushare/AkShare 落库后均为万元）；缺少大单/超大单列返回 None。

    被 industry_metrics._moneyflow_net20 与 monitoring.signal_scanner 共用，
    保证两处资金流口径一致。
    """
    if mf_df is None or mf_df.empty:
        return None
    for col in ('buy_lg_amount', 'sell_lg_amount', 'buy_elg_amount', 'sell_elg_amount'):
        if col not in mf_df.columns:
            return None
    return (pd.to_numeric(mf_df['buy_lg_amount'], errors='coerce').fillna(0)
            + pd.to_numeric(mf_df['buy_elg_amount'], errors='coerce').fillna(0)
            - pd.to_numeric(mf_df['sell_lg_amount'], errors='coerce').fillna(0)
            - pd.to_numeric(mf_df['sell_elg_amount'], errors='coerce').fillna(0))


def _moneyflow_net20(code: str) -> Optional[float]:
    """近20个交易日主力净流入合计（亿元）；数据源不可用返回 None"""
    from datetime import date, timedelta
    from tools.stock_tools import stock_tool_instance

    end = date.today()
    start = end - timedelta(days=40)
    df = stock_tool_instance.tushare.moneyflow(
        code, "", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    if df is None or df.empty:
        return None
    # ★ Tushare 的 net_mf_amount 字段值不准确，用 大单+超大单 重新计算（单位：万元）
    net = compute_main_force_net(df)
    if net is None:
        return None
    if "trade_date" in df.columns:
        net.index = df["trade_date"]
        net = net.sort_index(ascending=False)
    vals = net.dropna()
    if vals.empty:
        return None
    return round(float(vals.head(20).sum()) / 1e4, 2)  # 万元→亿元


def format_industry_valuation(metrics: Optional[Dict[str, Any]]) -> str:
    """格式化为 prompt 文本块；无数据返回空串"""
    if not metrics:
        return ""
    n = metrics["sample_count"]
    # 样本不足5只时中位数没有板块代表性（薄利股的失真PE会主导结果），标题降级并明示
    if n < 5:
        lines = [f"【候选池估值参考（程序按 {n} 只样本计算——样本不足，不代表板块/行业水位，仅供参考）】"]
    else:
        lines = [f"【行业估值与位置（程序按 {n} 只龙头样本计算，仅供风险评估参考）】"]
    if metrics.get("pe_median") is not None:
        pct = f"，PE历史分位中位数 {metrics['pe_percentile_median']}%" \
            if metrics.get("pe_percentile_median") is not None else ""
        lines.append(f"  估值：池内 PE(TTM) 中位数 {metrics['pe_median']}{pct}")
    if metrics.get("pos_52w_avg") is not None:
        lines.append(f"  位置：平均年内位置 {metrics['pos_52w_avg']}%（0=年内最低,100=年内最高）")
    if metrics.get("ret20_avg") is not None:
        lines.append(f"  短期：近20日平均涨幅 {metrics['ret20_avg']}%")
    if metrics.get("bias_ma20_avg") is not None:
        lines.append(f"  乖离：收盘相对MA20平均乖离 {metrics['bias_ma20_avg']}%")
    lines.append(f"  程序参考标签：{metrics['overall']}（{'、'.join(metrics['labels'])}）")
    # 逐股 PE 绝对值+分位（双阈值预警）
    per_stock_display = []
    for s in (metrics.get("per_stock") or []):
        pe = s.get("pe_ttm")
        pct = s.get("pe_percentile")
        if pe is None:
            continue
        pe_str = f"PE{pe:.1f}倍"
        pct_str = f"分位{pct:.0f}%" if pct is not None else ""
        # 双阈值：PE>200倍 → 🔴（无论分位）；PE>100倍 → 🟠；分位>80%且PE>50倍 → 🔴；其余🟢
        if pe > 200:
            level = "🔴"
        elif pe > 100:
            level = "🟠"
        elif (pct is not None and pct >= 80) and pe > 50:
            level = "🔴"
        elif (pct is not None and pct >= 50 and pct < 80) and 50 <= pe <= 100:
            level = "🟡"
        else:
            level = "🟢"
        per_stock_display.append(f"    {level} {s.get('code','')} {pe_str} {pct_str}".rstrip())
    if per_stock_display:
        lines.append("  逐股PE（双阈值预警：🔴PE>200倍/分位>80%且PE>50倍 🟠PE100-200倍 🟡分位50-80%且PE50-100倍 🟢其余）：")
        lines.extend(per_stock_display)
    lines.append("  ⚠️ 使用规则：以上为历史/当前状态的量化描述，回调风险分析须基于这些数字展开，"
                 "禁止在此之外编造估值或概率数字；乖离为负=价格已回落到MA20下方（回调已发生），"
                 "禁止表述为'即将回落/存在回落压力'")
    if n < 5:
        lines.append(f"  ⚠️ 样本仅 {n} 只：禁止称'板块/行业中位数'，引用时必须写明样本数")
    return "\n".join(lines)


def _num(value) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
