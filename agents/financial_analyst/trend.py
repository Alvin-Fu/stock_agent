# -*- coding: utf-8 -*-
"""
财报趋势构建（纯函数，数字全部由代码算，LLM 只解读）：
- 近 N 期营收/净利的同比序列（A股财报是累计口径，绝对值跨期不可比，
  同比序列才是趋势信号）
- 毛利率/净利率序列 + 与上年同期的变化（利润率改善/恶化）
- 单季拆分：同年相邻累计期差分出真实单季营收/净利，并算单季同比
- 程序判读：加速/放缓/改善/回落 由代码判定后写进文本，LLM 禁止另行心算
"""

from typing import Dict, List, Optional


def _f(v) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        return None if x != x else x
    except (TypeError, ValueError):
        return None


def _sign(v: float) -> str:
    return f"+{v:.1f}" if v >= 0 else f"{v:.1f}"


def _norm_rows(records: List[Dict]) -> List[Dict]:
    seen, rows = set(), []
    for r in records:
        d = str(r.get("report_date") or "")[:10]
        if len(d) != 10 or d in seen:
            continue
        seen.add(d)
        rev, np_ = _f(r.get("total_revenue")), _f(r.get("net_profit"))
        rows.append({
            "date": d,
            "rev": rev / 1e8 if rev is not None else None,       # 亿
            "np": np_ / 1e8 if np_ is not None else None,        # 亿
            "rev_yoy": _f(r.get("revenue_growth")),
            "np_yoy": _f(r.get("profit_growth")),
            "gm": _f(r.get("gross_margin")),
            "nm": np_ / rev * 100 if (rev and np_ is not None) else None,  # 净利率
        })
    rows.sort(key=lambda x: x["date"], reverse=True)
    return rows


def _trend_word(latest: float, prev: float, up: str, down: str, flat_eps: float = 0.5) -> str:
    diff = latest - prev
    if abs(diff) < flat_eps:
        return "基本持平"
    return up if diff > 0 else down


def _quarter_label(d: str) -> str:
    q = {"03-31": "Q1", "06-30": "Q2", "09-30": "Q3", "12-31": "Q4"}.get(d[5:])
    return f"{d[:4]}{q}" if q else d


def _single_quarters(rows: List[Dict]) -> List[Dict]:
    """同年相邻累计期差分出单季营收/净利；Q1 累计即单季"""
    by_date = {r["date"]: r for r in rows}
    prev_q = {"06-30": "03-31", "09-30": "06-30", "12-31": "09-30"}
    singles = []
    for r in rows:
        md = r["date"][5:]
        if md == "03-31":
            base = {"rev": 0.0, "np": 0.0}
        else:
            prev_md = prev_q.get(md)
            base = by_date.get(f"{r['date'][:4]}-{prev_md}") if prev_md else None
            if not base or base.get("rev") is None or base.get("np") is None:
                continue
        if r.get("rev") is None or r.get("np") is None:
            continue
        singles.append({
            "date": r["date"],
            "label": _quarter_label(r["date"]),
            "rev": r["rev"] - base["rev"],
            "np": r["np"] - base["np"],
        })
    # 单季同比：对上一年同一单季
    by_label_md = {(s["date"][:4], s["date"][5:]): s for s in singles}
    for s in singles:
        last = by_label_md.get((str(int(s["date"][:4]) - 1), s["date"][5:]))
        s["rev_yoy"] = (s["rev"] / last["rev"] - 1) * 100 if last and last["rev"] else None
        s["np_yoy"] = (s["np"] / last["np"] - 1) * 100 if last and last["np"] and last["np"] > 0 else None
    return singles


def build_income_trend(records: List[Dict], max_periods: int = 6) -> str:
    """输入 get_stock_income 的记录列表，输出趋势文本块；数据不足两期返回空串"""
    rows = _norm_rows(records)
    if len(rows) < 2:
        return ""
    shown = rows[:max_periods]

    lines = ["【财报趋势（程序计算，累计口径：绝对值跨期不可比，看同比列与利润率列）】",
             "报告期 | 营收(亿) | 营收同比% | 净利(亿) | 净利同比% | 毛利率% | 净利率%"]
    for r in shown:
        def _c(v, nd=1):
            return f"{v:.{nd}f}" if v is not None else "-"
        lines.append(f"{r['date']} | {_c(r['rev'])} | {_c(r['rev_yoy'])} | "
                     f"{_c(r['np'])} | {_c(r['np_yoy'])} | {_c(r['gm'])} | {_c(r['nm'])}")

    # ---- 程序判读 ----
    verdicts = []
    latest = shown[0]
    prev = shown[1]
    for key, name in (("rev_yoy", "营收同比"), ("np_yoy", "净利同比")):
        a, b = latest.get(key), prev.get(key)
        if a is not None and b is not None:
            word = _trend_word(a, b, "加速", "放缓")
            verdicts.append(f"{name} {_sign(b)}%→{_sign(a)}%，{word}")

    # 利润率：优先对上年同期（同 MM-DD），口径干净
    by_date = {r["date"]: r for r in rows}
    yoy_row = by_date.get(f"{int(latest['date'][:4]) - 1}{latest['date'][4:]}")
    for key, name in (("gm", "毛利率"), ("nm", "净利率")):
        a = latest.get(key)
        if a is None:
            continue
        if yoy_row and yoy_row.get(key) is not None:
            diff = a - yoy_row[key]
            word = "改善" if diff > 0.3 else ("回落" if diff < -0.3 else "基本持平")
            verdicts.append(f"{name}较上年同期 {_sign(diff)}pct，{word}（{yoy_row[key]:.1f}%→{a:.1f}%）")
        elif prev.get(key) is not None:
            diff = a - prev[key]
            verdicts.append(f"{name}较上一期 {_sign(diff)}pct（相邻累计期口径，仅作参考）")
    if verdicts:
        lines.append("程序判读：" + "；".join(verdicts))

    # ---- 单季拆分 ----
    singles = [s for s in _single_quarters(rows) if s.get("rev") is not None]
    singles.sort(key=lambda s: s["date"], reverse=True)
    if singles:
        sq_lines = []
        for s in singles[:4]:
            seg = f"{s['label']} 营收{s['rev']:.1f}亿"
            if s.get("rev_yoy") is not None:
                seg += f"(同比{_sign(s['rev_yoy'])}%)"
            seg += f" 净利{s['np']:.1f}亿"
            if s.get("np_yoy") is not None:
                seg += f"(同比{_sign(s['np_yoy'])}%)"
            sq_lines.append(seg)
        lines.append("单季拆分（累计差分）：" + "；".join(sq_lines))

    return "\n".join(lines)


def build_expense_trend(records: List[Dict], max_periods: int = 5) -> str:
    """费用率趋势：销售/管理/研发/财务费用占营收比多期序列 + 摊薄/抬升判读"""
    rows = []
    seen = set()
    for r in records:
        d = str(r.get("report_date") or "")[:10]
        rev = _f(r.get("total_revenue"))
        if len(d) != 10 or d in seen or not rev:
            continue
        seen.add(d)
        item = {"date": d}
        total = 0.0
        has_any = False
        for key, name in (("sell_exp", "sell"), ("admin_exp", "admin"),
                          ("rd_exp", "rd"), ("fin_exp", "fin")):
            v = _f(r.get(key))
            if v is not None:
                item[name] = v / rev * 100
                total += item[name]
                has_any = True
            else:
                item[name] = None
        if not has_any:
            continue
        item["total"] = total
        rows.append(item)
    rows.sort(key=lambda x: x["date"], reverse=True)
    if len(rows) < 2:
        return ""

    def _c(v):
        return f"{v:.1f}" if v is not None else "-"

    lines = ["【费用率趋势（占营收比%，累计口径）】",
             "报告期 | 销售 | 管理 | 研发 | 财务 | 合计"]
    for r in rows[:max_periods]:
        lines.append(f"{r['date']} | {_c(r['sell'])} | {_c(r['admin'])} | "
                     f"{_c(r['rd'])} | {_c(r['fin'])} | {_c(r['total'])}")

    latest = rows[0]
    by_date = {r["date"]: r for r in rows}
    yoy = by_date.get(f"{int(latest['date'][:4]) - 1}{latest['date'][4:]}")
    if yoy:
        diff = latest["total"] - yoy["total"]
        word = "摊薄（规模效应显现）" if diff < -0.3 else ("抬升（费用增速快于营收）" if diff > 0.3 else "基本持平")
        lines.append(f"程序判读：期间费用率较上年同期 {_sign(diff)}pct，{word}"
                     f"（{yoy['total']:.1f}%→{latest['total']:.1f}%）")
    return "\n".join(lines)


def build_cashflow_trend(cash_records: List[Dict], income_records: List[Dict],
                         max_periods: int = 5) -> str:
    """现金流趋势：经营现金流/净利润（净现比=利润的现金含量）多期序列 + 判读"""
    np_by_date = {}
    for r in income_records:
        d = str(r.get("report_date") or "")[:10]
        v = _f(r.get("net_profit"))
        if len(d) == 10 and v is not None:
            np_by_date[d] = v / 1e8
    rows, seen = [], set()
    for r in cash_records:
        d = str(r.get("report_date") or "")[:10]
        ocf = _f(r.get("operating_cashflow"))
        if len(d) != 10 or d in seen or ocf is None:
            continue
        seen.add(d)
        np_ = np_by_date.get(d)
        capex = _f(r.get("capex"))
        rows.append({
            "date": d,
            "ocf": ocf / 1e8,
            "np": np_,
            "ratio": ocf / 1e8 / np_ if np_ and np_ > 0 else None,  # 净现比
            "capex": capex / 1e8 if capex is not None else None,
        })
    rows.sort(key=lambda x: x["date"], reverse=True)
    if len(rows) < 2:
        return ""

    def _c(v, nd=1):
        return f"{v:.{nd}f}" if v is not None else "-"

    lines = ["【现金流趋势（累计口径，净现比=经营现金流/净利润，利润的现金含量）】",
             "报告期 | 经营现金流(亿) | 净利(亿) | 净现比 | 资本开支(亿)"]
    for r in rows[:max_periods]:
        lines.append(f"{r['date']} | {_c(r['ocf'])} | {_c(r['np'])} | "
                     f"{_c(r['ratio'], 2)} | {_c(r['capex'])}")

    verdicts = []
    latest = rows[0]
    by_date = {r["date"]: r for r in rows}
    yoy = by_date.get(f"{int(latest['date'][:4]) - 1}{latest['date'][4:]}")
    if latest["ratio"] is not None and yoy and yoy.get("ratio") is not None:
        diff = latest["ratio"] - yoy["ratio"]
        word = "改善" if diff > 0.05 else ("恶化" if diff < -0.05 else "基本持平")
        verdicts.append(f"净现比较上年同期 {yoy['ratio']:.2f}→{latest['ratio']:.2f}，{word}")
    low_ratio = [r for r in rows[:3] if r["ratio"] is not None and r["ratio"] < 0.8]
    if len(low_ratio) >= 2:
        verdicts.append("⚠️ 净现比连续多期低于0.8，利润的现金含量偏低，警惕应收/存货占用")
    if latest["ocf"] < 0:
        verdicts.append("⚠️ 最新一期经营现金流为负")
    if verdicts:
        lines.append("程序判读：" + "；".join(verdicts))
    return "\n".join(lines)


def build_working_capital_trend(balance_records: List[Dict], income_records: List[Dict],
                                max_periods: int = 5) -> str:
    """营运资本趋势：应收/存货占营收比多期变化（回款质量与积压信号）+ 营运资本与流动比率"""
    rev_by_date = {}
    for r in income_records:
        d = str(r.get("report_date") or "")[:10]
        v = _f(r.get("total_revenue"))
        if len(d) == 10 and v:
            rev_by_date[d] = v
    rows, seen = [], set()
    for r in balance_records:
        d = str(r.get("report_date") or "")[:10]
        if len(d) != 10 or d in seen:
            continue
        seen.add(d)
        ca, cl = _f(r.get("current_assets")), _f(r.get("current_liabilities"))
        ar, inv = _f(r.get("accounts_receivable")), _f(r.get("inventory"))
        rev = rev_by_date.get(d)
        rows.append({
            "date": d,
            "ar": ar / 1e8 if ar is not None else None,
            "inv": inv / 1e8 if inv is not None else None,
            "ar_pct": ar / rev * 100 if (ar is not None and rev) else None,
            "inv_pct": inv / rev * 100 if (inv is not None and rev) else None,
            "wc": (ca - cl) / 1e8 if (ca is not None and cl is not None) else None,
            "cr": _f(r.get("current_ratio")),
        })
    rows.sort(key=lambda x: x["date"], reverse=True)
    if len(rows) < 2:
        return ""

    def _c(v, nd=1):
        return f"{v:.{nd}f}" if v is not None else "-"

    lines = ["【营运资本趋势（应收/存货为期末余额，占比=期末余额÷该期累计营收，看变化方向）】",
             "报告期 | 应收(亿) | 应收/营收% | 存货(亿) | 存货/营收% | 营运资本(亿) | 流动比率"]
    for r in rows[:max_periods]:
        lines.append(f"{r['date']} | {_c(r['ar'])} | {_c(r['ar_pct'])} | {_c(r['inv'])} | "
                     f"{_c(r['inv_pct'])} | {_c(r['wc'], 0)} | {_c(r['cr'], 2)}")

    latest = rows[0]
    by_date = {r["date"]: r for r in rows}
    yoy = by_date.get(f"{int(latest['date'][:4]) - 1}{latest['date'][4:]}")
    verdicts = []
    if yoy:
        for key, up_word, down_word, label in (
                ("ar_pct", "回款占用加重（营收降但应收升要警惕渠道压货）", "回款改善", "应收占比"),
                ("inv_pct", "存货积压加重", "存货消化", "存货占比")):
            a, b = latest.get(key), yoy.get(key)
            if a is not None and b is not None:
                diff = a - b
                if abs(diff) >= 1.0:
                    verdicts.append(f"{label}较上年同期 {_sign(diff)}pct，"
                                    f"{up_word if diff > 0 else down_word}")
    if verdicts:
        lines.append("程序判读：" + "；".join(verdicts))
    return "\n".join(lines)


def build_full_trend(income_records: List[Dict],
                     cash_records: Optional[List[Dict]] = None,
                     balance_records: Optional[List[Dict]] = None) -> str:
    """组合：利润趋势 + 费用率趋势 + 现金流趋势 + 营运资本趋势（缺哪块跳哪块）"""
    blocks = [build_income_trend(income_records),
              build_expense_trend(income_records)]
    if cash_records:
        blocks.append(build_cashflow_trend(cash_records, income_records))
    if balance_records:
        blocks.append(build_working_capital_trend(balance_records, income_records))
    return "\n\n".join(b for b in blocks if b)
