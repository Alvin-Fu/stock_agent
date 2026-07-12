# -*- coding: utf-8 -*-
"""
主营业务构成（利润驱动分析的数字底座）：
- 数据源：东财「主营构成」接口（ak.stock_zygc_em，免费；tushare 同类接口 fina_mainbz
  需要高积分权限，故不作为来源）
- 输出：最新报告期各业务的收入/收入占比/利润占比/毛利率，并对比上年同期占比变化
  （占比持续提升的业务 = 正在放量的第二曲线；占比萎缩 = 旧驱动衰减），
  按产品和按地区两个维度都给（按地区维度直接反映"出海"这类驱动）
- 原则：失败返回空串并记 warning，绝不阻断分析主流程
"""

from typing import List, Dict, Optional

from utils.logger import logger

# 每个维度最多展示的业务条数（太长会稀释 prompt）
_MAX_ITEMS = 6


def _em_symbol(code: str) -> str:
    """6位代码转东财带市场前缀写法"""
    if code.startswith("6"):
        return f"SH{code}"
    if code.startswith(("0", "3")):
        return f"SZ{code}"
    return f"BJ{code}"


def _num(v) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace("%", "").replace(",", "").strip()
            if not v or v in ("-", "--"):
                return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def _pct_scale(values: List[Optional[float]]) -> float:
    """
    判断这一组占比是小数写法(0.79)还是百分数写法(79.41)：
    组内最大值 <=1.01 视为小数，需要 *100。按组判断，避免小占比项(0.8%)被误放大。
    """
    nums = [v for v in values if v is not None]
    if nums and max(nums) <= 1.01:
        return 100.0
    return 1.0


def _fmt_pct(v: Optional[float], scale: float) -> Optional[float]:
    return round(v * scale, 1) if v is not None else None


def build_main_business_text(records: List[Dict], source: str = "东方财富") -> str:
    """
    纯函数：把主营构成记录（list[dict]，键为东财中文列名）格式化为 prompt 文本块。
    records 需含：报告日期/分类类型/主营构成/主营收入/收入比例/主营利润/利润比例/毛利率
    """
    if not records:
        return ""

    periods = sorted({str(r.get("报告日期", ""))[:10] for r in records if r.get("报告日期")}, reverse=True)
    if not periods:
        return ""
    latest = periods[0]
    # 上年同期：同月同日、年份-1
    prev_year = None
    try:
        cand = f"{int(latest[:4]) - 1}{latest[4:]}"
        if cand in periods:
            prev_year = cand
    except ValueError:
        pass

    lines = [f"【主营业务构成（报告期 {latest}，来源 {source}，收入单位 亿元）】"]
    if prev_year:
        lines.append(f"  （占比变化对比上年同期 {prev_year}：占比持续提升的业务=正在放量的驱动，萎缩=旧驱动衰减）")

    # 按产品优先（利润驱动主视角），按地区其次（直接反映出海等区域驱动）
    type_order = ["按产品分类", "按产品", "按行业分类", "按行业", "按地区分类", "按地区"]
    present_types = []
    for t in type_order:
        if any(str(r.get("分类类型", "")).strip() == t for r in records):
            # 归并"按产品/按行业"族，只取第一个命中的产品族 + 第一个命中的地区族
            family = "地区" if "地区" in t else "产品"
            if family not in [f for f, _ in present_types]:
                present_types.append((family, t))

    section_count = 0
    for family, type_name in present_types:
        rows = [r for r in records
                if str(r.get("分类类型", "")).strip() == type_name
                and str(r.get("报告日期", ""))[:10] == latest
                and str(r.get("主营构成", "")).strip() not in ("", "合计")]
        if not rows:
            continue
        scale = _pct_scale([_num(r.get("收入比例")) for r in rows])
        rows.sort(key=lambda r: _num(r.get("主营收入")) or 0, reverse=True)

        prev_share = {}
        if prev_year:
            prev_rows = [r for r in records
                         if str(r.get("分类类型", "")).strip() == type_name
                         and str(r.get("报告日期", ""))[:10] == prev_year]
            prev_scale = _pct_scale([_num(r.get("收入比例")) for r in prev_rows])
            for r in prev_rows:
                share = _fmt_pct(_num(r.get("收入比例")), prev_scale)
                if share is not None:
                    prev_share[str(r.get("主营构成", "")).strip()] = share

        section_count += 1
        lines.append(f"◆ {type_name}：")
        for r in rows[:_MAX_ITEMS]:
            name = str(r.get("主营构成", "")).strip()
            revenue = _num(r.get("主营收入"))
            rev_share = _fmt_pct(_num(r.get("收入比例")), scale)
            profit_share = _fmt_pct(_num(r.get("利润比例")), scale)
            margin = _fmt_pct(_num(r.get("毛利率")), _pct_scale([_num(r.get("毛利率"))]))
            seg = f"  - {name}:"
            if revenue is not None:
                seg += f" 收入{revenue / 1e8:.1f}亿"
            if rev_share is not None:
                seg += f" 占收入{rev_share}%"
            if profit_share is not None:
                seg += f" 占利润{profit_share}%"
            if margin is not None:
                seg += f" 毛利率{margin}%"
            if rev_share is not None and name in prev_share:
                diff = round(rev_share - prev_share[name], 1)
                seg += f" | 上年同期占比{prev_share[name]}%（{'+' if diff >= 0 else ''}{diff}pct）"
            lines.append(seg)

    return "\n".join(lines) if section_count else ""


def latest_profit_split(records: List[Dict]) -> List[Dict]:
    """
    最新年报（12-31 期）按产品维度的分部利润占比（纯函数，分部估值 SOTP 用）。
    返回 [{"name", "profit_share_pct", "rev_share_pct}]；无年报数据返回 []。
    """
    if not records:
        return []
    fy_dates = sorted({str(r.get("报告日期", ""))[:10] for r in records
                       if str(r.get("报告日期", ""))[:10].endswith("12-31")}, reverse=True)
    if not fy_dates:
        return []
    latest_fy = fy_dates[0]
    rows = [r for r in records
            if str(r.get("报告日期", ""))[:10] == latest_fy
            and str(r.get("分类类型", "")).strip() in ("按产品分类", "按产品", "按行业分类", "按行业")
            and str(r.get("主营构成", "")).strip() not in ("", "合计")]
    if not rows:
        return []
    scale = _pct_scale([_num(r.get("利润比例")) for r in rows])
    rev_scale = _pct_scale([_num(r.get("收入比例")) for r in rows])
    out = []
    for r in rows:
        ps = _fmt_pct(_num(r.get("利润比例")), scale)
        rs = _fmt_pct(_num(r.get("收入比例")), rev_scale)
        if ps is None:
            continue
        out.append({"name": str(r.get("主营构成", "")).strip(),
                    "profit_share_pct": ps, "rev_share_pct": rs,
                    "period": latest_fy})
    out.sort(key=lambda x: x["profit_share_pct"], reverse=True)
    return out[:6]


def fetch_main_business_records(code: str) -> List[Dict]:
    """拉取主营业务构成原始记录；任何失败返回空列表"""
    try:
        import akshare as ak
        fetch = getattr(ak, "stock_zygc_em", None)
        if fetch is None:
            logger.warning("[信源] 当前 akshare 版本无 stock_zygc_em 接口，跳过主营构成")
            return []
        df = fetch(symbol=_em_symbol(code))
        if df is None or df.empty:
            return []
        return df.to_dict("records")
    except Exception as e:
        logger.warning(f"[信源] 主营业务构成获取失败 {code}: {e}")
        return []


def fetch_main_business_text(code: str) -> str:
    """拉取并格式化主营业务构成；任何失败返回空串"""
    return build_main_business_text(fetch_main_business_records(code))
