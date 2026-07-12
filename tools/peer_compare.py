# -*- coding: utf-8 -*-
"""
同行横向对比（可比公司表）：
- 同行来源：stock_basic 表的行业字段（tushare 口径）
- 估值/市值：东财全市场快照（一次调用当日缓存；PE为动态口径，表头注明）
- 增速/毛利率：库内利润表（前4家没有数据时现拉一次，之后走库）
输出：目标公司在同行中的相对位置，"贵不贵/强不强"从此有参照系。
分部估值(SOTP)也引用本表的同行 PE 作为倍数锚。
"""

import threading
import time
from datetime import date
from typing import Dict, List, Optional, Tuple

from utils.logger import logger

_SPOT_CACHE = {"day": None, "map": None}
_spot_lock = threading.Lock()

MAX_PEERS = 6          # 表中展示的同行数（按市值取最大的）
MAX_INCOME_FETCH = 4   # 单次分析最多为几家同行现拉利润表（其余显示"-"，下次分析会逐步补全）


def _num(v) -> Optional[float]:
    try:
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _load_market_spot() -> Dict[str, Dict]:
    """东财全市场行情快照（当日缓存）：code -> {pe, pb, mv亿, name}"""
    today = date.today()
    if _SPOT_CACHE["map"] is not None and _SPOT_CACHE["day"] == today:
        return _SPOT_CACHE["map"]
    with _spot_lock:
        if _SPOT_CACHE["map"] is not None and _SPOT_CACHE["day"] == today:
            return _SPOT_CACHE["map"]
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            m = {}
            for _, r in df.iterrows():
                code = str(r.get("代码", "")).strip()
                if len(code) != 6:
                    continue
                mv = _num(r.get("总市值"))
                m[code] = {
                    "pe": _num(r.get("市盈率-动态")),
                    "pb": _num(r.get("市净率")),
                    "mv": mv / 1e8 if mv else None,   # 亿
                    "name": str(r.get("名称", "")).strip(),
                }
            if m:
                _SPOT_CACHE.update(day=today, map=m)
            return m
        except Exception as e:
            logger.warning(f"[同行对比] 全市场快照获取失败: {e}")
            return _SPOT_CACHE["map"] or {}


def _income_metrics(db, code: str, allow_fetch: bool) -> Dict[str, Optional[float]]:
    """最新报告期营收/净利同比与毛利率；库里没有且允许时现拉一次"""
    out = {"rev_yoy": None, "np_yoy": None, "gm": None}
    try:
        df = db.get_stock_income(code)
        if (df is None or df.empty) and allow_fetch:
            from tools.stock_tools import stock_tool_instance
            stock_tool_instance.fetch_and_save_stock_income(code)
            df = db.get_stock_income(code)
        if df is not None and not df.empty:
            latest = df.iloc[0]
            out["rev_yoy"] = _num(latest.get("revenue_growth"))
            out["np_yoy"] = _num(latest.get("profit_growth"))
            out["gm"] = _num(latest.get("gross_margin"))
    except Exception as e:
        logger.warning(f"[同行对比] {code} 财务指标获取失败: {e}")
    return out


def build_peer_table(target: Dict, peers: List[Dict], industry: str) -> str:
    """纯格式化：rows 含 code/name/pe/pb/mv/rev_yoy/np_yoy/gm"""
    if not peers:
        return ""

    def _c(v, nd=1):
        return f"{v:.{nd}f}" if v is not None else "-"

    lines = [f"【同行对比（行业「{industry}」，市值前{len(peers)}家；PE为动态口径，"
             f"与PE(TTM)略有差异；增速为最新报告期累计同比）】",
             "公司 | 总市值(亿) | PE(动) | PB | 营收同比% | 净利同比% | 毛利率%"]
    for r in [target] + peers:
        tag = "★" if r is target else "· "
        lines.append(f"{tag}{r['name']}({r['code']}) | {_c(r.get('mv'), 0)} | {_c(r.get('pe'))} | "
                     f"{_c(r.get('pb'), 2)} | {_c(r.get('rev_yoy'))} | {_c(r.get('np_yoy'))} | {_c(r.get('gm'))}")

    # 程序判读：目标 PE / 毛利率在同行中的位置
    verdicts = []
    tpe = target.get("pe")
    peer_pes = sorted(p["pe"] for p in peers if p.get("pe") is not None and p["pe"] > 0)
    if tpe is not None and tpe > 0 and len(peer_pes) >= 3:
        below = sum(1 for p in peer_pes if p < tpe)
        verdicts.append(f"目标PE高于 {below}/{len(peer_pes)} 家同行（同行PE区间 {peer_pes[0]:.1f}~{peer_pes[-1]:.1f}）")
    tgm = target.get("gm")
    peer_gms = [p["gm"] for p in peers if p.get("gm") is not None]
    if tgm is not None and len(peer_gms) >= 3:
        verdicts.append(f"目标毛利率{'高于' if tgm > sum(peer_gms) / len(peer_gms) else '低于'}同行均值"
                        f"（{tgm:.1f}% vs {sum(peer_gms) / len(peer_gms):.1f}%）")
    if verdicts:
        lines.append("程序判读：" + "；".join(verdicts))
    return "\n".join(lines)


def fetch_peer_table(code: str) -> Tuple[str, List[Dict]]:
    """
    生成同行对比表。返回 (文本块, 同行指标列表——供分部估值取可比倍数)。
    任一环节失败降级：返回 ("", [])，不阻断分析。
    """
    try:
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        basic = db.get_stock_basic(code)
        industry = getattr(basic, "industry", None) if basic else None
        if not industry:
            logger.warning(f"[同行对比] {code} 无行业信息，跳过")
            return "", []
        candidates = db.get_peers_by_industry(industry, exclude_code=code, limit=20)
        if not candidates:
            return "", []

        spot = _load_market_spot()
        rows = []
        for p in candidates:
            s = spot.get(p["code"])
            if s and s.get("mv"):
                rows.append({"code": p["code"], "name": p["name"] or s["name"],
                             "pe": s["pe"], "pb": s["pb"], "mv": s["mv"]})
        rows.sort(key=lambda r: r["mv"], reverse=True)
        rows = rows[:MAX_PEERS]

        for i, r in enumerate(rows):
            r.update(_income_metrics(db, r["code"], allow_fetch=i < MAX_INCOME_FETCH))

        ts = spot.get(code) or {}
        target = {"code": code, "name": getattr(basic, "name", "") or ts.get("name", code),
                  "pe": ts.get("pe"), "pb": ts.get("pb"), "mv": ts.get("mv")}
        target.update(_income_metrics(db, code, allow_fetch=True))

        return build_peer_table(target, rows, industry), rows
    except Exception as e:
        logger.warning(f"[同行对比] 生成失败 {code}: {e}")
        return "", []
