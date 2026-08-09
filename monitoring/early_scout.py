# -*- coding: utf-8 -*-
"""
周六行业早期信号巡逻（Early Scout）
======================================
每周六自动扫描热门行业，发现处于"早期埋伏区间"的行业：
1. 行业已经跌了一段时间（60日跌幅大）
2. 最近开始企稳（20日不再创新低）
3. 估值不在高位（PE非极端）
4. 未被近期研究过（30天内不重复）

筛出候选行业后 → 自动运行深度产业链分析 → 生成带触发条件的预研报告 → 触发条件写 DB 自动盯梢。
"""

import json
import traceback
from datetime import date, timedelta
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.logger import logger


# ══════════════════════════════════════════════
# 配置
# ══════════════════════════════════════════════

# 扫描的行业列表（同 value_discovery 的 20 个热门申万二级行业）
SCOUT_INDUSTRIES: List[Tuple[str, str]] = [
    ("801730.SI", "汽车零部件"), ("801740.SI", "乘用车"),
    ("801081.SI", "半导体"), ("801082.SI", "元器件"),
    ("801771.SI", "航空装备"), ("801772.SI", "航天装备"),
    ("801736.SI", "电池"), ("801735.SI", "电网设备"),
    ("801761.SI", "医疗服务"), ("801762.SI", "医疗器械"),
    ("801153.SI", "白酒"), ("801151.SI", "食品加工"),
    ("801881.SI", "软件开发"), ("801882.SI", "IT服务"),
    ("801741.SI", "商用车"), ("801731.SI", "电机"),
    ("801884.SI", "通信服务"), ("801883.SI", "通信设备"),
    ("801711.SI", "装修建材"), ("801712.SI", "工程机械"),
]

# 早期信号判定阈值
EARLY_SIGNAL_THRESHOLDS = {
    "60d_decline_min": -10.0,       # 60 日跌幅超过此值认为"已经跌了"
    "20d_stability_max": -2.0,      # 20 日跌幅小于此值（即接近零）认为"企稳"
    "max_pe": 60,                   # PE 超过此值认为太贵
    "max_candidates": 3,            # 每轮最多研究几个行业
    "min_cache_days": 30,           # 研究过的行业 N 天内不重复
}

# 行业雷达评分权重
SCORE_WEIGHTS = {
    "decline_60d": 0.4,     # 60 日跌幅越大分越高（跌透了）
    "stability_20d": 0.3,   # 20 日越稳分越高（见底企稳）
    "pe_low": 0.3,          # PE 越低分越高（便宜）
}


# ══════════════════════════════════════════════
# 内部工具
# ══════════════════════════════════════════════

def _get_ts_pro():
    """获取 Tushare pro 实例"""
    import tushare as ts
    from utils.config import get_stock_tools_config
    cfg = get_stock_tools_config()
    ts.set_token(cfg["tushare_token"])
    return ts.pro_api()


def _get_industry_perf(code: str, name: str) -> Optional[Dict[str, Any]]:
    """获取一个行业的 60 日/20 日涨跌幅和最新 PE"""
    try:
        pro = _get_ts_pro()
        today_str = date.today().strftime("%Y%m%d")
        start_90 = (date.today() - timedelta(days=90)).strftime("%Y%m%d")

        df = pro.index_daily(ts_code=code, start_date=start_90, end_date=today_str,
                             fields="trade_date,pct_chg,close")
        if df is None or df.empty:
            return None
        df = df.sort_values("trade_date")
        ret60 = df["pct_chg"].tail(60).sum()
        ret20 = df["pct_chg"].tail(20).sum()

        # 最新收盘价
        close = float(df.iloc[-1]["close"]) if "close" in df.columns else None

        # 最新 PE
        pe_latest = None
        try:
            idx_d = pro.index_dailybasic(ts_code=code, start_date=today_str, end_date=today_str,
                                          fields="pe")
            if idx_d is not None and not idx_d.empty and "pe" in idx_d.columns:
                pe_latest = float(idx_d.iloc[0]["pe"])
        except Exception:
            pass

        # 近 5 日是否创新低（企稳判断）
        recent_5 = df.tail(5)
        new_low_5 = close is not None and len(recent_5) >= 5 and close <= recent_5["close"].min()

        return {
            "code": code, "name": name,
            "ret60": round(ret60, 2), "ret20": round(ret20, 2),
            "pe": pe_latest, "close": close,
            "new_low_5": new_low_5,
        }
    except Exception as e:
        logger.debug(f"行业指数[{code}]获取失败: {e}")
        return None


def _compute_early_signal_score(perf: Dict[str, Any]) -> float:
    """计算行业的"早期埋伏信号"综合分（0-100）。
    各维度先归一化到 0-100，再用 SCORE_WEIGHTS 加权求和，确保权重字典真正生效
    （修复原 decline_score * 0.4 / 0.4 这类冗余计算——权重被抵消、SCORE_WEIGHTS 形同虚设）。"""
    t = EARLY_SIGNAL_THRESHOLDS
    w = SCORE_WEIGHTS

    # 1) 60 日跌幅得分（0-100）：跌越多分越高（跌透了才有埋伏价值）
    ret60 = perf.get("ret60", 0) or 0
    if ret60 < 0:
        # 跌幅超 20% → 满分 100；跌幅 10% → 50 分；没跌 → 0
        decline_score = min(abs(ret60) / 20.0, 1.0) * 100
    else:
        decline_score = 0.0

    # 2) 20 日企稳得分（0-100）：最近不创新低 + 跌幅收窄
    ret20 = perf.get("ret20", 0) or 0
    new_low = perf.get("new_low_5", True)
    if not new_low and ret20 >= t["20d_stability_max"]:
        stabilize_score = 100.0  # 不创新低 + 企稳 → 满分
    elif not new_low:
        stabilize_score = 50.0   # 不创新低但还在跌
    elif ret20 >= t["20d_stability_max"]:
        stabilize_score = 33.0   # 创新低但跌幅收窄
    else:
        stabilize_score = 0.0

    # 3) PE 得分（0-100）：越低越好
    pe = perf.get("pe")
    if pe is not None and pe > 0:
        if pe <= 15:
            pe_score = 100.0
        elif pe <= 25:
            pe_score = 83.0
        elif pe <= 40:
            pe_score = 50.0
        elif pe <= 60:
            pe_score = 17.0
        else:
            pe_score = 0.0
    else:
        pe_score = 33.0  # 无 PE 数据给中等的分

    # 加权求和：SCORE_WEIGHTS 字典真正生效
    total = (decline_score * w["decline_60d"]
             + stabilize_score * w["stability_20d"]
             + pe_score * w["pe_low"])
    return round(total, 1)


def _get_recently_studied_industries(days: int = 30) -> List[str]:
    """查询最近 N 天已经研究过的行业，用于去重"""
    try:
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        from sqlalchemy import text
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        with db.get_session() as session:
            rows = session.execute(
                text("SELECT DISTINCT industry_name FROM industry_snapshot WHERE created_at >= :cutoff"),
                {"cutoff": cutoff}
            ).fetchall()
            return [r[0] for r in rows if r[0]]
    except Exception as e:
        logger.warning(f"查询最近研究行业失败（不影响主流程）: {e}")
        return []


def _push_to_feishu(title: str, content: str, task_id: str = ""):
    """推送消息到飞书"""
    try:
        from monitoring.notifier import FeishuNotifier
        notifier = FeishuNotifier()
        notifier.send_card_text(content[:3500], title, task_id=task_id)
    except Exception as e:
        logger.warning(f"飞书推送失败: {e}")


def _save_trigger_conditions(industry_name: str, conditions: List[Dict]):
    """保存触发条件到 DB"""
    try:
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        db.save_industry_triggers(industry_name, conditions)
    except Exception as e:
        logger.warning(f"触发条件保存失败（{industry_name}）: {e}")


def _format_scout_report(industry_name: str, analysis_summary: str, triggers: List[Dict]) -> str:
    """格式化巡逻报告"""
    lines = [
        f"## 📋 行业：{industry_name}\n",
    ]
    # 分析摘要
    if analysis_summary:
        lines.append(analysis_summary[:2000])
        lines.append("")

    # 触发条件
    if triggers:
        lines.append("### 🔔 可监控的触发条件（已自动写入 DB 盯梢）\n")
        for t in triggers:
            desc = t.get("description", "")
            ttype = t.get("trigger_type", "")
            lines.append(f"- [{ttype}] {desc}")
        lines.append("")

    return "\n".join(lines)


# ══════════════════════════════════════════════
# 核心逻辑
# ══════════════════════════════════════════════

def _run_industry_deep_research(industry_name: str) -> Tuple[str, List[Dict]]:
    """对候选行业运行深度产业链分析，返回(分析摘要, 触发条件列表)"""
    try:
        from orchestration.workflow import WorkflowExecutor
        executor = WorkflowExecutor()
        question = f"分析{industry_name}产业链上下游，筛选所有关键公司，分析基本面、护城河、边际变化"
        result = executor.run_sync(question, industry_name=industry_name)

        research_result = result.get("research_result") or {}
        summary = research_result.get("summary", "")
        triggers = research_result.get("reeval_triggers", [])

        # 兜底：从 summary 提取触发条件
        if not triggers and "重估触发条件" in summary:
            import re
            trigger_lines = re.findall(r"- \[(.*?)\]\s*(.*?)(?:\n|$)", summary)
            triggers = [{"trigger_type": t[0], "description": t[1]} for t in trigger_lines]

        return summary[:3000], triggers
    except Exception as e:
        logger.error(f"行业深度分析失败 [{industry_name}]: {e}")
        return f"（深度分析异常: {e}）", []


def run_early_scout() -> str:
    """
    主入口：周六早期信号巡逻
    1. 扫描 20 个热门行业的涨跌幅 + PE
    2. 计算早期信号综合分
    3. 去重（30 天内已研究的跳过）
    4. 选分最高的 2-3 个行业
    5. 跑深度产业链分析
    6. 生成报告 + 保存触发条件
    """
    t = EARLY_SIGNAL_THRESHOLDS
    reports = []

    try:
        # ── Step 1：获取所有行业的涨跌幅 + PE ──
        logger.info("[EarlyScout] Step 1/4: 扫描行业涨跌幅...")
        industry_perfs = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_get_industry_perf, c, n): n for c, n in SCOUT_INDUSTRIES}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    industry_perfs.append(r)

        if not industry_perfs:
            return "❌ 未获取到任何行业数据"

        # ── Step 2：计算综合分 + 去重 ──
        logger.info("[EarlyScout] Step 2/4: 计算早期信号分...")
        recently_studied = _get_recently_studied_industries(t["min_cache_days"])

        scored = []
        for perf in industry_perfs:
            name = perf.get("name", "")
            ret60 = perf.get("ret60", 0) or 0
            ret20 = perf.get("ret20", 0) or 0
            new_low = perf.get("new_low_5", True)
            pe = perf.get("pe")

            # 基本筛选：60 日跌幅不够、20 日还在暴跌、PE 过高 → 跳过
            if ret60 > t["60d_decline_min"]:
                continue  # 没跌够的不算"低位"
            if ret20 < t["20d_stability_max"] * 2:
                continue  # 最近还在暴跌的不算"企稳"
            if pe is not None and pe > t["max_pe"]:
                continue  # PE 太高不算便宜
            if name in recently_studied:
                logger.info(f"[EarlyScout] 跳过最近已研究过的行业: {name}")
                continue

            signal_score = _compute_early_signal_score(perf)
            scored.append((signal_score, perf))

        scored.sort(key=lambda x: x[0], reverse=True)
        candidates = scored[:t["max_candidates"]]

        if not candidates:
            return "🔭 本周未发现处于早期埋伏区间的行业。\n\n" + _format_snapshot(industry_perfs, recently_studied)

        # ── Step 3：对候选行业运行深度分析 ──
        logger.info(f"[EarlyScout] Step 3/4: 深度分析 {len(candidates)} 个候选行业...")
        for signal_score, perf in candidates:
            name = perf.get("name", "")
            logger.info(f"[EarlyScout] → 分析: {name}（信号分 {signal_score}）")

            analysis_summary, triggers = _run_industry_deep_research(name)

            # 保存触发条件
            if triggers:
                _save_trigger_conditions(name, triggers)

            report = _format_scout_report(name, analysis_summary, triggers)
            reports.append(report)

        # ── Step 4：汇总推送 ──
        logger.info("[EarlyScout] Step 4/4: 生成汇总报告...")
        summary_header = (
            f"📊 **周六早期信号巡逻报告**\n"
            f"扫描日期：{date.today().isoformat()}\n"
            f"扫描行业：{len(industry_perfs)} 个 | "
            f"去重跳过：{len([n for n,_ in SCOUT_INDUSTRIES if n in recently_studied])} 个\n"
            f"候选研究：{len(candidates)} 个行业\n\n"
        )

        # 候选行业信号摘要
        candidate_summary = "### 🎯 研究的行业\n\n"
        candidate_summary += "| 行业 | 信号分 | 60日涨跌 | 20日涨跌 | PE |\n"
        candidate_summary += "|------|--------|---------|---------|-----|\n"
        for sig, perf in candidates:
            candidate_summary += (
                f"| {perf.get('name','')} | {sig} | "
                f"{perf.get('ret60','N/A')}% | "
                f"{perf.get('ret20','N/A')}% | "
                f"{perf.get('pe','N/A')} |\n"
            )

        full_report = summary_header + candidate_summary + "\n\n" + "\n\n".join(reports)

        # 推送到飞书
        _push_to_feishu("周六早期信号巡逻", full_report[:3500], task_id="early_scout")

        return full_report

    except Exception as e:
        err_msg = f"❌ 早期信号巡逻异常: {e}\n{traceback.format_exc()}"
        logger.error(err_msg)
        _push_to_feishu("周六早期信号巡逻失败", str(e)[:500])
        return err_msg


def _format_snapshot(industry_perfs: List[Dict], recently_studied: List[str]) -> str:
    """当没有候选时，输出当前行业状态快照"""
    lines = [
        "### 📊 当前行业状态快照\n",
        "| 行业 | 60日涨跌 | 20日涨跌 | PE | 状态 |",
        "|------|---------|---------|-----|------|",
    ]
    for perf in sorted(industry_perfs, key=lambda x: x.get("ret60", 0) or 0):
        name = perf.get("name", "")
        status = "⏳ 已研究" if name in recently_studied else ""
        lines.append(
            f"| {name} | {perf.get('ret60','N/A')}% | "
            f"{perf.get('ret20','N/A')}% | "
            f"{perf.get('pe','N/A')} | {status} |"
        )
    return "\n".join(lines)
