# -*- coding: utf-8 -*-
"""
阈值校准报告：用复盘积累的事后收益检验系统里"拍出来"的阈值——
阶段门槛(7/6/5分)、盈亏比1.5、预期差调整(±1分)、排名权重。
样本不足时明确说"继续积累"，不给伪结论。

用法（repo 根目录）：python eval/calibrate_thresholds.py
只读库，不联网、不写库。
"""

import os
import sys
import statistics

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MIN_STOCK_SAMPLES = 30     # 个股方向判断最少可对账次数
MIN_INDUSTRY_SAMPLES = 15  # 产业链复盘最少次数
MIN_GATE_PAIRS = 8         # 进池/剔除对照最少配对数


def _auto_feedback_thresholds(reviews, acc, pairs, lines):
    """根据校准结果自动调整配置权重。

    当校准数据发现明显趋势时（剔除组反而更强、命中率偏低、排名反向占比高），
    自动微调 stage_weights / tech_weights 并写回 local.yaml，下次分析自动生效。
    每次调整受 STEP_LIMIT / CUMULATIVE_LIMIT 约束，防止过度矫正。
    """
    from tools.weight_adjuster import (
        get_stage_weights, get_tech_weights, _save_weights_to_config,
        _load_weights_from_config, DEFAULT_STAGE_WEIGHTS, DEFAULT_TECH_WEIGHTS,
        STEP_LIMIT, CUMULATIVE_LIMIT,
    )
    from datetime import datetime

    adjustments_made = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    cfg = _load_weights_from_config()

    # 1. 门槛有效性反馈：剔除组明显更强 → 降低 momentum，提升 moat
    if len(pairs) >= MIN_GATE_PAIRS:
        diffs = [p - e for p, e in pairs]
        if statistics.mean(diffs) < -1.0:
            stage_weights = get_stage_weights()
            changed = False
            for stage_name, sw in stage_weights.items():
                old_mom = sw.get("momentum", 0)
                old_moat = sw.get("moat", 0)
                new_mom = max(0.05, old_mom - STEP_LIMIT)
                new_moat = old_moat + (old_mom - new_mom)
                default_mom = DEFAULT_STAGE_WEIGHTS.get(stage_name, {}).get("momentum", old_mom)
                if abs(new_mom - default_mom) <= CUMULATIVE_LIMIT:
                    sw["momentum"] = round(new_mom, 2)
                    sw["moat"] = round(new_moat, 2)
                    changed = True
            if changed:
                cfg["stage_weights"] = stage_weights
                adjustments_made.append(
                    f"门槛反馈：降低momentum {STEP_LIMIT}，提升moat {STEP_LIMIT}")

    # 2. 方向判断偏差反馈：总体命中率偏低 → 降低 daily，提升 weekly
    judged = acc.get("judged", 0)
    if judged >= MIN_STOCK_SAMPLES:
        accuracy = acc.get("accuracy", 50)
        if accuracy < 45:
            tech_weights = get_tech_weights()
            old_daily = tech_weights.get("daily", 0.5)
            old_weekly = tech_weights.get("weekly", 0.3)
            new_daily = max(0.35, old_daily - STEP_LIMIT)
            new_weekly = old_weekly + (old_daily - new_daily)
            default_daily = DEFAULT_TECH_WEIGHTS.get("daily", old_daily)
            if abs(new_daily - default_daily) <= CUMULATIVE_LIMIT:
                tech_weights["daily"] = round(new_daily, 2)
                tech_weights["weekly"] = round(new_weekly, 2)
                cfg["tech_weights"] = tech_weights
                adjustments_made.append(
                    f"方向偏差反馈：降低daily {STEP_LIMIT}，提升weekly {STEP_LIMIT}")

    # 3. 产业链排名反向反馈：反向占比超 40% → 降低 fundamental，提升 momentum
    if len(reviews) >= MIN_INDUSTRY_SAMPLES:
        eff = [r.get("rank_effective") for r in reviews]
        total_eff = len([e for e in eff if e in ("有效", "无区分", "反向")])
        if total_eff > 0:
            reverse_ratio = eff.count("反向") / total_eff
            if reverse_ratio > 0.4:
                stage_weights = cfg.get("stage_weights") or get_stage_weights()
                changed = False
                for stage_name, sw in stage_weights.items():
                    old_fund = sw.get("fundamental", 0)
                    old_mom = sw.get("momentum", 0)
                    new_fund = max(0.10, old_fund - STEP_LIMIT)
                    new_mom = old_mom + (old_fund - new_fund)
                    default_fund = DEFAULT_STAGE_WEIGHTS.get(stage_name, {}).get("fundamental", old_fund)
                    if abs(new_fund - default_fund) <= CUMULATIVE_LIMIT:
                        sw["fundamental"] = round(new_fund, 2)
                        sw["momentum"] = round(new_mom, 2)
                        changed = True
                if changed:
                    cfg["stage_weights"] = stage_weights
                    adjustments_made.append(
                        f"排名反向反馈（反向占比{reverse_ratio:.0%}）："
                        f"降低fundamental {STEP_LIMIT}，提升momentum {STEP_LIMIT}")

    # 写回配置
    if adjustments_made:
        log = cfg.get("adjustment_log") or []
        for adj in adjustments_made:
            log.append(f"[{now_str}] [阈值校准] {adj}")
        cfg["adjustment_log"] = log
        ok = _save_weights_to_config(cfg)
        lines.append("\n## 自动反馈执行结果")
        if ok:
            for adj in adjustments_made:
                lines.append(f"- ✅ {adj}")
            lines.append("- 权重已写回 local.yaml，下次分析自动生效")
        else:
            lines.append("- ⚠️ 权重写回失败，请检查 local.yaml 权限")
    else:
        lines.append("\n## 自动反馈执行结果")
        lines.append("- 未触发任何调整（样本不足或偏差未达阈值）")


def main() -> None:
    from storage.sqlite.stock_storage import (
        get_db, AnalysisSnapshot, AnalysisReview, IndustryReview)
    from sqlalchemy import select

    db = get_db()
    lines = ["# 阈值校准报告", ""]

    # ---------- 个股方向判断 ----------
    acc = db.get_direction_accuracy(recent_n=200)
    lines.append("## 个股方向判断")
    if acc.get("judged", 0) < MIN_STOCK_SAMPLES:
        lines.append(f"- 样本不足：可对账 {acc.get('judged', 0)} 次"
                     f"（需 ≥{MIN_STOCK_SAMPLES}），继续积累，暂不下结论")
    else:
        lines.append(f"- 近 {acc['total']} 次复盘，可对账 {acc['judged']} 次，"
                     f"命中率 {acc['accuracy']}%")
        # 按判断方向拆分：偏多和偏空的命中率经常不对称（系统性追多/恐空偏差）
        with db.get_session() as session:
            rows = session.execute(
                select(AnalysisSnapshot.short_term_view, AnalysisReview.direction_verdict)
                .join(AnalysisReview, AnalysisReview.snapshot_id == AnalysisSnapshot.id)
            ).all()
        for view in ("偏多", "偏空"):
            sub = [v for s, v in rows if s == view and v in ("正确", "错误")]
            if sub:
                hit = sum(1 for v in sub if v == "正确")
                lines.append(f"  - {view}判断：{hit}/{len(sub)} 命中"
                             f"（{hit / len(sub) * 100:.0f}%）")
        lines.append("- 校准动作：某一侧命中率明显低于 50% → 该侧结论在 responder 里要求更保守")

    # ---------- 基本面前瞻 ----------
    facc = db.get_fundamental_accuracy()
    lines.append("\n## 基本面前瞻（下期财报方向）")
    if facc.get("judged", 0) < 5:
        lines.append(f"- 样本不足：已对账 {facc.get('judged', 0)} 次，继续积累")
    else:
        lines.append(f"- 已对账 {facc['judged']} 次，命中率 {facc['accuracy']}%")

    # ---------- 产业链：组合/排名/门槛 ----------
    with db.get_session() as session:
        reviews = session.execute(
            select(IndustryReview).order_by(IndustryReview.created_at.desc()).limit(100)
        ).scalars().all()
        reviews = [r.to_dict() for r in reviews]

    lines.append("\n## 产业链选股")
    if len(reviews) < MIN_INDUSTRY_SAMPLES:
        lines.append(f"- 样本不足：{len(reviews)} 次复盘（需 ≥{MIN_INDUSTRY_SAMPLES}），继续积累")
    else:
        excess = [r["excess_return"] for r in reviews if r.get("excess_return") is not None]
        if excess:
            lines.append(f"- 组合超额（vs 沪深300）：均值 {statistics.mean(excess):+.2f}%，"
                         f"中位数 {statistics.median(excess):+.2f}%，"
                         f"跑赢次数 {sum(1 for e in excess if e > 0)}/{len(excess)}")
        eff = [r.get("rank_effective") for r in reviews]
        lines.append(f"- 排名区分度：有效 {eff.count('有效')} / 无区分 {eff.count('无区分')} / "
                     f"反向 {eff.count('反向')}")
        lines.append("- 校准动作：『反向』占比高 → 检查评分权重（当前阶段权重表 STAGE_WEIGHTS）；"
                     "超额长期为负 → 行业筛选本身没有alpha，重看候选来源")

    # 门槛有效性：进池组 vs 剔除组事后收益（这是 7/6/5 分门槛松紧的唯一硬证据）
    pairs = [(r["portfolio_return"], r["excluded_avg_return"]) for r in reviews
             if r.get("portfolio_return") is not None and r.get("excluded_avg_return") is not None]
    lines.append("\n## 阶段门槛有效性（进池组 vs 剔除组）")
    if len(pairs) < MIN_GATE_PAIRS:
        lines.append(f"- 样本不足：{len(pairs)} 组对照（需 ≥{MIN_GATE_PAIRS}），继续积累")
    else:
        diffs = [p - e for p, e in pairs]
        win = sum(1 for d in diffs if d > 0)
        lines.append(f"- 进池组平均跑赢剔除组 {statistics.mean(diffs):+.2f}%，"
                     f"胜出 {win}/{len(pairs)} 次")
        if statistics.mean(diffs) < 0:
            lines.append("- ⚠️ 剔除组反而更强：门槛在剔除弹性标的，考虑放宽 momentum 门槛"
                         "或降低 moat 权重（改 STAGE_GATES / STAGE_WEIGHTS 后继续观察）")
        else:
            lines.append("- 门槛方向正确；差距<1% 时视为无区分，维持现状继续积累")

    _auto_feedback_thresholds(reviews, acc, pairs, lines)

    print("\n".join(lines))


if __name__ == "__main__":
    main()
