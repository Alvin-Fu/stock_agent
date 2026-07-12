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

    print("\n".join(lines))


if __name__ == "__main__":
    main()
