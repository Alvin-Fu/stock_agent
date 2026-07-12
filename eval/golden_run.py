# -*- coding: utf-8 -*-
"""
Golden set 回归：改 prompt/规则/评分逻辑之后跑这个，固定 5 个问题走完整链路，
用机械检查给每份报告打分，并与上一次基线对比——没有这个，每次改动都是裸奔
（修好一处、悄悄破坏另一处而不自知）。

用法（repo 根目录，需要联网与 LLM 配置）：
    python eval/golden_run.py                # 跑全部 case
    python eval/golden_run.py byd-full       # 只跑一个 case（调试用）
产出：eval/runs/<时间戳>/ 下每 case 一个 .md + summary.md；自动对比最近一次运行。
评分只用程序检查（不花 LLM 钱）：质量问题数、骨架完整性、必备小节、文风命中。
"""

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# case 设计：覆盖 成熟白马/亏损承压/技术面单项/导入期产业链/成熟产业链 五种形态
CASES = [
    {"id": "byd-full", "mode": "stock",
     "question": "全面分析比亚迪（002594）的基本面、技术面和投资价值"},
    {"id": "moutai-fund", "mode": "stock",
     "question": "分析贵州茅台（600519）的财务状况、估值与护城河"},
    {"id": "catl-tech", "mode": "stock",
     "question": "判断宁德时代（300750）当前的均线走势、MACD信号和支撑压力位"},
    {"id": "aerospace-chain", "mode": "industry",
     "question": "分析商业航天产业链上下游，筛选出所有关键公司，对比技术面和基本面，选出最值得投资的股票"},
    {"id": "liquor-chain", "mode": "industry",
     "question": "分析白酒产业链上下游，筛选出所有关键公司，对比技术面和基本面，选出最值得投资的股票"},
]

# 必备小节（缺一节记一个问题）
REQUIRED_SECTIONS = {
    "stock": ["📌", "利润驱动", "情景推演", "风险"],
    "industry": ["📌", "产业链", "行业风险", "最值得投资标的"],
}


def lint_report(text: str, mode: str) -> list:
    """机械评分：复用 compliance 的纯函数 + 小节完整性；返回问题列表"""
    from agents.compliance.compliance_agent import (
        run_quality_checks, check_conclusion_skeleton, scan_banned_phrases)
    issues = list(run_quality_checks(text))
    issues += check_conclusion_skeleton(text, mode)
    issues += [f"文风禁用词「{p}」×{n}" for p, n in scan_banned_phrases(text)]
    for section in REQUIRED_SECTIONS.get(mode, []):
        if section not in text:
            issues.append(f"缺少必备小节/标记：「{section}」")
    if len(text) < 800:
        issues.append(f"报告过短（{len(text)}字符），疑似链路失败")
    return issues


def main() -> None:
    only = sys.argv[1] if len(sys.argv) > 1 else None
    cases = [c for c in CASES if not only or c["id"] == only]
    if not cases:
        print(f"未找到 case: {only}；可选：{[c['id'] for c in CASES]}")
        return

    runs_dir = Path(__file__).parent / "runs"
    prev = sorted(runs_dir.glob("*/summary.tsv"))  # 上一次基线
    out_dir = runs_dir / datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)

    from orchestration.workflow import WorkflowExecutor
    executor = WorkflowExecutor(enable_memory=False)

    rows = []
    for case in cases:
        print(f"\n===== 运行 {case['id']} =====")
        t0 = time.time()
        try:
            state = executor.run_sync(case["question"], thread_id=f"golden-{case['id']}")
            answer = state.get("final_answer") or ""
        except Exception as e:
            answer = f"运行失败: {e}"
        elapsed = round(time.time() - t0)
        issues = lint_report(answer, case["mode"])
        (out_dir / f"{case['id']}.md").write_text(
            f"<!-- question: {case['question']} | 用时{elapsed}s | 问题数{len(issues)} -->\n\n"
            + answer + "\n\n## LINT\n" + "\n".join(f"- {i}" for i in issues),
            encoding="utf-8")
        rows.append((case["id"], len(issues), len(answer), elapsed))
        print(f"  {len(issues)} 个问题，{len(answer)} 字符，{elapsed}s")
        for i in issues[:8]:
            print(f"  - {i}")

    # 摘要 + 与上次基线对比
    tsv = "\n".join(f"{r[0]}\t{r[1]}\t{r[2]}\t{r[3]}" for r in rows)
    (out_dir / "summary.tsv").write_text("case\tissues\tchars\tseconds\n" + tsv, encoding="utf-8")

    summary = [f"# Golden 回归 {out_dir.name}", "", "| case | 问题数 | 字符数 | 用时s | 对比上次 |",
               "|---|---|---|---|---|"]
    prev_map = {}
    if prev:
        for line in prev[-1].read_text(encoding="utf-8").splitlines()[1:]:
            parts = line.split("\t")
            if len(parts) >= 2:
                prev_map[parts[0]] = int(parts[1])
    for cid, n_issues, chars, secs in rows:
        base = prev_map.get(cid)
        diff = "首跑" if base is None else (f"{n_issues - base:+d}" if n_issues != base else "持平")
        flag = " ⚠️回归" if base is not None and n_issues > base else ""
        summary.append(f"| {cid} | {n_issues} | {chars} | {secs} | {diff}{flag} |")
    (out_dir / "summary.md").write_text("\n".join(summary), encoding="utf-8")
    print("\n" + "\n".join(summary))
    print(f"\n产出目录：{out_dir}")


if __name__ == "__main__":
    main()
