"""
结构化分析报告生成器
将 Analyst Agent 的输出格式化为 Markdown 或 JSON
"""

from typing import Dict, Any
from datetime import datetime


class AnalysisReportGenerator:
    """生成标准化的财务分析报告"""

    @staticmethod
    def to_markdown(analysis_result: Dict[str, Any]) -> str:
        """生成 Markdown 格式报告"""
        summary = analysis_result.get("summary", "")
        ratios = analysis_result.get("ratios", {})
        confidence = analysis_result.get("confidence", "中")

        lines = [
            "# 财务分析报告",
            f"*生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            "",
            "## 📊 分析摘要",
            summary,
            "",
            "## 🔢 关键财务比率",
        ]

        if ratios:
            lines.append("| 指标 | 数值 |")
            lines.append("|------|------|")
            for key, value in ratios.items():
                lines.append(f"| {key} | {value} |")
        else:
            lines.append("*未提取到量化数据*")

        lines.extend([
            "",
            f"## ⚠️ 可信度评估",
            f"**{confidence}**",
            "",
            "---",
            "*本报告由 AI 财务分析师自动生成，仅供参考，不构成投资建议。*"
        ])

        return "\n".join(lines)

    @staticmethod
    def to_json(analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """生成 JSON 格式（供前端或 API 使用）"""
        return {
            "timestamp": datetime.now().isoformat(),
            "summary": analysis_result.get("summary", ""),
            "ratios": analysis_result.get("ratios", {}),
            "confidence": analysis_result.get("confidence", "中"),
            "disclaimer": "本报告仅供参考，不构成投资建议。"
        }