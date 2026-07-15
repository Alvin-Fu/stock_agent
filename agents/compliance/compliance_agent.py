"""
合规审查 Agent
职责：检查回答是否包含不当投资建议、是否标注风险提示、引用是否合规；
      同时执行机械质量检查（分位窗口/趋势期间/来源枚举/文风禁用词/夸大词）。
"""

import json
import re
from typing import Dict, Any

from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from core.llm import get_default_llm
from utils.logger import logger

from agents.prompts_common import ALLOWED_SOURCES, BANNED_PHRASES, EXAGGERATED_PHRASES

DISCLAIMER = "以上内容基于公开信息整理，不构成投资建议。"

# 趋势句附近应出现的期间标记
_PERIOD_MARK = re.compile(r"(20\d{2}|Q[1-4]|[一二三四]季|季报|年报|半年|全年|H[12]|同期|上年|去年|环比|近\d)")


def scan_banned_phrases(text: str) -> list:
    """扫描最终回答中的文风禁用词"""
    return [(p, (text or "").count(p)) for p in BANNED_PHRASES if p in (text or "")]


def scan_exaggerated_phrases(text: str) -> list:
    """扫描最终回答中的夸大性词汇"""
    return [(p, (text or "").count(p)) for p in EXAGGERATED_PHRASES if p in (text or "")]


def run_quality_checks(text: str) -> list:
    """机械质量检查：prompt 可客观判定的硬规则，靠 regex 保证"""
    issues = []
    text = text or ""

    # 1) 估值分位必须带窗口
    for m in re.finditer(r"历史\s*\d+(?:\.\d+)?%?\s*分位"
                         r"|\d+(?:\.\d+)?%\s*历史\s*分位"
                         r"|\d+(?:\.\d+)?%\s*分位"
                         r"|分位[从至为约]?\s*\d+(?:\.\d+)?%", text):
        ctx = text[max(0, m.start() - 12):m.start()]
        if "近" not in ctx:
            issues.append(f"估值分位缺统计窗口：「{m.group(0)}」")

    # 2) 趋势箭头必须标注两端报告期
    for m in re.finditer(r"[-+]?\d+(?:\.\d+)?%?(?:亿元?|万元|倍)?→", text):
        window = text[max(0, m.start() - 40):min(len(text), m.end() + 40)]
        if not _PERIOD_MARK.search(window):
            snippet = text[max(0, m.start() - 15):min(len(text), m.end() + 15)].replace("\n", " ")
            issues.append(f"趋势箭头未标注报告期：「…{snippet}…」")

    # 3) 来源表述封闭枚举
    for m in re.finditer(r"根据[^\s，。；、：）)]{2,12}", text):
        token = m.group(0)
        if any(token.startswith(s) for s in ALLOWED_SOURCES):
            continue
        if re.search(r"(数据|信息|分析|检索|公告|研报)$", token):
            issues.append(f"来源表述不在枚举内：「{token}」")

    # 4) 夸大性词汇扫描
    exaggeration = scan_exaggerated_phrases(text)
    if exaggeration:
        issues.append("夸大性禁用词：" + "、".join(f"「{p}」(×{n})" for p, n in exaggeration)
                      + "——请用具体数字代替形容词")

    # 5) 文风禁用词
    banned = scan_banned_phrases(text)
    if banned:
        issues.append("文风禁用词：" + "、".join(f"「{p}」(×{n})" for p, n in banned))

    # 6) 占位符数字
    for m in re.finditer(r"[XN]{1,2}\s*(?:亿|万|%|元)", text):
        snippet = text[max(0, m.start() - 12):min(len(text), m.end() + 8)].replace("\n", " ")
        issues.append(f"占位符数字：「…{snippet}…」")

    # 7) 技术指标名笔误
    for wrong, right in (("JDJ", "KDJ"), ("MCAD", "MACD")):
        if re.search(rf"(?<![A-Za-z]){wrong}(?![A-Za-z])", text):
            issues.append(f"技术指标名笔误：「{wrong}」应为「{right}」")

    return issues


class ComplianceAgent:
    def __init__(self):
        self.llm = get_default_llm()

    def review_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            final_answer = state.get("final_answer") or ""
            if not final_answer.strip():
                logger.info("最终回答为空，跳过合规审查")
                return {"intermediate_steps": [("compliance", {"skipped": "final_answer 为空"})]}

            # 程序质量检查
            quality_issues = run_quality_checks(final_answer)
            if quality_issues:
                logger.warning(f"[合规] 机械检查命中 {len(quality_issues)} 个问题: {quality_issues[:5]}")

            # LLM 内容审查
            review_result = self._review(final_answer)
            review_result["quality_issues"] = quality_issues

            # 追加免责声明
            revised = final_answer
            if review_result.get("required_disclaimer") and DISCLAIMER not in revised:
                revised += f"\n\n---\n*{DISCLAIMER}*"

            # 风险等级高时追加提示
            if review_result.get("risk_level") in ("high", "unknown"):
                issues = review_result.get("issues") or []
                if issues:
                    revised += f"\n\n*合规提示：{'；'.join(str(i) for i in issues[:3])}*"

            logger.info(f"合规审查完成，通过: {review_result['passed']}，"
                        f"风险等级: {review_result.get('risk_level')}，"
                        f"质量问题: {len(quality_issues)}")

            return {
                "final_answer": revised,
                "compliance_result": review_result,
                "intermediate_steps": [("compliance", {
                    "passed": review_result["passed"],
                    "risk_level": review_result.get("risk_level"),
                    "issues": review_result.get("issues", []),
                    "quality_issues": quality_issues,
                })],
            }
        except Exception as e:
            logger.error(f"合规审查异常: {e} {traceback.format_exc()}")
            return {
                "final_answer": state.get("final_answer", ""),
                "compliance_result": {"passed": False, "risk_level": "unknown", "issues": [str(e)]},
                "intermediate_steps": [("compliance", {"error": str(e)})],
            }

    def _review(self, final_answer: str) -> Dict[str, Any]:
        """LLM 内容审查（合规 + 风险等级）"""
        system_prompt = """你是金融合规审查专家。审查以下即将发给使用者的回答，检查是否存在：
1. 无条件的绝对化荐股（如"必涨""无脑买入""稳赚不赔"）
2. 对未来股价的确定性预测（如"股价将上涨到XX元"——标注为"目标参考位/压力位"的程序计算价位不算预测）
3. 操作参考缺少止损纪律或风险提示
4. 客观陈述历史涨跌与历史统计胜率不算股价预测，不要误判

请严格按照以下JSON格式输出（不要markdown包裹，不要解释）：
{
  "passed": true或false,
  "issues": ["问题1", "问题2"],
  "required_disclaimer": true或false,
  "risk_level": "low"或"medium"或"high"
}"""

        try:
            response = self.llm.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=f"待审查内容：\n{final_answer}"),
            ])
            raw = response.content if hasattr(response, 'content') else str(response)
            json_match = re.search(r'\{.*\}', raw, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(0))
                return {
                    "passed": bool(parsed.get("passed", False)),
                    "issues": parsed.get("issues", []) or [],
                    "required_disclaimer": bool(parsed.get("required_disclaimer", True)),
                    "risk_level": parsed.get("risk_level", "medium"),
                    "raw_response": raw,
                }
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.warning(f"解析合规审查结果失败: {e}")
        except Exception as e:
            logger.error(f"合规审查执行失败: {e}")

        # fail-close
        return {
            "passed": False,
            "issues": ["合规审查结果无法解析，已默认追加免责声明"],
            "required_disclaimer": True,
            "risk_level": "unknown",
        }


def create_compliance_node():
    agent = ComplianceAgent()
    return agent.review_node