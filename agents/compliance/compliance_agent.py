"""
合规审查 Agent
职责：对 responder 生成的**最终回答**做合规审查：
  - 检查是否含投资建议、股价预测、绝对化表述
  - 必要时直接在最终回答上追加免责声明/风险提示
时序：responder → compliance → END，保证用户实际收到的文本经过审查。
审查失败（LLM 异常/解析失败）时 fail-close：默认追加免责声明，绝不静默放行。
"""

import json
import re
from typing import Dict, Any

from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from core.llm import get_default_llm
from utils.logger import logger

DISCLAIMER = "以上内容基于公开信息整理，不构成投资建议。"


class ComplianceAgent:
    def __init__(self):
        self.llm = get_default_llm()

    def review_node(self, state: AgentState) -> Dict[str, Any]:
        final_answer = state.get("final_answer") or ""
        if not final_answer.strip():
            logger.info("最终回答为空，跳过合规审查")
            return {"intermediate_steps": [("compliance", {"skipped": "final_answer 为空"})]}

        review_result = self._review(final_answer)

        # 根据审查结果修订最终回答
        revised = final_answer
        if review_result.get("required_disclaimer") and DISCLAIMER not in revised:
            revised += f"\n\n---\n*{DISCLAIMER}*"
        if review_result.get("risk_level") in ("high", "unknown") and review_result.get("issues"):
            issues_text = "；".join(str(i) for i in review_result["issues"][:3])
            revised += f"\n\n*合规提示：{issues_text}*"

        logger.info(f"合规审查完成，通过: {review_result['passed']}，"
                    f"风险等级: {review_result.get('risk_level')}，问题数: {len(review_result.get('issues', []))}")

        return {
            "final_answer": revised,
            "compliance_result": review_result,
            "intermediate_steps": [("compliance", {
                "passed": review_result["passed"],
                "risk_level": review_result.get("risk_level"),
                "issues": review_result.get("issues", []),
            })],
        }

    def _review(self, final_answer: str) -> Dict[str, Any]:
        """审查最终回答文本；任何环节失败都返回 fail-close 结果"""
        system_prompt = """你是金融合规审查专家。本系统是使用者**个人的分析工具**（非对外发布），
审查以下即将发给使用者的回答，检查是否存在：
1. 无条件的绝对化荐股（如"必涨""无脑买入""稳赚不赔"）
2. 对未来股价的确定性预测（如"股价将上涨到XX元"——注意：标注为"目标参考位/压力位"
   的程序计算价位不算预测）
3. 操作参考缺少止损纪律或风险提示
4. 客观陈述历史涨跌与历史统计胜率不算股价预测，不要误判

【豁免规则】以下情况属于合规的操作参考，不算违规投资建议：
- 条件化表述（"若选择介入"）+ 明确止损位 + 风险提示 的买卖点位与仓位参考
- 引用程序计算的支撑/压力/止损/仓位数字

请严格按照以下JSON格式输出（不要markdown包裹，不要解释）：
{
  "passed": true或false,
  "issues": ["问题1", "问题2"],
  "required_disclaimer": true或false,
  "risk_level": "low"或"medium"或"high"
}

判断标准：
- passed=true：无绝对化荐股、无确定性股价预测、操作参考带止损与风险提示
- required_disclaimer=true：金融分析内容一律为true
- risk_level：low=无明显问题；medium=有少量绝对化表述或风险提示不足；high=有绝对化荐股或确定性预测"""

        try:
            response = self.llm.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=f"待审查内容：\n{final_answer}"),
            ])
            raw = response.content if hasattr(response, 'content') else str(response)
            return self._parse_review_result(raw)
        except Exception as e:
            logger.error(f"合规审查执行失败，按 fail-close 处理: {e}")
            return {
                "passed": False,
                "issues": [f"合规审查过程出错: {e}"],
                "required_disclaimer": True,
                "risk_level": "unknown",
            }

    def _parse_review_result(self, raw_response: str) -> Dict[str, Any]:
        """解析 LLM 的合规审查结果；解析不出 JSON 时 fail-close"""
        try:
            json_match = re.search(r'\{.*\}', raw_response, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(0))
                return {
                    "passed": bool(parsed.get("passed", False)),
                    "issues": parsed.get("issues", []) or [],
                    "required_disclaimer": bool(parsed.get("required_disclaimer", True)),
                    "risk_level": parsed.get("risk_level", "medium"),
                    "raw_response": raw_response,
                }
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.warning(f"解析合规审查结果失败，按 fail-close 处理: {e}")

        # fail-close：无法确认合规时，强制加免责声明
        return {
            "passed": False,
            "issues": ["合规审查结果无法解析，已默认追加免责声明"],
            "required_disclaimer": True,
            "risk_level": "unknown",
            "raw_response": raw_response,
        }


def create_compliance_node():
    agent = ComplianceAgent()
    return agent.review_node
