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
from core.llm import get_agent_llm
from utils.logger import logger

DISCLAIMER = "以上内容基于公开信息整理，不构成投资建议。"

# 文风禁用词与来源枚举：与 prompt 共用同一份定义（agents/prompts_common.py 单点维护），
# 改那边的常量，prompt 文本和这里的程序检查同时生效
from agents.prompts_common import ALLOWED_SOURCES, BANNED_PHRASES  # noqa: E402

# 趋势句附近应出现的期间标记
_PERIOD_MARK = re.compile(r"(20\d{2}|Q[1-4]|[一二三四]季|季报|年报|半年|全年|H[12]|同期|上年|去年|环比|近\d)")


def scan_banned_phrases(text: str) -> list:
    """扫描最终回答中的文风禁用词（纯函数）；返回命中列表 [(词, 次数)]"""
    hits = []
    for p in BANNED_PHRASES:
        n = (text or "").count(p)
        if n:
            hits.append((p, n))
    return hits


def run_quality_checks(text: str) -> list:
    """
    机械质量检查（纯函数）：responder prompt 里可客观判定的硬规则，靠 prompt 是许愿，
    靠 regex 是保证。返回问题描述列表，每条带原文片段，供 LLM 定点修复。
    """
    issues = []
    text = text or ""

    # 1) 估值分位必须带窗口：覆盖四种语序——"历史78%分位"/"78%历史分位"/"78%分位"/"历史分位78%"，
    #    前文没有"近N年/近N个交易日"即违规
    for m in re.finditer(r"历史\s*\d+(?:\.\d+)?%?\s*分位"
                         r"|\d+(?:\.\d+)?%\s*历史\s*分位"
                         r"|\d+(?:\.\d+)?%\s*分位"
                         r"|分位[从至为约]?\s*\d+(?:\.\d+)?%", text):
        ctx = text[max(0, m.start() - 12):m.start()]
        if "近" not in ctx:
            issues.append(f"估值分位缺统计窗口（应写作'近N年分位'，窗口以材料标注为准）：「{m.group(0)}」")

    # 2) 趋势箭头必须标注两端报告期：±40字符内找不到期间标记即违规
    for m in re.finditer(r"[-+]?\d+(?:\.\d+)?%?(?:亿元?|万元|倍)?→", text):
        window = text[max(0, m.start() - 40):min(len(text), m.end() + 40)]
        if not _PERIOD_MARK.search(window):
            snippet = text[max(0, m.start() - 15):min(len(text), m.end() + 15)].replace("\n", " ")
            issues.append(f"趋势箭头未标注两端报告期：「…{snippet}…」")

    # 3) 来源表述封闭枚举："根据XX数据/信息/公告"必须是五种之一
    for m in re.finditer(r"根据[^\s，。；、：）)]{2,12}", text):
        token = m.group(0)
        if any(token.startswith(s) for s in ALLOWED_SOURCES):
            continue
        if re.search(r"(数据|信息|分析|检索|公告|研报)$", token):
            issues.append(f"来源表述不在枚举内（只允许：{'、'.join(ALLOWED_SOURCES)}）：「{token}」")

    # 4) 仓位0成禁止"若选择介入"句式
    if re.search(r"若选择介入[^。\n]{0,80}?0\s*成", text):
        issues.append("出现'若选择介入……仓位0成'自相矛盾句式，"
                      "应改写为'程序判定不介入（附原因），回踩观察位XX再重估'")

    # 5) 占位符数字：搜索没给具体数就写"X亿元订单"是幻觉式凑数（实测出现过）
    for m in re.finditer(r"[XN]{1,2}\s*(?:亿|万|%|元)", text):
        snippet = text[max(0, m.start() - 12):min(len(text), m.end() + 8)].replace("\n", " ")
        issues.append(f"出现占位符数字（应改写为'金额/数值未披露'）：「…{snippet}…」")

    # 6) 技术指标名笔误（LLM 手滑高频款，实测出过"JDJ值106.30"）。
    #    注意 \b 对中文邻字不成立（CJK 也算 \w），用显式字母边界
    for wrong, right in (("JDJ", "KDJ"), ("MCAD", "MACD")):
        if re.search(rf"(?<![A-Za-z]){wrong}(?![A-Za-z])", text):
            issues.append(f"技术指标名笔误：「{wrong}」应为「{right}」")

    return issues


def check_conclusion_skeleton(text: str, mode: str) -> list:
    """
    「📌 结论」骨架完整性检查（纯函数）：行名固定的填空模板，缺行=违规。
    mode: "stock" / "industry" / ""（空=不检查）。实测 LLM 会自由发挥行名
    （把"操作"写成"观察参考区"）或漏行（丢"大盘环境"），机械检查+修复兜底。
    """
    if not mode:
        return []
    text = text or ""
    m = re.search(r"📌", text)
    if not m:
        return ["报告未以「📌 结论」固定骨架开头"]
    tail = text[m.start():]
    end = re.search(r"\n(?:---|## )", tail)
    block = tail[:end.start()] if end else tail[:1500]
    required = ["方向", "操作", "核心逻辑", "最大风险"]
    required += ["护城河", "大盘环境"] if mode == "stock" else ["行业阶段"]
    return [f"「📌 结论」骨架缺少「{name}」行（行名固定；对应数据在正文/材料里就搬上来，没有就填'无数据'）"
            for name in required if name not in block]


class ComplianceAgent:
    def __init__(self):
        self.llm = get_agent_llm("compliance")

    def review_node(self, state: AgentState) -> Dict[str, Any]:
        final_answer = state.get("final_answer") or ""
        if not final_answer.strip():
            logger.info("最终回答为空，跳过合规审查")
            return {"intermediate_steps": [("compliance", {"skipped": "final_answer 为空"})]}

        # 程序数字清单：操作参考的原始数字，供 LLM 回查报告引用是否一致
        technical = state.get("technical_result") or {}
        reference_numbers = "\n".join(
            t for t in (technical.get("trade_plan_text"), technical.get("trade_plans_text")) if t)

        # 程序质量检查（零成本、百分百执行）：分位窗口/趋势期间/来源枚举/0成句式/文风禁用词
        quality_issues = run_quality_checks(final_answer)
        # 结论骨架完整性：按本次模式（个股/产业链）核对固定行名
        stock_code = state.get("stock_code") or ""
        if state.get("industry_name") or "," in stock_code:
            mode = "industry"
        elif stock_code:
            mode = "stock"
        else:
            mode = ""
        quality_issues += check_conclusion_skeleton(final_answer, mode)
        style_hits = scan_banned_phrases(final_answer)
        if style_hits:
            quality_issues.append("文风禁用词：" + "、".join(f"「{p}」×{n}" for p, n in style_hits))

        # 有问题 → 一次 LLM 定点修复（只改问题处，其余逐字保留）；
        # 修复后复检：问题数没有净减少（没修好或引入新问题）就弃用，回退原文
        revised = final_answer
        if quality_issues:
            logger.warning(f"[质量守门] 命中 {len(quality_issues)} 个问题: {quality_issues[:5]}")
            repaired = self._repair(final_answer, quality_issues)
            if repaired:
                recheck = run_quality_checks(repaired) + check_conclusion_skeleton(repaired, mode)
                re_style = scan_banned_phrases(repaired)
                if re_style:
                    recheck.append("文风禁用词：" + "、".join(f"「{p}」×{n}" for p, n in re_style))
                if len(recheck) < len(quality_issues):
                    revised = repaired
                    logger.info(f"[质量守门] 定点修复完成（{len(quality_issues)}→{len(recheck)} 个问题）"
                                + (f"，仍残留: {recheck[:3]}" if recheck else ""))
                else:
                    logger.warning(f"[质量守门] 修复无净改善（{len(quality_issues)}→{len(recheck)}），"
                                   "弃用修复结果保留原文")

        review_result = self._review(revised, reference_numbers)
        review_result["style_hits"] = style_hits
        review_result["quality_issues"] = quality_issues
        if review_result.get("required_disclaimer") and DISCLAIMER not in revised:
            revised += f"\n\n---\n*{DISCLAIMER}*"
        all_issues = review_result.get("issues") or []
        corrections = [str(i) for i in all_issues if str(i).startswith("数字勘误")]
        other_issues = [str(i) for i in all_issues if not str(i).startswith("数字勘误")]
        # 数字勘误优先定点替换正文错误数字——曾把勘误贴在尾部而正文错数原样保留，
        # 读者在正文读到-446.99亿、翻到最后才发现被推翻。替换后复审确认无残留勘误
        # 才采纳；替换失败/复审仍有勘误则降级为尾部注记（错数上屏总比静默好）
        if corrections:
            adopted = False
            fixed = self._repair(revised, corrections)
            if fixed:
                still = [str(i) for i in (self._review(fixed, reference_numbers).get("issues") or [])
                         if str(i).startswith("数字勘误")]
                if not still:
                    revised = fixed
                    adopted = True
                    logger.info(f"[合规] 数字勘误已定点替换正文（{len(corrections)}处），复审通过")
            if not adopted:
                revised += "\n\n*🔧 " + "；".join(corrections[:3]) + "（正文对应数字以此勘误为准）*"
                logger.warning("[合规] 数字勘误定点替换未通过复审，降级为尾部注记")
        if review_result.get("risk_level") in ("high", "unknown") and other_issues:
            revised += f"\n\n*合规提示：{'；'.join(other_issues[:3])}*"

        # 数据源健康摘要：程序化附加（不经 LLM），静默降级从此可见
        try:
            from tools.source_health import format_health
            health = format_health()
            if health:
                revised += f"\n\n*{health}*"
        except Exception:
            pass

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

    def _repair(self, text: str, issues: list) -> str:
        """
        定点修复：把机械检查出的问题交给 LLM 逐一修复，其余内容逐字保留。
        修复后长度偏离原文过多（<60% 或 >150%）视为改写失控，弃用返回空串。
        """
        issues_text = "\n".join(f"{i + 1}. {s}" for i, s in enumerate(issues[:12]))
        prompt = f"""下面是一份分析报告和质量检查发现的问题清单。请只修复清单中列出的问题，
其余内容**逐字保留**（包括标题、表格、免责声明）。修复方式：
- 缺期间/窗口标注的：根据上下文补上报告期或"近3年"（上下文推不出来就在该数字后加"（期间未标注）"）
- 来源表述不在枚举内的：改成五种允许表述中最贴切的一种
- 自相矛盾句式：按问题描述改写
- 文风禁用词：改写为有数字支撑的表述，或删掉该句
- 数字勘误类：把正文中的错误数字（含结论、表格、推导说明里的同一个数）全部替换为
  勘误给出的正确值，与该数字绑定的定性判断（如"同期最大负值"）不再成立时一并修正；
  不得改动勘误未涉及的其他数字
直接输出修复后的完整报告全文，不要解释，不要 markdown 代码块包裹。

【问题清单】
{issues_text}

【报告原文】
{text}"""
        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            repaired = response.content if hasattr(response, "content") else str(response)
            repaired = (repaired or "").strip()
            if not repaired:
                return ""
            ratio = len(repaired) / max(len(text), 1)
            if ratio < 0.6 or ratio > 1.5:
                logger.warning(f"[质量守门] 修复后长度偏离原文（{ratio:.2f}倍），弃用修复结果")
                return ""
            return repaired
        except Exception as e:
            logger.error(f"[质量守门] 定点修复失败，保留原文: {e}")
            return ""

    def _review(self, final_answer: str, reference_numbers: str = "") -> Dict[str, Any]:
        """审查最终回答文本（合规 + 程序数字一致性）；任何环节失败都返回 fail-close 结果"""
        system_prompt = """你是金融合规与事实审查专家。本系统是使用者**个人的分析工具**（非对外发布），
审查以下即将发给使用者的回答，检查是否存在：
1. 无条件的绝对化荐股（如"必涨""无脑买入""稳赚不赔"）
2. 对未来股价的确定性预测（如"股价将上涨到XX元"——注意：标注为"目标参考位/压力位"
   的程序计算价位不算预测）
3. 操作参考缺少止损纪律或风险提示
4. 客观陈述历史涨跌与历史统计胜率不算股价预测，不要误判
5. 数字一致性回查：若提供了【程序数字清单】，逐一核对回答中引用的
   买卖区/止损/目标位/仓位/盈亏比数字是否与清单一致；发现数字抄错、方向词写反
   （如把"由负转正"写成"由正转负"）时，在 issues 里以"数字勘误：正确值是XX"格式列出
6. 报告内部数字一致性（用算术互相验证，发现矛盾以"数字勘误"格式列出）：
   - 累计同比必然介于各单月同比之间：出现"上半年同比+255%"但单月最高只有+95%这类，必为错数
   - 派生数字要对得上：EPS×总股本≈净利润、单季利润×4与全年预测量级不能差数倍、
     占比合计不能明显超过100%
   - 同一指标在结论与正文出现两个值时点出，以带来源标注的为准

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

        user_content = f"待审查内容：\n{final_answer}"
        if reference_numbers:
            user_content += f"\n\n【程序数字清单（引用数字必须与此一致）】\n{reference_numbers[:2000]}"

        try:
            response = self.llm.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_content),
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
