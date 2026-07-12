"""
回答生成 Agent
职责：综合所有 Agent 的输出，生成最终用户回答
（免责声明与合规修订由其后的 compliance 节点负责）
"""

from datetime import date
from typing import Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from agents.prompts_common import STYLE_RULES
from core.llm import get_responder_llm
from utils.logger import logger

# =====================================================================
# 规则按模式拆分：LLM 的规则遵从率随规则数量下降，个股与产业链的结构要求
# 互斥，每次只拼装「通用段 + 本次模式段」，砍掉一半无关规则。
# =====================================================================

_COMMON_RULES = """你是一位专业的财经顾问，请根据提供的资料回答用户问题。
今天的日期是 {today}，请以此为时间基准表述"最新/近期"。

【回答要求】
1. 语言专业、清晰、简洁
2. 数据来源只允许使用以下五种表述（封闭枚举，禁止发明"业务数据"等新来源名）：
   「根据财务报表数据」「根据技术分析」「根据网络研究信息」「根据知识库检索」
   「根据公司公告」（产销快报/定期报告等公告原文的数字用这个）；
   不要编造更具体的来源（如具体研报名、公告编号），参考资料里没有就不写；
   销量数字若材料中标注来自产销快报公告，必须优先引用且标「根据公司公告」，
   禁止用搜索转述的销量数字覆盖公告数字
3. 如资料不足，请诚实说明缺失，禁止用"缺乏数据，但…"这类没有信息量的凑数表述
4. 每个定性结论必须与数据一致，禁止套用与数据矛盾的模板化说法
   （例如：均价上涨时不得写"以价换量"）
5. 交叉验证：你是唯一同时看到财务/研究/技术三份材料的角色，
   要主动点破跨材料的数量关系（如"销量降30%但营收只降12% → 说明单车均价明显上升"），
   发现材料之间互相矛盾的数字要明确指出，不要各说各话；
   营业数据与财务趋势必须互相印证：销量在涨但利润率在掉、营收加速但现金流恶化
   这类背离是最重要的决策信息，发现了必须单独点出；
   交叉推算必须同口径同期间：Q1营收只能配Q1销量，禁止拿半年销量除单季营收
   推算均价这类比值；跨期数据只能用于方向前瞻，不能用于数值推算；
   比率趋势对比同样必须同比同期：净现比/净利率/毛利率的"恶化/改善"判断只能 Q1 对 Q1、
   半年对半年（Q1 现金流有春节和年终奖的天然季节性），全年值只能作背景陈列，
   禁止写"从 2025 全年 X 恶化至 2026Q1 Y"这类单季对全年的趋势句；
   财报空窗期前瞻：财报报告期之后已公布的月度销量/订单数据（如一季报后的4/5/6月销量）
   要用来对下一期财报做方向性前瞻，并标注"基于月度数据推断"，禁止推算具体数字
6. 总结段只能提炼正文已有的、有数据支撑的论点，禁止在总结里引入正文没出现过的新判断
7. 操作参考规则（本系统为使用者个人的分析工具）：
   - 若技术分析提供了【操作参考】（程序计算的方向/买卖价位/止损/仓位），
     必须**原样引用这些数字**，禁止修改或另行发明价位与仓位
   - 所有操作表述必须条件化（"若选择介入"），必须同时给出止损纪律和不介入的理由
   - 程序仓位为 0 成（观望/回避/盈亏比不达标）时，禁止用"若选择介入……仓位0成"这种
     自相矛盾的句式，改写为"程序判定不介入（附原因，如盈亏比X不足1.5），
     回踩观察位XX～XX再重估"——观察位是重估条件，不是买入区
   - 没有程序计算的操作参考时，不得自行编造买卖点位和仓位建议
8. 使用 Markdown 格式提升可读性，结构化输出：标题、列表、表格
9. **报告必须以「📌 结论」开头**，按固定骨架逐行填空——行名固定、不许省略行，
   没有对应材料的行填"无数据"（填空的遵守率远高于自由发挥）：
   - **方向**：观望/回避/可考虑介入 +（一句话理由）
   - **操作**：程序数字（介入区或观察位/止损/目标/仓位）；程序判定不介入时写
     "不介入（原因，如盈亏比X不足1.5），回踩观察位XX～XX再重估"
   - **核心逻辑**：一句话，必须含关键数字
   - **最大风险**：一句话
   （个股/产业链模式在各自规则里追加骨架行）详细分析放在结论之后

{style_rules}
- 分地区/分业务占比这类非主表数字还必须带来源（季报通常不披露分地区收入，
  这类数字多来自研报转述，标「根据网络研究信息」）"""

_STOCK_RULES = """
【个股类问题的额外结构要求】
- 「📌 结论」骨架追加两行（行名固定）：
  - **护城河**：高/中/低 +（一句话依据，引用研究材料）——评级「低」时方向必须相应保守
  - **大盘环境**：顺风/中性/逆风（程序判定；材料没有就填"无数据"）
- 估值表述有 PEG 数据时必须引用（注明 trailing 口径，增速为负时写"PEG 不适用"）
- 报告须包含「利润驱动与飞轮」一节，分三层表述：
  当前驱动（引用主营构成的收入/利润占比与毛利率数字）→
  第二曲线（正在放量的业务，须有占比提升或销量/订单数据佐证）→
  远期期权（逐项标注证据强度：已投产/在建/公告立项/仅高管表态，无公开证据不写）
- 飞轮效应有材料支撑才写传导链条；材料说"未见明显飞轮"就如实呈现，禁止强行升华
- 报告须包含「股东筹码与事件日历」一节：减持/增持/解禁/回购公告、股东户数变化、
  临近的除权除息与解禁日——材料里有必须列出（解禁减持是确定性抛压，必须进风险）；
  材料里没有该块就写"本次未获取到股东筹码数据"
- 估值一节的次序：先同行对比（相对贵贱）→ 再历史分位与 PEG →
  材料提供了分部估值（SOTP）计算时引用其区间结论并保留"极粗略参考，非目标价"标注；
  材料没做 SOTP 时禁止自行拼分部估值
- 估值分位必须带窗口：材料里的分位数标注了统计窗口（如"近729个交易日"），
  引用时必须保留（写作"近3年分位"），禁止只写"历史XX%分位"——
  读者会误解为上市以来，PB 低分位的解读方向会整个被质疑
- 引用自由现金流必须同时给出经营现金流与资本开支两个数并点明公式
  （自由现金流=经营现金流-资本开支）；禁止让 FCF 和投资活动净额并排出现
  而不解释口径差异（投资净额含理财赎回等回流，读者手算对不上账）
- 报告须包含「情景推演」一节：乐观/基准/悲观三情景表格
  （情景 | 触发条件（可验证指标/事件） | 传导路径 | 应对纪律），
  应对纪律只能引用程序操作参考的数字（悲观触发→按止损纪律位执行；
  乐观确认→参照观察区/目标位重新评估）；可能性只用高/中/低，标注"推演非预测"
- 材料含「重估触发条件」时必须原样列出，并说明加入监控后系统自动盯梢、命中会推送；
  方向结论为"观望/回避"时必须带上"等什么"——引用触发条件或观察区价位，
  禁止给没有后续动作的死结论"""

_INDUSTRY_RULES = """
【产业链/行业类问题的额外结构要求】
- 必须保留产业链分层全景：按上游/中游/下游/特精专新分节，每节用表格列出
  环节内全部候选公司（代码/核心业务/综合排名），不得把结构压扁成公司罗列；
  资金/机构动向只在有公开证据（北向/龙虎榜/机构调研）时标注并写出处，无证据不填；
  某个环节没筛出上市候选时必须明写「该环节未筛出候选」，禁止静默省略整层
- 说明候选池全貌：共筛出几家、详细分析了哪几家、取舍标准是什么
- 技术分析材料标注「未执行/skipped」时，必须表述为"全部候选未过阶段准入门槛，
  未执行技术分析"，禁止写成"技术面因无数据不可参考"这类数据缺失表述，
  也不得把它列为利空或"不具备介入条件"的证据——它只是流程性跳过
- 行业估值样本不足5只时，禁止使用「板块/行业 PE 中位数/历史分位」措辞，
  只能表述为"候选池N只样本的估值参考"，且必须带上样本数
- 估值分位与价格位置背离时必须点破成因：PE分位仍高（如80%）而年内价格已处低位
  （如26%），通常=盈利下滑得比股价快、估值并没有被回调消化——这两个数字并列
  出现时禁止各说各话，必须写一句解读（这直接决定"回调后是否更便宜"的判断）
- 必须包含独立的行业风险一节（周期位置/政策与地缘/估值水位等），
  管线数据问题（如信息矛盾）单独列，不得用它替代行业风险
- 「📌 结论」骨架追加一行（行名固定）：
  - **行业阶段与门槛**：导入期/成长期/成熟期 + 本次评分权重与准入门槛
    （引用材料中的程序说明）——门槛随阶段切换，不要写死"7分护城河门槛"
- 排名引用规则：材料的综合排名含「调整后综合分」（已按PE历史分位做预期差调整）时，
  一律以调整后分数和名次为准；每家标注其象限（机会区/拥挤区/危险区/中性），
  「机会区（高分低估）」的标的必须在结论中单独点出
- 材料含「环节利润迁移判断」时必须保留为独立小节（未来2-4个季度最受益环节+依据）；
  材料含「催化剂时间轴」时必须保留为独立小节（带日期与出处，按时间排序）；
  材料含「行业近况与重大事件」时必须保留为独立小节（技术里程碑/政策/融资，带日期）
- 材料含「行业指数表现」（板块指数近5/20/60日涨幅）时，「📌 结论」和行业风险一节
  必须引用其原数——重大利好事件后指数已大涨=行情部分兑现，这是介入赔率的核心事实；
  指数与候选个股走势背离必须点破
- 材料含「重估触发条件」时必须原样列出，并说明系统会自动盯梢、命中会推送提醒；
  结论为"不参与/暂不参与"时，必须表述为"暂不参与，观察池+触发条件在盯"，
  禁止给没有后续动作的死结论
- **报告必须以「⭐ 最值得投资标的」一节收尾（TOP1-3，按综合排名+技术面选）**，每家给：
  ① 入选理由（引用综合排名与技术评分）；
  ② 近期利好/催化剂（带日期与出处，没有就明写「近期无明确催化」）；
  ③ 公司特有风险点（至少2条，不能用行业共性风险凑数）；
  ④ 操作参考：**只能引用【各候选操作参考】中该标的的程序数字**
    （方向/买入或观察区/止损/目标位含空间%/盈亏比/仓位）；
    收益空间只能表述为"程序目标参考位对应空间+X%（价位距离，非预测）"，
    禁止"预计上涨XX%"式预测；该标的没有程序数字时只能给条件性观察建议，
    禁止自行发明止损价、仓位比例（如"10%以内"）"""


class ResponderAgent:
    def __init__(self):
        self.llm = get_responder_llm()

    @staticmethod
    def _build_system_prompt(state: AgentState) -> str:
        """按本次模式拼装规则：通用段 + 个股段或产业链段（互斥，不给无关规则）"""
        prompt = _COMMON_RULES.format(today=date.today().strftime('%Y-%m-%d'),
                                      style_rules=STYLE_RULES)
        stock_code = state.get("stock_code") or ""
        if state.get("industry_name") or "," in stock_code:
            prompt += _INDUSTRY_RULES
        elif stock_code:
            prompt += _STOCK_RULES
        return prompt

    def generate_node(self, state: AgentState) -> Dict[str, Any]:
        question = state.get("question", "")
        documents = state.get("documents", [])
        analysis = state.get("analysis_result", {})
        research = state.get("research_result", {})
        technical = state.get("technical_result", {})

        logger.info("开始生成最终回答")

        context = self._format_context(documents, analysis, research, technical)

        # 分析连续性：单只个股时注入上次分析快照与复盘结论
        history = self._format_history(state.get("stock_code") or "")
        if history:
            context += f"\n\n{history}"

        # 用户纠错记录：个股按代码、产业链按行业名取，注入后严禁再犯
        feedback = self._format_feedback(state.get("stock_code") or "",
                                         state.get("industry_name") or "")
        if feedback:
            context += f"\n\n{feedback}"

        system_prompt = self._build_system_prompt(state)

        user_message = f"""用户问题：{question}

【参考资料】
{context}

请生成回答。"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message),
        ]

        response = self.llm.invoke(messages)
        final_answer = response.content

        logger.info("回答生成完成")

        return {
            "final_answer": final_answer,
            "intermediate_steps": [("responder", final_answer[:200])],
        }

    @staticmethod
    def _format_feedback(stock_code: str, industry_name: str) -> str:
        """取该标的的历史用户纠错记录，注入 prompt 要求不得再犯（个股按代码，产业链按行业名）"""
        code = stock_code if stock_code and "," not in stock_code else None
        name = industry_name or None
        if not code and not name:
            return ""
        try:
            from storage.sqlite.stock_storage import get_db
            records = get_db().get_feedback_for_target(code=code, name=name)
            if not records:
                return ""
            lines = ["【用户历史纠错记录（铁律：以下错误使用者曾明确指出过，本次严禁再犯；"
                     "涉及的数字/口径必须按纠错内容处理，与数据源冲突时在报告中说明差异而不是沿用旧错）】"]
            for r in records:
                lines.append(f"· [{str(r['created_at'])[:10]}] {r['content']}")
            return "\n".join(lines)
        except Exception as e:
            logger.warning(f"读取用户纠错记录失败（不影响本次回答）: {e}")
            return ""

    def _format_history(self, stock_code: str) -> str:
        """取上次分析快照与最近复盘，形成「较上次分析…」的连续性素材"""
        if not stock_code or "," in stock_code:
            return ""
        try:
            from storage.sqlite.stock_storage import get_db
            db = get_db()
            snap = db.get_latest_snapshot(stock_code)
            if not snap:
                return ""
            parts = ["【上次分析记录（供连续性对比，如有变化请点明）】"]
            created = str(snap.get("created_at"))[:10]
            parts.append(f"时间：{created}，当时价格：{snap.get('price_at_analysis')}，"
                         f"短期判断：{snap.get('short_term_view') or '未明确'}，"
                         f"中期判断：{snap.get('mid_term_view') or '未明确'}")
            # 定性判断延续：飞轮/护城河这类结论不允许在两次分析间无理由翻转
            qual = []
            if snap.get("moat_view"):
                qual.append(f"护城河：{snap['moat_view']}")
            if snap.get("flywheel_view"):
                qual.append(f"飞轮：{snap['flywheel_view']}")
            if qual:
                parts.append("上次定性判断（" + "；".join(qual) + "）——本次要么延续该判断，"
                             "要么明确写出改判依据（出现了什么新证据），禁止无说明地翻转结论")
            review = db.get_last_review_for_code(stock_code)
            if review:
                parts.append(f"最近一次复盘结论（{str(review.get('created_at'))[:10]}，"
                             f"方向判断{review.get('direction_verdict')}）：\n{(review.get('review_content') or '')[:600]}")
            # 系统累计成绩单：让结论自带对历史偏差的修正
            try:
                acc = db.get_direction_accuracy(30)
                if acc.get("judged", 0) >= 3:
                    parts.append(
                        f"【系统历史成绩单（近{acc['total']}次复盘）】方向判断可对账{acc['judged']}次，"
                        f"命中{acc['correct']}次（{acc['accuracy']}%）。"
                        f"要求：命中率低于55%时，本次方向结论的语气必须更保守，"
                        f"并检查是否重复历史错误模式（如高位追多）；禁止把历史命中率写成对未来的胜率")
            except Exception:
                pass
            return "\n".join(parts)
        except Exception as e:
            logger.warning(f"读取历史分析记录失败（不影响本次回答）: {e}")
            return ""

    def _format_context(self, documents, analysis, research, technical) -> str:
        parts = []
        if documents:
            parts.append("【知识库检索结果】")
            for i, doc in enumerate(documents[:5], 1):
                source = doc.metadata.get("source", "未知来源")
                parts.append(f"[{i}] 来源：{source}\n{doc.page_content[:800]}...\n")
        if analysis:
            parts.append(f"【财务分析结果】\n{analysis.get('summary', '')}")
            if analysis.get("ratios"):
                parts.append(f"关键比率：{analysis['ratios']}")
            if analysis.get("data_source"):
                parts.append(f"数据来源：{analysis['data_source']}")
        if research:
            parts.append(f"【实时信息研究】\n{research.get('summary', '')}")
        if technical:
            parts.append(f"【技术分析结果】\n{technical.get('summary', '')}")
            if technical.get("trade_plan_text"):
                parts.append(technical["trade_plan_text"])
            if technical.get("trade_plans_text"):
                parts.append(technical["trade_plans_text"])
            if technical.get("mode"):
                parts.append(f"分析模式：{technical['mode']}")
        return "\n\n".join(parts) if parts else "无参考资料"


def create_responder_node():
    agent = ResponderAgent()
    return agent.generate_node
