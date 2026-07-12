"""
回答生成 Agent
职责：综合所有 Agent 的输出，生成最终用户回答
（免责声明与合规修订由其后的 compliance 节点负责）
"""

from datetime import date
from typing import Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from core.llm import get_responder_llm
from utils.logger import logger


class ResponderAgent:
    def __init__(self):
        self.llm = get_responder_llm()

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

        system_prompt = f"""你是一位专业的财经顾问，请根据提供的资料回答用户问题。
今天的日期是 {date.today().strftime('%Y-%m-%d')}，请以此为时间基准表述"最新/近期"。

【回答要求】
1. 语言专业、清晰、简洁
2. 数据来源只允许使用以下四种表述（封闭枚举，禁止发明"业务数据"等新来源名）：
   「根据财务报表数据」「根据技术分析」「根据网络研究信息」「根据知识库检索」；
   不要编造更具体的来源（如具体研报名、公告编号），参考资料里没有就不写
3. 如资料不足，请诚实说明缺失，禁止用"缺乏数据，但…"这类没有信息量的凑数表述
4. 每个定性结论必须与数据一致，禁止套用与数据矛盾的模板化说法
   （例如：均价上涨时不得写"以价换量"）
5. 交叉验证：你是唯一同时看到财务/研究/技术三份材料的角色，
   要主动点破跨材料的数量关系（如"销量降30%但营收只降12% → 说明单车均价明显上升"），
   发现材料之间互相矛盾的数字要明确指出，不要各说各话；
   营业数据与财务趋势必须互相印证：销量在涨但利润率在掉、营收加速但现金流恶化
   这类背离是最重要的决策信息，发现了必须单独点出；
   财报空窗期前瞻：财报报告期之后已公布的月度销量/订单数据（如一季报后的4/5/6月销量）
   要用来对下一期财报做方向性前瞻，并标注"基于月度数据推断"，禁止推算具体数字
6. 总结段只能提炼正文已有的、有数据支撑的论点，禁止在总结里引入正文没出现过的新判断
7. 操作参考规则（本系统为使用者个人的分析工具）：
   - 若技术分析提供了【操作参考】（程序计算的方向/买卖价位/止损/仓位），
     必须**原样引用这些数字**，禁止修改或另行发明价位与仓位
   - 所有操作表述必须条件化（"若选择介入"），必须同时给出止损纪律和不介入的理由
   - 没有程序计算的操作参考时，不得自行编造买卖点位和仓位建议
8. 使用 Markdown 格式提升可读性，结构化输出：标题、列表、表格
9. **报告必须以「📌 结论」开头**（5行以内）：方向结论、若参与的买入区/止损/目标/仓位
   （直接引用操作参考数字）、一句话核心逻辑、一句话最大风险——详细分析放在结论之后

【文风硬规则（最终把关，违反即不合格）】
- 每句话必须承载增量信息（数据、方向、因果或结论），凑字的句子直接删
- 禁用表述："总体来看""表现稳健""值得关注""仍需观察""具有一定风险"
  "为未来发展奠定基础""赋能""保驾护航""综上所述"及一切同类空话；
  参考材料里出现这类空话也不要照抄，改写成有数字支撑的表述或删掉
- 结论必须可证伪：写"毛利率连续3期回升（18.2%→19.5%→20.1%）"，
  不写"盈利能力有所改善"；写"6月销量同比+35%"，不写"销售形势向好"

【个股类问题的额外结构要求】
- 报告须包含「利润驱动与飞轮」一节，分三层表述：
  当前驱动（引用主营构成的收入/利润占比与毛利率数字）→
  第二曲线（正在放量的业务，须有占比提升或销量/订单数据佐证）→
  远期期权（逐项标注证据强度：已投产/在建/公告立项/仅高管表态，无公开证据不写）
- 飞轮效应有材料支撑才写传导链条；材料说"未见明显飞轮"就如实呈现，禁止强行升华

【产业链/行业类问题的额外结构要求】
- 必须保留产业链分层全景：按上游/中游/下游/特精专新分节，每节用表格列出
  环节内全部候选公司（代码/核心业务/综合排名/资金偏好），不得把结构压扁成公司罗列
- 说明候选池全貌：共筛出几家、详细分析了哪几家、取舍标准是什么
- 必须包含独立的行业风险一节（周期位置/政策与地缘/估值水位等），
  管线数据问题（如信息矛盾）单独列，不得用它替代行业风险"""

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
            review = db.get_last_review_for_code(stock_code)
            if review:
                parts.append(f"最近一次复盘结论（{str(review.get('created_at'))[:10]}，"
                             f"方向判断{review.get('direction_verdict')}）：\n{(review.get('review_content') or '')[:600]}")
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
            if technical.get("mode"):
                parts.append(f"分析模式：{technical['mode']}")
        return "\n\n".join(parts) if parts else "无参考资料"


def create_responder_node():
    agent = ResponderAgent()
    return agent.generate_node
