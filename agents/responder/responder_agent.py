"""
回答生成 Agent
职责：综合所有 Agent 的输出，生成最终用户回答
（免责声明与合规修订由其后的 compliance 节点负责）
"""

import traceback
from datetime import date
from typing import Dict, Any, Optional
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from agents.prompts_common import ALLOWED_SOURCES_TEXT, STYLE_RULES
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
2. 数据来源标注必须用以下封闭枚举（禁止发明新来源名），每个精确数字必须标注来源：
   - 公司公告/财报原文 → **[财报]**
   - 程序计算结果（PE分位/技术指标/交易计划等） → **[程序]**
   - 券商/机构研报 → **[研报来源]**
   - 网络公开数据（新闻/自媒体/行业平台） → **[公开数据]**
   禁止：无出处的精确数字、禁止编造更具体的来源（如具体研报名、公告编号）；
   销量数字若材料中标注来自产销快报公告，必须优先引用且标**[财报]**
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
8. 必须用 `##` Markdown 二级标题组织章节（格式：`## 📌 结论`、`## 公司概况与业务拆解`），
   每个独立章节都必须有且仅有一个 `##` 标题行，禁止用 `###` 或加粗代替章节标题；
   章节标题名必须精确使用个股/ETF/产业链规则中指定的标题（如个股必须写
   `## 公司概况与业务拆解`，不能写成 `## 业务拆解`），否则章节会被程序漏检
9. **报告必须以「## 📌 结论」标题开头**（`##` 后空一格再写 `📌 结论`），
   结论卡必须压缩为**5行以内**，格式如下（行名固定、不许省略）：
   - **方向**：观望/回避/可考虑介入 +（一句话理由）
   - **操作**：程序数字（介入区或观察位/止损/目标/仓位）；程序判定不介入时写
     "不介入（原因，如盈亏比X不足1.5），回踩观察位XX～XX再重估"
   - **核心逻辑**：一句话，必须含关键数字（如"2026Q1净利-55.38%，连续4季加速下滑"）
   - **最大风险**：一句话（如"欧盟关税落地-30~50亿"）
   - **等待信号**：满足可重新评估的具体条件（如"7月产销同比转正/中报降幅收窄/突破97.1"）
   （个股/产业链模式在各自规则里追加骨架行）详细分析放在结论之后

{style_rules}
- 分地区/分业务占比这类非主表数字还必须带来源（季报通常不披露分地区收入，
  这类数字多来自研报转述，标「根据网络研究信息」）

【输出精简规则（严格执行）】
1. 禁止在报告正文中写入系统内部运行统计（如"近N次方向判断命中率X%"、"历史成绩单"）；
   但必须在「分析局限性说明」中提及模型容错率（如"本模型历史方向判断命中率约50%，结论因此偏保守"）
2. 禁止写入中性/零信息含量的陈述（如"未来3个月无大额解禁"→没有解禁压力不是利好；
   只有存在大额解禁时才写入风险小节；同理"无重大公告"、"无特殊事项"等一律跳过）
3. 时序数据（季度营收/利润/销量等）禁止逐期罗列所有数字——只写结论：
   "连续4个季度净利润同比下滑，降幅从-5%扩大至-15%；营收由+3%转负至-8%"
   必要时只列首尾两个对比数字，不铺陈全序列
4. 产品/车型/客户等明细清单禁止逐项罗列——只写结论汇总：
   "销量主力为XX价位段产品，高端品牌占比Y%偏低"；不写"XX型号售出A台，YY型号售出B台"
5. 行情描述（大盘/板块/alpha/beta分析）不超过3行结论，不展开过时走势复盘"""

_STOCK_RULES = """
【个股类问题的额外结构要求】
- 「📌 结论」骨架追加行（行名固定）：
  - **大盘环境**：顺风/中性/逆风（程序判定；材料没有就填"无数据"）
- 报告章节标题必须严格按以下顺序和措辞使用 `##` 标题（禁止自行发明标题名）：
   `## 📌 结论` → `## 公司概况与业务拆解` → `## 财务分析` → `## 护城河` →
   `## 利润驱动与飞轮` → `## 大盘与筹码` → `## 关键支撑压力位` →
   `## 估值` → `## 操作参考与情景推演` → `## 分析局限性说明`
- 估值表述有 PEG 数据时必须引用（注明 trailing 口径，增速为负时写"PEG 不适用"）
- 估值分位**必须同时给出3年/5年/10年三个窗口**（如：PE(TTM) XX倍，3年分位84%（偏贵）/5年分位50%（中位）/10年分位33%（长周期偏低）），
  禁止只写单一窗口 cherry-pick 结论；各窗口矛盾时必须点破成因（如"盈利下滑被动抬高PE"）；
  材料中缺少某个窗口的分位数时标注"数据不足"，不可自行编造
- 估值一节**必须包含PS(TTM)历史分位**（3年/5年/10年），与PE/PB形成三维交叉验证
- PE分位与PB分位背离时必须专门解读：
  "PE/PB背离的核心矛盾：盈利下滑被动抬高PE，但资产端已处历史底部——"PE悬顶、PB托底"是当下估值辩证的完整表述"
- 报告须包含「利润驱动与飞轮」一节，分三层表述：
  当前驱动（引用主营构成的收入/利润占比与毛利率数字）→
  第二曲线（正在放量的业务，须有占比提升或销量/订单数据佐证）→
  远期期权（公司公开布局但尚未贡献利润的方向，**用表格格式**：
  方向 | 证据强度（已投产/在建/公告立项/仅高管表态）| 预计兑现时间
  每行一个方向，禁止展开大段文字描述；无公开证据的方向不列入）
- 「利润驱动与飞轮」中必须包含**业绩持续性判断**子节：对每项增长驱动力
  区分是**一次性脉冲**（大客户集采/补贴抢装/低基数/资产处置等）还是
  **持续性增长**（渗透率提升/客户覆盖面扩大/复购率高/产能有序释放等），
  综合判断可持续利润占比；分析材料中未明确则写"材料不足以判断持续性"
- 飞轮效应必须做双向判断：既有正向协同链条（如成本共享、渠道复用），也要列出反向约束（如低端降价损伤高端品牌、业务冲突）；材料说"未见明显飞轮"就如实呈现，禁止强行升华
- 报告须包含「股东筹码与事件日历」一节：减持/增持/解禁/回购公告、股东户数变化、
  临近的除权除息与解禁日——材料里有必须列出（解禁减持是确定性抛压，必须进风险）；
  材料里没有该块就写"本次未获取到股东筹码数据"
- 报告须包含「关键支撑压力位」一节：引用程序计算的关键位（支撑由近及远/压力由近及远）
  的具体价位与强度依据，结合均线/BOLL位给出多空分水岭判断；程序未计算时写
  "本次未获取到程序关键位数据（参考均线支撑/压力）"并自行从 K 线数据读出近期高低点
- 若技术分析材料包含**多周期技术打分表**（均线排列/MACD/动量量价/支撑压力等维度
  的0-10分项及综合总分），必须在「关键支撑压力位」小节之后**以表格形式原样呈现**，
  保留各维度得分和综合总分（日线×0.5+周线×0.3+月线×0.2），
  并附带技术入场阈值说明：综合分≥7.5为强势入场区间、6.0~7.5为中性区间、<6.0为谨慎区间；
  不得省略或仅文字转述——打分表提供了标准化可对比的技术质量判断，是完整性硬要求；
  材料中没有打分表则写"本次未获取到程序技术打分数据"
- 估值一节的次序：先同行对比（相对贵贱）→ 再历史分位多窗口（含PS）与PEG →
  材料提供了分部估值（SOTP）计算结果时，压缩为一句区间结论（如"SOTP每股内在价值XX-XX元"），
  不展开估值假设讨论；材料没做 SOTP 时禁止自行拼分部估值
- 财务材料含现金流数据（经营现金流/净现比/资本开支）时，报告必须保留「现金流质量」
  小节——净现比同比（同期对比）、经营现金流与利润的匹配度、资本开支强度；
  这一节漏掉等于把利润质量的核心证据丢了
- 自由现金流：材料有程序计算的 FCF 值则引用，同时给出经营现金流与资本开支两个数；
  材料未给程序计算值时，统一移至【分析局限性说明】中标注"缺资本开支数据"，正文不展开；
  **禁止自行心算 FCF**（实测心算错过一倍）；禁止让 FCF 和投资活动净额并排出现
  而不解释口径差异（投资净额含理财赎回等回流，读者手算对不上账）
- 报告须包含「情景推演」一节：乐观/基准/悲观三情景表格
  （情景 | 触发条件（可验证指标/事件） | 净利润区间（亿元）| 传导路径 | 应对纪律），
  每个情景必须给出**具体净利润量化区间**（如悲观情景250亿以下、基准情景320-350亿、乐观情景380亿以上），
  应对纪律只能引用程序操作参考的数字（悲观触发→按止损纪律位执行；
  乐观确认→参照观察区/目标位重新评估）；可能性只用高/中/低，标注"推演非预测"
- 操作结论为"不介入/观望"时，悲观情景的应对纪律禁止写"跌破XX无条件离场"这类
  持仓化措辞（0仓位无从离场），应写"若已持仓，跌破XX离场；未持仓维持不介入"
- 结论与情景推演中出现的每个关键价位（观察位/止损位/支撑压力位/目标参考位）
  都必须标注来源——"[程序]"或材料出处，全文标注口径一致，禁止一处标一处不标
- 材料含「重估触发条件」时必须原样列出，并说明加入监控后系统自动盯梢、命中会推送；
  方向结论为"观望/回避"时必须带上"等什么"——引用触发条件或观察区价位，
  禁止给没有后续动作的死结论
- 海外出口数据（如"海外收入占比43.5%"）必须标注统计口径：按出口量/总产量（含渠道库存与在途）
  还是终端交付口径，禁止只说"海外收入占比"；口径出处来自研究材料，材料未说明则写"统计口径待确认"
- 「分析局限性说明」中必须增加：
  ① 模型历史方向判断命中率约50%，结论因此偏保守
  ② 机构预测与实际的预期差风险（如"机构2026年EPS预测4.54元隐含全年净利润约413亿，
     但Q1实际仅40.85亿，后三季度需环比Q1增长200%+方能兑现"）"""

_ETF_RULES = """
【ETF 类问题的额外结构要求】
- 报告须包含以下独立小节（顺序固定）：
  1. **ETF 基本信息**：ETF 名称、类型（宽基/行业/主题/跨境）、规模（AUM）、成立日期
  2. **行情与折溢价**：最新价 vs IOPV（实时净值）、折溢价率、成交额、换手率
  3. **份额与资金流向**：最新份额、份额趋势（变动方向与幅度）、主力资金净流入/占比
  4. **行业配置**：前5大行业及占比，判断集中度与风格暴露
  5. **前5大重仓股穿透**：逐只分析基本面亮点与风险，附占净值比例
  6. **持仓组合评估**：持仓整体质量、集中度风险、是否涵盖龙头
- 折溢价异常（绝对值>1%）、份额大幅变动（>5%）必须单独警示
- 重仓股穿透分析引用研究材料中的个股信息，禁止自行捏造财务数字
- 不适用个股分析的维度（护城河/利润驱动/飞轮/股东筹码等）不写
- 不给出买卖操作建议，仅做客观分析与评估
- 不包含「情景推演」和「操作参考」段落
- 若材料提供了程序计算的关键位，须包含「行情关键位置」小节，引用支撑压力位价位；
  未计算时跳过该小节"""

_INDUSTRY_RULES = """
【产业链/行业类问题的额外结构要求】
- 报告章节标题必须严格按以下顺序和措辞使用 `##` 标题（禁止自行发明标题名）：
   `## 📌 结论` → `## 产业链全景图` → `## 关键环节` → `## 候选公司` →
   `## 行业趋势` → `## 环节利润迁移判断` → `## 投资建议` → `## 行业风险` →
   `## 分析局限性说明`
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
    （引用材料中的程序说明）——门槛随阶段切换，不要写死"7分护城河门槛"；
    门槛只能**原样引用**材料里的程序描述（如"护城河≥5分且边际变化≥6分"），
    禁止自创"综合分≥X"之类材料里不存在的门槛数值
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
        """按本次模式拼装规则：通用段 + 专项段（个股/ETF/产业链互斥，不给无关规则）"""
        prompt = _COMMON_RULES.format(today=date.today().strftime('%Y-%m-%d'),
                                      style_rules=STYLE_RULES,
                                      source_enum=ALLOWED_SOURCES_TEXT)
        stock_code = state.get("stock_code") or ""
        stock_type = state.get("stock_type") or ""
        if stock_type == "etf":
            prompt += _ETF_RULES
        elif state.get("industry_name") or "," in stock_code:
            prompt += _INDUSTRY_RULES
        elif stock_code:
            prompt += _STOCK_RULES
        return prompt

    @staticmethod
    def _build_title(stock_code: str, stock_type: str,
                     industry_name: str, raw_answer: str,
                     research_result: dict) -> str:
        """构建报告标题行：标的身份标识"""
        # ETF：优先从行情数据取名称，再试研究摘要，最后从 LLM 输出解析
        if stock_type == "etf":
            # 方法1：从行情数据查名称（最可靠）
            try:
                from tools.etf_tools import fetch_etf_spot
                spot = fetch_etf_spot(code)
                spot_name = spot.get("名称", "").strip() if spot else ""
                if spot_name:
                    return f"{spot_name}({code})"
            except Exception:
                pass

            # 方法2：从 research_result 摘要中提取 ETF 名称
            try:
                summary = (research_result or {}).get("summary", "") or ""
                for line in summary.split("\n")[:10]:
                    line = line.strip().strip("#").strip()
                    if "ETF" in line and len(line) > 4:
                        # 去掉括号里的代码
                        import re
                        clean = re.sub(r'[（(].*?[）)]', '', line).strip()
                        if clean:
                            return f"{clean}({code})"
            except Exception:
                pass

            # 方法3：从 raw_answer 开头取第一行有效文本
            name = ""
            for line in raw_answer.split("\n")[:5]:
                line = line.strip().strip("#").strip()
                if line and not line.startswith("**📌"):
                    name = line
                    break
            if name and len(name) > 2:
                # 去掉含代码的行尾
                import re
                clean = re.sub(r'[（(].*?[）)]', '', name).strip()
                if clean:
                    return f"{clean}({code})"
            return f"ETF {code}"

        # 行业/产业链：用 industry_name
        if industry_name or ("," in stock_code):
            ind = industry_name or stock_code
            return f"【{ind}】产业链分析"

        # 个股：从 research_result 或回答中提取公司名
        code = stock_code
        if not code:
            return ""

        # 方法1（优先）：从数据库/代码映射查公司名（最可靠）
        try:
            from tools.company_code_validator import find_company_name
            looked_up = find_company_name(code)
            if looked_up:
                return f"{looked_up}({code})"
        except Exception:
            pass

        # 方法2：从回答第一行取（LLM 开头写了名称时用）
        research = research_result or {}
        sources = research.get("sources") or []
        company_name = ""
        for line in raw_answer.split("\n")[:5]:
            stripped = line.strip().strip("#").strip()
            if stripped and len(stripped) > 2:
                # 跳过 📌 标记、结论骨架行、常见章节标题
                if stripped in ("📌 结论", "结论", "核心结论", "公司概况与业务拆解",
                                "护城河", "估值", "利润驱动与飞轮", "大盘与筹码",
                                "关键支撑压力位", "操作参考与情景推演",
                                "财务分析", "分析局限性说明"):
                    continue
                if stripped.startswith("📌"):
                    continue
                # 跳过明显不是公司名的行（LLM 意外输出如 "- **方向**：观望..."）
                if stripped.startswith("-") or stripped.startswith("**"):
                    continue
                company_name = stripped
                break

        if company_name:
            return f"{company_name}({code})"

        return f"代码 {code}"

    def generate_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            question = state.get("question", "")
            documents = state.get("documents", [])
            analysis = state.get("analysis_result", {})
            research = state.get("research_result", {})
            technical = state.get("technical_result", {})

            logger.info("开始生成最终回答")

            context = self._format_context(documents, analysis, research, technical)

            # 分析连续性：单只个股时注入上次分析快照与复盘结论（ETF 跳过）
            history = self._format_history(state.get("stock_code") or "",
                                           state.get("stock_type") or "")
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

            formatter_mode = self._get_formatter_mode(state)
            from tools.report_formatter import format_report
            _MODE_SECTION_COUNT = {"etf": 8, "stock": 10, "industry": 9}
            expected = _MODE_SECTION_COUNT.get(formatter_mode, 10)
            MAX_RETRIES = 2

            for attempt in range(MAX_RETRIES + 1):
                response = self.llm.invoke(messages)
                raw_answer = response.content

                # 格式后处理：内容重排、维度补缺
                if formatter_mode:
                    final_answer = format_report(raw_answer, formatter_mode)
                else:
                    final_answer = raw_answer

                # 标题行
                title = self._build_title(
                    state.get("stock_code", ""),
                    state.get("stock_type", ""),
                    state.get("industry_name", ""),
                    raw_answer,
                    state.get("research_result") or {},
                )
                if title:
                    final_answer = f"**{title}**\n\n{final_answer}"

                # 质量检测：综合评分（章节完整度 + 数据引用 + 风险提示）
                from tools.report_quality_scorer import assess_report_quality, should_retry, build_retry_hint
                quality = assess_report_quality(final_answer, formatter_mode)
                if not should_retry(quality):
                    logger.info(f"回答生成完成（{formatter_mode or '通用'}模式，质量评分{quality['score']}分，缺失{quality['missing_sections']}/{expected}章）")
                    return {
                        "final_answer": final_answer,
                        "intermediate_steps": [("responder", final_answer[:200])],
                    }
                if attempt < MAX_RETRIES:
                    logger.warning(
                        f"报告质量评分{quality['score']}分（缺失{quality['missing_sections']}/{expected}章），第{attempt+1}次重试"
                    )
                    retry_hint = build_retry_hint(quality)
                    messages = messages.copy()
                    messages.append(HumanMessage(content=retry_hint))

            # 重试用尽，返回最后一次的结果并标记质量警告
            logger.warning(f"报告质量未达标（{quality['score']}分），返回最佳结果")
            return {
                "final_answer": final_answer,
                "intermediate_steps": [("responder", f"⚠质量警告 质量评分{quality['score']}分 {final_answer[:200]}")],
            }
        except Exception as e:
            logger.error(f"Responder 生成回答失败: {e} {traceback.format_exc()}")
            return {
                "final_answer": f"抱歉，生成分析报告时发生了错误：{e}。请稍后重试或检查日志。",
                "intermediate_steps": [("responder", f"ERROR: {e}")],
            }

    @staticmethod
    def _get_formatter_mode(state: AgentState) -> Optional[str]:
        """根据 state 判断格式化模式"""
        stock_code = state.get("stock_code", "")
        stock_type = state.get("stock_type", "")
        industry_name = state.get("industry_name", "")
        if stock_type == "etf":
            return "etf"
        if industry_name or ("," in stock_code):
            return "industry"
        if stock_code:
            return "stock"
        return None

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
            # 追加自动生成的 prompt 优化补丁
            try:
                from tools.prompt_optimizer import get_prompt_patch_for_target
                patch = get_prompt_patch_for_target(code=code, name=name)
                if patch:
                    lines.append(f"\n{patch}")
            except Exception:
                pass
            return "\n".join(lines)
        except Exception as e:
            logger.warning(f"读取用户纠错记录失败（不影响本次回答）: {e}")
            return ""

    def _format_history(self, stock_code: str, stock_type: str = "") -> str:
        """取上次分析快照与最近复盘，形成「较上次分析…」的连续性素材"""
        if not stock_code or "," in stock_code or stock_type == "etf":
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

            # 改进规则注入：从历史复盘提炼的规则作为硬性要求
            try:
                rules = db.get_active_rules(code=stock_code, limit=8)
                if rules:
                    rule_lines = ["【历史复盘改进规则（硬性要求：本次分析必须遵循以下规则，避免重复犯同类错误）】"]
                    for r in rules:
                        source = f"（来自{r.get('source_stock_name') or '通用'}）" if r.get("source_stock_name") else ""
                        rule_lines.append(f"· [{r.get('error_pattern', '通用')}] {r['rule_text']}{source}")
                        db.increment_rule_hit(r["id"])  # 统计引用次数
                    # 附加该标的最近一次误判类别（如有），强化针对性
                    if review and review.get("error_pattern") and review.get("direction_verdict") == "错误":
                        rule_lines.append(f"【⚠️ 该标的最近一次复盘误判类别：{review['error_pattern']}】"
                                          f"本次必须特别避免同类型误判，如方向不同的判断需提供更充分的证据支撑")
                    parts.append("\n".join(rule_lines))
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
                v = technical["trade_plan_text"]
                parts.append(str(v) if not isinstance(v, str) else v)
            if technical.get("trade_plans_text"):
                v = technical["trade_plans_text"]
                parts.append(str(v) if not isinstance(v, str) else v)
            # 支撑压力位（独立于操作参考，供报告「关键支撑压力位」小节引用具体价位）
            if technical.get("sr_levels_text"):
                parts.append(str(technical["sr_levels_text"]))
            if technical.get("sr_levels_texts"):
                for code, t in technical["sr_levels_texts"].items():
                    parts.append(f"关键位({code}):\n{t}")
            if technical.get("mode"):
                parts.append(f"分析模式：{technical['mode']}")
        return "\n\n".join(parts) if parts else "无参考资料"


def create_responder_node():
    agent = ResponderAgent()
    return agent.generate_node
