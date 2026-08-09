# -*- coding: utf-8 -*-
"""
跨 Agent 共享的 prompt 片段：单点维护，避免 analyst / responder 各持一份后悄悄漂移。
改这里 = 同时改所有引用方；新增 Agent 需要文风约束时直接 import。
"""

from utils.logger import logger

# ============ 封闭枚举（单点定义）============
# prompt 文本和 compliance 的程序检查都从这两个常量渲染——改这里=两边同时生效，
# 杜绝"prompt 改了、regex 忘了"的镜像漂移。

# 文风禁用词（compliance.scan_banned_phrases 程序扫描同一份）
BANNED_PHRASES = ("总体来看", "表现稳健", "值得关注", "仍需观察", "具有一定风险",
                  "为未来发展奠定基础", "赋能", "保驾护航", "综上所述")

# 夸大性禁用词（compliance.scan_exaggerated_phrases 程序扫描同一份）
EXAGGERATED_PHRASES = ("腰斩", "崩塌", "断崖", "雪崩", "溃败", "惊现", "暴跌",
                       "暴涨", "飙涨", "踩踏")

# 数据来源表述封闭枚举（compliance.run_quality_checks 程序核对同一份）
ALLOWED_SOURCES = ("根据财务报表数据", "根据技术分析", "根据网络研究信息",
                   "根据知识库检索", "根据公司公告")

# 数字准确性红线（跨 Agent 共享）：禁止 LLM 自行编造或修改财报数字
NUMBER_ACCURACY_RULE = """【数字准确性红线（违反即不合格）】
- 你输出的每一个财务数字（营收/利润/同比增速/毛利率/费用率/资产/负债/现金流等）
  以及机构预测/一致预期数字（预测净利润/EPS/增速/forward PE等）
  必须与【参考资料】中程序提供的数字完全一致，不允许自行计算、修改、四舍五入或替换
- 如果你不确定某个数字的正确值，查看程序提供的原始数据段，使用那里的精确值
- 禁止将不同来源的同一指标混合（如用研报的营收数字配财报的利润数字）
- 如果你发现程序提供的数字之间存在矛盾，在报告中如实标注矛盾，禁止自行推算"折中值"
- 营收同比增速必须取自 revenue_growth / revenue_yoy 字段，这是程序按(本期-上年同期)/上年同期计算
  的精确值，禁止 LLM 自行用两年营收相除计算——精度不同会导致错误
- 机构预测净利润已由程序在【机构预测摘要】段按EPS×总股本直接算好（如"EPS 0.3700元（净利约4.825亿）"），
  禁止 LLM 自行从原始预测表取数心算；若【机构预测摘要】段的数字与【原始预测表】不一致，以摘要段为准
- 【趋势判读红线】"程序判读"/"单季净利判读"开头的行里的结论（加速/放缓/连续负值/降幅收窄
  等）由代码精确计算给出，禁止 LLM 修改判读结论或使用不同表述替换（如将"连续N期为负值"
  改写为"连续N个季度下滑"——"下滑"不等于"负值"，趋势方向不同）"""

# 渲染好的引用片段（prompt 里用）
BANNED_PHRASES_TEXT = "".join(f'"{p}"' for p in BANNED_PHRASES)
ALLOWED_SOURCES_TEXT = "、".join(f"「{s}」" for s in ALLOWED_SOURCES)

# 文风硬规则：analyst（产出原料）与 responder（产出终稿）共用
STYLE_RULES = f"""【文风硬规则（违反即不合格）】
- 每句话必须承载增量信息（数据、方向、因果或结论），凑字的句子直接删
- 禁用表述：{BANNED_PHRASES_TEXT}及一切同类空话；
  参考材料里出现这类空话也不要照抄，改写成有数字支撑的表述或删掉
- 结论必须可证伪：写"毛利率连续3期回升（18.2%→19.5%→20.1%）"，
  不写"盈利能力有所改善"；写"6月销量同比+35%"，不写"销售形势向好"
- 任何趋势箭头（X→Y）和"降幅扩大/占比跃升"类表述必须标注两端的报告期
  （如"毛利率 20.07%(2025Q1)→18.81%(2026Q1)"），无期间标注的趋势句一律不合格
- 每个负面数据点必须同时提及对应对冲正面变量（如"现金流同比-60%但经营现金流仍为正向，
  主因资本开支前置投入"），禁止只列利空不列对冲逻辑；每个正面数据点同理
- 严格区分「短期季度波动」与「长期趋势拐点」：
  单季度同比/环比变化只能定性为短期波动，至少连续3个季度同方向变化才可定性为趋势；
  只用单季度数据下结论时，必须加限定词"本季度""单季"并注明存在季度周期性，
  禁止用单季度数据推导"持续恶化/显著走弱"等长期结论
- 禁止使用夸大性定性词汇：不得使用"腰斩""崩塌""断崖""雪崩"等极端表述；
  下降/负增长用具体数字（如"同比-55.38%"）代替形容词
- 每个分析维度结束时标注数据来源类型（财报原始数据/程序计算/网络搜索/机构预测），
  机构预测数据必须标注"预测值，存在偏差风险"
- 文末必须包含「分析局限性说明」小节（3-5行）：声明数据滞后性、外部政策不可控、
  模型测算存在误差、以上内容不构成投资建议"""

# 中间产物定位：analyst / researcher 的输出是下游汇总 Agent 的原料，不是给人看的报告。
# 不声明这一点，模型会按"写份好报告"的方式产出过渡句和阅读性修辞，浪费 token 且传染空话。
INTERMEDIATE_PRODUCT_NOTE = """【输出定位】你的输出是下游汇总 Agent 的原料，不面向最终读者：
最大化数字密度与结论明确性，每个数字带期间与单位；不写开场白、过渡句和总结套话，
不需要照顾阅读体验——下游会重新组织表达。"""


# ============ 复盘教训注入（公共函数，单点维护）============
# analyst / researcher / technical_agent 三处原先各自实现了相同的 _format_review_lesson，
# 提取为公共函数后改这里 = 同时改所有调用方，避免三份逻辑悄悄漂移。

def format_review_lesson(stock_code: str = "", industry_name: str = "") -> str:
    """注入最近一次复盘的误判模式和相关改进规则（避免重复同类错误）。

    从历史复盘提炼规则作为分析约束：
    - 个股模式（stock_code）：读取该标的最近复盘 + 该标的及通用规则
    - 产业链模式（industry_name）：读取该产业链最近复盘 + 通用规则
    - 若最近复盘方向判断为"错误"或用户纠错复发，追加针对性警示

    Args:
        stock_code: 股票代码（个股模式）。为空且 industry_name 为空时返回空串。
        industry_name: 行业/产业链名（产业链模式）。

    Returns:
        格式化的复盘教训文本块；无数据时返回空串。
    """
    is_industry = bool(industry_name)
    if not stock_code and not is_industry:
        return ""
    if "," in stock_code:
        return ""
    try:
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        if is_industry:
            review = db.get_last_industry_review(industry_name)
            rules = db.get_active_rules(limit=8)  # 产业链规则存为通用规则
        else:
            review = db.get_last_review_for_code(stock_code)
            rules = db.get_active_rules(code=stock_code, limit=8)
        if not review and not rules:
            return ""
        parts = ["[历史复盘教训]"]
        if review:
            error_pattern = review.get("error_pattern") or "未记录"
            parts.append(f"上次分析{industry_name if is_industry else '该标的'}时存在以下误判模式：{error_pattern}；")
            review_brief = (review.get("review_content") or "")[:300]
            if review_brief:
                parts.append(f"复盘要点：{review_brief}；")
        rule_texts = []
        for r in rules:
            source = f"（来自{r.get('source_stock_name') or '通用'}）" if r.get("source_stock_name") else ""
            rule_texts.append(f"[{r.get('error_pattern', '通用')}] {r['rule_text']}{source}")
            try:
                db.increment_rule_hit(r["id"])  # 统计规则引用次数
            except Exception:
                pass
        if rule_texts:
            parts.append("改进规则：" + "；".join(rule_texts))
        if review and review.get("error_pattern") and review.get("direction_verdict") == "错误":
            parts.append(f"【⚠️ 最近一次复盘误判类别：{review['error_pattern']}】"
                         f"本次必须特别避免同类型误判，如方向不同的判断需提供更充分的证据支撑")
        if review and review.get("feedback_recurrence") == "复发":
            parts.append(f"【⚠️ 上次复盘确认用户纠错的问题仍复发】本次必须给出可验证的证据来支持相应结论，"
                         f"并在报告相应位置明确标注，避免再次复发")
        return "".join(parts)
    except Exception as e:
        logger.warning(f"读取复盘教训失败（不影响本次分析）: {e}")
        return ""