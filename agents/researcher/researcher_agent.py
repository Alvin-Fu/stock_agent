"""
信息研究 Agent（Researcher）
职责：
  - 个股模式：多维搜索全网信息 → LLM 综合分析
  - 行业模式：拆解产业链上中下游 → 搜公司 → 基本面/护城河分析 → 输出候选公司列表给下游 technical_agent
注意：不负责技术面分析（日线/周线/MACD 等），技术面由 technical_agent 专责
"""

import re
import traceback
from typing import Dict, Any, List, Optional
from datetime import date, datetime, timedelta
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from agents.prompts_common import INTERMEDIATE_PRODUCT_NOTE
from core.llm import get_agent_llm
from .web_search_tool import web_search
from tools.company_code_validator import find_stock_code, find_company_name
from utils.logger import logger


# 阶段化评分框架：机会=边际变化，护城河是存量质量——不同行业生命周期两者的权重完全不同。
# 用成熟行业的护城河框架去筛导入期行业（如商业航天），结论永远是"不参与"，等于对成长机会失明。
# momentum（边际变化）分项：只认近2个季度/近3个月的增量事实（增速拐点/新订单/产能爬坡/毛利率环比回升）。
STAGE_WEIGHTS = {
    "成熟期": {"business": 0.2, "fundamental": 0.3, "moat": 0.4, "momentum": 0.1},
    "成长期": {"business": 0.2, "fundamental": 0.25, "moat": 0.25, "momentum": 0.3},
    "导入期": {"business": 0.15, "fundamental": 0.2, "moat": 0.25, "momentum": 0.4},
}
# 阶段判定失败/缺失时的兜底：取中间档，避免把早期行业误按成熟框架全部拒之门外
DEFAULT_STAGE = "成长期"

# 阶段化准入门槛：门槛指标随行业阶段切换，指标分缺失（=无证据）一律不入池
# 成熟期看护城河；成长期护城河和边际变化都要过线；导入期边际变化（订单可见性/卡位）为王
STAGE_GATES = {
    "成熟期": [("moat", 7.0, "护城河")],
    "成长期": [("moat", 5.0, "护城河"), ("momentum", 6.0, "边际变化")],
    "导入期": [("momentum", 7.0, "边际变化（订单可见性/卡位）")],
}


def normalize_stage(stage) -> str:
    stage = str(stage or "").strip()
    return stage if stage in STAGE_WEIGHTS else DEFAULT_STAGE


def apply_stage_gate(ranked: List[Dict[str, Any]], stage: str):
    """
    阶段化硬门槛过滤（纯函数）：返回 (passed, excluded)。
    门槛指标分项缺失（被中性5分顶替）也视为未达标——证据不足不入池。
    passed 重新编排名。
    """
    stage = normalize_stage(stage)
    gates = STAGE_GATES[stage]
    passed, excluded = [], []
    for item in ranked:
        missing = item.get("missing") or []
        fails = []
        for metric, gate, label in gates:
            if metric in missing:
                fails.append(f"{label}分项缺失（无证据）")
            elif item.get(metric, 0) < gate:
                fails.append(f"{label}{item.get(metric)}分未达{gate:g}分门槛")
        if not fails:
            passed.append(dict(item))
        else:
            ex = dict(item)
            ex["exclude_reason"] = f"[{stage}] " + "；".join(fails)
            excluded.append(ex)
    for i, it in enumerate(passed, 1):
        it["rank"] = i
    return passed, excluded


def compute_composite_ranking(candidates: List[Dict[str, Any]], stage: str = DEFAULT_STAGE) -> List[Dict[str, Any]]:
    """
    程序计算综合评分与排名（LLM 只提供分项分数，加权与排序不交给它心算）。
    权重按行业阶段切换；分项缺失/非法时按 5.0 中性分处理并标注。
    返回按综合分降序的列表：[{code, business, fundamental, moat, momentum, composite, rank, note}]
    """
    stage = normalize_stage(stage)
    weights = STAGE_WEIGHTS[stage]
    ranked = []
    for c in candidates or []:
        code = str(c.get("code", "")).strip()
        if not code:
            continue
        scores, missing = {}, []
        for key in weights:
            try:
                v = float(c.get(key))
                if not (0 <= v <= 10):
                    raise ValueError
                scores[key] = round(v, 1)
            except (TypeError, ValueError):
                scores[key] = 5.0
                missing.append(key)
        composite = round(sum(scores[k] * w for k, w in weights.items()), 2)
        ranked.append({
            "code": code, **scores, "composite": composite, "stage": stage, "missing": missing,
            "note": "分项缺失按5分中性处理" if missing else "",
        })
    ranked.sort(key=lambda x: x["composite"], reverse=True)
    for i, item in enumerate(ranked, 1):
        item["rank"] = i
    return ranked


def apply_valuation_adjustment(ranked: List[Dict[str, Any]],
                               per_stock: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    预期差调整（纯函数）：基本面评分和估值分位必须见面——综合分9但PE分位95%不该排第一。
    对高历史分位减分、低分位加分，并标注机会/拥挤象限；按调整后综合分重排。
    per_stock 来自 industry_metrics 的逐股程序数据（pe_percentile/total_mv/mf_net20）。
    """
    metrics_map = {str(r.get("code", "")): r for r in per_stock or []}
    for item in ranked:
        m = metrics_map.get(item["code"]) or {}
        pct = m.get("pe_percentile")
        item["pe_percentile"] = pct
        item["total_mv"] = m.get("total_mv")
        item["mf_net20"] = m.get("mf_net20")
        adj = 0.0
        if pct is not None:
            if pct >= 80:
                adj = -1.0
            elif pct >= 60:
                adj = -0.5
            elif pct <= 30:
                adj = 0.5
        item["valuation_adj"] = adj
        item["composite_adj"] = round(item["composite"] + adj, 2)
        if pct is None:
            item["quadrant"] = "无估值分位数据"
        elif item["composite"] >= 7 and pct <= 40:
            item["quadrant"] = "机会区（高分低估）"
        elif item["composite"] >= 7 and pct >= 70:
            item["quadrant"] = "拥挤区（高分高估）"
        elif item["composite"] < 6 and pct >= 70:
            item["quadrant"] = "危险区（低分高估）"
        else:
            item["quadrant"] = "中性"
    ranked.sort(key=lambda x: x.get("composite_adj", x["composite"]), reverse=True)
    for i, item in enumerate(ranked, 1):
        item["rank"] = i
    return ranked


def format_ranking_table(ranked: List[Dict[str, Any]], name_of=None) -> str:
    """排名表文本（附在报告末尾，供 responder 展示与复盘留档抽取）"""
    if not ranked:
        return ""
    stage = ranked[0].get("stage", DEFAULT_STAGE)
    w = STAGE_WEIGHTS[normalize_stage(stage)]
    lines = [f"【综合排名（行业阶段：{stage}；程序按 业务{w['business']:.0%}+基本面{w['fundamental']:.0%}"
             f"+护城河{w['moat']:.0%}+边际变化{w['momentum']:.0%} 加权，再按PE历史分位做预期差调整）】"]
    for item in ranked:
        label = item["code"]
        if name_of:
            try:
                name = name_of(item["code"])
                if name:
                    label = f"{name}({item['code']})"
            except Exception:
                pass
        note = f"（{item['note']}）" if item.get("note") else ""
        extra = []
        if item.get("composite_adj") is not None:
            pct = item.get("pe_percentile")
            # 窗口必须在源头带上：下游 LLM 只会照抄，这里不写"近3年"，报告里就是裸分位
            extra.append(f"调整后{item['composite_adj']}"
                         + (f"（PE近3年分位{pct}%，{item.get('valuation_adj'):+g}）" if pct is not None else "（无估值分位）"))
        if item.get("quadrant"):
            extra.append(item["quadrant"])
        if item.get("total_mv") is not None:
            extra.append(f"市值{item['total_mv']:.0f}亿")
        if item.get("mf_net20") is not None:
            extra.append(f"20日主力净流入{item['mf_net20']:+.1f}亿")
        lines.append(f"{item['rank']}. {label} 综合{item['composite']} "
                     f"= 业务{item['business']} 基本面{item['fundamental']} 护城河{item['moat']} "
                     f"边际{item['momentum']}{note}"
                     + ("｜" + "｜".join(extra) if extra else ""))
    return "\n".join(lines)


# 触发条件可判定性标记：数字阈值或明确事件词，至少占其一
_DETERMINABLE_MARK = re.compile(
    r"\d|转正|扭亏|落地|公告|公布|发布|披露|中标|获批|签订|首次|突破|新高|新低")


def parse_company_triggers(summary: str) -> List[Dict[str, str]]:
    """
    从个股研究输出末尾抽取公司级重估触发条件 JSON（纯函数）。
    返回 [{"trigger_type":"news","description":...,"keywords":...}]，最多4条；
    解析失败返回 []；缺方向阈值/事件词的不可判定条目被丢弃。
    """
    import json as _json
    import re as _re
    try:
        m = _re.search(r'\{\s*"company_triggers"', summary or "")
        if not m:
            return []
        extracted, _ = _json.JSONDecoder().raw_decode(summary[m.start():])
        out = []
        for t in (extracted.get("company_triggers") or [])[:4]:
            if not isinstance(t, dict):
                continue
            desc = str(t.get("trigger") or "").strip()
            if not desc:
                continue
            # 可判定性校验：无数字阈值也无事件词的触发条件（如"毛利率环比变化"）
            # 任何时候都"成立"，监控没法判定命中，直接丢弃
            if not _DETERMINABLE_MARK.search(desc):
                logger.warning(f"[个股触发] 丢弃不可判定的触发条件：「{desc}」（缺方向阈值/事件词）")
                continue
            out.append({"trigger_type": "news", "description": desc,
                        "keywords": str(t.get("keywords") or "").strip()})
        return out
    except (ValueError, TypeError):
        return []


class ResearcherAgent:
    """研究 Agent：个股模式 + 产业链公司筛选与基本面分析"""

    def __init__(self):
        self.llm = get_agent_llm("researcher")

    # ========== 个股搜索模式 ==========

    def _build_stock_queries(self, stock_code: str) -> List[str]:
        today = date.today()
        one_month = today - timedelta(days=30)
        three_months = today - timedelta(days=90)
        recent_period = f"{today.year}年{today.month}月"
        three_month_range = f"{three_months.strftime('%Y-%m')} {today.strftime('%Y-%m')}"

        # 搜索引擎对公司名的召回远好于裸代码，先反查名字（失败则退回代码）
        try:
            name = find_company_name(stock_code) or stock_code
        except Exception:
            name = stock_code
        tag = f"{name}" if name != stock_code else stock_code

        # 上一个完整自然月（月度销量/产销快报一般在次月上旬发布）
        last_month_end = today.replace(day=1) - timedelta(days=1)
        last_month = f"{last_month_end.year}年{last_month_end.month}月"

        return [
            f"{tag} 公司公告 重大事项 {one_month.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",
            f"{tag} {last_month} 销量 出货量 同比 环比",
            f"{tag} 所属行业 产业政策 发展趋势 {three_month_range}",
            f"{tag} 经营状况 营收 利润 最新业绩 {recent_period}",
            f"{tag} 月度产销快报 销量 环比 同比 {recent_period}",
            f"{tag} 竞争对手 销量对比 市场份额 {today.year}",
            f"{tag} 业务构成 收入占比 毛利占比 各板块营收拆分",
            f"{tag} 出货量 产能 新增订单 订单来源 资本开支",
            f"{tag} 技术实力 研发投入 核心技术突破 专利",
            f"{tag} 护城河 技术壁垒 不可替代性 切换成本 市占率 竞争格局",
            f"{tag} 第二增长曲线 新业务 布局 进展 放量 {recent_period}",
            f"{tag} 产业链 上下游 市场地位 竞争格局 {recent_period}",
            f"{tag} 利好 利空 机构评级 目标价 {one_month.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",
        ]

    def _build_stock_system_prompt(self) -> str:
        today = date.today().strftime("%Y-%m-%d")
        return f"""你是一个专业的股票信息研究员和分析师。今天的日期是 {today}，请以此为时间基准判断"近期/最新"。

{INTERMEDIATE_PRODUCT_NOTE}

请基于下方搜索结果，对该公司的以下维度进行客观分析并给出核心结论：

1. **公司公告与重大事项**：近期是否有重大公告及影响
2. **产业信息**：行业景气度、政策、趋势
3. **业务拆解**：各板块收入/毛利占比、TOP3业务、出货量、产能利用率、资本开支、新增订单及来源；
   销量/出货量必须给近几个月的同比或环比序列并注明是单月还是累计，
   只有单月数据时不得外推为趋势
4. **财报空窗期前瞻**：最新财报报告期之后已公布的月度经营数据（销量/出货量/订单/中标）
   是下一期财报的领先指标，必须单独汇总为「报告期后经营数据」小节
   （如一季报后已出的4/5/6月销量），并给出方向性前瞻：延续加速/延续放缓/出现拐点。
   前瞻只能是方向判断且必须标注"基于月度数据推断"，禁止推算具体营收利润数字
5. **技术实力**：研发投入、核心技术突破、在研项目、专利壁垒
6. **产业链地位与护城河评级**：位置、议价力、竞争格局；
   必须给出护城河评级「高/中/低」+ 依据（独有技术/专利/牌照/切换成本/市占率等具体证据），
   证据不足时评「低」并写明"未找到壁垒证据"——护城河低的公司，
   后续所有多头结论都要显著降温（没有壁垒的景气随时会被竞争摊薄）
7. **利好与利空分析**：分别列出利好和利空条目，每条标注影响力(高/中/低)及依据出处；
   综合判断只用「偏多/中性/偏空」三档，禁止编造精确百分比
8. **利润驱动与飞轮（三段论）**：
   - 当前驱动：现在利润主要靠什么业务赚？必须引用主营构成数据的收入/利润占比与毛利率原数
   - 第二曲线：哪些业务正在放量接棒？需有占比同比提升、销量/订单/出货量数据或公告佐证，
     不能只凭新闻标题定性（按地区维度的海外占比提升=出海驱动）
   - 远期期权：公司公开布局但尚未贡献利润的方向，逐项标注证据强度
     （已投产/在建/公告立项/仅高管表态），没有公开证据的方向禁止列入
   - 飞轮效应：判断各业务之间是否共享技术/产能/渠道/品牌而相互强化，
     成立则写出具体传导链条（如"A业务的规模摊薄B业务的核心部件成本"）；
     不成立或证据不足要明说"未见明显飞轮"，禁止强行升华
9. **情景推演与重估触发**：
   - 给出 乐观/基准/悲观 三情景，每个情景必须包含：
     触发条件（具体可验证：指标+阈值或事件，如"单月销量同比转正""毛利率环比回升超1pct"
     "海外反补贴关税落地"）、传导路径（条件→业务→财务指标的方向变化）、
     可能性档位（只用 高/中/低，禁止编造百分比）。情景是推演不是预测，禁止写目标价
   - 在输出最末尾附一段纯JSON（不要markdown包裹）：
     {{"company_triggers": [{{"trigger": "重估触发条件（可被公开新闻验证的具体事件）",
       "keywords": "盯梢关键词 空格分隔"}}, ...]}}
     取三情景中最关键的1-4条可验证触发条件（利多利空都要有）；没有就给 []
   - 触发条件硬规则（违反的条目会被程序丢弃）：
     ①必须是**尚未发生**的前瞻事件：已披露报告期的数据不得作为触发条件
     （一季报已公布就写"中报/三季报净利润同比转正"，不能写"一季报转正"）；
     ②必须可判定：含明确方向+数字阈值（"毛利率回升至25%以上""同比转正"）
     或明确事件（"公告""正式落地""公布"），禁止"毛利率波动/环比变化"这类
     没有方向阈值、任何时候都成立的写法

【风险对称要求】
- 每条高影响力利好必须检查并列出对应风险（如：出口高增→关税/反补贴调查风险；大客户订单→客户集中度风险）
- 机构评级降权处理：A股卖方几乎不出卖出评级，"N家机构全部买入"不构成有效利好，最多作为关注度参考
- 目标价：搜索结果里有具体数字才引用，没有就不写"上涨空间较大"这类无依据表述

【输出要求】每个维度给出明确结论；业务数据尽可能用数字；搜索结果中没有的信息标注「信息不足」，
禁止用自身知识补数字；最后 3 句以内核心总结。
文风硬规则：每句话必须有增量信息（数据/方向/因果/结论）；禁用"总体来看""表现稳健"
"值得关注""仍需观察""为未来奠定基础""赋能""综上所述"等空话套话；结论要可证伪
（写"6月销量同比+35%、连续3个月加速"，不写"销售情况良好"）"""

    # ========== 产业链全景分析模式 ==========

    def _identify_chain_structure(self, industry: str) -> Dict[str, Any]:
        """第一步：联网搜索+LLM识别产业链上游/中游/下游 + 特精专新企业"""
        today = date.today()
        recent_year = today.year
        queries = [
            f"{industry} 产业链 上游 中游 下游 全景图 结构",
            f"{industry} 细分领域 核心环节 产业链拆解 {recent_year}",
            f"{industry} 专精特新 隐形冠军 小巨人 细分龙头 稀缺标的",
        ]
        results = self._do_search(queries)
        search_text = self._search_text(results)

        system_prompt = """你是一个产业研究专家。请基于搜索结果，拆解该行业的产业链结构，并判定行业生命周期阶段。

请严格按以下JSON格式输出（不要有markdown包裹）：

{
  "stage": "导入期/成长期/成熟期 三选一",
  "stage_reason": "一句话判定依据（渗透率/增速/盈利兑现程度等事实）",
  "upstream": [
    {"segment": "细分领域名", "keywords": "搜索该领域龙头用的关键词"},
    ...
  ],
  "midstream": [
    {"segment": "细分领域名", "keywords": "搜索该领域龙头用的关键词"},
    ...
  ],
  "downstream": [
    {"segment": "细分领域名", "keywords": "搜索该领域龙头用的关键词"},
    ...
  ],
  "niche_innovators": [
    {"segment": "特精专新细分领域", "keywords": "搜索该领域公司用的关键词"},
    ...
  ]
}

要求：
- stage 判定标准：导入期=渗透率低/多数公司未盈利/商业模式在验证（如商业航天、人形机器人）；
  成长期=渗透率快速提升/头部公司开始兑现利润；成熟期=格局稳定/增速回落/看份额与壁垒。
  判定必须基于搜索结果中的事实，拿不准时选"成长期"
- upstream/midstream/downstream 各 1-4 个细分领域
- niche_innovators：识别该产业链中技术壁垒极高、不可替代性强、市值未必最大但在细分领域有垄断地位的"专精特新/隐形冠军"型企业（至少1个，最多3个细分领域），如光刻胶、高纯试剂、特种气体等
- 每个细分领域要有明确的搜索关键词
- 只输出JSON，不要解释"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"请拆解「{industry}」行业的产业链结构（上游/中游/下游+细分领域+特精专新企业）：\n\n{search_text[:6000]}"),
        ]
        response = self.llm.invoke(messages)
        raw = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"产业链结构识别结果: {raw[:300]}")

        # 解析 JSON
        import json, re
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                pass

        # fallback
        return {
            "stage": DEFAULT_STAGE,
            "stage_reason": "结构识别解析失败，按成长期兜底",
            "upstream": [{"segment": f"{industry}上游原料/设备", "keywords": f"{industry} 上游 龙头"}],
            "midstream": [{"segment": f"{industry}中游制造", "keywords": f"{industry} 龙头"}],
            "downstream": [{"segment": f"{industry}下游应用", "keywords": f"{industry} 下游 龙头"}],
            "niche_innovators": [{"segment": f"{industry}专精特新隐形冠军", "keywords": f"{industry} 专精特新 隐形冠军 稀缺标的"}],
        }

    def _search_leaders_for_chain(self, industry: str, chain: Dict[str, Any]) -> Dict[str, Any]:
        """第二步：搜索每个产业链环节（含特精专新）的龙一龙二，并尝试从DB获取代码。
        东财板块成分股作为权威候选池同时喂给抽取——搜索降级时候选发现不再塌缩"""
        today = date.today()
        recent_period = f"{today.year}年{today.month}月"

        constituents_text = ""
        try:
            from tools.board_constituents import fetch_board_constituents, format_board_constituents
            constituents_text = format_board_constituents(fetch_board_constituents(industry))
        except Exception as e:
            logger.warning(f"板块成分股拉取失败（回退纯搜索发现）: {e}")

        all_leaders = {"upstream": [], "midstream": [], "downstream": [], "niche_innovators": []}

        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            segments = chain.get(level, [])
            level_leaders = []

            for seg in segments:
                seg_name = seg.get("segment", "")
                keywords = seg.get("keywords", seg_name)
                # 搜索该细分领域的主要上市公司（含二三线：弹性标的经常不在龙一龙二里）
                query = f"{keywords} 龙头公司 二线 主要上市公司 竞争格局 {recent_period}"
                try:
                    result = web_search.invoke({"query": query})
                except Exception as e:
                    logger.error(f"搜索细分领域龙头失败 [{seg_name}]: {e}")
                    result = ""

                leaders = self._extract_leaders_from_text(str(result), seg_name, level, constituents_text)

                # 保底重试：首搜零候选时换一条更直白的查询再试一次，
                # 避免整个环节（乃至整层中游/下游）静默为空、候选池塌缩
                if not leaders:
                    retry_query = f"{industry} {seg_name} 上市公司 A股 龙头 股票代码"
                    logger.info(f"[{level}/{seg_name}] 首搜零候选，重试: {retry_query[:50]}...")
                    try:
                        retry_result = web_search.invoke({"query": retry_query})
                        leaders = self._extract_leaders_from_text(str(retry_result), seg_name, level, constituents_text)
                        if leaders:
                            result = retry_result
                    except Exception as e:
                        logger.warning(f"细分领域重试搜索失败 [{seg_name}]: {e}")
                if not leaders:
                    logger.warning(f"[{level}/{seg_name}] 两次搜索均未筛出上市候选，该环节将标记为空")

                level_leaders.append({
                    "segment": seg_name,
                    "leaders": leaders,
                    "search_snippet": str(result)[:400],
                })

            all_leaders[level] = level_leaders

        return all_leaders

    def _extract_leaders_from_text(self, text: str, segment: str, level: str,
                                   constituents_text: str = "") -> List[Dict[str, Any]]:
        """从搜索结果+板块成分股清单中提取该细分领域的主要上市公司（最多4家，按地位排序）。
        搜索失败但有成分股清单时仍可抽取——候选发现不再被搜索质量卡死"""
        search_failed = not text or "搜索失败" in str(text)
        if search_failed and not constituents_text:
            return []
        search_block = "（本次搜索失败，仅依据下方板块成分股清单判断）" if search_failed else str(text)[:2500]

        # 用 LLM 提取。不只要龙一龙二：机会（弹性/低估）经常在二三线，收敛交给后面的评分，
        # 不在搜索抽取这一步就把候选面掐死
        cons_block = ""
        if constituents_text:
            cons_block = f"""
{constituents_text[:2000]}
（上方成分股清单是权威候选池：优先从中挑属于该细分领域的公司；搜索结果里提到、
清单里没有的公司也可列入，但必须是搜索结果明确提到的，禁止凭印象补）
"""

        prompt = f"""从以下资料中提取「{segment}」细分领域的主要A股上市公司，最多4家，
按行业地位从高到低排序（龙头在前，二三线也要列入）。只选主营业务确实属于该细分领域的公司。

请输出JSON数组（不要markdown包裹）：
[{{"name": "公司名", "code": "股票代码"}}, ...]

如果没有找到或无法确定，返回空数组 []。
{cons_block}
搜索结果：
{search_block}"""

        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            raw = response.content if hasattr(response, 'content') else str(response)
            import json, re
            match = re.search(r'\[.*\]', raw, re.DOTALL)
            if match:
                leaders = json.loads(match.group(0))
                validated = []
                for l in leaders:
                    name = (l.get("name") or "").strip()
                    code = (l.get("code") or "").strip()
                    # LLM 给的代码必须能反查到真实公司名，否则视为幻觉丢弃
                    if code:
                        try:
                            real_name = find_company_name(code)
                        except Exception:
                            real_name = None
                        if not real_name:
                            logger.warning(f"[{segment}] LLM 给出的代码 {code}({name}) 验证失败，尝试按名字查")
                            code = ""
                        else:
                            name = real_name
                    # 没有有效代码时，用公司名去股票基础表查
                    if not code and name:
                        try:
                            code = find_stock_code(name) or ""
                        except Exception as e:
                            logger.warning(f"按公司名查代码失败（{name}）: {e}")
                    if code:
                        validated.append({"name": name, "code": code, "rank": len(validated) + 1})
                    else:
                        logger.warning(f"[{segment}] 无法确认「{name}」的股票代码，跳过")
                return validated[:4]
        except Exception as e:
            logger.error(f"提取龙头失败: {e}")

        return []

    def _build_chain_queries(self, industry: str, chain: Dict) -> List[str]:
        """生成产业链全景+各环节龙头+资金偏好的搜索查询（含不可替代性/溢价能力维度)"""
        today = date.today()
        three_months = today - timedelta(days=90)
        recent_period = f"{today.year}年{today.month}月"
        three_month_range = f"{three_months.strftime('%Y-%m')} {today.strftime('%Y-%m')}"

        queries = [
            f"{industry} 行业 现状 景气度 市场规模 {recent_period}",
            f"{industry} 产业链 政策 利好 利空 {three_month_range}",
            f"{industry} 行业 发展趋势 投资机会 {today.year} {today.year + 1}",
            f"{industry} 资金流向 主力资金 北向资金 机构持仓 {today.year}",
            # 催化剂时间轴：机会有时间属性，招标/投产/发射/政策窗口是"什么时候涨"的依据
            f"{industry} 催化剂 招标 投产 量产 发射 时间表 {today.year}下半年 {today.year + 1}",
            # 近期重大事件：技术里程碑/政策落地/大额融资，标题往往不带行业名，单独搜
            f"{industry} 重大进展 里程碑 首次 成功 突破 {recent_period}",
            # 景气拐点与利润迁移：渗透率斜率、涨价传导、瓶颈环节=定价权所在
            f"{industry} 渗透率 行业增速 拐点 订单 排产 {recent_period}",
            f"{industry} 涨价 供需缺口 瓶颈环节 价格传导 利润分配 {recent_period}",
        ]

        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            for seg_data in chain.get(level, []):
                seg = seg_data.get("segment", "")
                for leader in seg_data.get("leaders", []):
                    name = leader.get("name", "")
                    code = leader.get("code", "")
                    tag = f"{name}({code})" if name and code else seg
                    # 经营竞争力
                    queries.append(f"{tag} {seg} 业绩 竞争力 利好 利空 {recent_period}")
                    # 业务拆解：收入毛利占比 + 出货量 + 资本开支 + 新增订单
                    queries.append(f"{tag} 业务构成 各板块收入占比 毛利占比 分业务毛利率")
                    queries.append(f"{tag} 出货量 产能 产能利用率 新增订单量 订单来源 资本开支")
                    # 不可替代性 + 护城河
                    queries.append(f"{tag} 不可替代性 护城河 技术壁垒 专利 供应链依赖 切换成本")
                    # 溢价能力 + 定价权
                    queries.append(f"{tag} 毛利率 溢价能力 议价权 定价权 品牌壁垒 成本转嫁")
                    # 资金偏好：主力/游资/散户
                    queries.append(f"{tag} 资金流向 主力资金 游资 散户 机构持仓 龙虎榜")

        return queries

    def _build_chain_system_prompt(self, stage: str = DEFAULT_STAGE, stage_reason: str = "") -> str:
        stage = normalize_stage(stage)
        w = STAGE_WEIGHTS[stage]
        gate_desc = "；".join(f"{label}≥{gate:g}分" for _, gate, label in STAGE_GATES[stage])
        return f"""你是一个顶级的产业链研究专家，擅长挖掘投资机会。你的任务是从产业链中筛选出所有关键公司（含特精专新企业），分析其基本面、边际变化、护城河、资金偏好，并输出清晰的候选公司清单（含股票代码），供下游技术分析 Agent 使用。

{INTERMEDIATE_PRODUCT_NOTE}

本行业已判定为【{stage}】{f'（{stage_reason}）' if stage_reason else ''}。
评分权重与准入门槛随阶段切换（程序执行）：本次权重为
业务{w['business']:.0%}+基本面{w['fundamental']:.0%}+护城河{w['moat']:.0%}+边际变化{w['momentum']:.0%}，
准入门槛为 {gate_desc}。

请基于下方数据完成任务：
1. 产业链结构（上游/中游/下游/特精专新 + 细分领域）及各环节主要上市公司
2. 各公司的经营基本面、业务拆解、护城河、资金流向搜索数据

请严格按照以下结构输出：

## 〇、候选公司清单与分项评分（必须放在最前面输出，供下游程序计算排名）
纯JSON格式（不要markdown包裹）：
{{
  "candidates": [
    {{"code": "股票代码", "business": 业务经营分0-10, "fundamental": 基本面分0-10,
      "moat": 不可替代与溢价分0-10, "momentum": 边际变化分0-10}},
    ...
  ],
  "reeval_triggers": [
    {{"type": "news", "trigger": "重估触发条件描述（可被新闻验证的具体事件）", "keywords": "盯梢用关键词，空格分隔"}},
    ...
  ]
}}
说明：
- 只给分项分数，**不要自行计算加权总分或排名**——综合排名由程序按上述阶段权重精确计算，
  并叠加 PE 历史分位的预期差调整，避免心算误差
- momentum（边际变化分）打分证据**只认近2个季度/近3个月的增量事实**：
  增速拐点、新订单/新客户导入、产能爬坡、毛利率环比回升、渗透率斜率变化；
  存量优势（多年积累的技术/份额）不算边际变化，那是 moat 的事
- 打分锚点（四个分项通用；准入门槛在7/6/5分档，锚点直接决定进池出池，必须校准）：
  5=行业平均/无差异化证据；6-7=有明确公开证据的相对优势；8=显著领先且证据充分；
  9-10=近乎垄断或爆发式增量；3以下=明显劣势。每个分数必须能对应写出证据，
  写不出证据就往5分靠，禁止靠印象给7分以上
- momentum 锚点举例：单季营收/订单同比+50%且已兑现=8；毛利率环比回升+新客户开始量产=7；
  仅增速企稳无新增量=5~6；增速下滑=3~4
- reeval_triggers：1-4 条"若发生XX则值得重新评估该行业"的具体条件（如"某公司砷化镓电池收入占比超20%"
  "可回收火箭完成商业化首飞"），必须可被公开新闻验证，程序会自动盯梢；没有就给空数组。
  **必须是尚未发生的前瞻事件**——已发生的大事写进「行业近况与重大事件」，
  禁止登记为触发条件（已发生事件落库后监控第一轮扫描就会全部误报命中）

## 一、产业链公司全景筛选
- 用表格列出产业链上中下游 + 特精专新共筛选出的所有公司
- 表头：环节 | 细分领域 | 排名 | 公司名称 | 代码 | 核心业务一句话 | 资金偏好

【资金偏好标签说明】
对每家公司标注其主要受哪类资金青睐（可标多个，用 / 分隔）：
- 主力：市值大、机构持仓高、北向资金持续流入、公募/社保重仓、走势稳健 — 判断依据：搜索结果中提及"机构""北向""公募""社保""重仓"
- 游资：龙虎榜常客、换手率高、题材炒作活跃、短线资金进出频繁、弹性大 — 判断依据：搜索结果中提及"游资""龙虎榜""涨停""短线"
- 散户：股东户数多、知名消费品牌、门槛低、舆论热度高 — 判断依据：搜索结果中提及"散户""股东户数""热门"

## 二、逐公司业务拆解与经营数据
对每只股票，详细列出（不足处标注「信息不足」）：
- **业务收入占比**：各业务板块/产品线的收入占比（%），列出TOP3业务
- **毛利占比**：整体毛利率，以及分业务的毛利率对比，高毛利业务是否为核心
- **出货量/产能**：主要产品的出货量、产能利用率、同比变化
- **资本开支**：最近年度/季度的资本开支规模，主要用于哪些方向（扩产/研发/并购）
- **新增订单**：近期新增订单量及同比变化，订单主要来源（国内/海外/大客户/政府）
- **技术突破**：近半年是否有重大技术突破、新产品量产、关键工艺突破、在研项目进展

## 三、逐公司基本面与竞争力评分
对每只股票基于搜索数据评分（满分 10 分）：
- 产业链地位（龙头/跟随/边缘）：0-3 分
- 近期经营表现（增速/利润/市场份额）：0-3 分
- 利好/利空倾向（偏多/中性/偏空）：0-2 分
- 行业景气度占位：0-2 分

## 四、不可替代性与溢价能力深度分析
对每只股票（满分 10 分）：
- 不可替代性（0-5 分）：核心技术是否独有？供应链是否存在单一依赖？切换成本？在产业链中是否"卡脖子"环节？
- 溢价能力（0-5 分）：毛利率水平？对上游议价能力？对下游定价权？品牌/专利/牌照壁垒？成本转嫁能力？
- 特别关注特精专新企业，它们在某些环节可能具有极高的不可替代性
- ⚠️ 准入门槛由程序按行业阶段执行（本次：{gate_desc}），未达标公司会被剔除出投资池。
  打分必须严格基于搜索证据（独有技术/专利/牌照/垄断地位的具体事实；
  边际变化则要有近2季/近3月的增量事实），禁止为照顾公司入围而抬分；
  找不到证据就给低分，宁缺毋滥
- 证据分级：打分的关键依据（如"出货量第一""市占率XX%"）若仅来自媒体报道、
  未见财报/公司公告佐证，必须在依据后标注「未经财报验证」，且不可替代性
  单项不得给满分——一个只有传闻支撑的地位撑不起满分护城河

## 五、评分依据说明
逐公司用一两句话说明四项分项分数的打分依据（业务经营/基本面/不可替代与溢价/边际变化），
**不要给出加权总分和名次**（由程序计算后附在报告末尾）

## 六、环节利润迁移判断
产业链分析最值钱的结论：利润正在向哪个环节集中、未来2-4个季度会往哪迁移。
- 当前瓶颈环节在哪（瓶颈=定价权）：供需缺口、涨价/降价的传导方向、谁在挤压谁的毛利
- 行业放量的兑现顺序：早周期（设备/材料）→ 中周期（制造）→ 晚周期（运营/服务），当前处于哪一段
- 明确给出结论：**未来2-4个季度最受益的环节是哪个、为什么**，对应环节的候选公司要点名
- 全部判断必须基于搜索证据（涨价新闻/排产数据/招标节奏），没有证据就写「证据不足，无法判断迁移方向」

## 七、催化剂时间轴（未来3-6个月）
机会有时间属性。用列表按时间先后列出可能的催化事件：
- 每条格式：预计时间 | 事件（招标/发射/投产/量产/政策落地/财报窗口）| 影响的环节或公司 | 出处
- 出处必须写具体来源（媒体名/公司公告/政府文件，尽量带日期），禁止笼统写
  「搜索数据」「搜索结果」——无法说出具体来源的事件视为依据不足，不列
- 只列搜索结果里有明确依据的事件，禁止编造时间；没有就明写「未发现明确催化剂」

## 八、行业近况与重大事件（近1-3个月已发生）
催化剂时间轴看未来，本节看已经发生的行情驱动：
- 技术里程碑（如"国产火箭回收试验成功"）、重要政策落地、大额融资/订单、事故与挫折，
  每条带日期与出处，并说明它是哪个环节的确认信号或风险信号
- 提供了【行业指数表现】时必须引用其近5/20/60日涨幅原数，与大事件对照解读
  （大事件出现后指数是否已经兑现了一波=当前介入的赔率基础）；
  指数与候选个股走势背离时必须点破（指数涨个股不涨=个股问题，反之=行业beta拉动）
- 没有重大事件就明写「近期无重大事件」，禁止拿日常新闻凑数

【重要原则】
- 候选公司清单 JSON 必须是输出的第一部分（输出过长被截断时后面的段落可丢，JSON 不能丢）
- 最后必须附「行业风险」小节：行业周期位置、政策与地缘风险、估值水位，各一两句，
  每条高影响力利好须对应检查风险（如国产替代→下游资本开支放缓风险）
- 所有结论必须基于搜索结果，不足处标注「信息不足」
- 业务拆解数据是评估公司质量和成长性的核心，请尽可能详细
- 资金偏好标签必须依据搜索数据中提及的资金类型判断，不可凭空猜测
- 特精专新企业的不可替代性是其核心竞争力，需重点分析
- 候选公司清单中的股票代码必须准确"""

    # ========== 通用工具 ==========

    def _do_search(self, queries: List[str]) -> Dict[str, str]:
        # 健康按"条"上报而非按"批"：整批只报一次时，5条查询全成功显示成裸"✓"，
        # 读起来像只搜了1条（实际误导过排查）；按条上报后显示"✓5/5"
        results = {}
        for q in queries:
            ok = True
            try:
                logger.info(f"搜索: {q[:50]}...")
                r = web_search.invoke({"query": q})
                results[q] = r
                # 工具内部兜底失败时返回"搜索失败:"前缀字符串（不抛异常），同样计入失败——
                # 否则配额耗尽时健康摘要显示"网页搜索✓"的假健康
                if isinstance(r, str) and r.startswith("搜索失败"):
                    ok = False
            except Exception as e:
                logger.error(f"搜索失败 [{q}]: {e}")
                results[q] = f"搜索失败: {e}"
                ok = False
            try:
                from tools.source_health import report_source
                report_source("网页搜索", ok, "" if ok else "部分查询失败")
            except Exception:
                pass
        return results

    def _search_text(self, results: Dict[str, str]) -> str:
        text = ""
        for q, r in results.items():
            text += f"\n{'='*40}\n【{q}】\n{r}\n"
        return text

    # ========== 统一入口 ==========

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            stock_code = state.get("stock_code", "")
            industry_name = state.get("industry_name", "")
            question = state.get("question", "")

            if industry_name and not stock_code:
                return self._analyze_industry(state)
            else:
                return self._analyze_stock(state)

        except Exception as e:
            logger.error(f"研究节点执行失败: {e}\n{traceback.format_exc()}")
            return {
                "messages": [],
                "research_result": {"summary": f"研究执行失败: {e}", "sources": []},
                "error": f"研究执行失败: {e}",
                "intermediate_steps": [("researcher", {"error": str(e)})],
            }

    def _analyze_stock(self, state: AgentState) -> Dict[str, Any]:
        stock_code = state.get("stock_code", "")
        question = state.get("question", "")
        logger.info(f"研究 Agent（个股模式），股票: {stock_code}")

        # ---- 结构化信源（主）：东财新闻 / 巨潮公告 / 财联社快讯 ----
        from tools.info_sources import (
            fetch_stock_news, fetch_stock_announcements, fetch_cls_telegraph, format_info_block)
        try:
            company_name = find_company_name(stock_code) or ""
        except Exception:
            company_name = ""
        from tools.main_business import fetch_main_business_text
        from tools.info_sources import fetch_sales_flash_text
        from tools.holder_events import fetch_holder_events_text
        structured_blocks = []
        for block in (
            fetch_sales_flash_text(stock_code),
            fetch_main_business_text(stock_code),
            fetch_holder_events_text(stock_code, company_name),
            format_info_block("巨潮公告（最近30天，重大事项第一手来源）",
                              fetch_stock_announcements(stock_code), with_content=False),
            format_info_block("东财个股新闻（最新15条）", fetch_stock_news(stock_code)),
            format_info_block("财联社快讯（含该公司的条目）",
                              fetch_cls_telegraph(keywords=[company_name] if company_name else None, limit=10)),
        ):
            if block:
                structured_blocks.append(block)
        structured_text = "\n\n".join(structured_blocks) if structured_blocks else "（结构化信源暂无数据）"

        # ---- 网页搜索（补充） ----
        queries = self._build_stock_queries(stock_code)
        all_results = self._do_search(queries)
        search_text = self._search_text(all_results)

        messages = [
            SystemMessage(content=self._build_stock_system_prompt()),
            HumanMessage(content=f"""用户问题：{question}
股票代码：{stock_code}{f'（{company_name}）' if company_name else ''}

========== 结构化信源（公告/新闻/快讯，可信度高，与搜索结果冲突时以此为准） ==========
{structured_text[:8000]}

========== 全网搜索结果（补充信息） ==========
{search_text[:10000]}
请基于以上信息进行全面分析。
销量/产销类数字的引用规则：提供了【产销快报公告原文】时**只能引用该原文的数字**并注明
"根据公司公告"；搜索结果里与之冲突的销量数字一律弃用；未提供该块时才可引用搜索结果的
销量数字，且必须注明具体出处与统计口径（乘用车/含商用车/单月/累计）。"""),
        ]

        logger.info("LLM 综合分析中...")
        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)

        # 情景推演的重估触发条件落库：以公司名为 key 复用行业触发表，
        # 公司加入监控清单后由新闻扫描自动盯梢（同一链路，无需新增机制）
        try:
            triggers = parse_company_triggers(summary)
            if triggers:
                from storage.sqlite.stock_storage import get_db
                key = company_name or stock_code
                get_db().save_industry_triggers(key, triggers)
                summary += (f"\n\n【重估触发条件（{len(triggers)}条已登记；"
                            f"发送「监控 {key}」加入监控后，新闻命中会自动推送提醒）】\n"
                            + "\n".join(f"- {t['description']}" for t in triggers))
                logger.info(f"[个股触发] {key} 登记 {len(triggers)} 条重估触发条件")
        except Exception as e:
            logger.warning(f"[个股触发] 落库失败（不影响分析）: {e}")

        return {
            "messages": [response],
            "research_result": {"summary": summary, "sources": queries},
            "intermediate_steps": [("researcher", {"mode": "stock", "stock_code": stock_code, "queries": len(queries)})],
        }

    def _analyze_industry(self, state: AgentState) -> Dict[str, Any]:
        """产业链分析：上中下游拆解 → 搜公司 → 基本面/护城河分析 → 输出候选代码供 technical_agent"""
        industry_name = state.get("industry_name", "")
        question = state.get("question", "")
        logger.info(f"研究 Agent（产业链模式），行业: {industry_name}")

        # ---- Step 1：识别产业链结构 ----
        logger.info("Step 1/3: 识别产业链上中下游+细分领域...")
        chain = self._identify_chain_structure(industry_name)
        stage = normalize_stage(chain.get("stage"))
        stage_reason = str(chain.get("stage_reason") or "")
        logger.info(f"产业链结构: upstream={len(chain.get('upstream',[]))}seg, midstream={len(chain.get('midstream',[]))}seg, downstream={len(chain.get('downstream',[]))}seg, niche={len(chain.get('niche_innovators',[]))}seg, 行业阶段={stage}")

        # ---- Step 2：搜索每个细分领域的龙一龙二 ----
        logger.info("Step 2/3: 搜索各环节龙一龙二（含特精专新）...")
        chain = self._search_leaders_for_chain(industry_name, chain)

        # 收集所有龙头代码
        all_leader_codes = set()
        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            for seg_data in chain.get(level, []):
                for leader in seg_data.get("leaders", []):
                    code = leader.get("code", "")
                    if code:
                        all_leader_codes.add(code)

        # ---- Step 3：全景搜索 + LLM 分析 ----
        logger.info("Step 3/3: 全景搜索 + LLM 产业链分析（基本面/护城河/资金偏好）...")

        # 拼产业链结构+龙头
        chain_summary = ""
        level_labels = [("upstream", "上游"), ("midstream", "中游"), ("downstream", "下游"), ("niche_innovators", "特精专新")]
        for level, label in level_labels:
            chain_summary += f"\n## {label}\n"
            for seg_data in chain.get(level, []):
                seg = seg_data.get("segment", "")
                chain_summary += f"\n### {seg}\n"
                for l in seg_data.get("leaders", []):
                    chain_summary += f"  龙{l.get('rank','?')}: {l.get('name','')} ({l.get('code','')})\n"
                snippet = seg_data.get("search_snippet", "")[:200]
                if snippet:
                    chain_summary += f"  搜索摘要: {snippet}\n"

        # 行业相关的财联社快讯（结构化信源，含政策面）。
        # 关键词扩展到各环节名：行业大事往往不带行业名（"XX火箭完成回收试验"这类
        # 标题里没有"商业航天"），只用行业名过滤会整体漏掉里程碑事件
        from tools.info_sources import fetch_cls_telegraph, format_info_block
        segment_kws = []
        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            for seg_data in chain.get(level, []):
                seg = str(seg_data.get("segment") or "").strip()
                if seg and seg not in segment_kws:
                    segment_kws.append(seg)
        cls_block = format_info_block(
            "财联社快讯（含该行业/各环节关键词的条目，政策与重大事件第一手来源）",
            fetch_cls_telegraph(keywords=[industry_name] + segment_kws[:10], limit=15))

        # 行业指数表现（东财概念/行业板块）：行业 beta 的权威事实，
        # 候选池只有两三家时拿池子代理行业会失真，指数不会
        index_block = ""
        industry_index = None
        try:
            from tools.industry_index import fetch_industry_index_metrics, format_industry_index
            industry_index = fetch_industry_index_metrics(industry_name)
            index_block = format_industry_index(industry_index)
        except Exception as e:
            logger.warning(f"行业指数获取失败（不影响分析主流程）: {e}")

        # 行业估值与位置（程序计算，用龙头池做行业代理样本）——回调风险分析的量化锚
        from tools.industry_metrics import collect_industry_valuation, format_industry_valuation
        industry_valuation = None
        try:
            if all_leader_codes:
                logger.info(f"计算行业估值与位置（样本 {len(all_leader_codes)} 只）...")
                industry_valuation = collect_industry_valuation(sorted(all_leader_codes))
        except Exception as e:
            logger.warning(f"行业估值计算失败（不影响分析主流程）: {e}")
        valuation_block = format_industry_valuation(industry_valuation)

        # 候选公司主营构成（程序拉取）：当前利润驱动的数字底座，
        # 没有这块 LLM 只能写"未提供主营构成数据"
        mb_text = ""
        try:
            from tools.main_business import fetch_main_business_text
            mb_blocks = []
            for c in sorted(all_leader_codes)[:10]:
                t = fetch_main_business_text(c)
                if t:
                    mb_blocks.append(f"◇ 候选 {c}\n{t}")
            mb_text = "\n\n".join(mb_blocks)
        except Exception as e:
            logger.warning(f"候选主营构成拉取失败（不影响分析）: {e}")

        # 全景搜索（含不可替代性/溢价能力维度）
        all_queries = self._build_chain_queries(industry_name, chain)
        all_results = self._do_search(all_queries)
        search_text = self._search_text(all_results)

        messages = [
            SystemMessage(content=self._build_chain_system_prompt(stage, stage_reason)),
            HumanMessage(content=f"""用户问题：{question}
目标行业：{industry_name}（行业阶段：{stage}{f'，{stage_reason}' if stage_reason else ''}）

{cls_block if cls_block else ''}

{index_block if index_block else ''}

{valuation_block if valuation_block else ''}

========== 产业链结构（上中下游+特精专新+细分领域+龙一龙二） ==========
{chain_summary}

========== 候选公司主营业务构成（程序拉取，各业务收入/利润占比与毛利率，当前利润驱动依据） ==========
{mb_text[:8000] if mb_text else '（未获取到，业务拆解只能依据搜索结果，缺数据处标注「信息不足」）'}

========== 全网搜索结果（含经营/竞争力/业务拆解/收入毛利占比/出货量/资本开支/新增订单/技术突破/护城河） ==========
{search_text[:15000]}

请按结构输出：〇、JSON 候选清单+分项评分+重估触发条件（放最前）→ 一、全景筛选
→ 二、业务拆解与经营数据（优先引用主营构成数据的占比/毛利率原数）→ 三、基本面评分
→ 四、护城河分析（重点关注特精专新）→ 五、评分依据说明
→ 六、环节利润迁移判断 → 七、催化剂时间轴 → 八、行业近况与重大事件。
资金偏好只在搜索结果里有北向/龙虎榜/机构调研等公开证据时标注并写明出处，
无证据写「无公开数据」，禁止凭板块印象填"主力/游资"。"""),
        ]

        logger.info("LLM 产业链分析中...")
        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"产业链分析完成，长度: {len(summary)}")

        # 从 LLM 输出中提取候选与分项评分，综合排名由程序计算。
        # JSON 里除 candidates 外还有 reeval_triggers，正则截到首个 ] 会解析失败，
        # 改用 raw_decode 从 {"candidates" 起始处解析完整对象
        import json, re
        candidate_codes = list(all_leader_codes)  # fallback：龙头搜索阶段已验证过的代码
        ranked = []
        gate_excluded = []
        extracted = None
        start_match = re.search(r'\{\s*"candidates"', summary)
        if start_match:
            try:
                extracted, _ = json.JSONDecoder().raw_decode(summary[start_match.start():])
            except json.JSONDecodeError:
                extracted = None
        if extracted:
            ranked = compute_composite_ranking(extracted.get("candidates") or [], stage)
            if ranked:
                # 阶段化硬门槛：未达标的直接剔除出投资池，不进排名与技术对比
                ranked, gate_excluded = apply_stage_gate(ranked, stage)
                # 预期差调整：评分与PE历史分位见面，高分位减分/低分位加分，标注机会/拥挤象限
                per_stock = (industry_valuation or {}).get("per_stock") or []
                ranked = apply_valuation_adjustment(ranked, per_stock)
                candidate_codes = [item["code"] for item in ranked]
                logger.info(f"程序计算综合排名完成（阶段={stage}，门槛剔除 {len(gate_excluded)} 家）: "
                            f"{[(i['code'], i.get('composite_adj', i['composite'])) for i in ranked]}")
        if not ranked and not extracted:
            # 兼容旧格式 candidate_codes（无分项分数时不产生排名）
            old_match = re.search(r'\{[^{}]*"candidate_codes"[^{}]*\}', summary, re.DOTALL)
            if old_match:
                try:
                    old_extracted = json.loads(old_match.group(0))
                    if old_extracted.get("candidate_codes"):
                        candidate_codes = old_extracted["candidate_codes"]
                except json.JSONDecodeError:
                    pass

        # 程序算的排名表附到报告末尾（responder 展示与复盘留档都以此为准）
        def _safe_name(c):
            try:
                return find_company_name(c)
            except Exception:
                return None

        gate_desc = "；".join(f"{label}≥{g:g}分" for _, g, label in STAGE_GATES[stage])
        if ranked:
            summary = summary + "\n\n" + format_ranking_table(ranked, name_of=_safe_name)
        if gate_excluded:
            ex_lines = [f"【阶段门槛剔除（行业阶段：{stage}，门槛：{gate_desc}；不入投资池，转入观察池，仅列示供了解全貌）】"]
            for it in gate_excluded:
                nm = _safe_name(it["code"])
                label = f"{nm}({it['code']})" if nm else it["code"]
                ex_lines.append(f"- {label}：{it['exclude_reason']}")
            summary = summary + "\n\n" + "\n".join(ex_lines)
        if not ranked and gate_excluded:
            summary += (f"\n\n⚠️ 本次全部候选均未达【{stage}】阶段准入门槛，无投资池标的——以下内容仅作行业观察，"
                        "不给介入建议；被剔除公司转入观察池，重估触发条件命中后系统会自动提醒重新评估")

        # 重估触发条件：LLM 给的 news 型 + 程序自动补的估值型，落库并加入监控清单，
        # 让"不参与"从死结论变成"暂不参与 + 自动盯"
        reeval_triggers = []
        if extracted:
            for t in (extracted.get("reeval_triggers") or [])[:4]:
                desc = str(t.get("trigger") or "").strip()
                if desc:
                    reeval_triggers.append({
                        "trigger_type": "news",
                        "description": desc[:300],
                        "keywords": str(t.get("keywords") or "")[:200],
                    })
        pe_pct = (industry_valuation or {}).get("pe_percentile_median")
        if pe_pct is not None and pe_pct >= 60:
            reeval_triggers.append({
                "trigger_type": "valuation",
                "description": f"候选池PE(TTM)历史分位中位数从{pe_pct}%回落至50%以下",
                "pe_percentile_below": 50.0,
                "pool_codes": sorted(all_leader_codes),
            })
        if reeval_triggers:
            try:
                from storage.sqlite.stock_storage import get_db
                _db = get_db()
                _db.save_industry_triggers(industry_name, reeval_triggers)
                kw = " ".join(t.get("keywords") or "" for t in reeval_triggers).strip()
                _db.add_watch_target(name=industry_name, target_type="industry",
                                     keywords=kw[:200] if kw else None, source="auto")
                trig_lines = ["【重估触发条件（已落库，监控任务自动盯梢，命中会推送提醒）】"]
                trig_lines += [f"- [{t['trigger_type']}] {t['description']}" for t in reeval_triggers]
                summary = summary + "\n\n" + "\n".join(trig_lines)
                logger.info(f"重估触发条件已保存 {len(reeval_triggers)} 条并加入监控清单: {industry_name}")
            except Exception as e:
                logger.warning(f"重估触发条件保存失败（不影响分析）: {e}")

        # 出口统一验证：只把能在股票基础表反查到的代码交给 technical_agent
        verified_codes = []
        for code in candidate_codes:
            code = str(code).strip()
            try:
                if find_company_name(code):
                    verified_codes.append(code)
                else:
                    logger.warning(f"候选代码 {code} 验证失败，剔除")
            except Exception:
                # 验证器不可用（如无 tushare token）时保留原代码，不让整条链断掉
                verified_codes.append(code)
        logger.info(f"候选代码验证: {len(candidate_codes)} -> {len(verified_codes)}")

        return {
            "messages": [response],
            "research_result": {"summary": summary, "sources": all_queries,
                                "industry_valuation": industry_valuation,
                                "industry_index": industry_index,
                                "industry_stage": stage,
                                # 门槛剔除组：留档后与进池组对照，用事后收益验证门槛有效性
                                "gate_excluded_codes": [it["code"] for it in gate_excluded]},
            "chain_leaders": chain,
            "stock_code": ",".join(verified_codes) if verified_codes else "",
            "intermediate_steps": [("researcher", {"mode": "chain", "industry": industry_name, "segments": sum(len(chain.get(k,[])) for k in ["upstream","midstream","downstream","niche_innovators"]), "candidates": len(verified_codes), "queries": len(all_queries)})],
        }

    def invoke(self, state: AgentState) -> Dict[str, Any]:
        return self.analyze_node(state)


def create_researcher_node():
    """创建研究节点（直通流程，支持个股/产业链双模式；技术面由下游 technical_agent 负责）"""
    agent = ResearcherAgent()
    return agent.analyze_node

