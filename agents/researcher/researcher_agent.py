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
from concurrent.futures import as_completed
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from agents.prompts_common import INTERMEDIATE_PRODUCT_NOTE
from core.llm import get_agent_llm
from .web_search_tool import web_search
from tools.company_code_validator import find_stock_code, find_company_name
from tools.weight_adjuster import get_stage_weights
from utils.constants import IntentType
from utils.logger import logger


# 产业链覆盖度 CheckList（针对人形机器人赛道，其他赛道可复用此结构）
# Key: 环节名, Value: (环节描述, 推荐补充标的列表)
# 报告候选池必须覆盖 ≥10/12 个环节，否则触发"覆盖度不足"警告并自动补充搜索
HUMANOID_CHAIN_CHECKLIST: Dict[str, tuple[str, list[str]]] = {
    "谐波减速器": ("减速器轻负载关节", ["绿的谐波"]),
    "RV减速器": ("减速器重负载关节", ["双环传动"]),
    "行星滚柱丝杠": ("线性传动/最大卡脖子环节", ["五洲新春", "双林股份", "恒立液压"]),
    "无框力矩电机": ("关节驱动电机", []),
    "空心杯电机": ("灵巧手微型驱动", ["鸣志电器"]),
    "伺服驱动器": ("伺服控制+编码器", []),
    "控制器": ("运动控制/轨迹规划芯片", []),
    "传感器": ("力矩/触觉/视觉传感", []),
    "灵巧手总成": ("集成手指关节执行器", ["兆威机电"]),
    "执行器总成": ("关节模组集成", ["三花智控", "拓普集团"]),
    "本体制造": ("整机集成", []),
    "系统集成": ("产线/场景部署", []),
}
# 别名映射：已知的产业链段名到标准环节名的模糊匹配
_CHAIN_SEGMENT_ALIASES: Dict[str, str] = {
    "减速器": "谐波减速器", "谐波减速器": "谐波减速器",
    "RV减速器": "RV减速器", "RV": "RV减速器",
    "丝杠": "行星滚柱丝杠", "行星滚柱丝杠": "行星滚柱丝杠", "滚柱丝杠": "行星滚柱丝杠",
    "电机": "无框力矩电机", "无框力矩电机": "无框力矩电机",
    "空心杯电机": "空心杯电机", "微型电机": "空心杯电机",
    "伺服": "伺服驱动器", "伺服驱动": "伺服驱动器", "驱动器": "伺服驱动器",
    "控制器": "控制器", "控制": "控制器",
    "传感器": "传感器", "传感": "传感器",
    "灵巧手": "灵巧手总成", "灵巧手总成": "灵巧手总成",
    "执行器总成": "执行器总成", "执行器": "执行器总成",
    "本体": "本体制造", "整机": "本体制造", "本体制造": "本体制造",
    "系统集成": "系统集成", "集成": "系统集成",
}


# 阶段化评分框架：机会=边际变化，护城河是存量质量——不同行业生命周期两者的权重完全不同。
# 用成熟行业的护城河框架去筛导入期行业（如商业航天），结论永远是"不参与"，等于对成长机会失明。
# momentum（边际变化）分项：只认近2个季度/近3个月的增量事实（增速拐点/新订单/产能爬坡/毛利率环比回升）。
# 权重值优先读取 config 中 weights.stage_weights 段（复盘自动调权后写入，热更新即时生效），
# 缺失时回退到以下硬编码默认值。
_STAGE_WEIGHTS_DEFAULT = {
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
    return stage if stage in get_stage_weights() else DEFAULT_STAGE


def apply_stage_gate(ranked: List[Dict[str, Any]], stage: str):
    """
    阶段化硬门槛过滤（纯函数）：返回 (passed, watch, excluded)。
    passed = 全部达标；watch = 距门槛差 ≤0.5 分，入观察备选池；
    excluded = 距门槛差 >0.5 分，彻底剔除。
    passed 重新编排名。
    """
    stage = normalize_stage(stage)
    gates = STAGE_GATES[stage]
    passed, watch, excluded = [], [], []
    for item in ranked:
        missing = item.get("missing") or []
        fails = []
        is_watch = True
        for metric, gate, label in gates:
            if metric in missing:
                fails.append(f"{label}分项缺失（无证据）")
                is_watch = False
            elif item.get(metric, 0) < gate:
                gap = gate - item.get(metric, 0)
                if gap > 0.5:
                    is_watch = False
                fails.append(f"{label}{item.get(metric)}分未达{gate:g}分门槛（差{gap:.1f}分）")
        if not fails:
            passed.append(dict(item))
        elif is_watch:
            ex = dict(item)
            ex["exclude_reason"] = f"[{stage}] 观察备选：" + "；".join(fails)
            watch.append(ex)
        else:
            ex = dict(item)
            ex["exclude_reason"] = f"[{stage}] " + "；".join(fails)
            excluded.append(ex)
    for i, it in enumerate(passed, 1):
        it["rank"] = i
    return passed, watch, excluded


def compute_composite_ranking(candidates: List[Dict[str, Any]], stage: str = DEFAULT_STAGE) -> List[Dict[str, Any]]:
    """
    程序计算综合评分与排名（LLM 只提供分项分数，加权与排序不交给它心算）。
    权重按行业阶段切换；分项缺失/非法时按 5.0 中性分处理并标注。
    返回按综合分降序的列表：[{code, business, fundamental, moat, momentum, composite, rank, note}]
    """
    stage = normalize_stage(stage)
    weights = get_stage_weights()[stage]
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
        name = (c.get("name") or "").strip()
        ranked.append({
            "code": code, "name": name, **scores, "composite": composite, "stage": stage, "missing": missing,
            "note": "分项缺失按5分中性处理" if missing else "",
        })
    ranked.sort(key=lambda x: x["composite"], reverse=True)
    for i, item in enumerate(ranked, 1):
        item["rank"] = i
    return ranked


def apply_valuation_adjustment(ranked: List[Dict[str, Any]],
                               per_stock: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    预期差调整 + 质量折扣（纯函数）：
    1. 基本面评分和估值分位必须见面——综合分9但PE分位95%不该排第一
    2. 质量指标否决权：ROE过低/扣非占比过低/商誉过高 → 质量折扣
    对高历史分位减分、低分位加分，并标注机会/拥挤象限；按调整后综合分重排。
    per_stock 来自 industry_metrics 的逐股程序数据（pe_percentile/total_mv/mf_net20/roe等）。
    拥挤度判定优先级：绝对PE > 分位（10年窗口统一口径）。
    调整阈值从 config weights.pe_adjustments 读取（复盘/校准可动态微调），不写死。
    """
    try:
        from tools.weight_adjuster import get_pe_adjustments
        pe_adj = get_pe_adjustments()
    except Exception:
        pe_adj = {}
    pe_gt_200 = pe_adj.get("pe_gt_200", -1.5)
    pe_gt_100 = pe_adj.get("pe_gt_100", -1.0)
    pct_ge_80 = pe_adj.get("pct_ge_80", -1.0)
    pct_ge_60 = pe_adj.get("pct_ge_60", -0.5)
    pct_le_30 = pe_adj.get("pct_le_30", 0.5)
    dual_high = pe_adj.get("dual_high", -1.5)

    metrics_map = {str(r.get("code", "")): r for r in per_stock or []}
    for item in ranked:
        m = metrics_map.get(item["code"]) or {}
        pct = m.get("pe_percentile")
        item["pe_percentile"] = pct
        item["pe_ttm"] = m.get("pe_ttm")
        item["total_mv"] = m.get("total_mv")
        item["mf_net20"] = m.get("mf_net20")
        # 多窗口分位（供报告展示，说明口径）
        item["pe_pct_3y"] = m.get("pe_pct_3y")
        item["pe_pct_5y"] = m.get("pe_pct_5y")
        item["pe_pct_window"] = m.get("pe_pct_window") or "10年"
        pe_abs = m.get("pe_ttm")
        adj = 0.0

        # 估值调整：绝对PE优先，其次分位（阈值来自 config，可动态微调）
        if pe_abs is not None and pe_abs > 200:
            adj = pe_gt_200  # 极端泡沫
        elif pe_abs is not None and pe_abs > 100:
            adj = pe_gt_100  # 高PE
        elif pct is not None:
            if pct >= 80:
                adj = pct_ge_80  # 高分位
            elif pct >= 60:
                adj = pct_ge_60  # 中高分位
            elif pct <= 30:
                adj = pct_le_30  # 低分位
        # 分位+绝对PE双高时额外扣分
        if pct is not None and pe_abs is not None and pct >= 80 and pe_abs > 100:
            adj = min(adj, dual_high)

        # === 质量指标否决权 ===
        quality_discount = 1.0
        quality_notes = []
        # 1. ROE 过低
        roe = m.get("roe")
        if roe is not None and isinstance(roe, (int, float)):
            item["roe"] = round(roe, 2)
            if roe < 5.0:
                quality_discount *= 0.85
                quality_notes.append(f"ROE仅{roe:.1f}%（<5%）")
            elif roe < 10.0:
                quality_notes.append(f"ROE{roe:.1f}%（偏低）")
        else:
            item["roe"] = None
        # 2. 扣非占比过低（如数据可用）
        deduct_ratio = m.get("deduct_net_ratio")
        if deduct_ratio is not None and isinstance(deduct_ratio, (int, float)):
            item["deduct_net_ratio"] = round(deduct_ratio, 1)
            if deduct_ratio < 30.0:
                quality_discount *= 0.7
                quality_notes.append(f"扣非/归母仅{deduct_ratio:.1f}%（<30%）")
        # 3. 商誉占比过高（如数据可用）
        goodwill_ratio = m.get("goodwill_ratio")
        if goodwill_ratio is not None and isinstance(goodwill_ratio, (int, float)):
            item["goodwill_ratio"] = round(goodwill_ratio, 1)
            if goodwill_ratio > 25.0:
                quality_discount *= 0.85
                quality_notes.append(f"商誉/净资产{goodwill_ratio:.1f}%（>25%）")

        # 应用质量折扣
        if quality_discount < 1.0:
            quality_adj = round(item["composite"] * (quality_discount - 1), 2)
            adj += quality_adj
            item["quality_discount"] = quality_discount
            item["quality_notes"] = "；".join(quality_notes)
        else:
            item["quality_discount"] = 1.0
            item["quality_notes"] = ""

        item["valuation_adj"] = adj
        item["composite_adj"] = round(item["composite"] + adj, 2)

        # 拥挤度标签（优先级：极端泡沫 > 极度拥挤 > 拥挤 > 机会 > 中性）
        if pe_abs is not None and pe_abs > 200:
            item["quadrant"] = "🔴极端泡沫(PE>200倍)"
        elif pe_abs is not None and pe_abs > 100 and (pct is None or pct >= 70):
            item["quadrant"] = "🔴极度拥挤"
        elif (pct is not None and pct >= 80) or (pe_abs is not None and pe_abs > 100):
            item["quadrant"] = "🟠拥挤"
        elif pct is not None and pct <= 30 and (pe_abs is None or pe_abs <= 50):
            if item["composite"] >= 7:
                item["quadrant"] = "🟢机会区"
            else:
                item["quadrant"] = "🟢低估"
        elif pct is None and pe_abs is None:
            item["quadrant"] = "无估值数据"
        else:
            item["quadrant"] = "🟡中性"

    ranked.sort(key=lambda x: x.get("composite_adj", x["composite"]), reverse=True)
    for i, item in enumerate(ranked, 1):
        item["rank"] = i
    return ranked


def format_ranking_table(ranked: List[Dict[str, Any]], name_of=None) -> str:
    """排名表文本（附在报告末尾，供 responder 展示与复盘留档抽取）"""
    if not ranked:
        return ""
    stage = ranked[0].get("stage", DEFAULT_STAGE)
    w = get_stage_weights()[normalize_stage(stage)]
    # 分位窗口口径（取第一只有效数据的窗口）
    pct_window = ""
    for it in ranked:
        if it.get("pe_pct_window"):
            pct_window = it["pe_pct_window"]
            break
    pct_label = f"（{pct_window}窗口）" if pct_window else ""
    lines = [f"【综合排名（行业阶段：{stage}；程序按 业务{w['business']:.0%}+基本面{w['fundamental']:.0%}"
             f"+护城河{w['moat']:.0%}+边际变化{w['momentum']:.0%} 加权，再按PE历史分位{pct_label}做预期差调整）】"]

    # ===== 硬伤检测：核心字段为 None 或 5.0 中性兜底时触发警告 =====
    _score_warnings = []
    for item in ranked:
        missing_fields = []
        for k in ("business", "fundamental", "moat", "momentum"):
            v = item.get(k)
            if v is None:
                missing_fields.append(k)
            elif k in item.get("missing", []) and v == 5.0:
                missing_fields.append(f"{k}(5分中性)")  # LLM没给出分数，程序兜底
        if missing_fields:
            _score_warnings.append(f"{item.get('code','?')} 缺少：{', '.join(missing_fields)}")
        pe_ttm = item.get("pe_ttm")
        if pe_ttm is None:
            _score_warnings.append(f"{item.get('code','?')} PE数据缺失")
    if _score_warnings:
        lines.append(f"⚠️ 打分数据警告（{len(_score_warnings)}项）：{'；'.join(_score_warnings)}")
        lines.append("  → 上述标的关键评分数据缺失，排名仅供参考，建议人工复核基本面后再做决策。")

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
        pe_ttm = item.get("pe_ttm")
        pct = item.get("pe_percentile")
        if pe_ttm is not None and pct is not None:
            # 双阈值预警标签（用户要求：绝对值优先于分位）
            if pe_ttm > 200:
                pe_tag = "🔴极端泡沫(PE>200倍)"
            elif pe_ttm > 100:
                pe_tag = "🟠极高PE(100-200倍)"
            elif pct >= 80 and pe_ttm > 50:
                pe_tag = "🔴高估(分位>80%且PE>50倍)"
            else:
                pe_tag = "🟢合理PE"
            extra.append(f"PE{pe_ttm:.1f}({pct:.0f}%{pct_window}分位){pe_tag}")
        elif pe_ttm is not None:
            extra.append(f"PE{pe_ttm:.1f}")
        if item.get("composite_adj") is not None:
            extra.append(f"调整后{item['composite_adj']}"
                         + (f"（{item.get('valuation_adj'):+g}）" if item.get('valuation_adj') is not None else ""))
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
        self._llm_timeout = 180  # LLM调用单次超时秒数

    def _llm_invoke_with_timeout(self, messages, timeout=None):
        """带超时的 LLM.invoke 封装，防止 LLM API 挂死后进程卡死"""
        import threading
        timeout = timeout or self._llm_timeout
        container, errors = [], []
        def _invoke():
            try:
                container.append(self.llm.invoke(messages))
            except Exception as e:
                errors.append(e)
        t = threading.Thread(target=_invoke, daemon=True)
        t.start()
        t.join(timeout=timeout)
        if t.is_alive():
            logger.warning(f"LLM 调用超时 ({timeout}s)，返回空结果")
            return None
        if errors:
            raise errors[0]
        return container[0] if container else None

    @staticmethod
    def _format_review_lesson(stock_code: str) -> str:
        """注入该标的最近一次复盘的误判模式和相关改进规则（公共函数代理）"""
        from agents.prompts_common import format_review_lesson
        return format_review_lesson(stock_code)

    # ========== 结构化数据提取（通用） ==========

    def _extract_structured_data_from_search(self, search_text: str, company_name: str = "",
                                              industry: str = "") -> Dict[str, Any]:
        """
        从搜索结果中提取结构化数据（销量/出货量/营收/毛利率/产能/订单等），
        作为结构化信源补充喂给主分析 LLM，减少 LLM 自己从大段文本找数字的幻觉风险。
        返回: {"tables": [...], "figures": [...], "time_series": [...]}
        """
        if not search_text or len(search_text) < 100:
            return {"tables": [], "figures": [], "time_series": []}

        context = company_name or industry or "该公司"
        prompt = f"""从以下搜索结果中，提取关于「{context}」的所有结构化经营数据。

请按 JSON 格式输出（不要markdown包裹）：
{{
  "time_series": [
    {{"metric": "指标名如'月销量'", "period": "2025-06", "value": 12345, "unit": "辆",
      "note": "数据来源/口径说明"}},
    ...
  ],
  "key_figures": [
    {{"metric": "指标名", "value": "具体数值", "unit": "单位",
      "note": "时间点/范围/来源"}},
    ...
  ]
}}

规则：
- time_series：按月/按季度的时序数据，用于判断趋势。每条一条时间点。
  如多个车型的销量，每个车型各一条。
- key_figures：非时序的关键数字（如"产能利用率85%""市占率30%"）。
- 只提取搜索结果中明确提到的数字，禁止用自己的知识补充。
- 单位必须写明（万辆/亿元/吨/千瓦时等）。
- note 要注明数据来源（如"公司公告""乘联会""懂车帝销量榜"）和口径。
- 没找到结构化数据的返回 {{"time_series": [], "key_figures": []}}

搜索结果：
{search_text[:5000]}"""

        try:
            import json, re
            response = self._llm_invoke_with_timeout([HumanMessage(content=prompt)], timeout=120)
            if response is None:
                logger.warning("结构化数据提取 LLM 超时，跳过")
                return {"tables": [], "figures": [], "time_series": []}
            raw = response.content if hasattr(response, 'content') else str(response)
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                result = json.loads(match.group(0))
                result.setdefault("time_series", [])
                result.setdefault("key_figures", [])
                logger.info(f"结构化数据提取: {len(result['time_series'])}条时序 + {len(result['key_figures'])}条关键数字")
                return result
        except Exception as e:
            logger.debug(f"结构化数据提取失败（不影响主流程）: {e}")

        return {"tables": [], "figures": [], "time_series": []}

    def _format_structured_data_block(self, structured: Dict[str, Any]) -> str:
        """将结构化数据格式化为 LLM 易读的文本块"""
        lines = []
        ts = structured.get("time_series") or []
        kf = structured.get("key_figures") or []
        if ts:
            lines.append("【结构化时序数据（程序提取，可信度高于 LLM 自行从搜索结果中找数字）】")
            # 按指标分组
            by_metric = {}
            for item in ts:
                m = item.get("metric", "其他")
                by_metric.setdefault(m, []).append(item)
            for metric, items in sorted(by_metric.items()):
                items.sort(key=lambda x: str(x.get("period", "")))
                lines.append(f"\n## {metric}")
                for it in items:
                    note = f"（{it['note']}）" if it.get("note") else ""
                    lines.append(f"  {it.get('period','')}: {it.get('value','')} {it.get('unit','')}{note}")
        if kf:
            lines.append("\n## 关键数字")
            for it in kf:
                note = f"（{it['note']}）" if it.get("note") else ""
                lines.append(f"  {it.get('metric','')}: {it.get('value','')} {it.get('unit','')}{note}")
        return "\n".join(lines)

    def _search_with_fallback(self, query: str, max_retries: int = 2,
                               per_query_timeout: int = 60) -> str:
        """搜索+降级重试 + 超时保护。

        单条query超过 per_query_timeout 秒自动放弃，不让慢引擎（如DuckDuckGo）
        拖垮整批并行搜索。超时后直接返回失败，不再重试。
        注意：不能在内层再包 ThreadPoolExecutor——嵌套池的超时只中断等待不中断任务，
        会导致外层 worker 被僵尸线程占满，8个worker全堵死后整批搜索瘫痪。
        """
        import threading
        for attempt in range(max_retries):
            try:
                logger.info(f"搜索: {query[:60]}...")
                result_container = []
                error_container = []
                def _do_search():
                    try:
                        r = web_search.invoke({"query": query})
                        result_container.append(r)
                    except Exception as e:
                        error_container.append(e)
                t = threading.Thread(target=_do_search, daemon=True)
                t.start()
                t.join(timeout=per_query_timeout)
                if t.is_alive():
                    logger.warning(f"搜索超时({per_query_timeout}s, attempt {attempt+1}): {query[:40]}")
                    return f"搜索失败: 超时{per_query_timeout}s"
                if error_container:
                    raise error_container[0]
                result = result_container[0] if result_container else ""
                if isinstance(result, str):
                    # 工具兜底失败返回"搜索失败:"前缀
                    if result.startswith("搜索失败"):
                        logger.warning(f"搜索失败(attempt {attempt+1}): {query[:40]}")
                        # 最后一次失败不再重试
                        if attempt == max_retries - 1:
                            return result
                        # 换表述重试：去掉一些限定词
                        query = query.replace(" 当月 ", " ").replace(" 最近 ", " ").replace(" 同比 环比", "")
                        continue
                    # 结果太短（< 50字）也重试
                    if len(result.strip()) < 50:
                        logger.warning(f"搜索结果过短({len(result)}字)，重试(attempt {attempt+1})")
                        continue
                    return result
                return str(result)
            except Exception as e:
                logger.error(f"搜索异常 [{query[:40]}](attempt {attempt+1}): {e}")
                if attempt == max_retries - 1:
                    return f"搜索失败: {e}"
        return "搜索失败: 多次重试后仍失败"

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

        # 从15条缩减到10条：合并主题相近的query（技术+护城河、竞争+产业链），
        # 减少搜索引擎降级链压力（每条query都要跑一轮引擎链）
        return [
            # 基础面（4条）
            f"{tag} 重大事项 公司公告 {one_month.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",
            f"{tag} 最新经营业绩 营收 利润 {recent_period}",
            f"{tag} 业务构成 收入占比 毛利拆分 各板块营收",
            f"{tag} 所属行业 产业政策 发展趋势 {three_month_range}",

            # 运营数据（2条）
            f"{tag} {last_month} 月度产销数据 销量 出货量 交付量 同比",
            f"{tag} 产能 产能利用率 新增订单 资本开支 {recent_period}",

            # 技术与竞争（2条）
            f"{tag} 技术实力 研发投入 核心竞争力 护城河 专利壁垒",
            f"{tag} 竞争对手 市场份额 产业链地位 竞争格局 {recent_period}",

            # 增长与预期（2条）
            f"{tag} 第二增长曲线 新业务 进展 放量 {recent_period}",
            f"{tag} 利好 利空 机构评级 目标价 {one_month.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",

            # 知乎补源（T3，但机构盈利预测拆解/出海毛利率/政策解读比泛搜索更有深度）
            f"site:zhihu.com {tag} 出海 毛利率 单车利润 本土 vs 出口 对比",
            f"site:zhihu.com {tag} 2026 盈利预测 券商 研报 东吴 花旗 中信 目标价",

            # 工信部/政策专项搜索（梯次利用、电池/汽车监管）
            f"工信部 2026 公告 梯次利用 电池 新能源汽车 白名单 移出 第20号",
            f"{tag} 相关 工信部 监管政策 行业规范 最新通知 {three_month_range}",
        ]

    def _build_stock_system_prompt(self) -> str:
        today = date.today().strftime("%Y-%m-%d")
        return f"""你是一个专业的股票信息研究员和分析师。今天的日期是 {today}，请以此为时间基准判断"近期/最新"。

{INTERMEDIATE_PRODUCT_NOTE}

【信源优先级】🟢 T1 权威（公告/财报/认证官方社交）> 🔵 T2 结构化（财经媒体）> 🟡 T3 未验证社交 > ⚪ T4 网络搜索
- 数据不一致时以高等级为准并标注差异；以下数据块前的等级标签代表其信誉级别

【禁止心算规则（最重要，放在最前）】
- 所有财务指标（净利率/毛利率同比pct/环比pct/期间费用率/净现比/FCF/OCF同比等）
  **必须直接引用【财务关键指标快照】区块中的原始数字**，严禁自己从三表文本块里
  找字段做除法/减法/环比再计算——实测 LLM 心算会把净利率 2.67% 写成 2.72%、
  FCF 算错一倍、期间费用率漏掉财务费用。快照里有就直接抄；快照里没有才引用
  三表文本块格式化后的精确行（必须标注"引用三表原始行"）。
- FCF（自由现金流）必须直接引用【财务关键指标快照】中的程序计算值，
  格式如 "FCF -504.97亿（程序计算=OCF - 资本开支）"，严禁自己 OCF 减 capex。

请基于下方搜索结果，对该公司的以下维度进行客观分析并给出核心结论：

1. **公司公告与重大事项**：近期是否有重大公告及影响

2. **产业信息**：行业景气度、政策、趋势；结合【同业横向对标】数据块中的行业整体估值和财务数据

3. **业务拆解**：各板块收入/毛利占比、TOP3业务、出货量、产能利用率、资本开支、新增订单及来源；
   销量/出货量必须给近几个月的同比或环比序列并注明是单月还是累计，
   只有单月数据时不得外推为趋势
   **海外业务（如有）必须拆分毛利率差异来源**：车型结构差异、关税/运费影响、
   定价策略差异、渠道费用差异等，分析海外毛利率高于/低于国内的原因及可持续性
   **高端品牌/高毛利业务（如有）必须披露其单车均价、单独毛利率、营收占比**，
   量化高端化对公司综合毛利率的拉动空间

4. **财报空窗期前瞻**：最新财报报告期之后已公布的月度经营数据（销量/出货量/订单/中标）
   是下一期财报的领先指标，必须单独汇总为「报告期后经营数据」小节
   （如一季报后已出的4/5/6月销量），并给出方向性前瞻：延续加速/延续放缓/出现拐点。
   前瞻只能是方向判断且必须标注"基于月度数据推断"，禁止推算具体营收利润数字

5. **技术实力**：研发投入、核心技术突破、在研项目、专利壁垒

6. **产业链地位与护城河评级**：位置、议价力、竞争格局；
   必须给出护城河评级「高/中/低」+ **双向依据**：
   - 正面支撑：独有技术/专利/牌照/切换成本/市占率等具体证据
   - 负面制约：削弱壁垒的因素（如净利率下行、价格战侵蚀、高端品牌占比低等）
   证据不足时评「低」并写明"未找到壁垒证据"——护城河低的公司，
   后续所有多头结论都要显著降温（没有壁垒的景气随时会被竞争摊薄）
   **护城河论证必须包含长期盈利验证指标**：连续3年以上ROE是否高于行业平均、
   净利率是否行业领先、超额利润的来源是什么——用这些财务壁垒佐证产业壁垒，
   数据不足时标注"缺少ROE/净利率的长期对标数据"

7. **利好与利空分析**：分别列出利好和利空条目，每条标注影响力(高/中/低)及依据出处；
   综合判断只用「偏多/中性/偏空」三档，禁止编造精确百分比

8. **大盘环境与同业对标**：
   - 基于【同业横向对标】数据块，分析公司相对同业的估值位置（PE/PB是偏高还是偏低）、
     毛利率对比、增速对比
   - 判断当前新能源车/所在板块指数近60日涨跌方向，公司走势相对板块的强弱
   - 综合判断：公司是行业beta驱动还是自身alpha驱动

9. **利润驱动与飞轮（三段论）**：
   - **当前驱动**：现在利润主要靠什么业务赚？必须引用主营构成数据的收入/利润占比与毛利率原数
   - **第二曲线**：哪些业务正在放量接棒？需有占比同比提升、销量/订单/出货量数据或公告佐证，
     不能只凭新闻标题定性（按地区维度的海外占比提升=出海驱动）。
     **第二曲线必须给出量化弹性测算**：未来2-3年该业务的营收空间、利润增量估算、
     对公司整体利润的贡献占比预期（如"预计2027年储能利润占比从5%升至20%"）
   - **远期期权**：公司公开布局但尚未贡献利润的方向，**用表格格式列出**：
     方向 | 证据强度（已投产/在建/公告立项/仅高管表态）| 预计兑现时间，
     每行一个方向，禁止展开大段文字；无公开证据的方向不列入
   - **飞轮效应**：判断各业务之间是否共享技术/产能/渠道/品牌而相互强化，
    必须做**双向判断**：
    - 正向协同：成立则写出具体传导链条（如"A业务的规模摊薄B业务的核心部件成本"）
    - 反向约束：是否存在内部冲突（如低端车型降价损伤高端品牌形象、各业务协同有限）
    不成立或证据不足要明说"未见明显飞轮"，禁止强行升华

10. **业绩持续性判断（增长质量分析）**：
    对每项增长驱动力（当前驱动/第二曲线/远期期权），明确区分是**一次性脉冲**还是**持续性增长**：
    - **一次性脉冲信号**（不可持续）：大客户一次性集采/压货、补贴退坡前抢装、
      原材料价格短期暴涨/暴跌、特殊事件驱动（疫情/战争/缺货恐慌）、
      会计准则变更、低基数效应、汇率短期波动、资产处置收益
    - **持续性增长信号**（可持续）：行业渗透率持续提升、客户覆盖面扩大、
      复购/续约率高、产能有节奏释放、海外渠道持续拓展、技术代差领先、
      规模效应持续摊薄成本
    - 对每项增长驱动标注「持续性/一次性/不确定」并说明判断依据
    - 综合判断：当前利润增长中**可持续部分占比多少**？若一次性因素占比过高，
      则在估值和展望中必须保守处理
    - 举例：AI服务器业务中——"GB200出货量爆发"可能是一次性大客户集中部署，
      "液冷解决方案收入占比持续提升"才是持续性增长信号

11. **资金筹码分析**（基于【资金筹码数据】程序数据块 + 【个股资金流向】补充数据块）：
    - **北向资金**：持股量变化趋势（增配/减配/新进）
    - **两融余额**：融资净买入方向，杠杆资金在加仓还是撤退
    - **股东户数**：户数变化趋势，筹码集中度在提升还是发散（户数↓=筹码集中，户数↑=筹码发散）
    - **机构持仓**：基金/机构家数及持仓占比变化
    - **限售解禁**：未来3个月大规模解禁预警（占流通股比>5%时单独警示）
    - **个股资金流向**（基于【个股资金流向】数据）：当日主力净流入（万元/亿）、近5/10/20日累计主力净流入方向与规模、游资与散户方向是否与主力一致（分歧/共振——主力买入+散户卖出=机构吸筹，主力卖出+散户买入=机构出货），综合判断"机构主导还是游资炒作"
    - **综合判断**：整体资金面偏多/中性/偏空

12. **SOTP分部估值**（如果程序计算模块提供了各业务净利润数据）：
    - 对公司主要业务板块进行分部估值
    - 汇总SOTP估值区间结论，**压缩为一句**（含每股内在价值区间），不展开PE倍数假设讨论

13. **业绩敏感性测算**：
    - 识别影响公司利润的核心变量（原材料价格/关税税率/销量/毛利率等）
    - 估算各变量每变动一个单位对净利润的影响量级
    - 给出"若XX变量变动±10%/±20%，净利润将在XX-XX亿元区间"的量化判断

14. **情景推演与重估触发**：
    - 给出 乐观/基准/悲观 三情景，每个情景必须包含：
      触发条件（具体可验证：指标+阈值或事件，如"单月销量同比转正""毛利率环比回升超1pct"
      "海外反补贴关税落地"）、传导路径（条件→业务→财务指标的方向变化）、
      可能性档位（只用 高/中/低，禁止编造百分比）。情景是推演不是预测，禁止写目标价
    - **催化事件分级（三级体系）**：在重估触发条件中标注级别：
      - **一级催化（影响板块β）**🟢：宏观/政策/行业巨头事件，影响全行业景气度
      - **二级催化（影响环节α）**🔵：环节涨价/头部公司IPO/认证突破，影响特定环节格局
      - **三级催化（影响个股α）**⚪：个股订单/财报超预期/高管增持，影响单一公司基本面
    - 在输出最末尾附一段纯JSON（不要markdown包裹）：
      {{"company_triggers": [{{"trigger": "重估触发条件（可被公开新闻验证的具体事件）",
        "keywords": "盯梢关键词 空格分隔", "level": "primary/secondary"}}, ...]}}
      取三情景中最关键的1-4条可验证触发条件（一级优先，利多利空都要有）；没有就给 []
    - 触发条件硬规则（违反的条目会被程序丢弃）：
      ①必须是**尚未发生**的前瞻事件：已披露报告期的数据不得作为触发条件
      （一季报已公布就写"中报/三季报净利润同比转正"，不能写"一季报转正"）；
      ②必须可判定：含明确方向+数字阈值（"毛利率回升至25%以上""同比转正"）
      或明确事件（"公告""正式落地""公布"），禁止"毛利率波动/环比变化"这类
      没有方向阈值、任何时候都成立的写法

【资本开支细分要求】
- 如果搜索结果中包含资本开支数据，必须拆分为细分投向：
  海外建厂、产能扩张、研发投入、充换电网络/销售渠道建设等
- 估算各方向的资本分配比例，判断投入的产能释放节奏与预期回报周期
- 没有细分数据则标注"仅有资本开支总额，无细分投向数据"

【现金流分析要求】
- 必须给出近 4 个季度的经营现金流/投资现金流/筹资现金流时序对比
- 标注经营现金流是否足以覆盖资本开支（自由现金流正负）
- 判断现金流趋势是季节性波动还是长期恶化
- 数据不足时标注"数据不足以判断现金流趋势"

【净利降幅归因要求】
- 净利润同比降幅大于营收降幅时，必须用三因子拆解归因，禁止笼统写"毛利率下滑导致"：
  ① 毛利率变动（同比±Xpct 对净利的影响量级，引用利润表毛利率原数）
  ② 期间费用率变动（同比±Xpct，含销售/管理/研发/财务费用率分项；财务费用须单独说明汇兑影响）
  ③ 非经常性损益（汇兑损益/资产减值/投资收益等一次性项，须标注"一次性"）
- 必须基于三大报表实际费用率/财务费用数据重算并给结论
  （模板：「净利降幅大于营收主因汇兑转亏 + 费用率 +2.47pct，
    非单纯毛利率 compression；毛利率环比 +1.37pct 说明
    产品结构/出海已托底」——若毛利率环比改善则必须补此句对冲负面归因）
- 三因子须分别量化贡献占比，数据不足时标注"缺少费用率分项数据"，禁止笼统归因
- 交叉验证（最末一句）必须落到【财务关键指标快照】给出的精确费用率和财务费用，
  禁止泛泛而谈

【护城河分析要求】
- 如果搜索/研报提到出海毛利率溢价（如「海外汽车毛利率较本土高 6–8pct」
  「招银估算出海毛利率溢价 5-7pct」），必须嵌入护城河段并注明来源券商/估算值。
  模板：「出海毛利率溢价（海外 vs 本土：+6–8pct，招银估算）
  → 第二曲线利润增量显著高于本土同等销量，护城河具结构性」
- 数据缺失则写"未检索到出海毛利率溢价公开数据，需后续研报补充"，不得编造具体溢价幅度

【等待信号要求】（在「重估触发条件」之前单独成段）
- 必须列出 2-4 个最关键的验证点等待信号，必须来自机构研报的明确验证点
  （如东吴/花旗/招银给出的 Q2 净利阈值、毛利率阈值等），禁止自己臆测。
  模板：「等待信号：① Q2 净利 ≥90 亿（东吴/花旗验证点）
   ② 毛利率回 20%+（东吴验证点）③ XX」
- 若无机构研报明确验证点，则写「当前缺少机构明确验证点，待后续研报更新」

【机构预期修正要求】
- 如果搜索信息显示当前季度/月度经营数据明显弱于去年同期或上季度（如销量同比下滑、毛利率走低），
  必须在报告中给出基于经营数据的**保守修正后全年净利润估算**（用当前月销量年化×单车净利等简易算法），
  并和Wind/Bloomberg 34家机构一致预期做差值对比，量化"预期差"——机构预期可能下行修正的空间
- 如果经营数据优于预期，同样需计算修正方向

【估值多窗口分析要求】
- PE分位必须同时给出3年/5年/10年三个窗口的分位数，禁止只写单一窗口
- 如果材料中只提供了一个窗口的分位数，标注"仅X年数据可用"
- 必须交叉验证PE/PB/PS三个指标：PE看贵贱、PB看资产底、PS看营收估值
- PE和PB分位背离时必须解读矛盾根源，必须用以下模板表述（禁止只写"PE偏高"而不拆因）：
  「PE 3年X% 悬顶（盈利同比-Y%被动抬高，非估值主动拉升）、PB 3年Z% 托底（资产端已处历史底部）；
   PS 3年W% 中性——结论不是PE低，而是3年PE偏贵（被盈利下滑被动抬高）」
  必须区分"盈利被动抬高PE"与"市场主动给估值"，两者方向相反结论相反
- PS(TTM)分位作为补充维度，与PE/PB形成三维估值画像

【基准情景强约束】
- 情景推演的「基准情景」净利润同比区间必须**以当期已披露财报为锚**（如中报已披露
  则绑定中报数据），不得使用"单季最差值同比"作为基准情景。必须同时引用：
  ① 当期财报实际净利同比（来自【财务关键指标快照】）
  ② 机构净利一致预期区间（26E 预测净利区间/机构家数/目标价，来自
     【候选公司一致预期数据】或【机构盈利预测（akshare 多源兜底）】）
- 格式约束：
  「基准情景：同比 -30%~-10%（中报验证；机构锚：东吴26E 403.67亿/东方财富 404亿/
   54家区间 412–443亿；目标价 XX元）」
- 严禁基准情景写"-55%""-50%"这种偏离已验证财报太远的极端值——那是悲观情景，
  不是基准；基准必须落在「财报数据±机构预期」的重叠区间内。

【海外口径说明要求】
- 海外收入/出口占比数据必须标注统计口径：
  - 出口量/总产量口径（含渠道库存与在途）= 非终端交付
  - 终端交付口径 = 实际销售给终端客户
  - 如果搜索结果中未明确口径，标注"统计口径待确认"
- 重要：海外收入%与出口量%必须明确标注分子分母（如：海外收入/总收入 vs 出口量/总产量 vs 终端交付量/总交付量），两个数字不可混用
- 如果材料中出现两个口径的数字（如年报38.65% vs 单月出口43.5%），必须分开表述并说明各自口径

【同行对标补充要求】
- 程序拉取的同行对比仅覆盖A股，不包含港股/美股同行
- 如果目标公司在港股/美股有重要竞争对手（如吉利汽车00175.HK、理想汽车02015.HK等），必须在搜索结果中补充交叉比对
- 同业对标必须区分"行业共性问题"（如价格战导致全行业毛利率下降）与"个股独特问题"（如某公司库存特别高）

【风险对称要求】
- 每条高影响力利好必须检查并列出对应风险（如：出口高增→关税/反补贴调查风险；大客户订单→客户集中度风险）
- 机构评级降权处理：A股卖方几乎不出卖出评级，"N家机构全部买入"不构成有效利好，最多作为关注度参考
- 目标价：搜索结果里有具体数字才引用，没有就不写"上涨空间较大"这类无依据表述

**【核心风险因素研报验证规则】**
- 报告中标注的"最大风险"必须有研报交叉验证，禁止仅凭直觉列风险
- 搜索结果中多家研报对同一风险的归因解释必须引用并标注来源
  （如"广发证券：'业绩受产品价税改革影响'；银河证券：'毛利率显著下滑主因价税改革'"）
- 核心风险因素必须区分"行业共性风险"（全行业面临）与"个股特有风险"（仅该公司）
- 风险归因须与财务数据交叉验证（如"价税改革→毛利率下滑5.87pct"需对应分部毛利率数据）

**【券商目标价明细引用规则】**
- 程序提供的【目标价矩阵】可能因数据源覆盖不全而缺少部分券商目标价
- 搜索结果中出现头部券商目标价时，**必须逐家引用**并标注：券商名称/报告日期/评级/EPS预测/目标价
  （如"中信建投(2026-05-09)：买入，EPS 0.37元；广发证券(2026-04-08)：增持，目标价23.95元（26年80倍PE）"）
- 目标价对应的估值逻辑（如"80倍PE"）也需一并引用，帮助读者理解定价假设
- 禁止将程序目标价矩阵与搜索结果中的券商目标价混为一谈，需分别标注来源

【价格反弹与基本面反转区分原则】
- 报告中必须区分两组独立判断：
  ① **短期价格反弹**：仅基于超卖/技术面支撑的反弹，不改变基本面方向，不可作为建仓依据
  ② **基本面反转**：销量/利润/毛利率出现可验证的拐点信号（如"单月销量同比转正且连续2个月"），
     才构成基本面建仓信号
- 两组判断在结论段落中分开表述，不得混为一谈

【技术面综合判断要求】
- 必须输出一张「技术面打分表」，综合程序提供的技术指标数据块（神奇九转等）+ 资金筹码数据 +
  搜索结果中的技术信号，按以下维度填表（表格格式）：
  | 维度 | 信号 | 判断 |
  - 趋势：日线多空（基于九转信号/均线方向，如"跌破XX后日线转空"）
  - 支撑压力：关键价位（前低/布林下轨/整数关/聚类位，如"布林下轨约87与88.55聚类"）
  - 资金：主力方向（引用资金筹码数据块的主力净流入/北向趋势）
  - 形态：突破/破位/震荡
- 技术面仅作短期参考，必须与基本面判断分开表述，不得用技术反弹作为建仓依据
- 技术指标数据不足时标注"技术指标数据不足"，禁止从搜索结果编造具体价位居中数值

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

    def _validate_chain_coverage(self, industry: str, chain: Dict[str, Any],
                                 all_leader_codes: set) -> tuple[str, list[Dict], set]:
        """产业链覆盖度校验：映射当前候选到12标准环节 → 统计覆盖数 → 缺环节自动补充搜索。
        返回 (coverage_warning, new_leaders, updated_codes)

        仅当行业名称包含"机器人"或"人形"时启用 HUMANOID_CHAIN_CHECKLIST 专用覆盖度校验；
        其他行业跳过专用覆盖度校验，仅记录日志。"""
        # 按行业动态判断：只有机器人/人形相关行业才使用专用覆盖度校验
        if "机器人" not in industry and "人形" not in industry:
            logger.info(f"非机器人行业（{industry}），跳过专用覆盖度校验")
            return ("", [], all_leader_codes)

        # Step 1: 建立候选->环节快照（用链结构中的 segment name 做别名映射）
        covered_segments = set()
        segment_to_codes: Dict[str, list[str]] = {}
        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            for seg_data in chain.get(level, []):
                seg_name = str(seg_data.get("segment", ""))
                # 别名映射
                std_seg = None
                for alias_k, alias_v in _CHAIN_SEGMENT_ALIASES.items():
                    if alias_k in seg_name or seg_name in alias_k:
                        std_seg = alias_v
                        break
                if std_seg:
                    # 该 segment 下的候选股
                    for leader in seg_data.get("leaders", []):
                        code = leader.get("code", "")
                        if code:
                            covered_segments.add(std_seg)
                            segment_to_codes.setdefault(std_seg, []).append(code)
        # 额外：用公司名反向匹配（有些 segment 名太泛，但公司名/主营直接指明了环节）
        for code in all_leader_codes:
            if code in covered_segments:
                continue
            name = find_company_name(code) or ""
            for alias_k, alias_v in _CHAIN_SEGMENT_ALIASES.items():
                if alias_k in name:
                    covered_segments.add(alias_v)
                    segment_to_codes.setdefault(alias_v, []).append(code)
                    break

        n_covered = len(covered_segments)
        total = len(HUMANOID_CHAIN_CHECKLIST)
        missing = [seg for seg in HUMANOID_CHAIN_CHECKLIST if seg not in covered_segments]

        warning = f"【产业链覆盖度校验】候选池覆盖 {n_covered}/{total} 个环节"
        if n_covered >= total:
            warning += "——完整覆盖。\n"
            return (warning, [], all_leader_codes)
        if n_covered == total - 1:
            warning += f"，仅缺失「{missing[0]}」。\n"
        elif n_covered >= total - 2:
            warning += f"，缺失{len(missing)}个：{'、'.join(missing)}。\n"
        else:
            warning += f"，⚠️ 覆盖度不足（<{total-2}），缺失{len(missing)}个：{'、'.join(missing)}。\n"
            warning += "将自动搜索补充缺失环节。\n"

        # Step 2: 补充搜索缺失环节的推荐标的
        new_leaders = []
        for seg in missing:
            _, rec_stocks = HUMANOID_CHAIN_CHECKLIST.get(seg, ("", []))
            if not rec_stocks:
                # 无推荐标的时搜索该环节的上市公司
                query = f"{industry} {seg} 上市公司 A股 股票代码 {date.today().year}"
                try:
                    result = web_search.invoke({"query": query})
                except Exception:
                    continue
                # 搜索结果中尝试提取
                extra = self._extract_leaders_from_text(str(result), seg, "auto_supplement", "")
                if extra:
                    for e in extra:
                        ec = e.get("code", "")
                        if ec:
                            new_leaders.append(e)
                            all_leader_codes.add(ec)
                            covered_segments.add(seg)
            else:
                # 有推荐标的，逐一验证是否已存在，不存在则尝试获取代码
                for rn in rec_stocks:
                    try:
                        rc = find_stock_code(rn)
                    except Exception:
                        rc = None
                    if rc and rc not in all_leader_codes:
                        new_leaders.append({"name": rn, "code": rc, "segment": seg, "rank": "补充"})
                        all_leader_codes.add(rc)
                        covered_segments.add(seg)

        if new_leaders:
            updated_total = len(covered_segments)
            warning += f"补充后覆盖 {updated_total}/{total} 个环节，新增 {len(new_leaders)} 只候选。\n"
            warning += "新增标的：" + "、".join(f'{l.get("name","")}({l.get("code","")})' for l in new_leaders) + "\n"
        return (warning, new_leaders, all_leader_codes)

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
        """生成产业链全景+各环节龙头+资金偏好的搜索查询（含不可替代性/溢价能力维度)

        query 数量上限 30 条：10 条行业级 + 排名前 10 的候选公司各 2 条
        （每家公司经营现状+护城河合并 1 条、资金面+估值合并 1 条），超出截断。
        """
        today = date.today()
        three_months = today - timedelta(days=90)
        recent_period = f"{today.year}年{today.month}月"
        three_month_range = f"{three_months.strftime('%Y-%m')} {today.strftime('%Y-%m')}"

        # ===== 10 条行业级 query =====
        industry_queries = [
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
            # 技术路径博弈：不同技术路线的份额变化决定利润在候选池内部的迁移方向
            f"{industry} 技术路线 对比 趋势 占比 市场份额 {recent_period}",
            # 量产进度与业绩兑现：各整机厂量产时间表对应的上游订单节奏
            f"{industry} 量产 时间表 SOP 交付 订单 供应链 业绩兑现 {today.year}",
        ]

        # ===== 每个候选公司 2 条 query（经营现状+护城河合并 1 条、资金面+估值合并 1 条）=====
        company_query_groups: List[List[str]] = []
        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            for seg_data in chain.get(level, []):
                seg = seg_data.get("segment", "")
                for leader in seg_data.get("leaders", []):
                    name = leader.get("name", "")
                    code = leader.get("code", "")
                    tag = f"{name}({code})" if name and code else seg
                    # 经营现状+护城河合并：业绩/竞争力/业务构成/不可替代性/技术壁垒
                    q_biz = (f"{tag} {seg} 业绩 竞争力 业务构成 收入占比 毛利率 "
                             f"护城河 技术壁垒 不可替代性 议价权 {recent_period}")
                    # 资金面+估值合并：主力资金/机构持仓/估值/PE/龙虎榜
                    q_capital = (f"{tag} 资金流向 主力资金 机构持仓 北向资金 "
                                 f"估值 PE 历史分位 龙虎榜 {today.year}")
                    company_query_groups.append([q_biz, q_capital])

        # ===== 合并：10 行业级 + 排名前 10 的候选公司各 2 条 = 最多 30 条 =====
        queries = list(industry_queries)
        top_company_count = min(len(company_query_groups), 10)
        for qs in company_query_groups[:top_company_count]:
            queries.extend(qs)

        # 安全兜底：总数超过 30 条时只取前 30 条
        if len(queries) > 30:
            queries = queries[:30]

        logger.info(f"产业链搜索query: 共{len(queries)}条"
                    f"（行业级{len(industry_queries)} + 候选公司{top_company_count}×2）")
        return queries

    def _build_chain_system_prompt(self, stage: str = DEFAULT_STAGE, stage_reason: str = "") -> str:
        stage = normalize_stage(stage)
        w = get_stage_weights()[stage]
        gate_desc = "；".join(f"{label}≥{gate:g}分" for _, gate, label in STAGE_GATES[stage])
        return f"""你是一个顶级的产业链研究专家，擅长挖掘投资机会。你的任务是从产业链中筛选出所有关键公司（含特精专新企业），分析其基本面、边际变化、护城河、资金偏好，并输出清晰的候选公司清单（含股票代码），供下游技术分析 Agent 使用。

{INTERMEDIATE_PRODUCT_NOTE}

【信源优先级】🟢 T1 权威（公告/财报/认证官方社交）> 🔵 T2 结构化（财经媒体）> 🟡 T3 未验证社交 > ⚪ T4 网络搜索
- 数据不一致时以高等级为准并标注差异；以下数据块前的等级标签代表其信誉级别

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
    {{"code": "股票代码", "name": "公司名称", "business": 业务经营分0-10, "fundamental": 基本面分0-10,
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
- **边际变化打分必须附加量化证据**：单季营收同比增速/环比增速、在手订单金额同比变化、
  产能利用率变动、毛利率变动幅度等，每个分数必须有具体数字支撑。
  例如"订单增长"须写明"订单同比+50%至XX亿元（来源XX）"，仅定性描述不给高分。
  写不出量化数据的边际变化评分上限为6分
- 打分锚点（四个分项通用；准入门槛在7/6/5分档，锚点直接决定进池出池，必须校准）：
  5=行业平均/无差异化证据；6-7=有明确公开证据的相对优势；8=显著领先且证据充分；
  9-10=近乎垄断或爆发式增量；3以下=明显劣势。每个分数必须能对应写出证据，
  写不出证据就往5分靠，禁止靠印象给7分以上
- momentum 锚点举例：单季营收/订单同比+50%且已兑现=8；毛利率环比回升+新客户开始量产=7；
  仅增速企稳无新增量=5~6；增速下滑=3~4
- reeval_triggers：1-4 条"若发生XX则值得重新评估该行业"的具体条件（如"某公司砷化镓电池收入占比超20%"
  "可回收火箭完成商业化首飞"），必须可被公开新闻验证，程序会自动盯梢；没有就给空数组。
  **必须是尚未发生的前瞻事件**——已发生的大事写进「行业近况与重大事件」，
  禁止登记为触发条件（已发生事件落库后监控第一轮扫描就会全部误报命中）。
  **触发条件选择原则**：优先选外部不可控事件（如供应链断供/制裁升级/重大政策落地），
  这类事件一旦发生会根本性改变行业格局。下游整机厂的价格调整（如涨价/降价）本质是成本传导，
  不是上游供需关系的直接信号，不应作为触发条件

## 一、产业链公司全景筛选
- 用表格列出产业链上中下游 + 特精专新共筛选出的所有公司
- 表头：环节 | 细分领域 | 公司名称 | 代码 | 核心业务一句话 | 资金偏好
  ⚠️ **表格不包含评分/PE列**——综合排名表由程序在报告末尾自动生成（加权+预期差调整后排序）
- **环节列**：必须引用【公司→环节映射】数据块中的映射关系填写，禁止自行推测
- **核心业务列**：必须从下方"候选公司主营业务构成"程序数据块或"全网搜索结果"中提取引用，不得为空或"-"
- **资金偏好列**：依据搜索数据中提及的资金类型判断，无公开证据写「无公开数据」（详见下方资金偏好标签说明）
- **分类精度要求**：航天业务关联极弱的公司（如慈星股份、创耀科技）单独标注为「纯题材概念股」，
  与真正受益标的分开列示，不得混入核心受益环节

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
- **客户集中度**：前五大客户收入占比，是否存在单一客户依赖（依赖>30%定义为高风险），
  或是向全行业多客户供货的独立第三方供应商——后者客户分散度高，抗爆雷能力强
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
  **弹性准入规则**：距门槛差 ≤0.5 分的标的仍列入「观察备选池」，在报告中单独标注。
  （例如 moat 门槛5分 → 4.5-4.9分的标的归入观察备选池，不入正式投资池但保持跟踪）
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
- **业绩持续性判断**：对各环节的增长驱动力，区分是**一次性脉冲**（大厂集中招标/补贴退坡前抢装/涨价囤货等）还是**持续性增长**（渗透率提升/国产替代加速/产能有序释放/复购需求稳定等）。综合判断各环节利润增量中可持续部分占比
- 全部判断必须基于搜索证据（涨价新闻/排产数据/招标节奏），没有证据就写「证据不足，无法判断迁移方向」

## 七、资金筹码分析（北向/两融/股东户数/机构持仓/解禁）
基于下方【候选公司资金筹码数据】程序数据块，对各家公司进行资金面分析：
- **北向资金**：持股量变化趋势（增配/减配/新进），谁是北向重点配置标的
- **两融余额**：融资净买入方向，杠杆资金在加仓还是撤退
- **股东户数**：户数变化趋势，筹码集中度在提升还是发散（户数↓=筹码集中，户数↑=筹码发散）
- **机构持仓**：基金/机构家数及持仓占比变化，机构是否在加仓
- **限售解禁**：未来3个月大规模解禁预警（占流通股比>5%时单独列出）
- **综合判断**：整体资金面偏多/中性/偏空，主力资金、游资、散户各自在增持哪类标的

## 八、催化事件分级体系（未来3-6个月）
机会有时间属性。用列表按以下**三级分级体系**组织催化事件，每条格式：
  事件 | 级别 | 影响的环节或公司 | 落地概率 | 影响幅度 | 兑现时间窗 | 出处

**三级催化分级标准**：
- **一级催化（影响整个板块β）**🟢：影响全行业景气度的事件——触发后**自动重估整个候选池**
  - 示例：特斯拉Optimus单季出货≥5000台、工信部专项补贴落地、美国对华机器人出口管制升级
  - 来源通常为国家政策、行业巨头公告、宏观事件
- **二级催化（影响环节α）**🔵：影响特定产业链环节格局的事件——触发后**自动重估对应环节所有标的**
  - 示例：减速器/丝杠/电机等环节涨价函、头部整机厂（宇树/智元/小米）IPO或新一轮融资、
    国产核心零部件通过特斯拉/Figure认证
  - 注意：「哈默纳科/纳博特斯克宣布对华涨价或断供」是真正的国产替代加速催化信号，
    与下游整机厂提价有本质区别（下游提价≠上游供不应求，切勿混淆）
- **三级催化（影响个股α）**⚪：影响单一公司基本面的事件——触发后**自动重估对应个股**
  - 示例：公司公告大额订单/定点函、财报超预期（营收/利润增速>一致预期20%）、高管增持/回购

**输出要求**：
- **落地概率**：每条标注高/中/低，说明判断依据，禁止默认全部"高概率"
- **影响幅度**：量化或半量化标注（如"Optimus量产→绿的订单翻倍""补贴落地→全行业毛利率+2-3pct"）
- **兑现时间窗**：精确到季度（如"2026Q3-Q4"），不确定时标注"待定"
- 出处必须写具体来源（媒体名/公司公告/政府文件，尽量带日期），禁止笼统写
  「搜索数据」「搜索结果」——无法说出具体来源的事件视为依据不足，不列
- 只列搜索结果里有明确依据的事件，禁止编造时间；没有就明写「未发现明确催化剂」
- ⚠️ 特别关注行业内正在推进 IPO（已注册/过会/获批/招股）的重要公司，
  其上市后的资本开支和产业链带动是独立的二级催化，必须在时间轴中列出并标注弹性

**【涨价事件归因分类规则】**
涉及"涨价"的事件必须先做归因分类，禁止笼统引用：
- **类型A：上游零部件厂涨价（供不应求信号）** 🟢 真正的供给紧张信号
  - 判定：零部件厂商（如减速器/丝杠/电机/传感器厂）主动提价、交期延长、排产爆满
  - 影响：上游环节定价权提升 → 利润向上游迁移 → 利好上游核心零部件标的
  - 示例：绿的谐波交期延长至12周+、哈默纳科对华断供/涨价
- **类型B：下游整机厂提价（成本传导信号）** 🟡 中性或偏利空（需求端承压）
  - 判定：整机厂/系统集成商（如埃斯顿/新时达/埃夫特）因铜/铝/芯片等原材料成本上涨而提价
  - 影响：成本压力从下游向上游传导的结果 ≠ 上游供不应求；
    若提价导致需求萎缩，反而利空整个产业链
  - 注意：整机厂提价绝对不能作为"上游议价权提升"或"行业景气"的证据，两者因果方向完全相反
- **类型C：渠道/经销商涨价（中间商行为）** ⚪ 参考价值低
  - 判定：贸易商/经销商囤货炒作导致的价格波动
  - 影响：不代表真实供需格局，通常不构成长期催化
- ⚠️ 引用涨价事件必须标注类型（A/B/C），不得将B类（整机厂提价）误判为行业景气信号

**【机器人纯度分类规则】**
涉及机器人的催化事件必须标注"机器人纯度"——即该事件对哪类机器人的拉动最大：
- **人形机器人核心催化** 🤖：直接利好执行人形机器人核心零部件（谐波减速器/行星滚柱丝杠/空心杯电机/力矩传感器/控制器）的事件
  - 示例：特斯拉Optimus量产、Figure拿到大额订单、国产人形机器人核心零部件通过认证
  - 受益标的：绿的谐波、拓普集团、三花智控等核心零部件厂
- **工业机器人催化** 🏭：主要利好工业机器人本体、系统集成、工业自动化的事件
  - 示例：制造业自动化改造补贴、3C/汽车行业扩产、工业机器人销量同比大增
  - 受益标的：汇川技术、埃斯顿、机器人等工业自动化/本体厂商
- **四足/巡检机器人催化** 🐕：主要利好四足机器人、巡检机器人、特种机器人的事件
  - 示例：国家电网/南方电网巡检机器人集采、安防机器人招标、消防机器人列装
  - 受益标的：宇树科技供应链、奥比中光（3D视觉）、柯力传感（传感器）等
- **混合催化**：同时涉及多类机器人，但拉动力度不同——必须说明各类受益程度排序
  - 示例："国家电网68亿集采" → 四足巡检🐕 > 工业操作臂🏭 > 人形带电作业🤖
    （5000台四足+3000台双臂+500台人形，四足是主力，人形占比<6%）
- ⚠️ 禁止笼统地把任何机器人相关事件都归为"人形机器人一级β催化"，
  必须实事求是地拆分受益结构和拉动力度。人形机器人是高弹性赛道，
  但非人形事件强行碰瓷人形机器人 = 误导读者

## 九、行业近况与重大事件（近1-3个月已发生）
催化剂时间轴看未来，本节看已经发生的行情驱动：
- 技术里程碑（如"国产火箭回收试验成功"）、重要政策落地、大额融资/订单、事故与挫折，
  每条带日期与出处，并说明它是哪个环节的确认信号或风险信号
- 提供了【行业指数表现】时必须引用其近5/20/60日涨幅原数，与大事件对照解读
  （大事件出现后指数是否已经兑现了一波=当前介入的赔率基础）；
  指数与候选个股走势背离时必须点破（指数涨个股不涨=个股问题，反之=行业beta拉动）
- 没有重大事件就明写「近期无重大事件」，禁止拿日常新闻凑数

## 十、3年盈利预测与PE Band（基于下方【一致预期数据】程序数据块）
如果下方提供了程序拉取的一致预期数据，请基于它生成以下分析（否则基于搜索结果）：
- 对每只候选股列出2026E/2027E/2028E的一致预期净利润及对应增速
- 计算营收和利润的复合增速（CAGR 2026-2028）
- 对应当前总市值的PE Band（例如：绿的谐波即使按最乐观2028E净利润，PE仍有XX倍）
- 标注共识度：覆盖机构家数、目标价均值/中位数、当前价相对目标价的空间
- 同步检查机构是否在最近1个月上调/下调了预测（方向比绝对值重要）

**【机构一致预期数据使用规则】**
- **产业链分析**：程序数据块中的数据来自**Tushare report_rc**（含多家券商汇总，样本量最大，约40+家机构），引用时必须标注"Tushare N家机构"
- **个股分析**：程序数据块中的数据来自**AkShare多源并取**（东财+同花顺），引用时必须标注来源和机构家数
  （如"19家机构一致预期2026年EPS 0.37元"、"同花顺8家均值EPS 0.31元"）
- 搜索结果中若出现同花顺/东方财富/万得等其他来源的一致预期数据，**必须标注来源和样本量**
  （如"同花顺近6月16家机构均值"、"东方财富14家机构"）
- 不同来源数据差异 >3% 以上时，**必须同时列出并说明差异原因**
  （如"41家Tushare均值60.6亿 vs 16家同花顺均值64.6亿，差异+6.6%，或因样本机构不同/时间窗口不同）
- **forward PE**：必须使用程序计算值（基于机构数最多的主源），禁止自行用原始预测表心算
- **forward PEG**：必须使用程序计算值（基于预测期CAGR而非相邻年度增速），禁止自行推算增速
- **目标价**：必须引用程序提供的【目标价矩阵】（区间/中位数/机构明细），禁止将单一券商目标价冒充"一致预期"
- 不得将单一券商的个别目标价冒充"一致预期"。"一致预期"定义：≥3家机构的汇总值（中位数或均值）

## 十一、技术路径博弈分析
基于搜索结果分析本行业关键技术路线的竞争格局与演变趋势：
- **各技术路线的定位**（如人形机器人：谐波减速器=轻负载关节、RV减速器=重负载关节、
  行星滚柱丝杠=线性传动/最大卡脖子环节）
- **份额变化趋势**：各路线当前市占率、过去12个月的变化方向、未来2年的预期方向
- **利润迁移含义**：路线占比变化直接决定利润在候选池内部哪个环节/公司受益最多。
  例如谐波占比扩大利好绿的谐波，行星滚柱丝杠突破利好对应稀缺标的
- **卡脖子环节**：哪个技术路线国产化率最低、替代难度最大——一旦突破弹性也最大

## 十二、量产进度 → 业绩兑现映射表
**"催化 → 业绩 → 股价"链条**。用表格列出关键量产里程碑及其对候选池的业绩影响路径：

| 整机厂/事件 | 预计时间 | 量产规模 | 直接影响环节 | 对应公司 | 业绩兑现窗口 | 弹性评估 |
关键要求：
- 事件必须对应到具体公司的具体产品线（如"特斯拉Optimus SOP → 绿的谐波谐波减速器订单"）
- 业绩兑现窗口标注到季度（如Q3-Q4财报验证）
- 弹性评估区分：直接受益（订单量可测算）vs 间接受益（情绪催化为主）
- 对于尚未量产但正在进行IPO的公司（如宇树科技），其上市后的资本开支溢出是独立催化

【重要原则】
- 候选公司清单 JSON 必须是输出的第一部分（输出过长被截断时后面的段落可丢，JSON 不能丢）
- 最后必须附「行业风险」小节，拆分为两个独立小节：
  **一、行业系统性风险**：行业周期位置、政策与地缘风险、估值水位，各一两句
  **二、个股特有风险**：客户集中度、航天业务占比不足、订单波动、产能爬坡不及预期等
  每条高影响力利好须对应检查风险（如国产替代→下游资本开支放缓风险）
- 所有结论必须基于搜索结果，不足处标注「信息不足」
- **每只公司分析必须同时列出核心看多逻辑和核心看空风险**，非对称时说明偏重方向和分析师置信度。
  仅列风险不提弹性、或仅列利好不提风险的分析会被视为片面、降低报告可信度
- 业务拆解数据是评估公司质量和成长性的核心，请尽可能详细
- 资金偏好标签必须依据搜索数据中提及的资金类型判断，不可凭空猜测
- 特精专新企业的不可替代性是其核心竞争力，需重点分析
- 候选公司清单中的股票代码必须准确"""

    # ========== 一致预期数据拉取 ==========

    def _fetch_consensus_forecasts(
            self, codes: List[str]) -> Dict[str, Dict[int, dict]]:
        """拉取一致预期数据 + 去年实际值（作为基准锚）+ 目标价矩阵。
        
        数据获取优先级：DB缓存 → Tushare API → 失败回退到DB缓存
        （Tushare report_rc 仅 10次/天 配额，必须优先用缓存）
        
        返回 {code: {year: {revenue, profit, eps, n_inst, is_actual}, 
                '_target_prices': [{'org', 'target', 'rating', 'date'}, ...],
                '_target_stats': {'avg', 'median', 'min', 'max', 'count'}}}}；
        失败返回空 dict"""
        import pandas as pd
        from datetime import date
        result: Dict[str, Dict[int, dict]] = {}
        # 数据库连接（优先用DB缓存，节省Tushare配额）
        db = None
        try:
            from storage.sqlite.stock_storage import get_db
            db = get_db()
        except Exception:
            pass
        # Tushare 实例（仅当需要刷新时调用）
        tf = None
        profit_col, rev_col, eps_col = 'forecast_np', 'forecast_revenue', 'forecast_eps'
        today = date.today()
        for code in codes[:12]:
            year_data: Dict[int, dict] = {}
            # 1. 2025A 实际值（年报，基准锚）
            if db is not None:
                try:
                    inc_df = db.get_stock_income(code)
                    if inc_df is not None and not inc_df.empty:
                        inc_df = inc_df.copy()
                        inc_df['_rd'] = pd.to_datetime(inc_df['report_date'], errors='coerce')
                        ann_df = inc_df[inc_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False)
                        if not ann_df.empty:
                            last_annual = ann_df.iloc[0]
                            yr = int(last_annual['_rd'].year)
                            np_ = last_annual.get('net_profit')
                            rev = last_annual.get('total_revenue')
                            eps = last_annual.get('basic_eps')
                            if np_ is not None and float(np_) > 0:
                                year_data[yr] = {
                                    'profit': round(float(np_) / 1e8, 2),
                                    'revenue': round(float(rev) / 1e8, 1) if rev is not None else None,
                                    'eps': round(float(eps), 4) if eps is not None else None,
                                    'n_inst': 0,
                                    'is_actual': True,
                                }
                except Exception:
                    pass
            # 2. 一致预期数据：DB缓存优先，API刷新兜底
            df = None
            data_source = "none"
            # 2.1 先读DB缓存
            if db is not None:
                try:
                    cached = db.get_stock_report_rc(code, limit=50)
                    if cached is not None and not cached.empty:
                        latest_date = cached['report_date'].max()
                        if isinstance(latest_date, pd.Timestamp):
                            latest_date = latest_date.date()
                        elif isinstance(latest_date, str):
                            from datetime import datetime as _dt
                            latest_date = _dt.strptime(str(latest_date)[:10], "%Y-%m-%d").date()
                        # 缓存7天内有效；非交易日不刷新
                        days_old = (today - latest_date).days if latest_date else 999
                        if days_old <= 7 or today.weekday() >= 5:
                            df = cached
                            data_source = "db_cache"
                except Exception:
                    pass
            # 2.2 缓存过期/不存在 → 尝试API刷新（有配额限制，一天最多10次）
            if df is None and tf is None:
                try:
                    from tools.stock.tushare_fetcher import TushareFetcher
                    tf = TushareFetcher()
                except Exception:
                    tf = None
            if df is None and tf is not None:
                try:
                    from datetime import date as _date
                    _today = _date.today()
                    start_date = f"{_today.year - 1}-01-01"
                    end_date_str = _today.strftime("%Y-%m-%d")
                    api_df = tf.report_rc(code, start_date=start_date, end_date=end_date_str)
                    if api_df is not None and not api_df.empty and profit_col in api_df.columns:
                        df = api_df
                        data_source = "tushare_api"
                        # 保存到DB缓存（供后续使用）
                        if db is not None:
                            try:
                                db.save_stock_report_rc(df, code)
                            except Exception:
                                pass
                except Exception as e:
                    # API失败（配额超限/网络问题）→ 回退到DB缓存（即使过期了也用）
                    if db is not None:
                        try:
                            cached2 = db.get_stock_report_rc(code, limit=50)
                            if cached2 is not None and not cached2.empty:
                                df = cached2
                                data_source = "db_cache_fallback"
                        except Exception:
                            pass
            # 3. 解析一致预期数据 + 目标价
            if df is not None and not df.empty:
                self._parse_forecast_data(df, year_data, profit_col, rev_col, eps_col)
            if year_data:
                result[code] = year_data
        return result

    def _parse_forecast_data(self, df, year_data, profit_col, rev_col, eps_col):
        """从report_rc DataFrame中解析一致预期和目标价数据，写入year_data"""
        import pandas as pd
        target_prices = []
        # 目标价数据
        tp_col = 'target_price'
        org_col = 'forecast_org' if 'forecast_org' in df.columns else 'org_name'
        if tp_col in df.columns and org_col in df.columns:
            tp_df = df[df[tp_col].notna()].copy()
            if not tp_df.empty:
                if 'report_date' in tp_df.columns:
                    tp_df['_rd'] = pd.to_datetime(tp_df['report_date'], format='%Y%m%d', errors='coerce')
                elif 'ann_date' in tp_df.columns:
                    tp_df['_rd'] = pd.to_datetime(tp_df['ann_date'], format='%Y%m%d', errors='coerce')
                else:
                    tp_df['_rd'] = pd.NaT
                tp_df = tp_df.sort_values('_rd', ascending=False)
                seen_orgs = set()
                for _, row in tp_df.head(20).iterrows():
                    org = str(row.get(org_col, '') or '')
                    if not org or org in seen_orgs:
                        continue
                    seen_orgs.add(org)
                    tp = float(row.get(tp_col) or 0)
                    if tp <= 0:
                        continue
                    date_val = row.get('report_date') or row.get('ann_date') or ''
                    rating = str(row.get('rating', '') or '')
                    target_prices.append({
                        'org': org,
                        'target': round(tp, 2),
                        'rating': rating,
                        'date': str(date_val)[:8],
                    })
                if target_prices:
                    tp_vals = [t['target'] for t in target_prices]
                    year_data['_target_stats'] = {
                        'avg': round(sum(tp_vals) / len(tp_vals), 2),
                        'median': round(sorted(tp_vals)[len(tp_vals)//2], 2),
                        'min': round(min(tp_vals), 2),
                        'max': round(max(tp_vals), 2),
                        'count': len(target_prices),
                    }
                    year_data['_target_prices'] = target_prices[:8]
        # 盈利预测数据
        if profit_col in df.columns:
            df2 = df[df[profit_col].notna()].copy()
            if not df2.empty and 'end_date' in df2.columns:
                df2['end_date_dt'] = pd.to_datetime(df2['end_date'], format='%Y%m%d', errors='coerce')
                df2['year'] = df2['end_date_dt'].dt.year
                cur_year = pd.Timestamp.now().year
                df2 = df2[df2['year'].between(cur_year, cur_year + 3)].copy()
                for yr in sorted(df2['year'].unique()):
                    yr_df = df2[df2['year'] == yr]
                    p_vals = pd.to_numeric(yr_df[profit_col], errors='coerce').dropna()
                    p_med = p_vals.median()
                    r_vals = pd.to_numeric(yr_df[rev_col], errors='coerce').dropna() if rev_col in yr_df.columns else pd.Series(dtype=float)
                    r_med = r_vals.median() if not r_vals.empty else None
                    e_vals = pd.to_numeric(yr_df[eps_col], errors='coerce').dropna() if eps_col in yr_df.columns else pd.Series(dtype=float)
                    e_med = e_vals.median() if not e_vals.empty else None
                    # 提取机构级净利预测明细（基准情景强约束需要「东吴 26E 403.67亿」这种锚）
                    org_col = 'forecast_org' if 'forecast_org' in yr_df.columns else ('org_name' if 'org_name' in yr_df.columns else None)
                    org_details = []
                    if org_col is not None:
                        seen_orgs = set()
                        # 按报告日期降序，取最新的8家不同机构
                        tmp = yr_df.copy()
                        if 'report_date' in tmp.columns:
                            tmp = tmp.sort_values('report_date', ascending=False)
                        for _, r in tmp.iterrows():
                            org = str(r.get(org_col, '') or '').strip()
                            if not org or org in seen_orgs or org.lower() == 'nan':
                                continue
                            try:
                                pv = float(r.get(profit_col) or 0)
                                if pv <= 0:
                                    continue
                            except (TypeError, ValueError):
                                continue
                            seen_orgs.add(org)
                            eps_val = None
                            if eps_col in tmp.columns:
                                try:
                                    ev = float(r.get(eps_col) or 0)
                                    if ev > 0:
                                        eps_val = round(ev, 4)
                                except (TypeError, ValueError):
                                    pass
                            date_val = str(r.get('report_date') or r.get('ann_date') or '')[:8]
                            org_details.append({
                                'org': org,
                                'profit_yi': round(pv / 1e8, 2),
                                'eps': eps_val,
                                'date': date_val,
                            })
                            if len(org_details) >= 8:
                                break
                    if p_med is not None and p_med > 0:
                        year_data[yr] = {
                            'profit': round(p_med / 1e8, 2),
                            'profit_p25': round(p_vals.quantile(0.25) / 1e8, 2) if len(p_vals) >= 3 else None,
                            'profit_p75': round(p_vals.quantile(0.75) / 1e8, 2) if len(p_vals) >= 3 else None,
                            'revenue': round(r_med / 1e8, 1) if r_med is not None else None,
                            'eps': round(e_med, 4) if e_med is not None else None,
                            'n_inst': len(yr_df),
                            'is_actual': False,
                            '_org_details': org_details if org_details else None,
                        }

    def _format_forecast_table(self, forecasts: Dict[str, Dict[int, dict]],
                               per_stock_valuation: Optional[List[Dict]] = None) -> str:
        """将一致预期数据格式化为表格 + 估值敏感性矩阵 + 基本面锚"""
        if not forecasts:
            return ""
        # 建立 code -> valuation 映射
        val_map: Dict[str, dict] = {}
        if per_stock_valuation:
            for s in per_stock_valuation:
                c = str(s.get("code", ""))
                if c:
                    val_map[c] = s
        lines = ["========== 候选公司一致预期数据（程序拉取，3年盈利预测+PE Band+敏感性矩阵） =========="]
        for code in sorted(forecasts.keys()):
            yd = forecasts[code]
            name = find_company_name(code) or code
            # 获取当前估值
            vm = val_map.get(code, {})
            cur_mv = vm.get("total_mv")  # 总市值(亿)
            cur_pe = vm.get("pe_ttm")
            pe_pct = vm.get("pe_percentile")
            # --- 表格头 ---
            years = sorted([k for k in yd.keys() if isinstance(k, int)])
            def _yr_label(y):
                if yd[y].get('is_actual'):
                    return f"{y}A(实际)"
                return f"{y}E"
            header_years = " | ".join(_yr_label(y) for y in years)
            target_stats = yd.get('_target_stats', {})
            target_prices = yd.get('_target_prices', [])
            cur_price = vm.get("current_price")
            target_upside = ""
            if target_stats and cur_price:
                upside = (target_stats['avg'] / cur_price - 1) * 100
                target_upside = f"  平均目标价空间: +{upside:.1f}%"
            lines.append(f"\n◇ {name}({code})  当前PE={cur_pe or 'N/A'} PE分位={pe_pct or 'N/A'}%  总市值={cur_mv or 'N/A'}亿{target_upside}")
            lines.append(f"  指标    | {header_years}")
            # 营收行
            revs = [f"{yd[y].get('revenue','-')}" for y in years]
            lines.append(f"  营收(亿) | {' | '.join(revs)}")
            # 净利润行（实际值标★，预测值附P25-P75区间）
            profits = []
            for y in years:
                p = yd[y]['profit']
                is_act = yd[y].get('is_actual', False)
                if is_act:
                    profits.append(f"★{p:.2f}")
                else:
                    p25 = yd[y].get('profit_p25')
                    p75 = yd[y].get('profit_p75')
                    if p25 is not None and p75 is not None:
                        profits.append(f"{p:.2f}（{p25:.1f}-{p75:.1f}亿）")
                    else:
                        profits.append(f"{p:.2f}")
            lines.append(f"  净利(亿) | {' | '.join(profits)}")
            # 营收同比
            rev_growths = []
            prev_rev = None
            for y in years:
                cur_rev = yd[y].get('revenue')
                if prev_rev and cur_rev:
                    g = (cur_rev / prev_rev - 1) * 100
                    rev_growths.append(f"{g:+.1f}%")
                else:
                    rev_growths.append("-")
                prev_rev = cur_rev or prev_rev
            lines.append(f"  营收增速 | {' | '.join(rev_growths)}")
            # 净利润同比
            growths = []
            prev = None
            for y in years:
                cur = yd[y]['profit']
                if prev:
                    g = (cur / prev - 1) * 100
                    growths.append(f"{g:+.1f}%")
                else:
                    growths.append("-")
                prev = cur
            lines.append(f"  净利增速 | {' | '.join(growths)}")
            # Forward PE（以最新价/总市值作为基准）
            fwd_pe = []
            for y in years:
                p = yd[y]['profit']
                if cur_mv and p > 0:
                    fwd_pe.append(f"{cur_mv / p:.0f}倍")
                else:
                    fwd_pe.append("-")
            lines.append(f"  ForwardPE | {' | '.join(fwd_pe)}")
            # 覆盖机构数
            n_insts = [f"{yd[y]['n_inst']}家" for y in years]
            lines.append(f"  机构覆盖 | {' | '.join(n_insts)}")

            # --- 目标价矩阵（券商一致预期）---
            if target_prices:
                tp_count = target_stats.get('count', 0)
                tp_avg = target_stats.get('avg', '-')
                tp_med = target_stats.get('median', '-')
                tp_min = target_stats.get('min', '-')
                tp_max = target_stats.get('max', '-')
                lines.append(f"\n  🎯 券商目标价矩阵（Tushare，共{tp_count}家机构）：")
                lines.append(f"  统计 | 均价{tp_avg}元  中位数{tp_med}元  区间{tp_min}-{tp_max}元")
                if cur_price and isinstance(tp_avg, (int, float)):
                    avg_upside = (tp_avg / cur_price - 1) * 100
                    med_upside = (tp_med / cur_price - 1) * 100 if isinstance(tp_med, (int, float)) else 0
                    lines.append(f"  现价 {cur_price}元，距均价空间 {avg_upside:+.1f}%，距中位数空间 {med_upside:+.1f}%")
                    lines.append(f"  ⚠️ 注意：目标价仅为券商一致预期均值，不构成投资建议；实际走势受大盘/行业/公司多重因素影响")
                lines.append(f"  主要机构目标价（按报告日期排序）：")
                for tp in target_prices[:8]:
                    rating_str = f" [{tp['rating']}]" if tp['rating'] else ""
                    date_str = f" ({tp['date'][:4]}-{tp['date'][4:6]}-{tp['date'][6:]})" if tp['date'] else ""
                    lines.append(f"    - {tp['org']}: {tp['target']}元{rating_str}{date_str}")

            # --- 券商级净利预测明细（给基准情景强约束提供「东吴 26E 403.67亿」锚）---
            forecast_years = [y for y in years if not yd[y].get('is_actual')]
            has_any_detail = False
            for y in forecast_years:
                od = yd[y].get('_org_details') or []
                if od:
                    has_any_detail = True
                    break
            if has_any_detail:
                lines.append("\n  🎯 券商级净利预测明细（Tushare per-broker，直接引用锚定基准情景）：")
                for y in forecast_years:
                    od = yd[y].get('_org_details')
                    if not od:
                        continue
                    med = yd[y]['profit']
                    lines.append(f"    {y}E 中位数 {med:.2f}亿（{yd[y]['n_inst']}家覆盖）：")
                    # 利润区间（min/max of org details）
                    profits_yi = [d['profit_yi'] for d in od]
                    if len(profits_yi) >= 2:
                        lines.append(f"      区间 {min(profits_yi):.1f}-{max(profits_yi):.1f}亿")
                    for d in od:
                        eps_s = f" EPS{d['eps']:.2f}" if d.get('eps') else ""
                        date_s = f" ({d['date'][:4]}-{d['date'][4:6]}-{d['date'][6:]})" if d['date'] else ""
                        lines.append(f"      - {d['org']}: {d['profit_yi']:.2f}亿{eps_s}{date_s}")
                lines.append("    ★ 直接引用例：「54家 411.36亿（EPS4.51 / +26.11%）、东吴 403.67亿」")
                # 同时给基准情景准备：全年同比增速（最接近预测年 vs 上一实际年）
                actual_years = [y for y in years if yd[y].get('is_actual')]
                if actual_years and forecast_years:
                    last_actual_y = max(actual_years)
                    first_forecast_y = forecast_years[0]
                    actual_p = yd[last_actual_y]['profit']
                    forecast_p = yd[first_forecast_y]['profit']
                    if actual_p > 0:
                        g_median = (forecast_p / actual_p - 1) * 100
                        p_025 = yd[first_forecast_y].get('profit_p25')
                        p_075 = yd[first_forecast_y].get('profit_p75')
                        g_low = (p_025 / actual_p - 1) * 100 if p_025 else None
                        g_high = (p_075 / actual_p - 1) * 100 if p_075 else None
                        lines.append(f"    ★ 全年同比锚（{first_forecast_y}E ÷ {last_actual_y}A）：")
                        range_s = f"  区间{g_low:+.0f}%~{g_high:+.0f}%" if g_low is not None and g_high is not None else ""
                        lines.append(f"      中位数{g_median:+.1f}%{range_s}（推荐直接嵌入基准情景："
                                     f"「全年同比 {g_low if g_low else g_median-5:+.0f}~{g_high if g_high else g_median+5:+.0f}%」）")

            # --- 估值锚：合理市值测算（基于2026E一致预期 + 行业合理PE）---
            if cur_mv and len(years) >= 2:
                # 取2026E（或最近一个预测年）作为基准年
                forecast_years = [y for y in years if not yd[y].get('is_actual')]
                if forecast_years:
                    base_year = forecast_years[0]  # 最近预测年（通常是2026E）
                    base_profit = yd[base_year]['profit']
                    # 合理PE：用当前PE × (50%分位 / 当前分位) 反推历史中位PE
                    # 简化：用当前PE的0.7倍作为合理PE（假设分位30%左右时合理）
                    # 或者直接给出三档PE供参考
                    lines.append(f"\n  🎯 合理市值锚（基准={base_year}E一致预期净利 {base_profit:.2f}亿）：")
                    lines.append(f"  估值情景 | 目标PE | 合理市值(亿) | 对应股价(元) | 相对现价空间")
                    # 获取总股本以计算对应股价
                    total_share = None
                    try:
                        from storage.sqlite.stock_storage import get_db
                        _db = get_db()
                        basic_df = _db.get_latest_daily_basic_data(code, 1)
                        if basic_df is not None and not basic_df.empty:
                            total_share = float(basic_df.iloc[0].get('total_share') or 0) / 1e4  # 万股→亿股
                    except Exception:
                        pass
                    # 三档合理PE：保守/中性/乐观
                    pe_scenarios = [
                        ("保守(行业下沿)", 25),
                        ("中性(行业中枢)", 35),
                        ("乐观(成长溢价)", 50),
                    ]
                    for label, pe_target in pe_scenarios:
                        implied_mv = round(base_profit * pe_target, 1)
                        room = round((implied_mv / cur_mv - 1) * 100, 1) if cur_mv > 0 else 0
                        room_str = f"+{room}%" if room >= 0 else f"{room}%"
                        price_str = "-"
                        if total_share and total_share > 0:
                            target_price = round(implied_mv / total_share, 2)
                            price_str = f"{target_price}"
                        lines.append(f"  {label} | {pe_target}x | {implied_mv} | {price_str} | {room_str}")
                    lines.append(f"  ⚠️ 注：合理PE为参考值，需结合行业增速、公司护城河、市场风险偏好动态调整")

            # --- 估值敏感性矩阵（远期视角）---
            if cur_mv and len(years) >= 1:
                last_profit = yd[years[-1]]['profit']  # 最远期
                last_year = years[-1]
                lines.append(f"\n  估值敏感性（远期视角，基准={last_year}E净利 {last_profit:.2f}亿）：")
                lines.append(f"  情景          | 远期PE | 隐含市值(亿) | 相对当前空间")
                scenarios = [
                    ("乐观(超预期)", 80),
                    ("中性(符合预期)", 60),
                    ("悲观(低于预期)", 40),
                ]
                for label, pe_assumption in scenarios:
                    implied_mv = round(last_profit * pe_assumption, 1)
                    if cur_mv > 0:
                        room = round((implied_mv / cur_mv - 1) * 100, 1)
                        room_str = f"+{room}%" if room >= 0 else f"{room}%"
                    else:
                        room_str = "N/A"
                    lines.append(f"  {label} | {pe_assumption}x | {implied_mv} | {room_str}")

        # --- 全局回踩位基本面锚 ---
        lines.append("\n  基本面锚（回踩观察位校准）：PE历史中位数对应价格——技术位与基本面锚差异>30%时取更保守值")
        for code in sorted(forecasts.keys()):
            vm = val_map.get(code, {})
            cur_pe = vm.get("pe_ttm")
            pe_pct = vm.get("pe_percentile")
            name = find_company_name(code) or code
            # 估算历史中位数PE：用线性反推
            # 假设历史PE近似均匀分布，中位数PE ≈ 当前PE × 50% / 分位%
            # 锚定价位 = 现价 × (中位PE / 当前PE)
            # 现价 ≈ 市值 / 总股本，但更简单：从PE反推，anchor_price = cur_price × median_pe / cur_pe
            # 先找收盘价
            cur_price = None
            if cur_pe:
                try:
                    from tools.forecast import _latest_close
                    cur_price = _latest_close(code)
                except Exception:
                    pass
            if cur_pe and pe_pct is not None and pe_pct > 0 and cur_price and cur_price > 0:
                median_pe_est = cur_pe * 0.5 / (pe_pct / 100)
                anchor_price = cur_price * median_pe_est / cur_pe
                ratio = anchor_price / cur_price
                lines.append(f"  {name}({code}): 当前PE={cur_pe:.1f}倍(分位{pe_pct:.0f}%), "
                             f"估算中位PE≈{median_pe_est:.0f}倍, "
                             f"基本面锚价≈{anchor_price:.2f}(现价×{ratio:.2f})——"
                             f"技术支撑若低于锚价>30%则以锚价为准, 反之亦然")
        return "\n".join(lines)

    # ========== 通用工具 ==========

    def _do_search(self, queries: List[str]) -> Dict[str, str]:
        """并行搜索：所有查询同时发出，大幅缩短等待时间"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = {}
        max_workers = min(len(queries), 8)  # 最多8个并发
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(self._search_with_fallback, q): q for q in queries}
            for future in as_completed(future_map):
                q = future_map[future]
                ok = True
                try:
                    result = future.result()
                    results[q] = result
                    if isinstance(result, str) and result.startswith("搜索失败"):
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

    # ---------- ETF 分析 ----------

    @staticmethod
    def _build_etf_system_prompt(has_holdings: bool = True) -> str:
        today = date.today().strftime("%Y-%m-%d")

        holdings_rule = (
            "【持仓穿透规则】\n"
            "- 对前 5 大重仓股逐只做简要基本面分析（主营业务、近期业绩趋势、估值水平）\n"
            "- 评估每只股票对 ETF 净值的贡献风险（占比过高则集中度高，单一标的风险大）\n"
            "- 汇总判断：持仓组合的整体质量——是否涵盖龙头、是否有过度集中风险\n\n"
            "【输出要求】\n"
            "- 结构化、分维度的研究报告形式\n"
            "- 需要数据支撑的观点请注明数据来源\n"
            "- 重点标注折溢价异常（绝对值>1%）、份额大幅变动（>5%）、行业集中度\n"
            "- 持仓分析要给出具体评判，而非泛泛描述"
            if has_holdings else
            "【数据缺失说明】\n"
            "- 持仓穿透数据暂不可用，分析将聚焦于行情面（价格、折溢价、份额、成交额）\n"
            "- 以及行业配置（行业集中度与风格暴露）\n"
            "- 可基于 ETF 名称和追踪指数类型来推断其持仓风格（宽基/行业/主题）\n\n"
            "【输出要求】\n"
            "- 结构化、分维度的研究报告形式\n"
            "- 需要数据支撑的观点请注明数据来源\n"
            "- 重点标注折溢价异常（绝对值>1%）、份额大幅变动（>5%）、行业集中度\n"
            "- 明确标注哪些数据因数据源不可用而无法分析"
        )

        return f"""你是一个专业的 ETF 研究员和基金分析师。今天的日期是 {today}，请以此为时间基准判断"近期/最新"。

【分析框架——覆盖以下维度，逐项分析】
1. **ETF 基本信息**：名称、类型（宽基/行业/主题/跨境）、规模（AUM）、成立日期
2. **行情与折溢价**：最新价 vs IOPV（实时净值）、折溢价率、成交额、换手率
3. **份额与资金流向**：最新份额变动趋势、主力资金流向（净流入/净占比）
4. **行业配置**：前 5 大行业及其占比，判断行业集中度与风格
5. **前 5 大重仓股穿透**：逐只分析其基本面亮点与风险，评估持仓质量

{holdings_rule}

【信源优先级——必须遵守】
**程序采集数据（标记 🟢 T1）级别最高，代表今日 {today} 的实时数据**。
网络研究信息（⚪ T4）有明确回源日期标注时必须保留该日期，若无日期则标注"根据网络研究信息"。
**当程序采集数据存在时，禁止用网络研究信息覆盖或替代它**。
多个来源对同一指标有矛盾时，以更新日期为准，日期同以 🟢 T1 为准。
**业绩快报/定期报告等公告原文的数字用「根据公司公告」，禁止搞错来源等级。**"""



    def _build_etf_queries(self, etf_name: str, stock_code: str) -> List[str]:
        """ETF 模式的搜索 query，聚焦 ETF 自身指标而非个股经营数据。"""
        today = date.today()
        one_month = f"{today.year}年{today.month}月"
        three_months = f"{(today - timedelta(days=90)).strftime('%Y-%m')} {today.strftime('%Y-%m')}"
        tag = etf_name if etf_name and etf_name != stock_code else f"ETF {stock_code}"

        return [
            # ETF 自身净值与份额
            f"{tag} 基金规模 净值 IOPV 折溢价 成交额 {one_month}",
            f"{tag} 份额变化 资金流向 净申购 净赎回 {one_month}",
            # 跟踪标的指数
            f"{tag} 跟踪指数 标的指数 走势 行情 {three_months}",
            f"{tag} 估值 PE PB 历史分位 指数估值 {one_month}",
            # 重仓股与板块
            f"{tag} 前十大重仓股 权重 调仓 {today.year}",
            f"{tag} 重仓股 业绩 财报 利好 利空 {one_month}",
            # 行业/主题
            f"{tag} 所属行业 景气度 政策 板块轮动 {three_months}",
            # 资金面
            f"{tag} 主力资金 净流入 北向资金 机构持仓 {one_month}",
        ]

    def _analyze_etf(self, state: AgentState) -> Dict[str, Any]:
        """ETF 分析：行情数据 + 前 5 大重仓股穿透分析"""
        stock_code = state.get("stock_code", "")
        question = state.get("question", "")
        logger.info(f"研究 Agent（ETF 模式），代码: {stock_code}")

        # ---- 1. ETF 行情与基本信息 ----
        from tools.etf_tools import (
            fetch_etf_spot, fetch_etf_holdings, fetch_etf_industry_allocation,
            format_etf_report, calculate_etf_valuation)

        spot = fetch_etf_spot(stock_code)
        etf_name = spot.get("名称", stock_code) if spot else stock_code

        holdings = fetch_etf_holdings(stock_code, year=str(date.today().year))
        industry = fetch_etf_industry_allocation(stock_code, year=str(date.today().year))
        valuation_text = calculate_etf_valuation(holdings)  # 成分股加权 PE/PB
        etf_data_block = format_etf_report(spot, holdings, industry, valuation_text)
        logger.info(f"[ETF] 数据获取完成: spot={'Y' if spot else 'N'}, "
                    f"holdings={len(holdings)}, industry={len(industry)}")

        # ---- 2. 持有股票搜索 ----
        top5 = holdings[:5]
        holding_reports = []
        for h in top5:
            code = h.get("code", "")
            name = h.get("name", "")
            ratio = h.get("ratio", "")
            if not code:
                continue
            try:
                from tools.company_code_validator import find_company_name
                from tools.main_business import fetch_main_business_text
                cname = find_company_name(code) or name
                business = fetch_main_business_text(code)
                biz_snippet = business[:300] if business else "（无业务数据）"
                # 网页搜索补充
                q_results = self._do_search([
                    f"{cname} {name} 主营业务 营收 净利润 市盈率 2026",
                ])
                search_snippet = ""
                for q, r in q_results.items():
                    search_snippet = r[:500]
                    break
                holding_reports.append(
                    f"【持仓 {name}({code}) 占比{ratio}】\n"
                    f"公司全称：{cname}\n"
                    f"主营业务：{biz_snippet}\n"
                    f"搜索补充：{search_snippet}\n"
                )
            except Exception as e:
                logger.warning(f"[ETF] 重仓股 {code} 基础信息获取失败: {e}")
                holding_reports.append(f"【持仓 {name}({code}) 占比{ratio}】\n数据获取失败\n")

        holdings_block = "\n\n".join(holding_reports) if holding_reports else "（无持仓数据）"

        # ---- 3. 额外搜索补充（ETF 专用 query，不搜个股经营指标） ----
        extra_queries = self._build_etf_queries(etf_name, stock_code)
        extra_results = self._do_search(extra_queries)
        extra_text = self._search_text(extra_results)

        # ---- 4. LLM 综合（根据数据可用性调整 prompt） ----
        has_holdings = bool(holdings)
        prompt = self._build_etf_system_prompt(has_holdings=has_holdings)

        # holdings_block 可能有也可能空，都塞给 LLM（LLM 能看到"暂无数据"）
        messages = [
            SystemMessage(content=prompt),
            HumanMessage(content=f"""用户问题：{question}
ETF 代码：{stock_code}
ETF 名称：{etf_name}

========== ETF 行情与持仓数据（🟢 T1 程序采集） ==========
{etf_data_block}

========== 前 5 大重仓股穿透分析（程序采集） ==========
{holdings_block}

========== 全网搜索结果（⚪ T4 补充） ==========
{extra_text[:8000]}

【指令】
1. 🟢 T1 程序采集数据代表今日实时数据，必须作为报告核心依据
2. ⚪ T4 网络信息仅作补充，有日期标注的保留日期，无日期的标"根据网络研究信息"
3. T1 数据不足的维度（如份额趋势），再引用 T4 补充并标注"根据网络研究信息"
4. **当 T1 和 T4 对同一指标有数据时，以 T1 为准，禁止用 T4 替换 T1**"""),
        ]

        logger.info("ETF LLM 综合分析中...")
        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)

        return {
            "messages": [response],
            "research_result": {"summary": summary, "sources": extra_queries},
            "intermediate_steps": [("researcher", {"mode": "etf", "stock_code": stock_code})],
        }

    # ========== 统一入口 ==========

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            stock_code = state.get("stock_code", "")
            industry_name = state.get("industry_name", "")
            stock_type = state.get("stock_type", "")
            question = state.get("question", "")
            intent = state.get("intent", "")

            # 宏观分析意图（无具体个股）：拉取宏观数据快照 → 分析宏观事件对市场/行业的影响
            if intent == IntentType.MACRO and not stock_code:
                return self._analyze_macro(state)
            if industry_name and not stock_code:
                return self._analyze_industry(state)
            if stock_type == "etf":
                return self._analyze_etf(state)
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

    def _build_macro_system_prompt(self) -> str:
        return f"""你是一个专业的宏观经济分析师，擅长分析宏观事件对A股市场和各行业的影响。

{INTERMEDIATE_PRODUCT_NOTE}

【分析要求】
- 总体判断：用一句话定性当前宏观环境（偏多/中性/偏空）及核心逻辑
- 关键信号：列出 2-4 个最具信息量的宏观数据变化（利率/流动性/通胀/汇率等），每条带数据支撑
- 行业影响映射：宏观事件对哪些行业构成利好/利空，传导逻辑是什么
  （如降息利好高负债率行业/地产/券商；美债收益率上行压制成长股估值等）
- 市场情绪与资金面：大盘资金流向、北向资金、两融余额等反映的市场情绪
- 关注点：当前最值得跟踪的宏观变数（1-2条）

【输出原则】
- 所有结论必须基于提供的宏观数据快照与搜索结果，不足处标注「信息不足」
- 禁止给出具体的个股买卖建议（如"建议买入XX"）
- 宏观数据为程序拉取的真实值，引用时注明来源口径
- 历史胜率/统计只能说"历史统计"，禁止说"上涨概率"
"""

    def _analyze_macro(self, state: AgentState) -> Dict[str, Any]:
        """宏观分析：拉取宏观数据快照 → LLM 分析宏观事件对市场/行业的影响（不分析具体个股）"""
        question = state.get("question", "")
        logger.info(f"研究 Agent（宏观模式）")

        # ---- 宏观数据快照（程序拉取：资金流向/利率/美债/CPI/PMI/M2/社融等）----
        macro_text = ""
        try:
            from monitoring.macro_watcher import fetch_macro_snapshot
            macro_text = fetch_macro_snapshot()
            if macro_text:
                logger.info(f"宏观数据快照获取成功，长度: {len(macro_text)}")
            else:
                macro_text = "（宏观数据暂不可用）"
        except Exception as e:
            logger.warning(f"宏观数据获取失败（不影响分析）: {e}")
            macro_text = "（宏观数据暂不可用）"

        # ---- 网页搜索：宏观事件最新动态与解读 ----
        queries = [
            f"{question} 最新动态 影响 {date.today().year}",
            "央行 货币政策 降息 加息 MLF LPR 降准 最新",
            "CPI PMI 社融 M2 宏观经济 最新数据 解读",
            "美债收益率 汇率 A股 影响 最新",
        ]
        search_text = self._search_text(self._do_search(queries))

        messages = [
            SystemMessage(content=self._build_macro_system_prompt()),
            HumanMessage(content=f"""用户问题：{question}

========== 宏观数据快照（程序拉取，含资金流向/利率/美债收益率/CPI/PMI/M2/社融等） ==========
{macro_text[:8000]}

========== 全网搜索结果（宏观事件最新动态，T4） ==========
{search_text[:8000]}

请基于以上信息分析宏观环境及其对市场和行业的影响。"""),
        ]

        logger.info("LLM 宏观分析中...")
        response = self._llm_invoke_with_timeout(messages)
        if response is None:
            logger.warning("LLM 宏观分析超时，返回空结果")
            return {
                "messages": [],
                "research_result": {"summary": "LLM分析超时", "sources": queries, "mode": "macro"},
                "intermediate_steps": [("researcher", {"mode": "macro", "queries": len(queries)})],
            }
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"宏观分析完成，长度: {len(summary)}")

        return {
            "messages": [response],
            "research_result": {"summary": summary, "sources": queries, "mode": "macro"},
            "intermediate_steps": [("researcher", {"mode": "macro", "queries": len(queries)})],
        }

    def _analyze_stock(self, state: AgentState) -> Dict[str, Any]:
        stock_code = state.get("stock_code", "")
        question = state.get("question", "")
        logger.info(f"研究 Agent（个股模式），股票: {stock_code}")

        # 标的属性分类（周期股/成长股/防御股/价值股）→ 差异化分析重点
        # 优先从 state 读取 router 统一判定的结果，避免重复调用 classify_stock_attribute
        stock_attr = state.get("stock_attribute") or {}
        if not stock_attr:
            from tools.stock_classifier import classify_stock_attribute
            stock_attr = classify_stock_attribute(stock_code)
        attr_label = stock_attr.get("label", "未分类")
        logger.info(f"标的属性分类: {stock_code} → {stock_attr.get('type', 'unknown')}({attr_label})")
        attr_block = ""
        if stock_attr.get("type") != "unknown":
            attr_block = f"""
========== 标的属性分类（程序判定，指导分析重点） ==========
属性：{attr_label}（行业：{stock_attr.get('industry', '未知')}）
差异化分析重点：{stock_attr.get('key_metrics', '')}
估值方法指导：{stock_attr.get('valuation_method', '')}
{stock_attr.get('valuation_warning', '')}
"""

        # ---- 结构化信源（主）：东财新闻 / 巨潮公告 / 财联社快讯 ----
        from tools.info_sources import (
            fetch_stock_news, fetch_stock_announcements, fetch_cls_telegraph, format_info_block)
        from tools.source_tiers import TIER
        try:
            company_name = find_company_name(stock_code) or ""
        except Exception:
            company_name = ""
        from tools.main_business import fetch_main_business_text
        from tools.info_sources import fetch_sales_flash_text
        from tools.holder_events import fetch_holder_events_text
        from tools.social_media import fetch_social_media_text
        # 产销快报单独抽出来放 prompt 最前（强制挂公司概况/核心逻辑），不跟 structured_text 合流怕被截断
        sales_flash_block_raw = ""
        try:
            sales_flash_block_raw = fetch_sales_flash_text(stock_code)
        except Exception as e:
            logger.debug(f"产销快报拉取跳过: {e}")
        sales_flash_block = ""
        if sales_flash_block_raw and "没有找到" not in sales_flash_block_raw and len(sales_flash_block_raw) > 30:
            sales_flash_block = ("========== 产销快报公告原文（T1权威，必须挂到公司概况/核心逻辑段）"
                                 " ==========\n" + sales_flash_block_raw[:3000])
        structured_blocks = []
        for block in (
            # 产销快报已单独提取，此处不重复
            fetch_main_business_text(stock_code),
            fetch_holder_events_text(stock_code, company_name),
            format_info_block("巨潮公告（最近30天，重大事项第一手来源）",
                              fetch_stock_announcements(stock_code), with_content=False, tier=TIER.T1),
            format_info_block("东财个股新闻（最新15条）", fetch_stock_news(stock_code), tier=TIER.T2),
            format_info_block("财联社快讯（含该公司的条目）",
                              fetch_cls_telegraph(keywords=[company_name] if company_name else None, limit=10),
                              tier=TIER.T2),
        ):
            if block:
                structured_blocks.append(block)
        # 社交媒体（微博+公众号），比新闻更早释放经营信号
        try:
            social_text = fetch_social_media_text(stock_code, company_name)
            if social_text:
                structured_blocks.append(social_text)
        except Exception as e:
            logger.debug(f"社交媒体信息获取跳过（不影响主流程）: {e}")
        # 增量财经信息源（雪球/新浪财经/华尔街见闻/证券时报），补充专业财经视角
        try:
            from tools.financial_sources import fetch_financial_sources_text
            fin_text = fetch_financial_sources_text(stock_code, company_name)
            if fin_text:
                structured_blocks.append(fin_text)
        except Exception as e:
            logger.debug(f"财经信息源获取跳过（不影响主流程）: {e}")
        structured_text = "\n\n".join(structured_blocks) if structured_blocks else "（结构化信源暂无数据）"

        # ---- 并行采集：网页搜索 + 程序数据拉取（互不依赖，同时进行） ----
        from concurrent.futures import ThreadPoolExecutor
        collected = {}
        queries = self._build_stock_queries(stock_code)
        def _run_search():
            return self._search_text(self._do_search(queries))
        def _run_industry():
            try:
                from tools.data_router import fetch_industry_data
                r = fetch_industry_data(stock_code, company_name)
                if r.get("has_data"):
                    return (f"========== 行业专用数据源（{r.get('std_industry','')}）"
                            f" ==========\n{r.get('data_text','')}")
            except Exception as e:
                logger.debug(f"行业数据路由未命中: {e}")
            return ""
        def _run_capital():
            try:
                from tools.stock_capital_fetcher import fetch_all_capital_data
                from tools.stock_tools import call_fetch_moneyflow
                parts = []
                # 主力资金流向放最前（逐日明细+近5/10/20日累计，LLM 优先注意到）
                try:
                    mf = call_fetch_moneyflow(stock_code)
                    if mf and '❌' not in mf and len(mf) > 50:
                        parts.append(f"【主力资金流向（逐日明细+近5/10/20日累计，最新日在前）】\n{mf}")
                except Exception as e:
                    logger.debug(f"主力资金流向拉取失败: {e}")
                raw = fetch_all_capital_data([stock_code])
                if raw:
                    parts.append(raw)
                if parts:
                    return ("========== 资金筹码数据（程序拉取，含主力流向/北向/两融/股东户数/机构持仓/解禁）"
                            f" ==========\n" + "\n\n".join(parts))[:8000]
            except Exception as e:
                logger.warning(f"资金筹码拉取失败: {e}")
            return ""
        def _run_peer():
            try:
                from tools.peer_comparison import fetch_peer_comparison
                raw = fetch_peer_comparison(stock_code)
                if raw:
                    return ("========== 同业横向对标（程序根据东财板块成分股计算）"
                            f" ==========\n{raw[:5000]}")
            except Exception as e:
                logger.warning(f"同业对标拉取失败: {e}")
            return ""
        def _run_sensitivity_sotp():
            try:
                import pandas as pd
                base_profit = None
                base_year = None
                base_period_type = None
                try:
                    from storage.sqlite.stock_storage import get_db
                    db = get_db()
                    inc_df = db.get_stock_income(stock_code)
                    if inc_df is not None and not inc_df.empty:
                        inc_df = inc_df.copy()
                        inc_df['_rd'] = pd.to_datetime(inc_df['report_date'], errors='coerce')
                        ann_df = inc_df[inc_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False)
                        if not ann_df.empty:
                            last_annual = ann_df.iloc[0]
                            np_val = last_annual.get('net_profit')
                            if np_val is not None and float(np_val) > 0:
                                base_profit = round(float(np_val) / 1e8, 2)
                                base_year = int(last_annual['_rd'].year)
                                base_period_type = '年报'
                except Exception:
                    pass
                if base_profit is None:
                    from tools.stock_tools import call_fetch_fina_indicator
                    raw_fina = call_fetch_fina_indicator(stock_code)
                    import re
                    np_match = re.search(r'(?:归母净利润|净利润)[：:]\s*([\d.]+)\s*亿', str(raw_fina))
                    base_profit = float(np_match.group(1)) if np_match else None
                from tools.main_business import fetch_main_business_text
                mb_text = fetch_main_business_text(stock_code)
                segments = []
                if mb_text and base_profit:
                    lines = [l.strip() for l in mb_text.split('\n') if l.strip()]
                    seg_lines = [l for l in lines if any(k in l for k in ('收入', '占比', '%', '业务', '产品'))][:5]
                    if seg_lines:
                        n = len(seg_lines)
                        for sl in seg_lines:
                            segments.append({"name": sl[:20], "profit": round(base_profit / n, 2),
                                             "pe_assumed": 15, "weight": round(1.0 / n, 2)})
                    else:
                        segments.append({"name": "主营业务", "profit": base_profit, "pe_assumed": 15, "weight": 1.0})
                blocks = []
                if base_profit and base_profit > 0:
                    base_label = f"{base_year}年{base_period_type}" if base_year and base_period_type else "最新报告期"
                    from tools.sensitivity_analysis import build_sensitivity_table
                    sens_vars = [
                        {"name": "营收增速变动", "impact": round(base_profit * 0.3, 2),
                         "unit": "亿元/±1%增速", "range": [-20, -10, 0, 10, 20]},
                        {"name": "毛利率变动", "impact": round(base_profit * 0.15, 2),
                         "unit": "亿元/±1%毛利率", "range": [-5, -2, 0, 2, 5]},
                    ]
                    sens = build_sensitivity_table(base_profit, sens_vars)
                    if sens:
                        blocks.append(f"========== 敏感性分析（百分比弹性，基准={base_label}净利润{base_profit}亿） ==========\n{sens}")
                    car_sens_vars = [
                        {"name": "欧盟反补贴关税", "impact": round(base_profit * 0.1, 2),
                         "unit": "亿元/±10%关税强度", "range": [-40, -20, 0, 20, 40]},
                        {"name": "锂/原材料价格", "impact": round(base_profit * 0.05, 2),
                         "unit": "亿元/±10%原材料价格", "range": [-30, -15, 0, 15, 30]},
                        {"name": "整车毛利率(1pct)", "impact": round(base_profit * 0.12, 2),
                         "unit": "亿元/±1pct毛利率", "range": [-3, -1, 0, 1, 3]},
                        {"name": "海外月销(万辆)", "impact": round(base_profit * 0.15, 2),
                         "unit": "亿元/±1万辆月销", "range": [-10, -5, 0, 5, 10]},
                    ]
                    car_sens = build_sensitivity_table(base_profit, car_sens_vars)
                    if car_sens:
                        blocks.append(f"========== 核心情景敏感性（关税/原材料/毛利率/海外月销，基准={base_label}净利润{base_profit}亿）==========\n"
                                      f"{car_sens}\n"
                                      f"**说明**：关税每±10%强度±{round(base_profit*0.1,2)}亿；"
                                      f"毛利率±1pct±{round(base_profit*0.12,2)}亿；"
                                      f"海外月销±1万辆±{round(base_profit*0.15,2)}亿")
                if segments:
                    from tools.sotp_valuation import build_sotp_valuation
                    import akshare as ak
                    try:
                        spot = ak.stock_zh_a_spot_em()
                        sr = spot[spot["代码"].astype(str).str.strip() == stock_code]
                        total_mv = float(sr.iloc[-1].get("总市值", 0)) / 1e8 if not sr.empty else 500.0
                    except Exception:
                        total_mv = 500.0
                    sotp = build_sotp_valuation(segments, total_mv)
                    if sotp:
                        blocks.append(f"========== SOTP 分部估值（程序按主营构成+财报数据推算） ==========\n{sotp}")
                return "\n\n".join(blocks) if blocks else ""
            except Exception as e:
                logger.warning(f"敏感性/SOTP推算失败: {e}")
                return ""
        def _run_nine_turn():
            try:
                from tools.magic_nine_turn import fetch_magic_nine_turn
                raw = fetch_magic_nine_turn(stock_code, months=6)
                if raw:
                    return ("========== 神奇九转（TD Sequential，程序根据K线自动计算）"
                            f" ==========\n{raw[:4000]}")
            except Exception as e:
                logger.warning(f"神奇九转获取失败: {e}")
            return ""
        def _run_extra_stock():
            try:
                from tools.stock_tools import (
                    call_fetch_forecast, call_fetch_express,
                    call_fetch_dividend_data, call_fetch_report_rc,
                    call_fetch_holder_trade,
                    call_fetch_pledge, call_fetch_block_trade,
                    call_fetch_repurchase,
                )
                parts = []
                for name, func in [
                    ("业绩预告", call_fetch_forecast), ("业绩快报", call_fetch_express),
                    ("分红送股", call_fetch_dividend_data), ("卖方盈利预测", call_fetch_report_rc),
                    ("股东增减持", call_fetch_holder_trade),
                    ("股权质押", call_fetch_pledge), ("大宗交易", call_fetch_block_trade),
                    ("股票回购", call_fetch_repurchase),
                ]:
                    try:
                        t = func(stock_code)
                        if t and t.strip(): parts.append(f"【{name}】\n{t.strip()[:2000]}")
                    except Exception:
                        pass
                if parts:
                    return "========== 补充个股基本面数据（程序拉取） ==========\n" + "\n\n".join(parts)
            except Exception as e:
                logger.warning(f"补充个股数据拉取失败: {e}")
            return ""

        def _run_consensus_forecast():
            try:
                forecasts = self._fetch_consensus_forecasts([stock_code])
                consensus_text = ""
                if forecasts and stock_code in forecasts:
                    consensus_text = self._format_forecast_table(forecasts)
                # Tushare report_rc 配额耗尽或 DB 无缓存时，用 akshare 多源盈利预测兜底
                # （东财+同花顺，含机构家数/EPS/净利/forward PE/PEG）
                if not consensus_text:
                    try:
                        from tools.forecast import fetch_profit_forecast_text
                        fb = fetch_profit_forecast_text(stock_code, company_name)
                        if fb and '不可用' not in fb and len(fb) > 50:
                            consensus_text = ("========== 机构盈利预测（akshare 多源兜底，"
                                              "Tushare report_rc 配额受限时的回退） ==========\n" + fb)
                            logger.info("一致预期走 akshare 多源兜底（report_rc 配额/缓存不可用）")
                    except Exception as e:
                        logger.debug(f"akshare 盈利预测兜底失败: {e}")
                return consensus_text
            except Exception as e:
                logger.warning(f"一致预期拉取失败: {e}")
            return ""

        def _run_financials():
            try:
                from tools.stock_tools import (
                    call_fetch_income_data, call_fetch_cashflow_data,
                    call_fetch_balance_sheet_data,
                )
                parts = []
                for name, func in [
                    ("利润表（营收/净利/研发/销售/管理费用分拆）", call_fetch_income_data),
                    ("现金流量表（资本开支/自由现金流）", call_fetch_cashflow_data),
                    ("资产负债表（存货/应收账款/总资产）", call_fetch_balance_sheet_data),
                ]:
                    try:
                        t = func(stock_code)
                        if t and '❌' not in t and len(t) > 50:
                            parts.append(f"【{name}】\n{t.strip()[:3000]}")
                    except Exception as e:
                        logger.debug(f"三大报表[{name}]拉取失败: {e}")
                if parts:
                    return ("========== 三大财务报表（程序拉取，T1权威）"
                            " ==========\n" + "\n\n".join(parts))
            except Exception as e:
                logger.warning(f"三大报表拉取失败: {e}")
            return ""

        # 宏观环境注入：仅当问题含宏观关键词时拉取（避免无关个股分析额外开销），
        # 用于"降息对XX股票影响"这类宏观+个股混合问题
        macro_keywords = ["降息", "加息", "mlf", "lpr", "cpi", "pmi", "m2", "社融",
                          "美债", "国债收益率", "汇率", "宏观", "货币政策", "降准"]
        need_macro = any(kw in question.lower() for kw in macro_keywords)

        def _run_macro():
            try:
                from monitoring.macro_watcher import fetch_macro_snapshot
                text = fetch_macro_snapshot()
                if text:
                    return ("========== 宏观环境快照（程序拉取，含资金流向/利率/美债收益率/CPI/PMI/M2/社融等）"
                            f" ==========\n{text[:6000]}")
            except Exception as e:
                logger.debug(f"宏观数据获取跳过（不影响主流程）: {e}")
            return ""

        def _run_financial_snapshot():
            """财务关键指标快照（程序直接提取+计算，放 prompt 最前，LLM 直接引用不准心算）"""
            try:
                from tools.stock_tools import call_extract_financial_snapshot
                s = call_extract_financial_snapshot(stock_code)
                if s and len(s) > 50:
                    return s
            except Exception as e:
                logger.debug(f"财务关键指标快照跳过: {e}")
            return ""

        with ThreadPoolExecutor(max_workers=11) as _exe:
            _futs = {
                _exe.submit(_run_search): 'search',
                _exe.submit(_run_industry): 'industry',
                _exe.submit(_run_capital): 'capital',
                _exe.submit(_run_peer): 'peer',
                _exe.submit(_run_sensitivity_sotp): 'sens_sotp',
                _exe.submit(_run_nine_turn): 'nine_turn',
                _exe.submit(_run_extra_stock): 'extra_stock',
                _exe.submit(_run_consensus_forecast): 'consensus',
                _exe.submit(_run_financials): 'financials',
                _exe.submit(_run_financial_snapshot): 'financial_snapshot',
            }
            if need_macro:
                _futs[_exe.submit(_run_macro)] = 'macro'
                logger.info("检测到宏观关键词，并行拉取宏观环境快照注入个股分析")
            for _future in as_completed(_futs):
                _key = _futs[_future]
                try:
                    collected[_key] = _future.result()
                except Exception as e:
                    logger.warning(f"并行采集[{_key}]异常: {e}")
                    collected[_key] = ""

        search_text = collected.get('search', '')
        industry_section = collected.get('industry', '')
        capital_block = collected.get('capital', '')
        peer_block = collected.get('peer', '')
        sensitivity_sotp_block = collected.get('sens_sotp', '')
        nine_turn_block = collected.get('nine_turn', '')
        extra_stock_block = collected.get('extra_stock', '')
        consensus_forecast_block = collected.get('consensus', '')
        financials_block = collected.get('financials', '')
        financial_snapshot_block = collected.get('financial_snapshot', '')
        macro_block = collected.get('macro', '')

        # 注入历史复盘教训（误判模式 + 改进规则），避免重复同类错误
        review_lesson = self._format_review_lesson(stock_code)
        review_block = f"\n========== 历史复盘教训 ==========\n{review_lesson}\n" if review_lesson else ""

        # ---- 结构化数据提取（依赖搜索结果，顺序执行） ----
        structured_data = self._extract_structured_data_from_search(search_text, company_name=company_name)
        structured_data_block = self._format_structured_data_block(structured_data)
        has_structured = bool(structured_data.get("time_series") or structured_data.get("key_figures"))
        structured_section = ""
        if has_structured:
            logger.info(f"结构化数据提取成功，共 {len(structured_data.get('time_series',[]))} 条时序")
            structured_section = ("========== 程序提取的结构化数据（时序数据+关键数字）"
                                  " ==========\n" + structured_data_block)

        messages = [
            SystemMessage(content=self._build_stock_system_prompt()),
            HumanMessage(content=f"""用户问题：{question}
股票代码：{stock_code}{f'（{company_name}）' if company_name else ''}

{attr_block}

{sales_flash_block}

{financial_snapshot_block}

========== 信源优先级规则 ==========
🟢 T1 权威（公告/财报/年报实际值/认证官方社交）> 🔵 T2 结构化（一致预期/财经媒体）> 🟡 T3 未验证社交 > ⚪ T4 网络搜索
- 高等级信源与低等级信源数据不一致时，以高等级为准并标注差异
- 销量/产销类数字：提供了【产销快报公告原文】时**只能引用该原文的数字**并注明"根据公司公告"
- 若上方存在【产销快报公告原文】区块，**必须将其关键数据（当月销量/同比/环比、出口量、累计同比、高端占比等）
  嵌入到「公司概况 / 核心逻辑」段的第一或第二句，不可只在"运营数据"段单独陈列**
- 运营数据（销量/出货量等）优先引用【程序提取的结构化数据】区块中的时序数字
- **净利润基准锚定规则**：【候选公司一致预期数据】区块中带 ★ 标记的年份为年报实际值（T1 权威），所有情景推演、同比计算、估值分析必须以此为基准，不得使用搜索结果中的估算值或过时数据

========== 结构化信源 ==========
{structured_text[:8000]}

{structured_section}

{industry_section}

{consensus_forecast_block}

{financials_block}

{capital_block}

{peer_block}

{sensitivity_sotp_block}

{macro_block}

{nine_turn_block}

{extra_stock_block}

{review_block}

========== 全网搜索结果（补充信息，T4，已精简保关键摘要） ==========
{search_text[:7000]}
请基于以上信息进行全面分析。"""),
        ]

        logger.info("LLM 综合分析中...")
        response = self._llm_invoke_with_timeout(messages)
        if response is None:
            logger.warning("LLM 综合分析超时，返回空结果")
            return {
                "messages": [],
                "research_result": {"summary": "LLM分析超时", "sources": queries},
                "intermediate_steps": [("researcher", {"mode": "stock", "stock_code": stock_code, "queries": len(queries)})],
            }
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
            "stock_attribute": stock_attr,
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

        # Step 2.5：产业链覆盖度校验（映射到12标准环节，缺环节自动补充）
        coverage_warning, new_leaders, all_leader_codes = self._validate_chain_coverage(
            industry_name, chain, all_leader_codes)
        logger.info(f"覆盖度check: {coverage_warning.split(chr(10))[0]}")

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

        # 覆盖度校验结果
        chain_summary += f"\n{coverage_warning}" if coverage_warning else ""

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
            # 降级：明确说明指数数据缺失，禁止LLM自行编造
            index_block = ("【行业指数表现】程序获取失败，无法提供近5/20/60日指数涨跌幅数据。\n"
                           "⚠️ 指数行情是判断板块 beta 趋势的事实依据，获取失败意味着走势脱锚。\n"
                           "   LLM 禁止凭记忆或推断编造指数涨跌幅数字，仅提供定性描述。")

        # 公司→环节扁平映射（程序拼表，LLM 直接引用填入全景表格，防止 LLM 自行映射出错）
        code_to_segment = {}  # {code: segment_name}
        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            for seg_data in chain.get(level, []):
                seg = seg_data.get("segment", "")
                for l in seg_data.get("leaders", []):
                    c = str(l.get("code", "")).strip()
                    if c:
                        code_to_segment[c] = seg
        company_chain_map_lines = ['========== 公司→环节映射（每家公司所属产业链环节，LLM必须引用此映射填写全景表格的"环节"列） ==========']
        for c in sorted(all_leader_codes):
            seg = code_to_segment.get(c, "未分类")
            seg_alt = {v: k for k, v in _CHAIN_SEGMENT_ALIASES.items()}.get(seg, seg)
            company_chain_map_lines.append(f"  {c} → {seg_alt}")
        company_chain_map = "\n".join(company_chain_map_lines)

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

        # ---- 候选标的关键财务/筹码/估值数据（程序拉取）----
        stock_snapshot_text = ""
        try:
            from tools.stock_tools import (
                call_fetch_fina_indicator, call_fetch_financial_health_summary,
                call_fetch_holder_number, call_fetch_northbound_hold,
                call_fetch_cost_basis, call_fetch_cashflow_data,
                call_fetch_income_data, call_fetch_balance_sheet_data,
                call_fetch_batch_valuation, call_fetch_batch_sotp_valuation,
            )
            snapshot_blocks = []

            # 先做一次批量估值对比（所有候选代码一起，不用按个股重复）
            batch_val_text = ""
            try:
                codes_str = ",".join(sorted(all_leader_codes)[:10])
                if codes_str:
                    r = call_fetch_batch_valuation(codes_str)
                    if r and '❌' not in r and len(r) > 50:
                        batch_val_text = f"◇ 同业估值对比（来源: batch_valuation_fetcher）\n{r.strip()[:3000]}"
            except Exception:
                pass
            if batch_val_text:
                snapshot_blocks.append(batch_val_text)

            for c in sorted(all_leader_codes)[:12]:
                snap_lines = [f"◇ 候选 {c}"]
                # 利润表（营收/净利/研发费用/销售费用/管理费用分拆）
                try:
                    r = call_fetch_income_data(c)
                    if r and '❌' not in r and len(r) > 50:
                        snap_lines.append(r.strip().split('\n')[:15])
                    else:
                        snap_lines.append("  ⚠️ 利润表数据获取失败")
                except Exception:
                    snap_lines.append("  ⚠️ 利润表数据获取失败")
                # 财务指标（ROE/毛利率/费用率）
                try:
                    r = call_fetch_fina_indicator(c)
                    if r and '❌' not in r and len(r) > 50:
                        snap_lines.append(r.strip().split('\n')[:20])
                    else:
                        snap_lines.append("  ⚠️ 财务指标数据获取失败")
                except Exception:
                    snap_lines.append("  ⚠️ 财务指标数据获取失败")
                # 资产负债表（存货/应收账款/总资产等）
                try:
                    r = call_fetch_balance_sheet_data(c)
                    if r and '❌' not in r and len(r) > 50:
                        snap_lines.append(r.strip().split('\n')[:12])
                    else:
                        snap_lines.append("  ⚠️ 资产负债表数据获取失败")
                except Exception:
                    snap_lines.append("  ⚠️ 资产负债表数据获取失败")
                # 现金流量表（资本开支/CAPEX/自由现金流）
                try:
                    r = call_fetch_cashflow_data(c)
                    if r and '❌' not in r and len(r) > 50:
                        snap_lines.append(r.strip().split('\n')[:12])
                    else:
                        snap_lines.append("  ⚠️ 现金流量表数据获取失败")
                except Exception:
                    snap_lines.append("  ⚠️ 现金流量表数据获取失败")
                # 财务健康度（周转天数/FCF/杜邦等）
                try:
                    r = call_fetch_financial_health_summary(c)
                    if r and '❌' not in r and len(r) > 50:
                        snap_lines.append(r.strip().split('\n')[:15])
                    else:
                        snap_lines.append("  ⚠️ 财务健康度数据获取失败")
                except Exception:
                    snap_lines.append("  ⚠️ 财务健康度数据获取失败")
                # 筹码成本估算
                try:
                    r = call_fetch_cost_basis(c)
                    if r and '❌' not in r and len(r) > 50:
                        snap_lines.append(r.strip().split('\n')[:10])
                    else:
                        snap_lines.append("  ⚠️ 筹码成本数据获取失败")
                except Exception:
                    snap_lines.append("  ⚠️ 筹码成本数据获取失败")
                snapshot_blocks.append("\n".join(snap_lines))
            stock_snapshot_text = "\n\n".join(snapshot_blocks)
        except Exception as e:
            logger.warning(f"候选标的关键数据拉取失败（不影响分析）: {e}")

        # 核心财务指标摘要（直接从DB cache提取，独立于API调用）
        # 用户要求：每只候选股至少输出营收/归母净利/毛利率/净利率/3年复合增速
        # 缺失任一项触发"数据缺口"警告而非沉默跳过
        core_fin_block = ""
        try:
            fin_lines = ["========== 候选公司核心财务指标（程序从DB cache提取，非LLM编造） =========="]
            fin_lines.append("说明：最新年报(2025A) + 最新季报(2026Q1) 双列展示，毛利率/净利率/ROE 均来自正式财报")
            from storage.sqlite.stock_storage import get_db
            _db = get_db()
            for c in sorted(all_leader_codes)[:12]:
                c_fin = []
                # 从DB拉取利润表+财务指标+资产负债表
                try:
                    inc_df = _db.get_stock_income(c)
                    fina_df = _db.get_stock_fina_indicator(c)
                    bs_df = _db.get_stock_balance_sheet(c)
                except Exception:
                    inc_df, fina_df, bs_df = None, None, None
                # ===== 提取最新年报数据（12月31日）=====
                ann_rev, ann_np, ann_gm, ann_nm, ann_roe = None, None, None, None, None
                ann_date = None
                q_rev, q_np, q_rev_yoy, q_np_yoy = None, None, None, None
                q_date = None
                if inc_df is not None and not inc_df.empty:
                    inc_df = inc_df.copy()
                    inc_df['_rd'] = pd.to_datetime(inc_df['report_date'], errors='coerce')
                    # 最新年报
                    ann_rows = inc_df[inc_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False)
                    if not ann_rows.empty:
                        ann = ann_rows.iloc[0]
                        ann_date = ann['_rd']
                        ann_rev = round(float(ann.get('total_revenue') or 0) / 1e8, 2) if ann.get('total_revenue') is not None else None
                        ann_np = round(float(ann.get('net_profit') or 0) / 1e8, 2) if ann.get('net_profit') is not None else None
                        ann_gm = round(float(ann.get('gross_margin') or 0), 1) if ann.get('gross_margin') is not None else None
                    # 最新季报
                    if len(inc_df) > 0:
                        q = inc_df.iloc[0]
                        q_date = q.get('_rd')
                        if q_date and (q_date.month != 12 or (ann_date and q_date > ann_date)):
                            q_rev = round(float(q.get('total_revenue') or 0) / 1e8, 2) if q.get('total_revenue') is not None else None
                            q_np = round(float(q.get('net_profit') or 0) / 1e8, 2) if q.get('net_profit') is not None else None
                            q_rev_yoy = round(float(q.get('revenue_growth') or 0), 1) if q.get('revenue_growth') is not None else None
                            q_np_yoy = round(float(q.get('profit_growth') or 0), 1) if q.get('profit_growth') is not None else None
                # 财务指标（净利率、ROE）
                if fina_df is not None and not fina_df.empty:
                    fina_df = fina_df.copy()
                    fina_df['_rd'] = pd.to_datetime(fina_df['report_date'], errors='coerce')
                    # 找最新年报期的fina指标
                    if ann_date is not None:
                        ann_fina = fina_df[fina_df['_rd'] == ann_date]
                        if not ann_fina.empty:
                            frow = ann_fina.iloc[0]
                            # netprofit_margin 是小数（如0.12=12%），需要×100
                            nm_raw = frow.get('netprofit_margin') or frow.get('net_margin')
                            if nm_raw is not None:
                                nm_val = float(nm_raw)
                                ann_nm = round(nm_val * 100, 1) if nm_val <= 1.0 else round(nm_val, 1)
                            roe_raw = frow.get('roe')
                            if roe_raw is not None:
                                roe_val = float(roe_raw)
                                ann_roe = round(roe_val * 100, 1) if roe_val <= 1.0 else round(roe_val, 1)
                # 3年营收复合增速（用近3年年报营收计算）
                cagr = None
                if inc_df is not None and not inc_df.empty:
                    ann_rows2 = inc_df[inc_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False).head(4)
                    if len(ann_rows2) >= 4:
                        revs = [float(r.get('total_revenue') or 0) for _, r in ann_rows2.iterrows()]
                        if revs[-1] > 0:
                            cagr = round(((revs[0] / revs[-1]) ** (1/3) - 1) * 100, 1)
                # ===== 组装输出 =====
                line1 = f"◆ {c}"
                if ann_date is not None:
                    line1 += f"  {ann_date.year}年报"
                parts = []
                if ann_rev is not None: parts.append(f"营收{ann_rev}亿")
                if ann_np is not None: parts.append(f"归母净利{ann_np}亿")
                if ann_gm is not None: parts.append(f"毛利率{ann_gm}%")
                if ann_nm is not None: parts.append(f"净利率{ann_nm}%")
                if ann_roe is not None: parts.append(f"ROE{ann_roe}%")
                if cagr is not None: parts.append(f"营收3年CAGR{cagr}%")
                if parts:
                    line1 += ": " + " / ".join(parts)
                c_fin.append(line1)
                # 最新季报
                if q_date is not None:
                    q_parts = []
                    if q_rev is not None: q_parts.append(f"营收{q_rev}亿")
                    if q_rev_yoy is not None: q_parts.append(f"营收同比{'+' if q_rev_yoy >= 0 else ''}{q_rev_yoy}%")
                    if q_np is not None: q_parts.append(f"归母净利{q_np}亿")
                    if q_np_yoy is not None: q_parts.append(f"净利同比{'+' if q_np_yoy >= 0 else ''}{q_np_yoy}%")
                    if q_parts:
                        q_label = f"{q_date.year}Q{q_date.month}" if q_date.month in (3, 6, 9) else f"{q_date.year}{q_date.month}月"
                        c_fin.append(f"    {q_label}: " + " / ".join(q_parts))
                # 数据缺口检查
                missing = []
                if ann_rev is None: missing.append("年报营收")
                if ann_np is None: missing.append("年报净利")
                if ann_gm is None: missing.append("年报毛利率")
                if ann_nm is None: missing.append("年报净利率")
                if ann_roe is None: missing.append("年报ROE")
                if cagr is None: missing.append("3年CAGR")
                if missing:
                    c_fin.append(f"    ⚠️ 数据缺口：{'/'.join(missing)}")
                fin_lines.extend(c_fin)
            core_fin_block = "\n".join(fin_lines)
        except Exception as e:
            logger.warning(f"核心财务指标提取失败（不影响分析）: {e}")
            core_fin_block = "========== 核心财务指标提取失败，使用上述API调用数据 =========="

        # 资金筹码数据（程序拉取北向/两融/股东户数/机构持仓/解禁）
        capital_block = ""
        try:
            from tools.stock_capital_fetcher import fetch_all_capital_data
            codes_str = sorted(all_leader_codes)[:10]
            if codes_str:
                capital_block_raw = fetch_all_capital_data(codes_str)
                if capital_block_raw:
                    capital_block = f"========== 候选公司资金筹码数据（程序拉取，含北向/两融/股东户数/机构持仓/解禁） ==========\n{capital_block_raw[:8000]}"
        except Exception as e:
            logger.warning(f"资金筹码数据拉取失败（不影响分析）: {e}")

        # 一致预期数据（3年盈利预测+PE Band）
        forecast_raw = self._fetch_consensus_forecasts(list(all_leader_codes))
        per_stock_valuation = (industry_valuation or {}).get("per_stock", []) if industry_valuation else []
        forecast_block = self._format_forecast_table(forecast_raw, per_stock_valuation) if forecast_raw else ""

        # 全景搜索（含不可替代性/溢价能力维度）
        all_queries = self._build_chain_queries(industry_name, chain)
        all_results = self._do_search(all_queries)
        search_text = self._search_text(all_results)

        # 产业链历史复盘教训注入（误判模式 + 通用改进规则，避免重复同类错误）
        try:
            from agents.prompts_common import format_review_lesson
            industry_review_lesson = format_review_lesson(industry_name=industry_name)
        except Exception:
            industry_review_lesson = ""
        industry_review_block = (f"========== 历史复盘教训 ==========\n"
                                 f"{industry_review_lesson}\n") if industry_review_lesson else ""

        messages = [
            SystemMessage(content=self._build_chain_system_prompt(stage, stage_reason)),
            HumanMessage(content=f"""用户问题：{question}
目标行业：{industry_name}（行业阶段：{stage}{f'，{stage_reason}' if stage_reason else ''}）

{industry_review_block}
========== 信源优先级规则 ==========
🟢 T1 权威（公告/财报/认证官方社交）> 🔵 T2 结构化（财经媒体）> 🟡 T3 未验证社交 > ⚪ T4 网络搜索
- 高等级信源与低等级信源数据不一致时，以高等级为准并标注差异
- 【程序提取的结构化数据】（主营构成/财务指标/估值数据）置信度高于搜索结果中的碎片信息

{cls_block if cls_block else ''}

{index_block if index_block else ''}

{valuation_block if valuation_block else ''}

{company_chain_map if company_chain_map else ''}

========== 产业链结构（上中下游+特精专新+细分领域+龙一龙二） ==========
{chain_summary}

========== 候选公司主营业务构成（程序拉取，各业务收入/利润占比与毛利率，当前利润驱动依据） ==========
{mb_text[:8000] if mb_text else '（未获取到，业务拆解只能依据搜索结果，缺数据处标注「信息不足」）'}

========== 候选公司关键财务数据快照（程序拉取，含利润表/财务指标/资产负债表/现金流/资本开支/健康度/股东户数/北向资金/筹码成本+同业估值对比） ==========
{stock_snapshot_text[:10000] if stock_snapshot_text else '（未获取到个股级关键数据）'}

{core_fin_block if core_fin_block else ''}

{capital_block if capital_block else ''}

========== 全网搜索结果（含经营/竞争力/业务拆解/收入毛利占比/出货量/资本开支/新增订单/技术突破/护城河） ==========
{search_text[:15000]}

{forecast_block if forecast_block else ''}

请按**标准化7节结构**输出（这是固定模板，每期报告必须严格遵循）：

**第1节 结论与操作（放最前面）**
- 包含：〇、JSON 候选清单+分项评分+重估触发条件（必须是最先输出的部分，防止截断丢失）
- 方向性判断（偏多/中性/偏空）、综合排名、仓位建议、回踩观察位（双重校验）、触发条件清单

**第2节 产业链全景图**
- 产业链上中下游结构 + 环节完整度自检（覆盖了12个标准环节中的几个、缺了哪几个、已自动补充）
- 全景筛选结果：用**简单表格**列出各环节龙头公司及所属环节（表头：环节 | 公司名称 | 代码 | 核心业务 | 资金偏好）
  ⚠️ **不要在此表格中包含评分列（业务分/基本面分/护城河分/边际变化分/综合分/PE分位/拥挤度等）**
  — 评分排名表由程序在报告末尾自动生成（含加权计算+PE分位调整），你在JSON中给出分项评分即可

**第3节 关键环节深度分析**
- 环节利润迁移判断（当前瓶颈在哪、利润正在向哪个环节集中、未来2-4季度的迁移方向）
- 技术路径博弈分析（各路线定位/份额变化/卡脖子环节）
- 量产进度→业绩兑现映射表

**第4节 候选公司详情（核心差异内容）**
对每只候选公司，按以下结构输出（每只2-3页级别）：
- **公司概况与业务拆解**：主营构成数据（各业务收入/利润占比与毛利率）、出货量/产能、客户集中度
- **基本面与护城河评分**：产业链地位、近期经营表现、不可替代性、溢价能力，含双向依据
- **核心财务指标**：引用上方【核心财务指标】程序块中的营收/归母净利/毛利率/净利率/营收CAGR数据
  （⚠️ 禁止写"该维度未覆盖"——以上数据由程序直接从DB提取，有缺口时已标注⚠️数据缺口，照实引用即可）
- **财务预测表**：3年一致预期（营收/净利/增速/Forward PE/机构覆盖数），引用下方【一致预期数据】程序块
- **估值锚**：当前PE+PE分位+双阈值预警（🔴/🟡/🟢）+ 估值敏感性矩阵（乐观/中性/悲观情景）
- **回踩观察位（双重校验）**：技术位（程序计算）+ 基本面锚（PE历史中位对应价格）+ 校准取保守值
- **催化跟踪**：针对该标的的三级催化事件（对应第5节的催化时间轴）

**第5节 行业趋势与催化时间轴**
- 行业近况与重大事件（近1-3个月已发生的行情驱动）
- 催化事件分级体系（未来3-6个月）：按一级β/二级α/三级个股分级，每条含概率/影响幅度/兑现时间窗

**第6节 风险提示**
- 个股特有风险：客户集中度、业务占比不足、订单波动、产能爬坡不及预期等
- 环节风险：该环节的技术替代、竞争加剧、利润率下行
- 系统性风险：行业周期位置、政策与地缘风险、估值水位

**第7节 数据准确性声明**
- 信源清单：哪些数据来自 Tushare/Akshare 程序拉取，哪些来自网页搜索
- 数据交叉验证结果：毛利率/净利率等多信源比对，差异>5%的冲突标记
- 估值数据时效说明：PE/TTM 数据截止日期
- 预测数据性质声明：一致预期来自机构汇总，预测≠事实，仅供参考

资金偏好只在搜索结果里有北向/龙虎榜/机构调研等公开证据时标注并写明出处，
无证据写「无公开数据」，禁止凭板块印象填"主力/游资"。

**重要原则**：
- 候选公司清单 JSON 必须是输出的**第一部分**（输出过长被截断时后面的段落可丢，JSON 不能丢）
- 所有结论必须基于搜索结果，不足处标注「信息不足」
- 每只公司分析必须同时列出核心看多逻辑和核心看空风险
- 特精专新企业的不可替代性是其核心竞争力，需重点分析
- 候选公司清单中的股票代码必须准确"""),
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
                ranked, gate_watch, gate_excluded = apply_stage_gate(ranked, stage)
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
        if gate_watch:
            w_lines = ["", "### 🔭 观察备选池（距门槛 ≤0.5 分）"]
            w_lines.append("")
            w_lines.append("以下标的分项评分接近但未完全达门槛，保持跟踪：")
            w_lines.append("")
            w_lines.append("| 标的 | 备选原因 | 业务分 | 护城河 | 边际变化 | 综合分 |")
            w_lines.append("|------|---------|-------|--------|---------|-------|")
            for it in gate_watch:
                cd = it.get("code", "")
                ex_reason = (it.get("exclude_reason") or "")[:45]
                biz = it.get("business", "-")
                moat = it.get("moat", "-")
                mom = it.get("momentum", "-")
                comp = it.get("composite", "-")
                w_lines.append(f"| {cd} | {ex_reason} | {biz} | {moat} | {mom} | {comp} |")
            w_lines.append("")
            summary = summary + "\n\n" + "\n".join(w_lines)
        if gate_excluded:
            e_lines = ["", "### 长期赛道跟踪备选池"]
            e_lines.append("")
            e_lines.append("以下标的因当前阶段不达标被门槛剔除，但具备长期战略跟踪价值：")
            e_lines.append("")
            e_lines.append("| 标的 | 剔除原因 | 业务分 | 护城河 | 边际变化 | 综合分 |")
            e_lines.append("|------|---------|-------|--------|---------|-------|")
            for it in gate_excluded:
                cd = it.get("code", "")
                ex_reason = (it.get("exclude_reason") or "")[:40]
                biz = it.get("business", "-")
                moat = it.get("moat", "-")
                mom = it.get("momentum", "-")
                comp = it.get("composite", "-")
                e_lines.append(f"| {cd} | {ex_reason} | {biz} | {moat} | {mom} | {comp} |")
            e_lines.append("")
            summary = summary + "\n\n" + "\n".join(e_lines)
        if not ranked and (gate_watch or gate_excluded):
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

        # ---- 排名存档 ----
        try:
            from storage.sqlite.stock_storage import get_db
            _db = get_db()
            # 构建候选列表（ranked为主，回退到 verified_codes）
            candidates_json = []
            if ranked:
                for i, item in enumerate(ranked, 1):
                    nm = _safe_name(item.get("code", ""))
                    candidates_json.append({
                        "code": item.get("code", ""),
                        "name": nm or "",
                        "rank": i,
                        "composite": item.get("composite_adj", item.get("composite", 0)),
                    })
            else:
                for i, c in enumerate(verified_codes, 1):
                    nm = _safe_name(c)
                    candidates_json.append({"code": c, "name": nm or "", "rank": i, "composite": 0})

            # 估价位置（从 industry_valuation 提取）
            pe_pct = (industry_valuation or {}).get("pe_percentile_median")
            pb_pct = (industry_valuation or {}).get("pb_percentile_median")

            _db.save_industry_snapshot(
                industry_name=industry_name,
                question=question[:500] if question else "",
                candidates=candidates_json,
                top_pick=candidates_json[0]["code"] if candidates_json else "",
                industry_view=("偏多" if stage in ("导入期","成长期") else "中性"),
                valuation={"pe_percentile": pe_pct, "pb_percentile": pb_pct},
                excluded=[{
                    "code": it["code"],
                    "reason": it.get("exclude_reason",""),
                    "business": it.get("business"),
                    "fundamental": it.get("fundamental"),
                    "moat": it.get("moat"),
                    "momentum": it.get("momentum"),
                    "composite": it.get("composite"),
                } for it in (gate_excluded or [])],
                watch=[{
                    "code": it["code"],
                    "reason": it.get("exclude_reason",""),
                    "business": it.get("business"),
                    "fundamental": it.get("fundamental"),
                    "moat": it.get("moat"),
                    "momentum": it.get("momentum"),
                    "composite": it.get("composite"),
                } for it in (gate_watch or [])],
                benchmark_price=None,
            )
            logger.info(f"排名结果已存档: {industry_name} ({len(candidates_json)} 家)")
        except Exception as e:
            logger.warning(f"排名存档失败（不影响分析）: {e}")

        return {
            "messages": [response],
            "research_result": {"summary": summary, "sources": all_queries,
                                "industry_valuation": industry_valuation,
                                "industry_index": industry_index,
                                "industry_stage": stage,
                                # 门槛剔除组：留档后与进池组对照，用事后收益验证门槛有效性
                                "gate_excluded_codes": [it["code"] for it in gate_excluded],
                                "gate_watch_codes": [it["code"] for it in gate_watch],
                                # 程序计算的排名数据（供下游 formatter 直接使用，不依赖 LLM 输出格式）
                                "ranked_candidates": ranked},
            "chain_leaders": chain,
            # 排名数据写入 state 顶层字段，供 technical_agent 做基本面×技术面交叉分析
            "ranked_candidates": ranked,
            "stock_code": ",".join(verified_codes) if verified_codes else "",
            "intermediate_steps": [("researcher", {"mode": "chain", "industry": industry_name, "segments": sum(len(chain.get(k,[])) for k in ["upstream","midstream","downstream","niche_innovators"]), "candidates": len(verified_codes), "queries": len(all_queries)})],
        }

    def invoke(self, state: AgentState) -> Dict[str, Any]:
        return self.analyze_node(state)


def create_researcher_node():
    """创建研究节点（直通流程，支持个股/产业链双模式；技术面由下游 technical_agent 负责）"""
    agent = ResearcherAgent()
    return agent.analyze_node

