"""
信息研究 Agent（Researcher）
职责：
  - 个股模式：多维搜索全网信息 → LLM 综合分析
  - 行业模式：拆解产业链上中下游 → 搜公司 → 基本面/护城河分析 → 输出候选公司列表给下游 technical_agent
注意：不负责技术面分析（日线/周线/MACD 等），技术面由 technical_agent 专责
"""

import traceback
from typing import Dict, Any, List, Optional
from datetime import date, datetime, timedelta
from langchain_core.messages import SystemMessage, HumanMessage

from agents.base import AgentState
from core.llm import get_default_llm
from .web_search_tool import web_search
from tools.company_code_validator import find_stock_code, find_company_name
from utils.logger import logger


class ResearcherAgent:
    """研究 Agent：个股模式 + 产业链公司筛选与基本面分析"""

    def __init__(self):
        self.llm = get_default_llm()

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

        return [
            f"{tag} 公司公告 重大事项 {one_month.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",
            f"{tag} 所属行业 产业政策 发展趋势 {three_month_range}",
            f"{tag} 经营状况 营收 利润 最新业绩 {recent_period}",
            f"{tag} 月度产销快报 销量 环比 同比 {recent_period}",
            f"{tag} 竞争对手 销量对比 市场份额 {today.year}",
            f"{tag} 业务构成 收入占比 毛利占比 各板块营收拆分",
            f"{tag} 出货量 产能 新增订单 订单来源 资本开支",
            f"{tag} 技术实力 研发投入 核心技术突破 专利",
            f"{tag} 产业链 上下游 市场地位 竞争格局 {recent_period}",
            f"{tag} 利好 利空 机构评级 目标价 {one_month.strftime('%Y-%m-%d')} {today.strftime('%Y-%m-%d')}",
        ]

    def _build_stock_system_prompt(self) -> str:
        today = date.today().strftime("%Y-%m-%d")
        return f"""你是一个专业的股票信息研究员和分析师。今天的日期是 {today}，请以此为时间基准判断"近期/最新"。

请基于下方搜索结果，对该公司的以下维度进行客观分析并给出核心结论：

1. **公司公告与重大事项**：近期是否有重大公告及影响
2. **产业信息**：行业景气度、政策、趋势
3. **业务拆解**：各板块收入/毛利占比、TOP3业务、出货量、产能利用率、资本开支、新增订单及来源
4. **技术实力**：研发投入、核心技术突破、在研项目、专利壁垒
5. **产业链地位**：位置、议价力、竞争格局
6. **利好与利空分析**：分别列出利好和利空条目，每条标注影响力(高/中/低)及依据出处；
   综合判断只用「偏多/中性/偏空」三档，禁止编造精确百分比

【风险对称要求】
- 每条高影响力利好必须检查并列出对应风险（如：出口高增→关税/反补贴调查风险；大客户订单→客户集中度风险）
- 机构评级降权处理：A股卖方几乎不出卖出评级，"N家机构全部买入"不构成有效利好，最多作为关注度参考
- 目标价：搜索结果里有具体数字才引用，没有就不写"上涨空间较大"这类无依据表述

【输出要求】每个维度给出明确结论；业务数据尽可能用数字；搜索结果中没有的信息标注「信息不足」，
禁止用自身知识补数字；最后 3 句以内核心总结"""

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

        system_prompt = """你是一个产业研究专家。请基于搜索结果，拆解该行业的产业链结构。

请严格按以下JSON格式输出（不要有markdown包裹）：

{
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
            "upstream": [{"segment": f"{industry}上游原料/设备", "keywords": f"{industry} 上游 龙头"}],
            "midstream": [{"segment": f"{industry}中游制造", "keywords": f"{industry} 龙头"}],
            "downstream": [{"segment": f"{industry}下游应用", "keywords": f"{industry} 下游 龙头"}],
            "niche_innovators": [{"segment": f"{industry}专精特新隐形冠军", "keywords": f"{industry} 专精特新 隐形冠军 稀缺标的"}],
        }

    def _search_leaders_for_chain(self, industry: str, chain: Dict[str, Any]) -> Dict[str, Any]:
        """第二步：搜索每个产业链环节（含特精专新）的龙一龙二，并尝试从DB获取代码"""
        today = date.today()
        recent_period = f"{today.year}年{today.month}月"

        all_leaders = {"upstream": [], "midstream": [], "downstream": [], "niche_innovators": []}

        for level in ["upstream", "midstream", "downstream", "niche_innovators"]:
            segments = chain.get(level, [])
            level_leaders = []

            for seg in segments:
                seg_name = seg.get("segment", "")
                keywords = seg.get("keywords", seg_name)
                # 搜索该细分领域的龙头
                query = f"{keywords} 龙一 龙二 龙头公司 竞争格局 {recent_period}"
                try:
                    result = web_search.invoke({"query": query})
                except Exception as e:
                    logger.error(f"搜索细分领域龙头失败 [{seg_name}]: {e}")
                    result = ""

                leaders = self._extract_leaders_from_text(str(result), seg_name, level)

                level_leaders.append({
                    "segment": seg_name,
                    "leaders": leaders,
                    "search_snippet": str(result)[:400],
                })

            all_leaders[level] = level_leaders

        return all_leaders

    def _extract_leaders_from_text(self, text: str, segment: str, level: str) -> List[Dict[str, Any]]:
        """从搜索结果中提取前两名龙头股的代码"""
        if not text or "搜索失败" in str(text):
            return []

        # 用 LLM 提取
        prompt = f"""从以下搜索结果中提取「{segment}」细分领域的龙一和龙二公司。

请输出JSON数组（不要markdown包裹）：
[{{"name": "公司名", "code": "股票代码"}}, ...]

如果没有找到或无法确定，返回空数组 []。
搜索结果：
{text[:2000]}"""

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
                return validated[:2]
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

    def _build_chain_system_prompt(self) -> str:
        return """你是一个顶级的产业链研究专家。你的任务是从产业链中筛选出所有关键公司（含特精专新企业），分析其基本面、业务拆解、护城河、资金偏好，并输出清晰的候选公司清单（含股票代码），供下游技术分析 Agent 使用。

请基于下方数据完成任务：
1. 产业链结构（上游/中游/下游/特精专新 + 细分领域）及各环节龙一龙二
2. 各公司的经营基本面、业务拆解、护城河、资金流向搜索数据

请严格按照以下结构输出：

## 〇、候选公司清单（必须放在最前面输出，供下游技术分析用）
纯JSON格式（不要markdown包裹）：
{
  "candidate_codes": ["代码1", "代码2", ...]
}

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

## 五、综合排名（业务+基本面+护城河）
用表格按"业务经营20% + 基本面30% + 不可替代性&溢价50%"权重降序：
表头：排名 | 公司 | 代码 | 业务经营 | 基本面 | 不可替代&溢价 | 综合分 | 所处环节 | 资金偏好

【重要原则】
- 候选公司清单 JSON 必须是输出的第一部分（输出过长被截断时后面的段落可丢，JSON 不能丢）
- 所有结论必须基于搜索结果，不足处标注「信息不足」
- 业务拆解数据是评估公司质量和成长性的核心，请尽可能详细
- 资金偏好标签必须依据搜索数据中提及的资金类型判断，不可凭空猜测
- 特精专新企业的不可替代性是其核心竞争力，需重点分析
- 候选公司清单中的股票代码必须准确"""

    # ========== 通用工具 ==========

    def _do_search(self, queries: List[str]) -> Dict[str, str]:
        results = {}
        for q in queries:
            try:
                logger.info(f"搜索: {q[:50]}...")
                results[q] = web_search.invoke({"query": q})
            except Exception as e:
                logger.error(f"搜索失败 [{q}]: {e}")
                results[q] = f"搜索失败: {e}"
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

        queries = self._build_stock_queries(stock_code)
        all_results = self._do_search(queries)
        search_text = self._search_text(all_results)

        messages = [
            SystemMessage(content=self._build_stock_system_prompt()),
            HumanMessage(content=f"""用户问题：{question}
股票代码：{stock_code}
========== 全网搜索结果 ==========
{search_text[:12000]}
请基于以上信息进行全面分析。"""),
        ]

        logger.info("LLM 综合分析中...")
        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)

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
        logger.info(f"产业链结构: upstream={len(chain.get('upstream',[]))}seg, midstream={len(chain.get('midstream',[]))}seg, downstream={len(chain.get('downstream',[]))}seg, niche={len(chain.get('niche_innovators',[]))}seg")

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

        # 全景搜索（含不可替代性/溢价能力维度）
        all_queries = self._build_chain_queries(industry_name, chain)
        all_results = self._do_search(all_queries)
        search_text = self._search_text(all_results)

        messages = [
            SystemMessage(content=self._build_chain_system_prompt()),
            HumanMessage(content=f"""用户问题：{question}
目标行业：{industry_name}

========== 产业链结构（上中下游+特精专新+细分领域+龙一龙二） ==========
{chain_summary}

========== 全网搜索结果（含经营/竞争力/业务拆解/收入毛利占比/出货量/资本开支/新增订单/技术突破/护城河/溢价/资金偏好） ==========
{search_text[:15000]}

请按结构输出：〇、JSON 候选代码清单（放最前）→ 一、全景筛选（含资金偏好标签） → 二、业务拆解与经营数据（收入毛利占比/出货量/资本开支/新增订单/技术突破） → 三、基本面评分 → 四、护城河分析（重点关注特精专新） → 五、综合排名"""),
        ]

        logger.info("LLM 产业链分析中...")
        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"产业链分析完成，长度: {len(summary)}")

        # 从 LLM 输出中提取 candidate_codes，供下游 technical_agent 使用
        import json, re
        candidate_codes = list(all_leader_codes)  # fallback：龙头搜索阶段已验证过的代码
        json_match = re.search(r'\{[^{}]*"candidate_codes"[^{}]*\}', summary, re.DOTALL)
        if json_match:
            try:
                extracted = json.loads(json_match.group(0))
                if extracted.get("candidate_codes"):
                    candidate_codes = extracted["candidate_codes"]
                    logger.info(f"从 LLM 输出中提取候选代码: {candidate_codes}")
            except json.JSONDecodeError:
                pass

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
            "research_result": {"summary": summary, "sources": all_queries},
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

