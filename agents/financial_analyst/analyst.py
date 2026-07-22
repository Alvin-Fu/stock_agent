"""
财务分析 Agent
职责：拉取真实财务报表(利润表+资产负债表) → 调研报补充 → 计算比率 → LLM分析 → 保存到数据库
"""

import pandas as pd
from typing import Dict, Any
from langchain_core.messages import SystemMessage, HumanMessage
from datetime import date

from core.llm import get_analyst_llm
from agents.base import AgentState
from .tools import (
    calculate_profitability_ratios,
    calculate_liquidity_ratios,
    calculate_solvency_ratios,
    calculate_valuation_ratios,
    calculate_growth_rates,
    perform_dupont_analysis,
)
from tools.stock_tools import (
    call_fetch_stock_research_report,
    call_fetch_income_data,
    call_fetch_balance_sheet_data,
    call_fetch_cashflow_data,
)
from utils.logger import logger
from storage.sqlite.stock_storage import get_db


class AnalystAgent:
    """财务分析 Agent：基于真实财务报表数据进行分析"""

    def __init__(self):
        self.llm = get_analyst_llm()
        self.financial_tools = [
            ("盈利能力", calculate_profitability_ratios),
            ("短期偿债", calculate_liquidity_ratios),
            ("长期偿债", calculate_solvency_ratios),
            ("估值比率", calculate_valuation_ratios),
            ("增长率", calculate_growth_rates),
            ("杜邦分析", perform_dupont_analysis),
        ]
        self.db = get_db()

    def _fetch_report(self, stock_code: str) -> str:
        """调研报，只调一次"""
        try:
            return call_fetch_stock_research_report(stock_code)
        except Exception as e:
            logger.error(f"调研报失败 {stock_code}: {e}")
            return ""

    def _fetch_real_financial_data(self, stock_code: str) -> Dict[str, Any]:
        """从数据库/Tushare 拉取真实财务报表数据"""
        result = {"income": "", "balance_sheet": "", "cashflow": "", "main_business": "",
                  "trend": "", "peer_table": "", "forecast": "", "profit_split": "", "parsed": {}}

        # 先更新每日指标（PE/PB/市值），估值比率依赖它；失败不阻断其余分析
        try:
            from tools.stock_tools import stock_tool_instance
            stock_tool_instance.fetch_and_save_stock_basic_daily(stock_code)
        except Exception as e:
            logger.warning(f"更新每日指标失败（不影响其余分析）: {e}")

        try:
            income_text = call_fetch_income_data(stock_code)
            if income_text and "未获取到" not in income_text:
                result["income"] = income_text
        except Exception as e:
            logger.error(f"获取利润表失败 {stock_code}: {e}")

        try:
            balance_text = call_fetch_balance_sheet_data(stock_code)
            if balance_text and "未获取到" not in balance_text:
                result["balance_sheet"] = balance_text
        except Exception as e:
            logger.error(f"获取资产负债表失败 {stock_code}: {e}")

        try:
            cashflow_text = call_fetch_cashflow_data(stock_code)
            if cashflow_text and "未获取到" not in cashflow_text:
                result["cashflow"] = cashflow_text
        except Exception as e:
            logger.error(f"获取现金流量表失败 {stock_code}: {e}")

        # 主营业务构成：利润驱动分析的数字底座（内部已容错，失败返回空串/空列表）
        from tools.main_business import fetch_main_business_records, build_main_business_text
        mb_records = fetch_main_business_records(stock_code)
        result["main_business"] = build_main_business_text(mb_records)

        # 财报趋势：利润同比/利润率/单季拆分 + 费用率 + 现金流净现比 + 营运资本
        income_records = []
        try:
            income_df = self.db.get_stock_income(stock_code)
            if income_df is not None and not income_df.empty:
                income_records = income_df.to_dict("records")
                from .trend import build_full_trend
                cash_records, balance_records = None, None
                try:
                    cash_df = self.db.get_stock_cashflow(stock_code)
                    if cash_df is not None and not cash_df.empty:
                        cash_records = cash_df.to_dict("records")
                except Exception:
                    pass
                try:
                    balance_df = self.db.get_stock_balance_sheet(stock_code)
                    if balance_df is not None and not balance_df.empty:
                        balance_records = balance_df.to_dict("records")
                except Exception:
                    pass
                result["trend"] = build_full_trend(income_records, cash_records, balance_records)
        except Exception as e:
            logger.warning(f"构建财报趋势失败（不影响其余分析）: {e}")

        # 同行对比表（横向估值参照系）+ 机构盈利预测（forward 估值锚）
        try:
            from tools.peer_compare import fetch_peer_table
            result["peer_table"], _ = fetch_peer_table(stock_code)
        except Exception as e:
            logger.warning(f"同行对比生成失败（不影响其余分析）: {e}")
        try:
            from tools.forecast import fetch_profit_forecast_text
            result["forecast"] = fetch_profit_forecast_text(stock_code)
        except Exception as e:
            logger.warning(f"盈利预测获取失败（不影响其余分析）: {e}")

        # 分部利润拆分（SOTP 数字底座）：最新年报净利 × 分部利润占比，程序算死
        try:
            result["profit_split"] = self._build_profit_split_text(mb_records, income_records)
        except Exception as e:
            logger.warning(f"分部利润拆分失败（不影响其余分析）: {e}")

        result["parsed"] = self._parse_latest_financial_data()

        # 数据源健康上报：财报块拿没拿到一眼可见，静默降级是最危险的失败模式
        try:
            from tools.source_health import report_source
            for label, key in (("利润表", "income"), ("资产负债表", "balance_sheet"),
                               ("现金流量表", "cashflow"), ("主营构成", "main_business"),
                               ("同行对比", "peer_table")):
                report_source(label, bool(result.get(key)))
        except Exception:
            pass
        return result

    def _parse_latest_financial_data(self) -> Dict[str, Any]:
        """从数据库读取最新一期财务数据，用于计算比率"""
        parsed = {}
        try:
            stock_code = getattr(self, "_current_stock_code", "")
            if not stock_code:
                return parsed

            def _num(row, key):
                """取真实数值：缺失/无法转换返回 None，不用 0 或 1 兜底伪造"""
                value = row.get(key)
                if value is None:
                    return None
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            income_df = self.db.get_stock_income(stock_code)
            if income_df is not None and not income_df.empty:
                latest = income_df.iloc[0]
                # 报告期：让 LLM 知道数据是哪一期的，才能正确表述同比/环比
                report_date = latest.get("report_date")
                if report_date is not None:
                    parsed["report_period"] = str(report_date)
                for src, dst in [("total_revenue", "revenue"), ("net_profit", "net_income"),
                                 ("operating_profit", "operating_profit")]:
                    value = _num(latest, src)
                    if value is not None:
                        parsed[dst] = value / 1e8
                for src, dst in [("revenue_growth", "revenue_yoy"), ("profit_growth", "profit_yoy"),
                                 ("gross_margin", "gross_margin")]:
                    value = _num(latest, src)
                    if value is not None:
                        parsed[dst] = value

            balance_df = self.db.get_stock_balance_sheet(stock_code)
            if balance_df is not None and not balance_df.empty:
                latest_b = balance_df.iloc[0]
                for src, dst in [("total_assets", "total_assets"), ("total_liabilities", "total_liabilities"),
                                 ("total_equity", "total_equity"), ("current_assets", "current_assets"),
                                 ("current_liabilities", "current_liabilities")]:
                    value = _num(latest_b, src)
                    if value is not None:
                        parsed[dst] = value / 1e8
                for src, dst in [("asset_liability_ratio", "debt_ratio"), ("current_ratio", "current_ratio")]:
                    value = _num(latest_b, src)
                    if value is not None:
                        parsed[dst] = value

            # 衍生字段：只在有真实依据时才填，缺就缺着（比率工具会标注「缺少XX数据」）
            if parsed.get("revenue") and parsed.get("gross_margin"):
                parsed["cost_of_goods_sold"] = parsed["revenue"] * (1 - parsed["gross_margin"] / 100)
            if parsed.get("operating_profit") is not None:
                parsed["ebit"] = parsed["operating_profit"]

            # 估值分位：PE(TTM)/PB 多窗口（3年 / 5年 / 10年）交叉验证
            try:
                basic_df = self.db.get_latest_daily_basic_data(stock_code, 2500)
                if basic_df is not None and not basic_df.empty:
                    latest_basic = basic_df.iloc[0]
                    total_mv = latest_basic.get("total_mv")
                    if total_mv:
                        parsed["market_cap"] = float(total_mv) / 1e4
                    for col, name in [("pe_ttm", "pe_ttm"), ("pb", "pb"), ("ps_ttm", "ps_ttm")]:
                        cur = latest_basic.get(col)
                        if cur is None or pd.isna(cur):
                            continue
                        parsed[name] = round(float(cur), 2)
                        hist = pd.to_numeric(basic_df[col], errors="coerce").dropna()
                        # 多窗口分位
                        # 注意：basic_df 按 trade_date DESC（最新在前），用 head() 取最近 N 条
                        windows = {"近3年": 750, "近5年": 1250, "近10年": 2500}
                        pct_parts = []
                        for win_label, win_days in windows.items():
                            sub = hist.head(min(len(hist), win_days))
                            if len(sub) >= 60:
                                pct = float((sub < float(cur)).mean() * 100)
                                pct_parts.append(f"{win_label} {pct:.0f}%分位")
                        if pct_parts:
                            parsed[f"{name}_分位"] = "，".join(pct_parts)
                        # PE/PB 背离判断（用近3年数据）
                        pe_pct = float((hist.head(750) < float(parsed.get("pe_ttm", 0))).mean() * 100) if len(hist) >= 750 and "pe_ttm" in parsed else None
                        pb_pct = float((hist.head(750) < float(parsed.get("pb", 0))).mean() * 100) if len(hist) >= 750 and "pb" in parsed else None
                        if pe_pct and pb_pct and pe_pct > 70 and pb_pct < 30:
                            parsed["估值背离"] = "PE悬顶、PB托底——盈利下滑被动抬高PE，但资产端已处历史底部"

                    # PEG = PE(TTM) ÷ 净利同比增速（trailing 口径，非预期增速；增速≤0 时不适用）
                    pe = parsed.get("pe_ttm")
                    growth = parsed.get("profit_yoy")
                    if pe and pe > 0 and growth is not None:
                        if growth > 0:
                            parsed["peg"] = round(pe / growth, 2)
                            parsed["peg_口径"] = "PE(TTM)÷最新报告期净利累计同比增速%（trailing，非预期增速）"
                        else:
                            parsed["peg_口径"] = "净利同比为负，PEG 不适用"
            except Exception as e:
                logger.warning(f"获取市值/估值数据失败（不影响其余比率）: {e}")

        except Exception as e:
            logger.error(f"解析最新财务数据失败: {e}")
        return parsed

    @staticmethod
    def _build_profit_split_text(mb_records, income_records) -> str:
        """分部利润拆分（程序计算）：最新年报净利 × 主营构成的分部利润占比"""
        from tools.main_business import latest_profit_split
        split = latest_profit_split(mb_records)
        if not split:
            return ""
        fy_np = None
        fy = split[0]["period"]
        for r in income_records or []:
            if str(r.get("report_date") or "")[:10] == fy:
                try:
                    v = float(r.get("net_profit"))
                    fy_np = v / 1e8 if v == v else None
                except (TypeError, ValueError):
                    pass
                break
        if not fy_np or fy_np <= 0:
            return ""
        lines = [f"【分部利润拆分（程序计算：{fy[:4]}年报净利 {fy_np:.1f}亿 × 分部利润占比，粗拆口径）】"]
        for s in split:
            seg = f"  - {s['name']}: 约{fy_np * s['profit_share_pct'] / 100:.1f}亿" \
                  f"（利润占比{s['profit_share_pct']}%"
            if s.get("rev_share_pct") is not None:
                seg += f"，收入占比{s['rev_share_pct']}%"
            lines.append(seg + "）")
        lines.append("  （分部间未剔除内部抵消；供分部估值参考用，不是精确分部净利）")
        return "\n".join(lines)

    def _call_financial_tools(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """全部财务工具各调一次，返回{工具名: 结果}"""
        results = {}
        wrapped_data = {"financial_statements": data}

        for name, func in self.financial_tools:
            try:
                if name == "增长率":
                    if data.get("revenue") and data.get("revenue_yoy") is not None:
                        results[name] = {
                            "营收同比增长率": round(data.get("revenue_yoy", 0), 2),
                            "净利润同比增长率": round(data.get("profit_yoy", 0), 2),
                            "毛利率": round(data.get("gross_margin", 0), 2),
                        }
                    else:
                        results[name] = "数据不足"
                else:
                    results[name] = func.invoke(wrapped_data)
                logger.info(f"财务工具 {name} 计算完成")
            except Exception as e:
                logger.error(f"财务工具 {name} 失败: {e}")
                results[name] = f"计算失败: {e}"
        return results

    def _build_system_prompt(self) -> str:
        from agents.prompts_common import STYLE_RULES, INTERMEDIATE_PRODUCT_NOTE
        today = date.today().strftime("%Y-%m-%d")
        return f"""你是一位资深财务分析师（CFA），拥有 15 年上市公司财报分析经验。
今天的日期是 {today}，请以此为时间基准表述"最新/近期"。

{INTERMEDIATE_PRODUCT_NOTE}

请基于下方提供的真实财务报表数据和研报观点，进行专业解读。

【分析原则】
1. 优先使用真实财务报表数据（利润表/资产负债表），这是定量依据
2. 研报观点作为定性补充，用于理解市场预期和分析师观点
3. 重要数据变化（>10%）需特别标注
4. 财务比率必须结合行业特征解读：
   - 资产负债率要区分有息负债与经营性占款（如车企/零售的应付账款是无息占用上游资金，
     高负债率不等于高杠杆风险，不要直接定性为"高杠杆运营风险高"）
   - 金融业不适用流动比率；制造业/车企流动比率常年低于1属行业常态
5. 现金流解读：对比经营现金流净额与净利润的匹配度（现金流长期低于净利润要警惕利润质量），
   结合资本开支规模判断扩张强度；重资产/占用供应链账期的公司，经营现金流比流动比率更能说明偿债能力
6. 数据中标注"缺少XX数据"的项：直接说明缺失，禁止估算或用行业均值代替
7. 每个定性结论必须有对应数据支撑，禁止使用与数据矛盾的模板化表述
   （例如：单车均价上涨时不得使用"以价换量"的说法）
8. 利润驱动结构：如提供了主营业务构成数据，必须回答"利润主要靠什么业务赚"——
   引用各业务的收入/利润占比与毛利率原数；重点关注占比同比明显提升的业务
   （正在放量的第二曲线）和占比萎缩的业务（旧驱动衰减）；按地区维度的海外
   占比变化直接反映出海驱动。没有该数据时标注"缺主营构成数据"，禁止凭印象拆分
9. PEG 解读（数据中提供时）：约<1 表示估值相对当期增速偏低、1~2 大致匹配、>2 偏贵；
   必须注明这是 trailing 口径（用已披露增速，非未来预期），增速为负时不得使用 PEG；
   高 PE 但 PEG 低（高增速消化估值）与低 PE 但 PEG 高（增长停滞）要分别点破
10. 趋势优先于绝对值：股价的支撑来自"变化"而非"存量"。所有盈利结论必须落在
   改善/恶化/加速/放缓上，并引用【财报趋势】表中程序算好的同比序列、利润率变化、
   单季拆分数字；禁止自行心算趋势，禁止只报单期绝对值就下结论；
   财务趋势要与主营构成占比变化、研报中的销量/出货量数据互相印证
11. 同行对比：估值贵贱、毛利率高低必须放在【同行对比】表里说（"PE 高于 4/5 家同行"
    比"PE 处于历史 89% 分位"更有决策含义，两者都要说）；缺同行数据时明说，
    禁止凭记忆引用"同行水平"
12. 分部估值参考（SOTP）：仅当同时提供了【分部利润拆分】和【同行对比】时才做——
    每个分部的估值倍数必须取自同行对比表中可比公司的实际 PE（写明取自哪家），
    给低/高两档得到市值区间，与当前总市值比较，结论只说"当前市值处于/高于/低于
    分部估值区间"；必须标注"极粗略参考，非目标价，未计分部协同与控股折价"；
    亏损分部用 0 或注明无法估值，禁止发明倍数
13. 避免给出投资建议，仅做客观分析

{STYLE_RULES}

【输出要求】
- 先总览（明确标注报告期，如"2026年一季报"）
- 再逐项解读：盈利能力、偿债能力、成长能力、运营效率、现金流质量、利润驱动结构
- 结合研报观点做定性补充
- 最后给出总结性观点"""

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        """
        直通流程：拉取真实财务报表 → 调研报补充 → 计算比率 → LLM分析 → 保存DB
        """
        try:
            stock_code = state.get("stock_code", "")
            question = state.get("question", "")
            self._current_stock_code = stock_code
            logger.info(f"财务分析开始，股票: {stock_code}，问题: {question[:50]}...")

            logger.info("拉取真实财务报表数据(利润表+资产负债表)...")
            real_data = self._fetch_real_financial_data(stock_code)
            financial_data = real_data["parsed"]
            income_text = real_data["income"]
            balance_text = real_data["balance_sheet"]
            cashflow_text = real_data["cashflow"]
            main_business_text = real_data["main_business"]
            trend_text = real_data["trend"]
            peer_text = real_data["peer_table"]
            forecast_text = real_data["forecast"]
            profit_split_text = real_data["profit_split"]

            logger.info("获取研报作为定性补充...")
            report_text = self._fetch_report(stock_code)
            if not report_text or "未获取到" in report_text:
                report_text = state.get("documents", [])
                if report_text:
                    report_text = "\n".join([d.page_content for d in report_text])
                else:
                    report_text = "未获取到研报数据"

            calculated = self._call_financial_tools(financial_data)

            system_prompt = self._build_system_prompt()
            user_message = f"""请分析股票 {stock_code} 的财务状况。

【用户问题】
{question}

========== 真实利润表数据 ==========
{income_text if income_text else '未获取到利润表数据'}

========== 真实资产负债表数据 ==========
{balance_text if balance_text else '未获取到资产负债表数据'}

========== 真实现金流量表数据 ==========
{cashflow_text if cashflow_text else '未获取到现金流量表数据'}

========== 财报趋势（程序计算：同比序列/利润率变化/单季拆分） ==========
{trend_text if trend_text else '历史期数不足，无法给趋势（只有单期数据时禁止下趋势结论）'}

========== 主营业务构成（利润驱动结构） ==========
{main_business_text if main_business_text else '缺主营构成数据'}

========== 同行对比（横向估值参照系） ==========
{peer_text if peer_text else '缺同行对比数据（不得凭印象与同行比较）'}

========== 机构盈利预测（forward 估值锚，预测≠事实） ==========
{forecast_text if forecast_text else '无机构预测数据（只能用 trailing 口径估值）'}

========== 分部利润拆分（分部估值 SOTP 底座） ==========
{profit_split_text if profit_split_text else '缺分部利润数据（禁止做分部估值）'}

========== 最新一期财务数据(单位：亿元) ==========
{financial_data}

========== 计算出的财务比率 ==========
{calculated}

========== 研报观点（定性补充）==========
{str(report_text)[:3000]}

请基于以上真实财务数据给出专业分析意见。优先使用真实报表数据，研报观点作为补充。"""

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]

            logger.info("LLM 财务分析中...")
            response = self.llm.invoke(messages)
            summary = response.content if hasattr(response, 'content') else str(response)
            logger.info(f"财务分析完成，长度: {len(summary)}")

            self._save_analysis_to_db(state, summary, calculated)

            return {
                "messages": [response],
                "financial_data": financial_data,
                "analysis_result": {"summary": summary, "ratios": calculated, "data_source": "real_financial_statements"},
                "intermediate_steps": [("analyze", {"stock_code": stock_code, "content": summary[:200]})],
            }

        except Exception as e:
            logger.error(f"财务分析节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"分析执行失败: {e}",
                "intermediate_steps": [("analyze", {"error": str(e)})],
            }

    def _save_analysis_to_db(self, state: AgentState, analysis_content: str, ratios: Dict[str, Any]):
        try:
            question = state.get("question", "") or ""
            stock_code = state.get("stock_code", "")
            if not stock_code:
                import re
                m = re.search(r'[0-9]{6}', question)
                stock_code = m.group(0) if m else ""
            if stock_code:
                self.db.save_financial_analyze(
                    code=stock_code,
                    date=date.today(),
                    pdf_name=f"analysis_{stock_code}_{date.today().strftime('%Y%m%d')}.pdf",
                    report_type="机构研报",
                    analyze_content=analysis_content,
                    ratios=ratios,
                    confidence="high",
                )
                logger.info(f"分析结果已保存: {stock_code}")
        except Exception as e:
            logger.error(f"保存分析结果失败: {e}")

    def invoke(self, state: AgentState) -> Dict[str, Any]:
        return self.analyze_node(state)


def create_analyst_node():
    agent = AnalystAgent()
    return agent.analyze_node