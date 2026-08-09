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


def calc_quality_metrics(code: str, db) -> Dict[str, Any]:
    """计算个股质量否决权指标：ROE/扣非占比/商誉占比，任一触发则打折。

    参考 tools/industry_metrics.py 中刚修复的数据采集逻辑。
    - ROE < 5% → quality_discount *= 0.85
    - 扣非/归母 < 30%（用 dt_eps/eps 近似）→ quality_discount *= 0.7
    - 商誉/净资产 > 25%（通过 TushareFetcher.balancesheet 获取）→ quality_discount *= 0.85
    """
    import pandas as pd
    from datetime import date as _date, timedelta as _td

    result: Dict[str, Any] = {
        "quality_discount": 1.0,
        "triggers": [],
        "roe": None,
        "deduct_net_ratio": None,
        "goodwill_ratio": None,
        "data_complete": True,
    }

    # ROE + 扣非占比（来自财务指标，最新年报）
    try:
        fina_df = db.get_stock_fina_indicator(code)
        if fina_df is not None and not fina_df.empty:
            fina_df = fina_df.copy()
            fina_df['_rd'] = pd.to_datetime(fina_df['report_date'], errors='coerce')
            ann_fina = fina_df[fina_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False)
            if not ann_fina.empty:
                latest = ann_fina.iloc[0]
                roe_val = latest.get('roe')
                if roe_val is not None and pd.notna(roe_val):
                    roe_float = float(roe_val)
                    # 统一为百分数（如0.15 → 15%）
                    roe_pct = roe_float * 100 if roe_float <= 1.0 else roe_float
                    result["roe"] = round(roe_pct, 2)
                    if roe_pct < 5.0:
                        result["quality_discount"] *= 0.85
                        result["triggers"].append(f"ROE {roe_pct:.2f}% < 5%（质量折扣×0.85）")
                # 扣非净利润/归母净利润（<30%触发否决权）
                # 用 dt_eps/eps 近似扣非占比（两者在同一报告期的比率≈扣非/归母）
                dt_eps = latest.get('dt_eps')
                eps_val = latest.get('eps')
                if dt_eps is not None and eps_val is not None and pd.notna(dt_eps) and pd.notna(eps_val):
                    eps_f = float(eps_val)
                    if abs(eps_f) > 1e-6:
                        deduct_ratio = float(dt_eps) / eps_f * 100
                        result["deduct_net_ratio"] = round(deduct_ratio, 1)
                        if deduct_ratio < 30.0:
                            result["quality_discount"] *= 0.7
                            result["triggers"].append(f"扣非/归母 {deduct_ratio:.1f}% < 30%（质量折扣×0.7）")
    except Exception as e:
        logger.warning(f"质量否决权 ROE/扣非获取失败 {code}: {e}")

    # ROE/扣非数据缺失检测：数据不可用时质量否决权未执行，需显式提示
    if result["roe"] is None:
        result["data_complete"] = False
        result["triggers"].append("⚠️ 质量数据缺失（ROE/扣非数据不可用），质量否决权未执行，请标注'质量未验证'")

    # 商誉/净资产（>25%触发否决权）
    try:
        from tools.stock.tushare_fetcher import TushareFetcher
        tf = TushareFetcher()
        if tf._api is not None:
            end_d = _date.today().strftime("%Y-%m-%d")
            start_d = (_date.today() - _td(days=400)).strftime("%Y-%m-%d")
            bs_df = tf.balancesheet(code, start_date=start_d, end_date=end_d)
            if bs_df is not None and not bs_df.empty:
                bs_df = bs_df.copy()
                if 'end_date' in bs_df.columns:
                    bs_df['_rd'] = pd.to_datetime(bs_df['end_date'], errors='coerce')
                    ann_bs = bs_df[bs_df['_rd'].dt.month == 12].sort_values('_rd', ascending=False)
                else:
                    ann_bs = bs_df
                if not ann_bs.empty:
                    gw = ann_bs.iloc[0].get('goodwill')
                    equity = ann_bs.iloc[0].get('total_hldr_eqy_inc_min_int')
                    if gw is not None and equity is not None and pd.notna(gw) and pd.notna(equity):
                        eq_float = float(equity)
                        if abs(eq_float) > 1e-6:
                            gw_ratio = float(gw) / eq_float * 100
                            result["goodwill_ratio"] = round(gw_ratio, 1)
                            if gw_ratio > 25.0:
                                result["quality_discount"] *= 0.85
                                result["triggers"].append(f"商誉/净资产 {gw_ratio:.1f}% > 25%（质量折扣×0.85）")
    except Exception as e:
        logger.debug(f"质量否决权 商誉获取失败 {code}: {e}")

    # 商誉数据缺失检测：数据不可用时质量否决权未执行，需显式提示
    if result["goodwill_ratio"] is None:
        result["data_complete"] = False
        result["triggers"].append("⚠️ 质量数据缺失（商誉数据不可用），质量否决权未执行，请标注'质量未验证'")

    result["quality_discount"] = round(result["quality_discount"], 4)
    return result


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

    @staticmethod
    def _format_review_lesson(stock_code: str) -> str:
        """注入该标的最近一次复盘的误判模式和相关改进规则（公共函数代理）"""
        from agents.prompts_common import format_review_lesson
        return format_review_lesson(stock_code)

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

        # 并行拉取三张报表 + 主营构成（四者相互独立、无依赖，并行节省网络等待）
        # 后续 trend/peer_table/forecast 等步骤有依赖关系，保持串行
        import concurrent.futures
        from tools.main_business import fetch_main_business_records, build_main_business_text

        def _fetch_income() -> str:
            try:
                income_text = call_fetch_income_data(stock_code)
                if income_text and "未获取到" not in income_text:
                    return income_text
                return ""
            except Exception as e:
                logger.error(f"获取利润表失败 {stock_code}: {e}")
                return ""

        def _fetch_balance() -> str:
            try:
                balance_text = call_fetch_balance_sheet_data(stock_code)
                if balance_text and "未获取到" not in balance_text:
                    return balance_text
                return ""
            except Exception as e:
                logger.error(f"获取资产负债表失败 {stock_code}: {e}")
                return ""

        def _fetch_cashflow() -> str:
            try:
                cashflow_text = call_fetch_cashflow_data(stock_code)
                if cashflow_text and "未获取到" not in cashflow_text:
                    return cashflow_text
                return ""
            except Exception as e:
                logger.error(f"获取现金流量表失败 {stock_code}: {e}")
                return ""

        def _fetch_main_business() -> tuple:
            # 主营业务构成：利润驱动分析的数字底座（内部已容错，失败返回空串/空列表）
            # 返回 (text, records)，records 供后续 profit_split 使用
            try:
                mb_records = fetch_main_business_records(stock_code)
                return build_main_business_text(mb_records), mb_records
            except Exception as e:
                logger.error(f"获取主营构成失败 {stock_code}: {e}")
                return "", []

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            fut_income = executor.submit(_fetch_income)
            fut_balance = executor.submit(_fetch_balance)
            fut_cashflow = executor.submit(_fetch_cashflow)
            fut_mb = executor.submit(_fetch_main_business)
            result["income"] = fut_income.result()
            result["balance_sheet"] = fut_balance.result()
            result["cashflow"] = fut_cashflow.result()
            mb_text, mb_records = fut_mb.result()
            result["main_business"] = mb_text

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
                # 财务指标数据（含毛利率），用于交叉验证 income 表数据
                fina_records = []
                try:
                    fina_df = self.db.get_stock_fina_indicator(stock_code)
                    if fina_df is not None and not fina_df.empty:
                        fina_records = fina_df.to_dict("records")
                except Exception:
                    pass
                result["trend"] = build_full_trend(income_records, cash_records, balance_records, fina_records)
        except Exception as e:
            logger.warning(f"构建财报趋势失败（不影响其余分析）: {e}")

        # 同行对比表（横向估值参照系）
        try:
            from tools.peer_compare import fetch_peer_table
            result["peer_table"], _ = fetch_peer_table(stock_code)
        except Exception as e:
            logger.warning(f"同行对比生成失败（不影响其余分析）: {e}")

        # 先解析当期财务数据（含 PE(TTM)），供预测块 forward PEG 使用
        result["parsed"] = self._parse_latest_financial_data()

        # 机构盈利预测（forward 估值锚）
        try:
            from tools.forecast import fetch_profit_forecast_text
            pe_ttm = result["parsed"].get("pe_ttm") if result.get("parsed") else None
            result["forecast"] = fetch_profit_forecast_text(stock_code, pe_ttm=pe_ttm)
        except Exception as e:
            logger.warning(f"盈利预测获取失败（不影响其余分析）: {e}")

        # 分部利润拆分（SOTP 数字底座）：最新年报净利 × 分部利润占比，程序算死
        try:
            result["profit_split"] = self._build_profit_split_text(mb_records, income_records)
        except Exception as e:
            logger.warning(f"分部利润拆分失败（不影响其余分析）: {e}")

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
                        # 排除当前值自身（basic_df 按 trade_date DESC，iloc[0] 是最新一行=当前值）
                        hist_excl = hist.iloc[1:] if len(hist) > 1 else hist
                        # 多窗口分位
                        # 注意：basic_df 按 trade_date DESC（最新在前），用 head() 取最近 N 条
                        windows = {"近3年": 750, "近5年": 1250, "近10年": 2500}
                        pct_parts = []
                        for win_label, win_days in windows.items():
                            sub = hist_excl.head(min(len(hist_excl), win_days))
                            if len(sub) >= 60:
                                pct = float((sub < float(cur)).mean() * 100)
                                pct_parts.append(f"{win_label} {pct:.0f}%分位")
                        if pct_parts:
                            # 附数据源和时点
                            src_date = latest_basic.get("trade_date")
                            src_str = f"数据源：Tushare daily_basic"
                            if src_date is not None and not pd.isna(src_date):
                                src_str += f"，截至{src_date}"
                            pct_parts.append(src_str)
                            parsed[f"{name}_分位"] = "；".join(pct_parts)
                        # PE/PB 背离判断（在循环内，确保用对应col的hist_excl）
                        if col == "pe_ttm" and "pe_ttm" in parsed:
                            _pe_hist = hist_excl.head(750)
                            _pe_pct = float((_pe_hist < float(cur)).mean() * 100) if len(_pe_hist) >= 750 else None
                            # 存储供 pb 判断时使用
                            parsed["_temp_pe_pct"] = _pe_pct
                        if col == "pb" and "pb" in parsed:
                            _pb_hist = hist_excl.head(750)
                            _pb_pct = float((_pb_hist < float(cur)).mean() * 100) if len(_pb_hist) >= 750 else None
                            _pe_pct = parsed.pop("_temp_pe_pct", None) if "_temp_pe_pct" in parsed else None
                            if _pe_pct and _pb_pct and _pe_pct > 70 and _pb_pct < 30:
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
        """分部利润拆分（程序计算）：最新年报净利 × 主营构成的分部利润占比
        增强展示：分部营收/毛利率/同比增速（硬数据）"""
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
            # 增强硬数据：营收绝对值 + 毛利率 + 同比增速
            rev = s.get("revenue")
            if rev and rev > 0:
                seg += f"，营收{rev / 1e8:.2f}亿"
            gm = s.get("gross_margin")
            if gm is not None:
                seg += f"，毛利率{gm}%"
            rev_yoy = s.get("rev_yoy")
            if rev_yoy is not None:
                seg += f"，营收同比{rev_yoy:+.1f}%"
            lines.append(seg + "）")
        lines.append("  （分部间未剔除内部抵消；供分部估值参考用，不是精确分部净利）")
        # 添加分部趋势判读
        declining = [s for s in split if s.get("rev_yoy") is not None and s["rev_yoy"] < 0]
        growing = [s for s in split if s.get("rev_yoy") is not None and s["rev_yoy"] > 0]
        if declining or growing:
            lines.append("  ※ 分部趋势判读：")
            if growing:
                parts = [f"{s['name']}({s['rev_yoy']:+.1f}%)" for s in growing]
                lines.append(f"    增长分部: {', '.join(parts)}")
            if declining:
                parts = [f"{s['name']}({s['rev_yoy']:+.1f}%)" for s in declining]
                lines.append(f"    下滑分部: {', '.join(parts)}")
            # 毛利率变化方向提示
            gm_changes = []
            for s in split:
                gm = s.get("gross_margin")
                if gm is not None:
                    gm_changes.append((s['name'], gm))
            if gm_changes:
                high_gm = max(gm_changes, key=lambda x: x[1])
                low_gm = min(gm_changes, key=lambda x: x[1])
                lines.append(f"    毛利率最高: {high_gm[0]}({high_gm[1]}%)，最低: {low_gm[0]}({low_gm[1]}%)")
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
        from agents.prompts_common import STYLE_RULES, INTERMEDIATE_PRODUCT_NOTE, NUMBER_ACCURACY_RULE
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
    禁止凭记忆引用"同行水平"；必须直接引用【同行对比】表末尾"程序判读"行中的
    原始表述（如"目标PE 21.9倍（同行中位数 23.5倍，高于2/5家）"），
    禁止改写程序判读结论
12. 剩余季度投射计算（硬性要求）：当机构全年预测数据和Q1实际净利润均可获得时，
    必须计算"后三季度需达成净利润 = 全年预测 − Q1实际"及其环比倍数。
    公式：(全年预测 − Q1单季归母) ÷ Q1单季归母 × 100%，
    Q1单季归母必须从【财报趋势】→单季拆分中取最新一期的"归母净利润"（单位：亿元），
    禁止使用任何其他数字（如营收、营业利润、全年累计等）代入公式；
    引用示例："后三季度需达成约XX亿（约Q1的X.X倍，环比+XXX%）"
13. 分部估值参考（SOTP）：仅当同时提供了【分部利润拆分】和【同行对比】时才做——
    每个分部的估值倍数必须取自同行对比表中可比公司的实际 PE（写明取自哪家），
    给低/高两档得到市值区间，与当前总市值比较，结论只说"当前市值处于/高于/低于
    分部估值区间"；必须标注"极粗略参考，非目标价，未计分部协同与控股折价"；
    亏损分部用 0 或注明无法估值，禁止发明倍数
14. 避免给出投资建议，仅做客观分析
15. PB/PE 等估值数据时效性：引用 PE/PB/市值等每日变动数据时，必须注明数据日期
    （如"PB 4.12（7月23日，股票数据库）"），禁止仅写数值不带时间标签；
    若研报中提供了券商目标价和评级，必须在"估值分析"节中以矩阵形式汇总：
    - 列出各券商名称、目标价、评级依据（如"国信证券：36.29元（合理估值）"）
    - 标注来源为研报，并注明"券商目标价仅供参考，不构成投资建议"
    - 给出现价相对平均目标价的空间比例
16. 关键数据点源引用：对创新业务分类/海外业务收入/研发投入等非三表主表数据，必须标注数据来源
    （如"2025年报披露"、"根据网络研究信息"），禁止仅写数据不标注出处

{NUMBER_ACCURACY_RULE}

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
            # 产业链模式：stock_code 为逗号分隔多代码，analyst 取排名第一的候选做财务分析
            if "," in stock_code:
                codes = [c.strip() for c in stock_code.split(",") if c.strip()]
                original_code = stock_code
                stock_code = codes[0] if codes else ""
                logger.info(f"产业链模式：analyst 取第一个候选代码 {stock_code} 做财务分析（原始: {original_code}）")
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

            # 质量否决权计算（ROE/扣非占比/商誉占比）
            quality_metrics = calc_quality_metrics(stock_code, self.db)
            if quality_metrics["triggers"]:
                logger.info(f"质量否决权触发: {quality_metrics['triggers']}，"
                            f"折扣系数 {quality_metrics['quality_discount']}")

            # 标的属性分类（周期股/成长股/防御股/价值股）→ 差异化估值方法
            # 优先从 state 读取 router 统一判定的结果，避免重复调用 classify_stock_attribute
            stock_attr = state.get("stock_attribute") or {}
            if not stock_attr:
                from tools.stock_classifier import classify_stock_attribute
                stock_attr = classify_stock_attribute(stock_code)
            attr_label = stock_attr.get("label", "未分类")
            logger.info(f"标的属性分类: {stock_code} → {stock_attr.get('type', 'unknown')}({attr_label})")
            attr_block = f"""
========== 标的属性分类（程序判定） ==========
属性：{attr_label}（行业：{stock_attr.get('industry', '未知')}）
估值方法指导：{stock_attr.get('valuation_method', '')}
关键关注指标：{stock_attr.get('key_metrics', '')}
{stock_attr.get('valuation_warning', '')}
""" if stock_attr.get("type") != "unknown" else ""

            logger.info("获取研报作为定性补充...")
            report_text = self._fetch_report(stock_code)
            if not report_text or "未获取到" in report_text:
                report_text = state.get("documents", [])
                if report_text:
                    report_text = "\n".join([d.page_content for d in report_text])
                else:
                    report_text = "未获取到研报数据"

            calculated = self._call_financial_tools(financial_data)

            # 构建质量否决权提示块（仅在触发时注入）
            quality_block = ""
            if quality_metrics.get("triggers"):
                quality_block = f"""
========== 质量否决权（程序计算） ==========
ROE：{quality_metrics.get('roe', 'N/A')}%
扣非/归母（dt_eps/eps 近似）：{quality_metrics.get('deduct_net_ratio', 'N/A')}%
商誉/净资产：{quality_metrics.get('goodwill_ratio', 'N/A')}%
质量折扣系数：{quality_metrics['quality_discount']}
触发项：{'；'.join(quality_metrics['triggers'])}

⚠️ 质量否决权提示：以上质量指标触发了否决权，请在分析中明确标注质量折扣及触发原因，
对盈利质量、利润可持续性给出审慎评价；不得回避或隐藏质量风险。
"""
            elif not quality_metrics.get("data_complete", True):
                quality_block = """
========== 质量否决权（程序计算） ==========
⚠️ 质量数据缺失：ROE/扣非/商誉数据均不可用，质量否决权未执行。
请在报告中明确标注“本标的质量指标未经程序验证”，盈利质量评价需格外审慎。
"""

            system_prompt = self._build_system_prompt()
            # 注入历史复盘教训（误判模式 + 改进规则），避免重复同类错误
            review_lesson = self._format_review_lesson(stock_code)
            review_block = (f"\n========== 历史复盘教训 ==========\n{review_lesson}\n"
                            if review_lesson else "")
            user_message = f"""请分析股票 {stock_code} 的财务状况。

【用户问题】
{question}{review_block}

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

🔴 关键事实（必须精确引用，禁止修改）：
- 营收绝对值：{financial_data.get("revenue", "N/A")}亿元
- 营收同比增速：{financial_data.get("revenue_yoy", "N/A")}%
- 归母净利润：{financial_data.get("net_income", "N/A")}亿元
- 归母净利润同比增速：{financial_data.get("profit_yoy", "N/A")}%
- 毛利率：{financial_data.get("gross_margin", "N/A")}%
- PE(TTM)：{financial_data.get("pe_ttm", "N/A")}
- 以上数字由程序从财报精确计算，你在报告中输出的每一个数字必须与此完全一致

========== 计算出的财务比率 ==========
{calculated}
{quality_block}{attr_block}
========== 研报观点（定性补充）==========
{str(report_text)[:3000]}

请基于以上真实财务数据给出专业分析意见。优先使用真实报表数据，研报观点作为补充。"""

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]

            logger.info("LLM 财务分析中...")
            import concurrent.futures
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(self.llm.invoke, messages)
                    response = future.result(timeout=180)
            except concurrent.futures.TimeoutError:
                logger.error("财务分析LLM调用超时（180s）")
                return {"messages": [], "error": "LLM调用超时", "intermediate_steps": [("analyze", {"error": "LLM timeout 180s"})]}
            summary = response.content if hasattr(response, 'content') else str(response)
            logger.info(f"财务分析完成，长度: {len(summary)}")

            # 根据数据完整度判定 confidence（4块齐全=high，2块以上=medium，否则 low）
            data_blocks = [real_data.get("income"), real_data.get("balance_sheet"),
                           real_data.get("cashflow"), real_data.get("main_business")]
            filled_count = sum(1 for d in data_blocks if d and "未获取到" not in str(d))
            if filled_count >= 4:
                confidence = "high"
            elif filled_count >= 2:
                confidence = "medium"
            else:
                confidence = "low"
            logger.info(f"数据完整度: {filled_count}/4 块 → confidence={confidence}")

            self._save_analysis_to_db(state, summary, calculated, confidence=confidence)

            return {
                "messages": [response],
                "financial_data": financial_data,
                "analysis_result": {"summary": summary, "ratios": calculated, "data_source": "real_financial_statements"},
                "quality_metrics": quality_metrics,
                "stock_attribute": stock_attr,
                "intermediate_steps": [("analyze", {"stock_code": stock_code, "content": summary[:200]})],
            }

        except Exception as e:
            logger.error(f"财务分析节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"分析执行失败: {e}",
                "intermediate_steps": [("analyze", {"error": str(e)})],
            }

    def _save_analysis_to_db(self, state: AgentState, analysis_content: str, ratios: Dict[str, Any],
                             confidence: str = "high"):
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
                    confidence=confidence,
                )
                logger.info(f"分析结果已保存: {stock_code}")
        except Exception as e:
            logger.error(f"保存分析结果失败: {e}")

    def invoke(self, state: AgentState) -> Dict[str, Any]:
        return self.analyze_node(state)


def create_analyst_node():
    agent = AnalystAgent()
    return agent.analyze_node