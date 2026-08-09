"""
技术分析Agent
职责：
  - 单股模式：拉取日线/周线/月线 → 程序交易计划 → LLM打分分析
  - 产业链模式（stock_code 含逗号分隔多代码）：逐股拉 K 线 → 技术面对比评分 → 选出技术面最强
"""

from datetime import date
from typing import Dict, Any, List, Optional
import concurrent.futures
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import StructuredTool

from agents.base import AgentState
from core.llm import get_technical_llm
from tools.weight_adjuster import get_tech_weights
from tools import all_stock_tools
from utils.logger import logger

KLINE_DATA_NOTES = """【K线数据说明（重要）】
1. K线行情表按日期倒序（最新在前），价格为前复权
2. 均线(MA5~MA200)、MACD(DIF/DEA/MACD)、RSI、KDJ、BOLL均为程序计算值，请直接引用
3. 信号区（金叉/死叉/均线形态/放量缩量）已由程序精确判定，禁止自行从数字推算交叉
4. 历史信号胜率是条件频率统计，不是对未来的预测，表述时只能说"历史胜率"或"历史统计"，
   禁止说"上涨概率"；胜率在45%~55%区间视为信号意义有限"""

# ======================================================================
# 各属性的技术策略差异化规则（注入到技术分析 prompt 中）
# 周期股 → 均值回归；成长股 → 趋势跟踪；防御股 → 区间操作；价值股 → 低位布局
# ======================================================================

_ATTR_STRATEGY_RULES = {
    "cyclical": """周期股 → 均值回归策略：
- RSI<30（超卖）是周期底部买入信号，RSI>70（超买）是周期顶部卖出信号——与成长股逻辑相反
- PB历史分位<10%时关注超跌反弹机会，PB>90%时警惕景气见顶回落
- 均线死叉+放量下跌在周期底部可能是"最后一跌"（恐慌出清），而非趋势延续信号
- MACD底背离在周期股中可靠性强于顶背离（周期底部信号比顶部信号更可信）
- 禁止用"均线多头排列→加仓"的成长股逻辑——周期股均线多头排列时往往已接近景气顶部
- 关注BOLL下轨突破后的回归：周期股超跌后均值回归速度快于其他属性""",

    "growth": """成长股 → 趋势跟踪策略：
- 均线多头排列（MA5>MA10>MA20>MA60）是持有/加仓信号，不因短期超买而减仓
- 跌破MA60是趋势破坏信号，需减仓而非"逢低买入"——与周期股逻辑相反
- 突破52周新高是趋势加速信号（加仓时机），而非"涨太多该卖了"
- RSI持续高位（>65）在成长股趋势中是强势特征，不一定是卖出信号
- MACD金叉+放量突破是趋势确认信号，可信度高于均值回归信号
- 禁止用"RSI超买→卖出"的周期股逻辑——成长股超买可能只是趋势起步
- 量能持续放大+价格创新高=资金共识形成，是成长股最强的技术信号""",

    "defensive": """防御股 → 区间操作策略：
- 适合大波段操作：在PE/PB历史低位区间买入，高位区间卖出，不追趋势突破
- RSI极端值（<20或>80）回归中轴的概率高，适合做均值回归但周期更长
- BOLL收口（波动率收缩）后选择方向时，防御股向下突破的概率低于其他属性
- 均线缠绕/粘合是防御股常态（低波动特征），不作为弱势信号
- 关注股息率对应的价位：股息率>4%的价位区间是技术面买入参考
- 禁止用"突破新高→加仓"的成长股逻辑——防御股突破后回落的概率高""",

    "value": """价值股 → 低位布局策略：
- PB<1（破净）是深度价值技术信号，关注分批建仓机会
- 均线长期空头排列但价格企稳+量能萎缩=底部磨盘特征，是布局信号而非卖出信号
- RSI长期低位（<35）在价值股中可能反映"价值陷阱"，需结合资产质量判断
- 突破MA120/MA200（长期均线）是估值修复启动信号，可信度较高
- 关注底部放量（单日量比>2）后的回踩确认——价值股底部放量后的回踩是可靠买点
- 禁止用"趋势跟踪"逻辑——价值股趋势形成慢、回撤大，适合左侧分批而非右侧追涨""",
}


class TechnicalAgent:

    def __init__(self):
        self.name = "technical"
        self.llm = get_technical_llm()
        self.tools = all_stock_tools
        self._tool_map = {tool.name: tool for tool in self.tools}

    @staticmethod
    def _resolve_name(code: str) -> str:
        """反查公司名称，失败返回空串"""
        try:
            from tools.company_code_validator import find_company_name
            return find_company_name(code) or ""
        except Exception:
            return ""

    @staticmethod
    def _format_review_lesson(stock_code: str) -> str:
        """注入该标的最近一次复盘的误判模式和相关改进规则（公共函数代理）"""
        from agents.prompts_common import format_review_lesson
        return format_review_lesson(stock_code)

    def _build_single_prompt(self, analysis: Optional[Dict[str, Any]] = None,
                             quality_metrics: Optional[Dict[str, Any]] = None,
                             code: str = "") -> str:
        today_str = date.today().strftime('%Y-%m-%d')
        # 按标的取技术权重：有 per-code 复盘调权时用专属权重，否则用全局
        w = get_tech_weights(code=code) if code else get_tech_weights()
        w_line = f"- 综合判断 = 日线×{w['daily']:.1f} + 周线×{w['weekly']:.1f} + 月线×{w['monthly']:.1f}"
        # 注入财务分析摘要（供技术分析参考基本面）
        analysis_brief = ""
        if analysis and analysis.get("summary"):
            analysis_brief = f"\n\n【财务分析摘要（供技术面交叉验证参考）】\n{analysis.get('summary', '')[:800]}"
        if quality_metrics and quality_metrics.get("triggers"):
            analysis_brief += f"\n\n⚠️ 质量否决权已触发：{'; '.join(quality_metrics.get('triggers', []))}"
            analysis_brief += "\n技术分析结论需考虑基本面质量风险，不得仅凭技术面信号给出乐观建议。"
        elif quality_metrics and not quality_metrics.get("data_complete", True):
            analysis_brief += "\n\n⚠️ 质量数据缺失，基本面质量未经验证，技术分析结论需附加'质量未验证'提示。"
        return f"""你是一个专业的股票技术分析师。今天的日期是 {today_str}。
请基于下方提供的日线、周线、月线数据，进行分析。

{KLINE_DATA_NOTES}
{analysis_brief}

【分析要求】
- 趋势：均线排列形态与金叉死叉（直接引用信号区的程序判定结果）、MACD状态与背离
- 动量：RSI/KDJ 的超买超卖状态与拐点
- 位置与区间：BOLL 上中下轨的位置关系、年内位置高低
- 量能：量比/换手率/放缩量信号、OBV 趋势与价格是否背离
- 支撑位和压力位：必须使用 K 线数据中的程序计算精确数值（至少精确到小数点后两位，如 MA10=34.30），禁止取整或近似；
  支撑按由近及远列出（MA10 → MA20/BOLL中轨 → BOLL下轨等），压力按由近及远列出（近日最高 → BOLL上轨 → 52周最高等）；
  每个价位必须注明来源（如"34.30（MA10）"），禁止仅写数字不注明依据；
  下方【程序计算关键位】块是摆动点聚类+成交密集区结果（历史真实博弈过的固定价位），
  **必须与均线/BOLL等动态指标位并列展示**，两者互补——指标位看"当前趋势支撑"，聚类位看"历史筹码博弈位"；
  若程序聚类位与技术指标位距离≤1.5%，标注"⭐交叉验证"（两种方法独立算出同一位，可信度高）；
  若程序聚类位与技术指标位偏差>3%，分别列出并说明各自意义；
  压力位至少展示3档：第一档（最近的指标位/聚类位）、第二档（BOLL上轨或MA50）、第三档（52周高点或远位聚类压力）
- 波动风险：用 ATR 说明当前波动幅度，给出风险参考位
- 多周期共振分析（日/周/月是否一致）

【程序打分规则（透明公开）】
在"多周期综合研判"中对以下维度逐项打分（满分 10 分）：
- 均线排列形态与趋势（多头/空头/缠绕/粘合）：0-3 分
- MACD信号（金叉/死叉/背离/DIF-DEA位置/柱状线方向）：0-2 分
- 动量与位置（RSI/KDJ超买超卖/年内位置/BOLL位置）：0-2 分
- 量价配合（放量涨/缩量跌/异常放量、OBV趋势）：0-2 分
- 支撑位与压力位清晰度：0-1 分
{w_line}
  （日线主导短期动能、周线定中期方向、月线约束长期空间）

【技术入场阈值（透明公开）】
- 综合分 ≥ 7.5：强势入场区间，技术面支持积极操作
- 综合分 6.0~7.5：中性区间，需结合基本面判断
- 综合分 < 6.0：谨慎区间，建议观望或减仓

【多周期空头判定规则（禁止一刀切）】
- 日线金叉出现在周线空头排列阶段：结合月线超卖状态判断，
  不单独给周线空头更高权重
- 周线空头但月线超卖（RSI<35 / 年内位置<20%）：降级为
  「底部磨盘阶段的空头排列」，权重降一档
- 严格区分两种空头：
  A) 下跌中段空头：均线发散向下+MACD柱状线放大 → 标准扣分
  B) 底部磨盘空头：均线粘合/缠绕+MACD柱状线收窄 → 减半扣分，标注"底部磨盘"

【多周期分歧处理（重要）】
当出现"日内单项得分≥7（短线已有反弹动能）但综合分因周/月线拖累落在<6.0（谨慎区间）"时：
- **必须**在"总结"段补充以下解释结构：
  1. 先确认日线信号（均线/MACD/量价）已具备短线反弹条件
  2. 再说明周线和月线的具体拖累因素（如周线空头排列、月线趋势未翻转）
  3. 最后给出综合判定：短线可博弈反弹但中期风险未释放，建议观望/控制仓位
- **必须**在报告中标注当前技术权重配置：
  "本版技术打分采用日线权重{w['daily']:.1f}、周线权重{w['weekly']:.1f}、月线权重{w['monthly']:.1f}"
  并说明权重选择理由（如"鉴于'日线强反弹、周月线空头'的短多长空结构，本版采用周线权重0.5、日线0.3、月线0.2"）
- 目标是让读者理解"为什么日线强但综合弱"，避免因总分单一数字放弃正确机会

【关键位跳变处理（重要）】
程序每期重新计算关键位（支撑/压力/止损），当价格大幅波动时，均线/BOLL/ATR等
跟随价格移动的指标会相应位移，导致本期关键位与上一期不同。
- 说明关键位时，必须标出现价与各关键位的距离（如"距现价-3.5%"）
- 若发现某支撑位距现价极近（≤2%）或已被盘中触及/跌破，
  必须说明该位"正在经受考验"并提示可能演化为压力位
- 大盘环境变化（如沪深300转为空头）导致关键位重算时，应注明环境驱动因素
- 禁止将价位变化归因于"算法不稳定"——均线/BOLL/ATR 跟随价格移动是正常的技术特征

【输出格式】
## 日线分析
## 周线分析
## 月线分析
## 多周期综合研判（含打分表）
## 技术面总结与风险提示"""

    def _build_chain_prompt(self) -> str:
        today_str = date.today().strftime('%Y-%m-%d')
        return f"""你是一个专业的股票技术分析师。今天的日期是 {today_str}。
你的任务是：对比分析多只股票的技术面，选出技术走势最强的 1 只。

{KLINE_DATA_NOTES}

【逐只分析】
对每只股票（均线形态/金叉死叉直接引用信号区的程序判定结果）：
- 均线排列形态与趋势（多头/空头/缠绕）：0-3分
- MACD信号（金叉/死叉/背离/DIF-DEA位置）：0-2分
- 动量与位置（RSI/KDJ超买超卖、年内位置、BOLL位置）：0-2分
- 量价配合（放量涨/缩量跌/异常放量、OBV趋势、相对强弱）：0-2分
- 支撑位与压力位清晰度：0-1分
- 技术面总分（满分10分），附一句话判断

【技术面对比排名】
用表格按技术面总分降序：排名 | 股票代码 | 均线 | MACD | 动量位置 | 量价 | 支撑压力 | 总分

【基本面×技术面交叉分析（重要）】
若上方提供了【基本面排名】数据（来自 researcher 产业链分析，含业务/基本面/护城河/边际变化综合评分），
必须在技术面对比中做交叉分析：
- 基本面第1但技术面过热（RSI超买/连续大涨/远离均线/放量滞涨）→ 提示追高风险，建议回踩关注而非追高
- 基本面排名靠后但技术面最强 → 说明资金短期博弈/题材驱动，需提示基本面不支撑的中长期风险
- 基本面与技术面共振（基本面排名靠前+技术面强势）→ 最优标的，可优先考虑
- 在"技术面最强"结论中，须说明该标的基本面排名与技术面是否一致，不一致时给出风险提示

【🏆 技术面最强】
- 股票代码
- 核心理由（技术面角度，至少2条）
- 基本面排名对照（若提供排名数据，说明其基本面名次与一致性判断）
  - 风险提示（1条）"""

    @staticmethod
    def _format_ranking_block(ranked_candidates: Optional[List[Dict[str, Any]]]) -> str:
        """将 researcher 产出的候选公司排名数据格式化为文本块，供技术面对比时交叉分析"""
        if not ranked_candidates:
            return ""
        lines = ["========== 基本面排名（来自 researcher 产业链分析，程序计算的综合排名） =========="]
        for item in ranked_candidates:
            rank = item.get("rank", "?")
            code = item.get("code", "")
            name = item.get("name", "")
            comp = item.get("composite_adj", item.get("composite", 0))
            biz = item.get("business", "-")
            fund = item.get("fundamental", "-")
            moat = item.get("moat", "-")
            mom = item.get("momentum", "-")
            label = f"{name}({code})" if name else code
            extras = []
            pe = item.get("pe_ttm")
            pct = item.get("pe_percentile")
            if pe is not None and pct is not None:
                extras.append(f"PE{pe:.1f}({pct:.0f}%分位)")
            elif pe is not None:
                extras.append(f"PE{pe:.1f}")
            if item.get("total_mv") is not None:
                extras.append(f"市值{item['total_mv']:.0f}亿")
            extra_str = ("｜" + "｜".join(extras)) if extras else ""
            lines.append(f"  第{rank}名 {label} 综合{comp} = 业务{biz} 基本面{fund} "
                         f"护城河{moat} 边际{mom}{extra_str}")
        lines.append("⚠️ 请结合技术面做交叉分析：基本面排名靠前但技术面过热需提示追高风险，"
                     "基本面一般但技术面强势需说明是否为短期博弈。")
        return "\n".join(lines)

    def _call_tool(self, tool_name: str, stock_code: str) -> str:
        tool = self._tool_map.get(tool_name)
        if tool is None:
            return f"工具 {tool_name} 不存在"
        try:
            result = tool.invoke({"stock_code": stock_code})
            return str(result)
        except Exception as e:
            logger.error(f"工具 {tool_name}({stock_code}) 执行失败: {e}")
            return f"获取失败: {e}"

    def _fetch_kline_raw(self, code: str) -> Dict[str, str]:
        """一次性拉取单只股票的日线/周线/月线原始文本，避免重复 API 调用。

        返回 dict: {"daily": raw_text, "weekly": raw_text, "monthly": raw_text}
        其中 raw_text 为 _call_tool 的完整输出（未截断），可供
        _fetch_kline / _build_trade_plan / _compute_sr_text 复用解析。
        """
        raw_data: Dict[str, str] = {}
        for tool_name, key, label in [
            ("stock_daily_fetcher", "daily", "日线"),
            ("stock_weekly_fetcher", "weekly", "周线"),
            ("stock_monthly_fetcher", "monthly", "月线"),
        ]:
            logger.info(f"  {code} 获取{label}数据...")
            raw_data[key] = self._call_tool(tool_name, code)
        return raw_data

    def _fetch_kline(self, code: str, raw_data: Optional[Dict[str, str]] = None) -> str:
        """拉取单只股票的日线/周线/月线，拼成文本。

        raw_data: 若传入 {"daily":..., "weekly":..., "monthly":...}（由 _fetch_kline_raw 产出），
                  则直接复用其原始文本，避免重复调用 _call_tool；否则才调用 _call_tool 拉取。
        截断逻辑 data[:3000] 始终保留。
        """
        parts = []
        for tool_name, key, label in [
            ("stock_daily_fetcher", "daily", "日线"),
            ("stock_weekly_fetcher", "weekly", "周线"),
            ("stock_monthly_fetcher", "monthly", "月线"),
        ]:
            if raw_data is not None and raw_data.get(key):
                data = raw_data[key]
            else:
                logger.info(f"  {code} 获取{label}数据...")
                data = self._call_tool(tool_name, code)
            truncated = data[:3000] if len(data) > 3000 else data
            parts.append(f"=== {label} ===\n{truncated}")
        return "\n\n".join(parts)

    def _build_trade_plan(self, code: str, market_env: Optional[Dict] = None,
                         raw_data: Optional[Dict[str, str]] = None) -> tuple:
        """程序计算操作参考计划（买卖区/止损/目标/盈亏比/仓位）

        market_env: 大盘环境 dict（get_market_env 返回，含 label/close/ma20/ma60），
                    传入后 build_trade_plan 会在大盘逆风时自动降一档仓位。
        raw_data: 若传入 {"daily":..., "weekly":..., "monthly":...}（由 _fetch_kline_raw 产出），
                  则直接复用其原始文本进行 _parse_kline 解析，避免重复调用 _call_tool；
                  否则才调用 _call_tool 拉取。内部 _parse_kline 解析逻辑不变。
        """
        try:
            from tools.trade_plan import build_trade_plan, format_trade_plan
            if raw_data is not None:
                df_daily = raw_data.get("daily", "")
                df_weekly = raw_data.get("weekly", "")
                df_monthly = raw_data.get("monthly", "")
            else:
                df_daily = self._call_tool("stock_daily_fetcher", code)
                df_weekly = self._call_tool("stock_weekly_fetcher", code)
                df_monthly = self._call_tool("stock_monthly_fetcher", code)

            import pandas as pd
            from io import StringIO

            def _smart_split(s: str) -> List[str]:
                """容错分隔：依次尝试逗号 / Tab / 空格"""
                s = s.strip()
                if "," in s:
                    return [c.strip() for c in s.split(",") if c.strip()]
                if "\t" in s:
                    return [c.strip() for c in s.split("\t") if c.strip()]
                return s.split()

            # 列名模糊匹配别名表（header 中没有精确 close 时尝试 收盘/Close/CLOSE 等）
            _COLUMN_ALIASES = {
                "close": ["close", "收盘", "收盘价", "Close", "CLOSE"],
                "high": ["high", "最高", "最高价", "High", "HIGH"],
                "low": ["low", "最低", "最低价", "Low", "LOW"],
                "volume": ["volume", "成交量", "vol", "Vol", "VOL"],
                "date": ["date", "日期", "trade_date", "Date", "DATE"],
            }

            def _normalize_columns(row: Dict[str, str]) -> Dict[str, str]:
                """对已解析的行做列名模糊匹配，确保 close 等标准字段可访问"""
                for canonical, aliases in _COLUMN_ALIASES.items():
                    if canonical not in row:
                        for alias in aliases:
                            if alias in row:
                                row[canonical] = row[alias]
                                break
                return row

            def _parse_kline(raw: str) -> Optional[Dict[str, Any]]:
                """从 K 线文本解析最新一行指标快照（容错：空格/逗号/Tab 分隔 + 列名模糊匹配）"""
                if not raw or "获取失败" in raw:
                    logger.warning(f"K线文本为空或获取失败，无法解析（前80字符: {(raw or '')[:80]}）")
                    return None
                try:
                    lines = raw.strip().split("\n")
                    table_start = None
                    for i, line in enumerate(lines):
                        if "date" in line.lower() or "日期" in line:
                            table_start = i
                            break
                    if table_start is None:
                        logger.warning(f"K线文本中未找到表头行（含 date/日期），解析失败（前200字符: {raw[:200]}）")
                        return None
                    header = lines[table_start]
                    data_line = lines[table_start + 1] if table_start + 1 < len(lines) else None
                    if not data_line:
                        logger.warning(f"K线表头后无数据行，解析失败（表头: {header[:100]}）")
                        return None
                    cols = _smart_split(header)
                    vals = _smart_split(data_line)
                    if len(cols) != len(vals):
                        logger.warning(f"K线表头列数({len(cols)})与数据列数({len(vals)})不匹配，"
                                       f"尝试对齐（表头前8列: {cols[:8]}）")
                        if len(vals) < len(cols):
                            cols = cols[:len(vals)]
                        else:
                            vals = vals[:len(cols)]
                    row = _normalize_columns(dict(zip(cols, vals)))
                    return row
                except Exception as e:
                    logger.warning(f"K线文本解析异常: {e}（前200字符: {raw[:200]}）")
                    return None

            daily_row = _parse_kline(df_daily)
            weekly_row = _parse_kline(df_weekly)
            monthly_row = _parse_kline(df_monthly)

            if not daily_row:
                logger.warning(f"[{code}] 日线K线解析失败(_parse_kline 返回 None)，无法生成交易计划")
                return None, ""

            # 大盘环境降档：传入 label（顺风/逆风/中性），逆风时 build_trade_plan 自动降一档仓位
            market_env_label = market_env.get("label") if market_env else None
            if market_env_label:
                logger.info(f"  {code} 大盘环境: {market_env_label}（接入交易计划仓位降档）")
            plan = build_trade_plan(daily_row, weekly_row, monthly_row, market_env=market_env_label)
            plan_text = format_trade_plan(plan)
            return plan, plan_text
        except Exception as e:
            logger.warning(f"[{code}] 生成交易计划失败: {e}")
            return None, ""

    @staticmethod
    def _compute_fundamental_anchor(code: str) -> Optional[float]:
        """
        计算基本面锚价 = 历史中位PE对应价格。
        用 PE(TTM) 历史分位数线性反推中位PE，再乘以 TTM EPS。
        失败返回 None（不影响支撑位主流程）。
        """
        try:
            from storage.sqlite.stock_storage import get_db
            db = get_db()
            basic = db.get_latest_daily_basic_data(code, 750)
            if basic is None or basic.empty:
                return None
            cur_pe = None
            for _, row in basic.iterrows():
                try:
                    v = float(row.get("pe_ttm", 0) or 0)
                    if v > 0:
                        cur_pe = v
                        break
                except (TypeError, ValueError):
                    continue
            if not cur_pe:
                return None
            hist = pd.to_numeric(basic["pe_ttm"], errors="coerce").dropna()
            if len(hist) < 60:
                return None
            pe_pct = float((hist < cur_pe).mean() * 100)
            if pe_pct <= 0:
                return None
            # 收盘价
            close = None
            for _, row in basic.iterrows():
                try:
                    v = float(row.get("close", 0) or 0)
                    if v > 0:
                        close = v
                        break
                except (TypeError, ValueError):
                    continue
            if not close:
                return None
            # 线性反推历史中位PE
            median_pe_est = cur_pe * 0.5 / (pe_pct / 100)
            anchor_price = close * median_pe_est / cur_pe
            return round(anchor_price, 2)
        except Exception as e:
            logger.debug(f"[技术分析] 基本面锚计算失败 {code}: {e}")
            return None

    def _compute_sr_text(self, code: str, raw_daily: Optional[str] = None) -> str:
        """计算支撑压力位文本块（独立于交易计划，供报告直接引用）。

        raw_daily: 若传入日线原始文本（由 _fetch_kline_raw 返回的 raw_data["daily"]），
                   则直接复用进行 DataFrame 解析，避免重复调用 _call_tool；
                   否则才调用 _call_tool 拉取。DataFrame 解析逻辑不变。
        """
        try:
            if raw_daily is not None:
                raw = raw_daily
            else:
                raw = self._call_tool("stock_daily_fetcher", code)
            if not raw or "获取失败" in raw:
                logger.warning(f"[{code}] 支撑压力位计算：日线文本为空或获取失败（前80字符: {(raw or '')[:80]}）")
                return ""
            import pandas as pd
            from io import StringIO

            # 容错分隔：依次尝试逗号 / Tab / 空格
            def _sr_smart_split(s: str) -> List[str]:
                s = s.strip()
                if "," in s:
                    return [c.strip() for c in s.split(",") if c.strip()]
                if "\t" in s:
                    return [c.strip() for c in s.split("\t") if c.strip()]
                return s.split()

            # 列名模糊匹配别名表
            _SR_COL_ALIASES = {
                "close": ["close", "收盘", "收盘价", "Close", "CLOSE"],
                "high": ["high", "最高", "最高价", "High", "HIGH"],
                "low": ["low", "最低", "最低价", "Low", "LOW"],
                "volume": ["volume", "成交量", "vol", "Vol", "VOL"],
            }

            lines = raw.strip().split("\n")
            table_start = None
            for i, line in enumerate(lines):
                if "date" in line.lower() or "日期" in line:
                    table_start = i
                    break
            if table_start is None:
                logger.warning(f"[{code}] 支撑压力位计算：未找到表头行（含 date/日期），解析失败（前200字符: {raw[:200]}）")
                return ""
            header = _sr_smart_split(lines[table_start])
            data_rows = []
            for line in lines[table_start + 1:]:
                line = line.strip()
                if not line:
                    continue
                vals = _sr_smart_split(line)
                if len(vals) == len(header):
                    row = dict(zip(header, vals))
                    # 列名模糊匹配：确保 close/high/low/volume 可访问
                    for canonical, aliases in _SR_COL_ALIASES.items():
                        if canonical not in row:
                            for alias in aliases:
                                if alias in row:
                                    row[canonical] = row[alias]
                                    break
                    numeric = {}
                    for key in ("close", "high", "low", "volume"):
                        try:
                            numeric[key] = float(row[key])
                        except (ValueError, TypeError, KeyError):
                            numeric[key] = None
                    data_rows.append(numeric)
            if len(data_rows) < 10:
                logger.warning(f"[{code}] 支撑压力位计算：有效数据行不足({len(data_rows)}<10)，无法计算")
                return ""
            df = pd.DataFrame(data_rows)
            from tools.support_resistance import compute_sr_levels, format_sr_levels
            sr = compute_sr_levels(df)
            # 基本面锚校准
            anchor = self._compute_fundamental_anchor(code)
            return format_sr_levels(sr, fundamental_anchor=anchor) if sr else ""
        except Exception as e:
            logger.warning(f"[{code}] 支撑压力位计算失败: {e}")
            return ""

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            stock_code = state.get("stock_code", "")
            question = state.get("question", "")
            analysis = state.get("analysis_result", {})
            quality_metrics = state.get("quality_metrics", {})

            codes = [c.strip() for c in stock_code.split(",") if c.strip()] if stock_code else []

            if len(codes) > 1:
                return self._analyze_chain(state, codes)
            else:
                return self._analyze_single(state, codes[0] if codes else "", analysis, quality_metrics)

        except Exception as e:
            logger.error(f"技术分析节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"技术分析执行失败: {e}",
                "intermediate_steps": [("technical_analyze", {"error": str(e)})],
            }

    def _analyze_single(self, state: AgentState, code: str, analysis: Optional[Dict[str, Any]] = None, quality_metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """单股技术分析"""
        question = state.get("question", "")
        logger.info(f"技术分析（单股模式），股票: {code}")

        # 大盘环境（沪深300程序判定）：逆风时交易计划仓位自动降一档，文本注入分析 prompt
        market_env_dict = None
        market_env_text = ""
        try:
            from tools.market_context import get_market_env, format_market_env
            market_env_dict = get_market_env()
            market_env_text = format_market_env(market_env_dict)
            if market_env_text:
                logger.info(f"  {code} 大盘环境已注入分析 prompt")
        except Exception as e:
            logger.debug(f"  {code} 大盘环境获取失败（不影响分析）: {e}")

        # 一次性拉取日/周/月线原始文本，供后续 _fetch_kline / _build_trade_plan / _compute_sr_text 复用
        raw_data = self._fetch_kline_raw(code)
        kline_text = self._fetch_kline(code, raw_data=raw_data)
        name = self._resolve_name(code)
        label = f"{name}({code})" if name else code
        plan, plan_text = self._build_trade_plan(code, market_env=market_env_dict, raw_data=raw_data)

        # 支撑压力位（独立于交易计划，供 LLM 引用具体价位）
        sr_text = self._compute_sr_text(code, raw_daily=raw_data.get("daily"))

        # 信号历史胜率权重调整建议（需在 kline 数据完成后拉取）
        weight_text = ""
        try:
            from tools.signal_weight_adjuster import get_weight_adjustment_for_code
            weight_text = get_weight_adjustment_for_code(code)
            if weight_text:
                logger.info(f"  {code} 信号权重调整: {weight_text[:80]}")
        except Exception as e:
            logger.debug(f"  {code} 信号权重计算失败: {e}")

        trade_block = plan_text if plan_text else ""
        if sr_text:
            trade_block = f"{sr_text}\n\n{trade_block}" if trade_block else sr_text
        if weight_text:
            trade_block = f"{weight_text}\n\n{trade_block}" if trade_block else weight_text

        # 标的属性分类（周期股/成长股/防御股/价值股）→ 差异化技术策略
        # 优先从 state 读取 router 统一判定的结果，避免重复调用 classify_stock_attribute
        attr_strategy_block = ""
        stock_attr = state.get("stock_attribute") or {}
        if not stock_attr:
            try:
                from tools.stock_classifier import classify_stock_attribute
                stock_attr = classify_stock_attribute(code)
            except Exception as e:
                logger.debug(f"  {code} 标的属性分类失败（不影响分析）: {e}")
        attr_label = stock_attr.get("label", "未分类")
        attr_type = stock_attr.get("type", "unknown")
        if attr_type != "unknown":
            logger.info(f"  {code} 标的属性: {attr_type}({attr_label})")
            attr_strategy_block = f"""
========== 标的属性技术策略（程序判定，必须遵循） ==========
属性：{attr_label}（行业：{stock_attr.get('industry', '未知')}）
技术策略指导：{stock_attr.get('technical_strategy', '')}

【属性差异化分析规则】
{_ATTR_STRATEGY_RULES.get(attr_type, '')}
"""

        # 注入历史复盘教训（误判模式 + 改进规则），避免重复同类错误
        review_lesson = self._format_review_lesson(code)
        review_block = f"\n========== 历史复盘教训 ==========\n{review_lesson}\n" if review_lesson else ""

        market_env_block = market_env_text if market_env_text else ""

        messages = [
            SystemMessage(content=self._build_single_prompt(analysis=analysis, quality_metrics=quality_metrics, code=code)),
            HumanMessage(content=f"""请分析股票 {label} 的技术指标。

【用户问题】{question}

{market_env_block}

{attr_strategy_block}{trade_block}{review_block}

========== K线数据 ==========
{kline_text}

请按日线→周线→月线→综合研判（含打分表）→技术面总结与风险提示顺序分析。
若上方提供了【操作参考】，在"总结"中原样引用其方向/价位/仓位数字并解释依据，禁止修改数字。
若上方提供了【大盘环境】，需在总结中说明大盘环境对个股信号的影响（逆风时多头信号胜率打折、参考仓位已降一档）。
若上方提供了【标的属性技术策略】，必须在分析中遵循该属性的技术策略指导：
  周期股用均值回归逻辑（超卖=底部信号、超买=顶部风险），禁止用趋势跟踪；
  成长股用趋势跟踪逻辑（均线多头排列=持有、破位=减仓），禁止用均值回归；
  防御股用区间操作逻辑（低位买入、高位卖出），关注股息率而非趋势突破；
  价值股用低位布局逻辑（PB历史低位分批建仓），关注破净信号。"""),
        ]

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.llm.invoke, messages)
                response = future.result(timeout=300)
        except concurrent.futures.TimeoutError:
            logger.error("技术分析LLM调用超时（300s）")
            return {"messages": [], "error": "技术分析LLM调用超时", "intermediate_steps": [("technical", {"error": "LLM timeout 300s"})]}
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"技术分析完成，长度: {len(summary)}")

        return {
            "messages": [response],
            "current_node": self.name,
            "technical_result": {
                "summary": summary,
                "mode": "single",
                "code": code,
                "trade_plan": plan,
                "trade_plan_text": plan_text,
                "sr_levels_text": sr_text,  # 供 responder 报告引用
                "stock_attribute": stock_attr,  # 标的属性分类（供 responder 标题标注）
            },
            "intermediate_steps": [("technical_analyze", {"mode": "single", "code": code})],
        }

    def _analyze_chain(self, state: AgentState, codes: List[str]) -> Dict[str, Any]:
        """产业链多股技术面对比分析"""
        question = state.get("question", "")
        logger.info(f"技术分析（产业链模式），共 {len(codes)} 只: {codes}")

        # 大盘环境（沪深300程序判定）：逆风时各股交易计划仓位自动降一档，文本注入对比 prompt
        market_env_dict = None
        market_env_text = ""
        try:
            from tools.market_context import get_market_env, format_market_env
            market_env_dict = get_market_env()
            market_env_text = format_market_env(market_env_dict)
            if market_env_text:
                logger.info("  产业链模式：大盘环境已注入对比分析 prompt")
        except Exception as e:
            logger.debug(f"  大盘环境获取失败（不影响分析）: {e}")

        # 逐只拉 K 线 + 交易计划 + 支撑压力位
        all_kline = ""
        plans, plans_text = {}, {}
        sr_texts = {}  # code → sr_levels_text
        weight_texts = {}  # code → weight_adjustment_text
        for code in codes:
            name = self._resolve_name(code)
            label = f"{name}({code})" if name else code
            # 一次性拉取日/周/月线原始文本，供后续 _fetch_kline / _build_trade_plan / _compute_sr_text 复用
            raw_data = self._fetch_kline_raw(code)
            all_kline += f"\n{'#'*60}\n### 股票 {label}\n{self._fetch_kline(code, raw_data=raw_data)}\n"
            try:
                plan, plan_text = self._build_trade_plan(code, market_env=market_env_dict, raw_data=raw_data)
                if plan:
                    plans[code] = plan
                    plans_text[code] = plan_text
            except Exception as e:
                logger.warning(f"[{code}] 交易计划生成失败: {e}")
            # 支撑压力位
            try:
                sr_t = self._compute_sr_text(code, raw_daily=raw_data.get("daily"))
                if sr_t:
                    sr_texts[code] = sr_t
            except Exception:
                pass
            # 信号历史胜率权重调整建议
            try:
                from tools.signal_weight_adjuster import get_weight_adjustment_for_code
                wt = get_weight_adjustment_for_code(code)
                if wt:
                    weight_texts[code] = wt
                    logger.info(f"  {code} 信号权重调整: {wt[:80]}")
            except Exception as e:
                logger.debug(f"  {code} 信号权重计算失败: {e}")

        # 拼交易计划块
        trade_block = ""
        if plans_text:
            trade_block = "\n\n".join(
                f"=== 操作参考({code}) ===\n{t}" for code, t in plans_text.items())
            trade_block = f"\n{trade_block}\n"
        # 拼支撑压力位块
        sr_block = ""
        if sr_texts:
            sr_block = "\n\n".join(
                f"=== 关键位({code}) ===\n{t}" for code, t in sr_texts.items())
            sr_block = f"\n{sr_block}\n"
        # 拼权重调整建议块
        weight_block = ""
        if weight_texts:
            weight_block = "\n\n".join(
                f"=== 权重调整({code}) ===\n{t}" for code, t in weight_texts.items())
            weight_block = f"\n{weight_block}\n"

        market_env_block = market_env_text if market_env_text else ""

        # 产业链历史复盘教训注入（误判模式 + 通用改进规则）
        industry_review_block = ""
        try:
            from agents.prompts_common import format_review_lesson
            industry_name = state.get("industry_name", "")
            lesson = format_review_lesson(industry_name=industry_name) if industry_name else ""
            if lesson:
                industry_review_block = f"========== 历史复盘教训 ==========\n{lesson}\n"
        except Exception:
            pass

        # 基本面排名（来自 researcher 产业链分析）：注入后供技术面做基本面×技术面交叉分析
        ranked_candidates = state.get("ranked_candidates") or []
        ranking_block = self._format_ranking_block(ranked_candidates)
        if ranking_block:
            logger.info(f"产业链模式：注入基本面排名数据（{len(ranked_candidates)} 家候选）供交叉分析")

        messages = [
            SystemMessage(content=self._build_chain_prompt()),
            HumanMessage(content=f"""请对比分析以下 {len(codes)} 只股票的技术面。

【用户问题】{question}

{industry_review_block}
{market_env_block}

{ranking_block}

{trade_block}{sr_block}{weight_block}

{all_kline[:25000]}

请逐只打分 → 排名 → 选出技术面最强的1只。
点评你选出的最强标的时，引用其【操作参考】的程序数字（方向/价位/盈亏比/仓位），禁止修改。
若上方提供了【大盘环境】，需在对比中说明大盘环境对各股信号的影响（逆风时多头信号胜率打折、参考仓位已降一档）。
若上方提供了【基本面排名】，须做基本面×技术面交叉分析（基本面第1但技术面过热需提示追高风险等）。"""),
        ]

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.llm.invoke, messages)
                response = future.result(timeout=300)
        except concurrent.futures.TimeoutError:
            logger.error("产业链技术面对比LLM调用超时（300s）")
            return {"messages": [], "error": "产业链技术面对比LLM调用超时", "intermediate_steps": [("technical", {"error": "LLM timeout 300s"})]}
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"产业链技术面对比完成，长度: {len(summary)}")

        return {
            "messages": [response],
            "current_node": self.name,
            "technical_result": {
                "summary": summary,
                "mode": "chain",
                "codes": codes,
                "trade_plans": plans,
                "trade_plans_text": plans_text,
                "sr_levels_texts": sr_texts,  # 逐股的支撑压力位
                "ranked_candidates": ranked_candidates,  # 基本面排名（供 responder 展示交叉分析结果）
            },
            "intermediate_steps": [("technical_analyze", {"mode": "chain", "count": len(codes)})],
        }


def create_technical_node():
    agent = TechnicalAgent()
    return agent.analyze_node