"""
技术分析Agent
职责：
  - 单股模式：拉取日线/周线/月线 → 程序交易计划 → LLM打分分析
  - 产业链模式（stock_code 含逗号分隔多代码）：逐股拉 K 线 → 技术面对比评分 → 选出技术面最强
"""

from datetime import date
from typing import Dict, Any, List, Optional
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import StructuredTool

from agents.base import AgentState
from core.llm import get_technical_llm
from tools import all_stock_tools
from utils.logger import logger

KLINE_DATA_NOTES = """【K线数据说明（重要）】
1. K线行情表按日期倒序（最新在前），价格为前复权
2. 均线(MA5~MA200)、MACD(DIF/DEA/MACD)、RSI、KDJ、BOLL均为程序计算值，请直接引用
3. 信号区（金叉/死叉/均线形态/放量缩量）已由程序精确判定，禁止自行从数字推算交叉
4. 历史信号胜率是条件频率统计，不是对未来的预测，表述时只能说"历史胜率"或"历史统计"，
   禁止说"上涨概率"；胜率在45%~55%区间视为信号意义有限"""


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

    def _build_single_prompt(self) -> str:
        today_str = date.today().strftime('%Y-%m-%d')
        return f"""你是一个专业的股票技术分析师。今天的日期是 {today_str}。
请基于下方提供的日线、周线、月线数据，进行分析。

{KLINE_DATA_NOTES}

【分析要求】
- 趋势：均线排列形态与金叉死叉（直接引用信号区的程序判定结果）、MACD状态与背离
- 动量：RSI/KDJ 的超买超卖状态与拐点
- 位置与区间：BOLL 上中下轨的位置关系、年内位置高低
- 量能：量比/换手率/放缩量信号、OBV 趋势与价格是否背离
- 支撑位和压力位：结合均线、BOLL 轨道和近期高低点给出具体价位
- 波动风险：用 ATR 说明当前波动幅度，给出风险参考位
- 多周期共振分析（日/周/月是否一致）

【程序打分规则（透明公开）】
在"多周期综合研判"中对以下维度逐项打分（满分 10 分）：
- 均线排列形态与趋势（多头/空头/缠绕/粘合）：0-3 分
- MACD信号（金叉/死叉/背离/DIF-DEA位置/柱状线方向）：0-2 分
- 动量与位置（RSI/KDJ超买超卖/年内位置/BOLL位置）：0-2 分
- 量价配合（放量涨/缩量跌/异常放量、OBV趋势）：0-2 分
- 支撑位与压力位清晰度：0-1 分
- 综合判断 = 日线×0.5 + 周线×0.3 + 月线×0.2
  （日线主导短期动能、周线定中期方向、月线约束长期空间）

【多周期空头判定规则（禁止一刀切）】
- 日线金叉出现在周线空头排列阶段：结合月线超卖状态判断，
  不单独给周线空头更高权重
- 周线空头但月线超卖（RSI<35 / 年内位置<20%）：降级为
  「底部磨盘阶段的空头排列」，权重降一档
- 严格区分两种空头：
  A) 下跌中段空头：均线发散向下+MACD柱状线放大 → 标准扣分
  B) 底部磨盘空头：均线粘合/缠绕+MACD柱状线收窄 → 减半扣分，标注"底部磨盘"

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

【🏆 技术面最强】
- 股票代码
- 核心理由（技术面角度，至少2条）
- 风险提示（1条）"""

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

    def _fetch_kline(self, code: str) -> str:
        """拉取单只股票的日线/周线/月线，拼成文本"""
        parts = []
        for tool_name, label in [
            ("stock_daily_fetcher", "日线"),
            ("stock_weekly_fetcher", "周线"),
            ("stock_monthly_fetcher", "月线"),
        ]:
            logger.info(f"  {code} 获取{label}数据...")
            data = self._call_tool(tool_name, code)
            truncated = data[:3000] if len(data) > 3000 else data
            parts.append(f"=== {label} ===\n{truncated}")
        return "\n\n".join(parts)

    def _build_trade_plan(self, code: str) -> tuple:
        """程序计算操作参考计划（买卖区/止损/目标/盈亏比/仓位）"""
        try:
            from tools.trade_plan import build_trade_plan, format_trade_plan
            df_daily = self._call_tool("stock_daily_fetcher", code)
            df_weekly = self._call_tool("stock_weekly_fetcher", code)
            df_monthly = self._call_tool("stock_monthly_fetcher", code)

            import pandas as pd
            from io import StringIO

            def _parse_kline(raw: str) -> Optional[Dict[str, Any]]:
                """从 K 线文本解析最新一行指标快照"""
                if not raw or "获取失败" in raw:
                    return None
                try:
                    lines = raw.strip().split("\n")
                    table_start = None
                    for i, line in enumerate(lines):
                        if "date" in line.lower() or "日期" in line:
                            table_start = i
                            break
                    if table_start is None:
                        return None
                    header = lines[table_start]
                    data_line = lines[table_start + 1] if table_start + 1 < len(lines) else None
                    if not data_line:
                        return None
                    cols = header.strip().split()
                    vals = data_line.strip().split()
                    return dict(zip(cols, vals))
                except Exception:
                    return None

            daily_row = _parse_kline(df_daily)
            weekly_row = _parse_kline(df_weekly)
            monthly_row = _parse_kline(df_monthly)

            if not daily_row:
                return None, ""

            plan = build_trade_plan(daily_row, weekly_row, monthly_row)
            plan_text = format_trade_plan(plan)
            return plan, plan_text
        except Exception as e:
            logger.warning(f"[{code}] 生成交易计划失败: {e}")
            return None, ""

    def _compute_sr_text(self, code: str) -> str:
        """计算支撑压力位文本块（独立于交易计划，供报告直接引用）"""
        try:
            raw = self._call_tool("stock_daily_fetcher", code)
            if not raw or "获取失败" in raw:
                return ""
            import pandas as pd
            from io import StringIO
            lines = raw.strip().split("\n")
            table_start = None
            for i, line in enumerate(lines):
                if "date" in line.lower() or "日期" in line:
                    table_start = i
                    break
            if table_start is None:
                return ""
            header = [c.strip() for c in lines[table_start].strip().split()]
            data_rows = []
            for line in lines[table_start + 1:]:
                line = line.strip()
                if not line:
                    continue
                vals = line.split()
                if len(vals) == len(header):
                    row = dict(zip(header, vals))
                    numeric = {}
                    for key in ("close", "high", "low", "volume"):
                        try:
                            numeric[key] = float(row[key])
                        except (ValueError, TypeError, KeyError):
                            numeric[key] = None
                    data_rows.append(numeric)
            if len(data_rows) < 10:
                return ""
            df = pd.DataFrame(data_rows)
            from tools.support_resistance import compute_sr_levels, format_sr_levels
            sr = compute_sr_levels(df)
            return format_sr_levels(sr) if sr else ""
        except Exception as e:
            logger.warning(f"[{code}] 支撑压力位计算失败: {e}")
            return ""

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            stock_code = state.get("stock_code", "")
            question = state.get("question", "")

            codes = [c.strip() for c in stock_code.split(",") if c.strip()] if stock_code else []

            if len(codes) > 1:
                return self._analyze_chain(state, codes)
            else:
                return self._analyze_single(state, codes[0] if codes else "")

        except Exception as e:
            logger.error(f"技术分析节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"技术分析执行失败: {e}",
                "intermediate_steps": [("technical_analyze", {"error": str(e)})],
            }

    def _analyze_single(self, state: AgentState, code: str) -> Dict[str, Any]:
        """单股技术分析"""
        question = state.get("question", "")
        logger.info(f"技术分析（单股模式），股票: {code}")

        kline_text = self._fetch_kline(code)
        name = self._resolve_name(code)
        label = f"{name}({code})" if name else code
        plan, plan_text = self._build_trade_plan(code)

        # 支撑压力位（独立于交易计划，供 LLM 引用具体价位）
        sr_text = self._compute_sr_text(code)

        trade_block = plan_text if plan_text else ""
        if sr_text:
            trade_block = f"{sr_text}\n\n{trade_block}" if trade_block else sr_text

        messages = [
            SystemMessage(content=self._build_single_prompt()),
            HumanMessage(content=f"""请分析股票 {label} 的技术指标。

【用户问题】{question}

{trade_block}

========== K线数据 ==========
{kline_text}

请按日线→周线→月线→综合研判（含打分表）→技术面总结与风险提示顺序分析。
若上方提供了【操作参考】，在"总结"中原样引用其方向/价位/仓位数字并解释依据，禁止修改数字。"""),
        ]

        response = self.llm.invoke(messages)
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
            },
            "intermediate_steps": [("technical_analyze", {"mode": "single", "code": code})],
        }

    def _analyze_chain(self, state: AgentState, codes: List[str]) -> Dict[str, Any]:
        """产业链多股技术面对比分析"""
        question = state.get("question", "")
        logger.info(f"技术分析（产业链模式），共 {len(codes)} 只: {codes}")

        # 逐只拉 K 线 + 交易计划 + 支撑压力位
        all_kline = ""
        plans, plans_text = {}, {}
        sr_texts = {}  # code → sr_levels_text
        for code in codes:
            name = self._resolve_name(code)
            label = f"{name}({code})" if name else code
            all_kline += f"\n{'#'*60}\n### 股票 {label}\n{self._fetch_kline(code)}\n"
            try:
                plan, plan_text = self._build_trade_plan(code)
                if plan:
                    plans[code] = plan
                    plans_text[code] = plan_text
            except Exception as e:
                logger.warning(f"[{code}] 交易计划生成失败: {e}")
            # 支撑压力位
            try:
                sr_t = self._compute_sr_text(code)
                if sr_t:
                    sr_texts[code] = sr_t
            except Exception:
                pass

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

        messages = [
            SystemMessage(content=self._build_chain_prompt()),
            HumanMessage(content=f"""请对比分析以下 {len(codes)} 只股票的技术面。

【用户问题】{question}

{trade_block}{sr_block}

{all_kline[:25000]}

请逐只打分 → 排名 → 选出技术面最强的1只。
点评你选出的最强标的时，引用其【操作参考】的程序数字（方向/价位/盈亏比/仓位），禁止修改。"""),
        ]

        response = self.llm.invoke(messages)
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
            },
            "intermediate_steps": [("technical_analyze", {"mode": "chain", "count": len(codes)})],
        }


def create_technical_node():
    agent = TechnicalAgent()
    return agent.analyze_node