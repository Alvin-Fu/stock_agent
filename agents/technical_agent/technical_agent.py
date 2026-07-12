"""
技术分析Agent
职责：
  - 单股模式：拉取日线/周线/月线 → 均线/MACD分析
  - 产业链模式（stock_code 含逗号分隔多代码）：逐股拉 K 线 → 技术面对比评分 → 选出技术面最强
"""

from datetime import date
from typing import Dict, Any, List
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import StructuredTool

from agents.base import AgentState
from core.llm import get_technical_llm
from tools import all_stock_tools
from utils.logger import logger

# K线数据说明：喂给 LLM 前必须讲清数据格式，否则模型会自行猜测甚至推算
KLINE_DATA_NOTES = """【数据说明（重要）】
每个周期的数据由四部分组成：
1. 最新指标快照：收盘/涨跌幅/量比/换手率、均线(MA5~MA200)及排列形态、MACD(DIF/DEA/MACD)、
   RSI(6/12/24)、KDJ(K/D/J)、BOLL上中下轨、ATR14、年内位置(0=年内最低,100=年内最高)、OBV趋势
2. 近20根K线信号：金叉/死叉、均线形态、放量/缩量、跳空缺口——**这些信号已由程序精确判定，
   请直接引用解读，禁止自行从数字推算交叉**
   公司名称以数据中提供的为准，**禁止凭记忆推断代码对应的公司名**（未提供名称时只写代码）
3. 信号历史胜率：该股全部历史上同类信号出现后 N 根K线的涨跌统计（胜率/均值/中位数/样本数）。
   使用规则：这是历史条件频率，**不是对未来的预测**，表述时只能说"历史胜率"，禁止说"上涨概率"；
   胜率在45%~55%区间视为信号意义有限；标注"样本偏少"的仅作弱参考；除此之外禁止编造任何概率数字
4. 近10根K线行情表：按日期倒序（最新在前），价格为前复权，volume 单位为股

指标解读参考：RSI>70超买/<30超跌；KDJ的J>100超买/<0超跌；股价触及BOLL上轨承压/下轨支撑；
ATR代表单根K线平均波动幅度，可作为风险提示与止损参考；年内位置反映当前价格在近一年区间的高低。"""


class TechnicalAgent:

    def __init__(self):
        self.name = "technical"
        self.llm = get_technical_llm()
        self.tools = all_stock_tools
        self._tool_map = {tool.name: tool for tool in self.tools}

    def _build_single_prompt(self) -> str:
        return f"""
你是一个专业的股票技术分析师。今天的日期是 {date.today().strftime('%Y-%m-%d')}。
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

【输出格式】
## 日线分析
## 周线分析
## 月线分析
## 多周期综合研判
## 技术面总结与风险提示
"""

    def _build_chain_prompt(self) -> str:
        return f"""你是一个专业的股票技术分析师。今天的日期是 {date.today().strftime('%Y-%m-%d')}。
你的任务是：对比分析多只股票的技术面，选出技术走势最强的 1 只。

{KLINE_DATA_NOTES}

请基于下方提供的每只股票的日线/周线/月线数据，逐只分析并对比如下维度：

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

    @staticmethod
    def _resolve_name(code: str) -> str:
        """从股票基础表反查公司名（查不到返回空串，绝不让 LLM 自行猜名）"""
        try:
            from tools.company_code_validator import find_company_name
            return find_company_name(code) or ""
        except Exception:
            return ""

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

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        try:
            stock_code = state.get("stock_code", "")
            question = state.get("question", "")

            # 判断是单股还是多股（产业链）场景
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

    def _build_trade_plan(self, code: str) -> tuple:
        """拉三周期 df，程序计算操作参考计划；返回 (plan_dict, plan_text)"""
        try:
            from tools.stock_tools import stock_tool_instance, _ensure_indicators
            from tools.trade_plan import build_trade_plan, format_trade_plan
            from tools.support_resistance import compute_sr_levels, format_sr_levels

            def _latest_row(fetch_fn, freq):
                df = fetch_fn(code)
                if df is None or df.empty:
                    return None, None
                df = _ensure_indicators(df, freq)
                return df, df.iloc[0].to_dict()

            df_d, daily_row = _latest_row(stock_tool_instance.fetch_and_save_stock_daily_data, "daily")
            _, weekly_row = _latest_row(stock_tool_instance.fetch_and_save_stock_weekly_data, "week")
            _, monthly_row = _latest_row(stock_tool_instance.fetch_and_save_stock_monthly_data, "month")
            if daily_row is None:
                return None, ""
            recent_low20 = float(df_d.head(20)["low"].min()) if "low" in df_d.columns else None
            recent_high60 = float(df_d.head(60)["high"].max()) if "high" in df_d.columns else None

            # 程序关键位：摆动点聚类+成交密集区（内部已容错，失败返回 None）
            sr = compute_sr_levels(df_d)
            sr_sups = [c["price"] for c in sr["supports"]] if sr else None
            sr_ress = [c["price"] for c in sr["resistances"]] if sr else None

            plan = build_trade_plan(daily_row, weekly_row, monthly_row, recent_low20, recent_high60,
                                    sr_supports=sr_sups, sr_resistances=sr_ress)
            plan_text = format_trade_plan(plan)
            sr_text = format_sr_levels(sr)
            if sr_text:
                plan_text = f"{plan_text}\n{sr_text}" if plan_text else sr_text
            return plan, plan_text
        except Exception as e:
            logger.warning(f"操作参考计划计算失败（不影响技术分析）: {e}")
            return None, ""

    def _analyze_single(self, state: AgentState, code: str) -> Dict[str, Any]:
        """单股技术分析"""
        question = state.get("question", "")
        logger.info(f"技术分析（单股模式），股票: {code}")

        kline_text = self._fetch_kline(code)
        name = self._resolve_name(code)
        label = f"{name}({code})" if name else code
        plan, plan_text = self._build_trade_plan(code)

        messages = [
            SystemMessage(content=self._build_single_prompt()),
            HumanMessage(content=f"""请分析股票 {label} 的技术指标。

【用户问题】{question}

{plan_text if plan_text else ''}

========== K线数据 ==========
{kline_text}

请按日线→周线→月线→综合研判顺序分析；若上方提供了【操作参考】，
在"技术面总结与风险提示"中原样引用其方向/价位/仓位数字并解释依据，禁止修改数字。"""),
        ]

        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"技术分析完成，长度: {len(summary)}")

        return {
            "messages": [response],
            "technical_result": {"summary": summary, "mode": "single", "code": code,
                                 "trade_plan": plan, "trade_plan_text": plan_text},
            "intermediate_steps": [("technical_analyze", {"mode": "single", "code": code})],
        }

    def _analyze_chain(self, state: AgentState, codes: List[str]) -> Dict[str, Any]:
        """产业链多股技术面对比分析"""
        question = state.get("question", "")
        logger.info(f"技术分析（产业链模式），共 {len(codes)} 只: {codes}")

        # 逐只拉 K 线：按股票数均分字符预算，避免从头截断把排在后面的股票整段丢掉
        per_stock_budget = max(4000, 25000 // len(codes))
        all_kline = ""
        for code in codes:
            # 附上验证过的公司名，防止 LLM 凭记忆给代码配错名字（如把鼎龙股份写成别家）
            name = self._resolve_name(code)
            label = f"{name}({code})" if name else code
            kline = self._fetch_kline(code)[:per_stock_budget]
            all_kline += f"\n{'#'*60}\n### 股票 {label}\n{kline}\n"

        messages = [
            SystemMessage(content=self._build_chain_prompt()),
            HumanMessage(content=f"""请对比分析以下 {len(codes)} 只股票的技术面。

【用户问题】{question}

{all_kline}

请逐只打分 → 排名 → 选出技术面最强的1只。"""),
        ]

        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"产业链技术面对比完成，长度: {len(summary)}")

        return {
            "messages": [response],
            "technical_result": {"summary": summary, "mode": "chain", "codes": codes},
            "intermediate_steps": [("technical_analyze", {"mode": "chain", "count": len(codes)})],
        }


def create_technical_node():
    agent = TechnicalAgent()
    return agent.analyze_node
