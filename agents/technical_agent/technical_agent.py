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

# K线数据说明：喂给 LLM 前必须讲清列含义和行序，否则模型会猜错方向
KLINE_DATA_NOTES = """【数据说明（重要）】
- 数据行按日期**倒序**排列：第一行是最新交易日，越往下越早
- 价格列（open/high/low/close）为**前复权**价格
- ma5/ma10/ma20/ma50/ma120/ma200 为对应周期均线，dif/dea/macd 为 MACD 指标值
- volume 单位为股，volume_ratio 为量比（当日量/前5日均量）
- 判断金叉死叉时注意行序：交叉发生在时间上更晚的行（即更靠上的行）"""


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
- 均线的金叉死叉信号（5日/10日/20日/50日/120日/200日交叉）
- MACD的金叉/死叉/背离信号
- 支撑位和压力位（基于均线和近期高低点）
- K线趋势方向和强度
- 成交量变化及异常
- 多周期共振分析（日/周/月是否一致）

【输出格式】
## 日线分析
## 周线分析
## 月线分析
## 多周期综合研判
## 技术面总结与操作建议
"""

    def _build_chain_prompt(self) -> str:
        return f"""你是一个专业的股票技术分析师。今天的日期是 {date.today().strftime('%Y-%m-%d')}。
你的任务是：对比分析多只股票的技术面，选出技术走势最强的 1 只。

{KLINE_DATA_NOTES}

请基于下方提供的每只股票的日线/周线/月线数据，逐只分析并对比如下维度：

【逐只分析】
对每只股票：
- 均线排列形态（多头/空头/缠绕，5/10/20/50/120/200日）：0-4分
- MACD信号（金叉/死叉/背离/DIF-DEA位置）：0-3分
- 量价配合（放量涨/缩量跌/异常放量）：0-2分
- 支撑位与压力位：0-1分
- 技术面总分（满分10分），附一句话判断

【技术面对比排名】
用表格按技术面总分降序：排名 | 股票代码 | 均线 | MACD | 量价 | 支撑压力 | 总分

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

    def _analyze_single(self, state: AgentState, code: str) -> Dict[str, Any]:
        """单股技术分析"""
        question = state.get("question", "")
        logger.info(f"技术分析（单股模式），股票: {code}")

        kline_text = self._fetch_kline(code)

        messages = [
            SystemMessage(content=self._build_single_prompt()),
            HumanMessage(content=f"""请分析股票 {code} 的技术指标。

【用户问题】{question}

========== K线数据 ==========
{kline_text}

请按日线→周线→月线→综合研判顺序分析。"""),
        ]

        response = self.llm.invoke(messages)
        summary = response.content if hasattr(response, 'content') else str(response)
        logger.info(f"技术分析完成，长度: {len(summary)}")

        return {
            "messages": [response],
            "technical_result": {"summary": summary, "mode": "single", "code": code},
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
            kline = self._fetch_kline(code)[:per_stock_budget]
            all_kline += f"\n{'#'*60}\n### 股票 {code}\n{kline}\n"

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
