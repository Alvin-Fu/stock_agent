"""
技术分析Agent
负责分析股票的均线和MACD等技术指标
"""
from typing import Dict, Any, List
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.tools import StructuredTool

from agents.base import AgentState
from core.llm import get_technical_llm
from tools import all_stock_tools
from utils.logger import logger


class TechnicalAgent:
    """
    技术分析Agent
    负责分析股票的均线和MACD等技术指标
    """

    def __init__(self):
        self.name = "technical"
        self.llm = get_technical_llm()
        self.tools = all_stock_tools
        # 工具名→工具对象映射，供直接调用
        self._tool_map = {tool.name: tool for tool in self.tools}

    def _build_system_prompt(self) -> str:
        return """
你是一个专业的股票技术分析师，擅长分析股票的均线、MACD等技术指标和K线数据。

请基于下方提供的日线、周线、月线数据，对每类数据分别做分析，然后汇总。

【分析要求】
- 均线的金叉死叉信号（5日/10日/20日交叉）
- MACD的金叉死叉信号
- 支撑位和压力位（基于均线位置）
- K线趋势方向和强度
- 成交量变化及异常
- 多周期共振分析（日/周/月是否一致）
- 给出综合评分和操作建议

【输出格式】
## 日线分析
（具体分析...）

## 周线分析
（具体分析...）

## 月线分析
（具体分析...）

## 多周期综合研判
（日/周/月周期是否共振，趋势是否一致...）

## 投资建议与风险提示
（操作建议、支撑/压力位、止损参考...）
"""

    def _call_tool(self, tool_name: str, stock_code: str) -> str:
        """直接调用工具获取数据，只调一次"""
        tool = self._tool_map.get(tool_name)
        if tool is None:
            return f"工具 {tool_name} 不存在"
        try:
            result = tool.invoke({"stock_code": stock_code})
            return str(result)
        except Exception as e:
            logger.error(f"工具 {tool_name}({stock_code}) 执行失败: {e}")
            return f"获取失败: {e}"

    def analyze_node(self, state: AgentState) -> Dict[str, Any]:
        """
        分析节点：直接调用工具获取数据，交给LLM一次性分析
        """
        try:
            stock_code = state.get("stock_code", "")
            question = state.get("question", "")
            logger.info(f"开始技术分析，股票: {stock_code}，问题: {question[:50]}...")

            # 并行（实际是顺序）调用三个工具，每个只调一次
            logger.info("获取日线数据...")
            daily_data = self._call_tool("stock_daily_fetcher", stock_code)

            logger.info("获取周线数据...")
            weekly_data = self._call_tool("stock_weekly_fetcher", stock_code)

            logger.info("获取月线数据...")
            monthly_data = self._call_tool("stock_monthly_fetcher", stock_code)

            # 构建分析提示，直接嵌入所有数据
            system_prompt = self._build_system_prompt()
            user_message = f"""请分析股票 {stock_code} 的技术指标。

【用户问题】
{question}

========== 日线数据 ==========
{daily_data}

========== 周线数据 ==========
{weekly_data}

========== 月线数据 ==========
{monthly_data}

请分别分析日线、周线、月线，然后综合研判，给出专业意见。"""

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message),
            ]

            logger.info("调用LLM进行技术分析...")
            response = self.llm.invoke(messages)
            logger.info(f"技术分析完成，响应长度: {len(str(response.content))}")

            return {
                "messages": [response],
                "current_node": self.name,
                "technical_result": {"summary": response.content},
                "intermediate_steps": state.get("intermediate_steps", []) + [
                    ("technical_analyze", {"stock_code": stock_code, "content": str(response.content)[:200]})
                ],
            }

        except Exception as e:
            logger.error(f"技术分析节点执行失败: {e}")
            return {
                "messages": [],
                "error": f"技术分析执行失败: {e}",
                "intermediate_steps": state.get("intermediate_steps", []) + [("technical_analyze", {"error": str(e)})],
            }

    def invoke(self, state: AgentState) -> Dict[str, Any]:
        return self.analyze_node(state)


def create_technical_node():
    agent = TechnicalAgent()
    return agent.analyze_node
