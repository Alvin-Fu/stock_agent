# -*- coding: utf-8 -*-
"""
大盘宏观数据观察器（Macro Watcher）
====================================
定时拉取宏观数据，经 LLM 总结后推送格式化报告到飞书。
每日两次：
  - 开盘前（09:00）：更新前日宏观数据，预判当日情绪
  - 收盘后（19:30）：更新当日数据，总结大环境

所有数据优先读 DB 缓存（同一天不重复请求 API）。
"""

from datetime import date, datetime
from typing import Dict, Any, Optional, List

from utils.logger import logger


def _fmt(val) -> str:
    """格式化数值"""
    if val is None or val == "" or val != val:
        return "N/A"
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return str(val)


def _llm_summarize(data_text: str, session: str = "") -> str:
    """用 LLM 对宏观原始数据进行提炼总结"""
    if not data_text or len(data_text) < 100:
        return ""
    label = {"pre": "开盘", "post": "收盘"}.get(session, "当日")
    prompt = f"""你是一个专业的宏观经济分析师。以下是最新的中国经济与金融市场宏观数据。
请用简洁专业的语言（200字以内）总结核心要点。要求：
1. 先给出**总体判断**（一句话定性当前宏观环境：偏多/中性/偏空，及核心逻辑）
2. 列出 **2-4 个关键信号**（最具信息量的数据变化，每条一行，带数据支撑）
3. 附上 **关注点**（当前最值得跟踪的变数，1-2条，每条一句话）
4. 禁止罗列全部数字——只挑有意义的边际变化和异常值
5. 禁止给出具体的投资建议（如"建议买入"）

{label}宏观数据：
{data_text[:4000]}"""

    try:
        from core.llm import get_agent_llm
        from langchain_core.messages import HumanMessage
        llm = get_agent_llm("researcher")  # 复用 researcher 的模型配置
        resp = llm.invoke([HumanMessage(content=prompt)])
        summary = resp.content if hasattr(resp, 'content') else str(resp)
        summary = summary.strip().strip('"').strip("'")
        # 去掉可能的 markdown 代码块包裹
        if summary.startswith("```"):
            lines = summary.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            summary = "\n".join(lines).strip()
        return summary
    except Exception as e:
        logger.debug(f"[宏观] LLM 总结跳过: {e}")
        return ""


def fetch_macro_snapshot(session: str = "") -> str:
    """
    获取大盘宏观数据快照（文本格式，用于飞书推送）。
    session="pre" 开盘前，session="post" 收盘后，默认空=不加标签。
    含缓存：同一天同 indicator 不重复调 API。
    先通过 LLM 总结，再附上原始数据。"""
    from tools.stock_tools import stock_tool_instance, call_fetch_macro

    parts = []
    today = date.today().isoformat()

    # ===== 1. 大盘资金流向 =====
    try:
        mkt = call_fetch_macro("moneyflow_mkt_dc")
        if mkt:
            parts.append("📊 **大盘资金流向**\n" + mkt[:1500])
    except Exception as e:
        logger.debug(f"[宏观] 大盘资金流向跳过: {e}")

    # ===== 2. 沪深港通资金流向 =====
    try:
        hsgt = call_fetch_macro("moneyflow_hsgt")
        if hsgt:
            parts.append("🔄 **沪深港通资金流向**\n" + hsgt[:1500])
    except Exception as e:
        logger.debug(f"[宏观] 沪深港通跳过: {e}")

    # ===== 3. 融资融券汇总 =====
    try:
        margin = call_fetch_macro("margin")
        if margin:
            parts.append("💰 **融资融券汇总**\n" + margin[:1500])
    except Exception as e:
        logger.debug(f"[宏观] 两融汇总跳过: {e}")

    # ===== 4. 宏观利率 =====
    rates = []
    for name, label in [
        ("shibor", "Shibor利率"),
        ("shibor_lpr", "LPR"),
    ]:
        try:
            d = call_fetch_macro(name)
            if d:
                rates.append(f"**{label}**\n" + d[:1000])
        except Exception:
            pass
    if rates:
        parts.append("📈 **利率环境**\n" + "\n\n".join(rates))

    # ===== 5. 美债收益率 =====
    us_rates = []
    for name, label in [
        ("us_tycr", "国债收益率曲线（1Y/5Y/10Y）"),
        ("us_tltr", "长期利率（10Y/20Y/30Y）"),
    ]:
        try:
            d = call_fetch_macro(name)
            if d:
                us_rates.append(f"**{label}**\n" + d[:800])
        except Exception:
            pass
    if us_rates:
        parts.append("🇺🇸 **美债收益率**\n" + "\n\n".join(us_rates))

    # ===== 6. 宏观经济指标（月频，有更新才显示） =====
    for name, label in [
        ("cn_gdp", "GDP"),
        ("cn_cpi", "CPI"),
        ("cn_ppi", "PPI"),
        ("cn_m", "货币供应量（M2/M1/M0）"),
        ("sf_month", "社融数据"),
    ]:
        try:
            d = call_fetch_macro(name)
            if d:
                parts.append(f"📊 **{label}**\n" + d[:1000])
        except Exception:
            pass

    if not parts:
        return "（宏观数据暂不可用）"

    raw_text = "\n\n---\n\n".join(parts)

    # LLM 总结（失败时返回原始数据核心要点）
    summary = _llm_summarize(raw_text, session=session)
    label = {"pre": "【开盘前】", "post": "【收盘后】"}.get(session, "")
    if summary:
        output = f"{label} 🏛 **大盘宏观数据快照**（{today}）\n"
        output += f"**LLM 核心解读**\n{summary}"
    elif raw_text:
        # LLM 失败时只取原始数据的前 1000 字符核心数值
        output = f"{label} 🏛 **大盘宏观数据快照**（{today}）\n"
        output += raw_text[:1000]
    else:
        return "（宏观数据暂不可用）"
    return output


# ===== 简易运行测试 =====
if __name__ == "__main__":
    print(fetch_macro_snapshot())
