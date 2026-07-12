# -*- coding: utf-8 -*-
"""
机构盈利预测（forward 估值的锚）：
trailing PEG 用的是已披露增速，真正有用的是预期增速。东财有机构一致预测数据，
拉到后原样给 LLM，并要求：计算 forward PE / forward PEG 时必须展示算式、
标注"基于机构预测，仅供参考"。接口列名随版本漂移，generic 渲染，失败跳过。
"""

from typing import Optional

from utils.logger import logger


def fetch_profit_forecast_text(code: str, name: str = "") -> str:
    """机构盈利预测文本块；失败返回空串"""
    try:
        import akshare as ak
        df = None
        for fname, kwargs in (
                ("stock_profit_forecast_em", {"symbol": code}),
                ("stock_profit_forecast_ths", {"symbol": code}),
                ("stock_profit_forecast", {})):
            fn = getattr(ak, fname, None)
            if fn is None:
                continue
            try:
                df = fn(**kwargs)
                if df is not None and not df.empty:
                    # 全市场表时按代码过滤
                    for col in ("代码", "股票代码"):
                        if col in df.columns:
                            df = df[df[col].astype(str).str.contains(code)]
                            break
                    if not df.empty:
                        break
            except Exception as e:
                logger.warning(f"[盈利预测] {fname} 失败: {e}")
                df = None
        if df is None or df.empty:
            return ""
        text = df.head(6).to_string(index=False)[:900]
        return ("【机构盈利预测（东财汇总，预测值仅供参考，不是事实）】\n" + text
                + "\n（使用规则：可据此计算 forward PE=当前总市值÷对应年度预测净利、"
                  "forward PEG=forward PE÷预测增速，计算必须展示算式并标注\"基于机构预测\"；"
                  "预测与已披露实际数矛盾时以实际数为准）")
    except Exception as e:
        logger.warning(f"[盈利预测] 获取失败 {code}: {e}")
        return ""
