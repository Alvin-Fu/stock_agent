# -*- coding: utf-8 -*-
"""
大盘环境（沪深300）：个股操作不能不看大盘脸色。
程序判定三档：顺风（收盘>MA20>MA60）/ 逆风（收盘<MA20<MA60）/ 中性（其余）。
逆风时操作参考的仓位自动降一档（trade_plan 里执行）。当日缓存。
"""

import threading
from datetime import date
from typing import Dict, List, Optional

from utils.logger import logger

_ENV_CACHE = {"day": None, "env": None}
_lock = threading.Lock()


def judge_market_env(closes: List[float]) -> Optional[Dict]:
    """纯函数：closes 为时间升序收盘序列（至少60根）；返回 {label, close, ma20, ma60, chg20}"""
    closes = [c for c in closes if c is not None]
    if len(closes) < 60:
        return None
    close = closes[-1]
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60
    chg20 = (close / closes[-21] - 1) * 100 if len(closes) >= 21 and closes[-21] else None
    if close > ma20 > ma60:
        label = "顺风"
    elif close < ma20 < ma60:
        label = "逆风"
    else:
        label = "中性"
    return {"label": label, "close": round(close, 2), "ma20": round(ma20, 2),
            "ma60": round(ma60, 2), "chg20": round(chg20, 2) if chg20 is not None else None}


def format_market_env(env: Optional[Dict]) -> str:
    if not env:
        return ""
    seg = (f"【大盘环境（沪深300，程序判定）】{env['label']}："
           f"收盘 {env['close']}，MA20 {env['ma20']}，MA60 {env['ma60']}")
    if env.get("chg20") is not None:
        seg += f"，近20日 {'+' if env['chg20'] >= 0 else ''}{env['chg20']}%"
    notes = {"顺风": "指数多头结构，个股信号可按常规执行",
             "逆风": "指数空头结构，个股多头信号胜率打折，参考仓位已自动降一档",
             "中性": "指数方向不明，仓位保持常规偏保守"}
    return seg + f"。{notes[env['label']]}"


def get_market_env() -> Optional[Dict]:
    """取当日大盘环境（当日缓存；失败返回 None 不阻断）"""
    today = date.today()
    if _ENV_CACHE["env"] is not None and _ENV_CACHE["day"] == today:
        return _ENV_CACHE["env"]
    with _lock:
        if _ENV_CACHE["env"] is not None and _ENV_CACHE["day"] == today:
            return _ENV_CACHE["env"]
        try:
            import akshare as ak
            df = ak.stock_zh_index_daily(symbol="sh000300")
            closes = [float(c) for c in df["close"].tolist()[-120:]]
            env = judge_market_env(closes)
            if env:
                _ENV_CACHE.update(day=today, env=env)
            return env
        except Exception as e:
            logger.warning(f"[大盘环境] 沪深300获取失败（不影响分析）: {e}")
            return _ENV_CACHE["env"]
