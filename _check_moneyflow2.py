# -*- coding: utf-8 -*-
"""对比多只股票 Tushare moneyflow 数据"""
from tools.stock_tools import stock_tool_instance
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

codes = ["002371", "300750", "600519"]  # 北华、宁德、茅台

for code in codes:
    df = stock_tool_instance.tushare.moneyflow(code, trade_date='', start_date='2026-07-20', end_date='2026-07-22')
    if df is not None and not df.empty:
        row = df.iloc[0]
        mf = float(row.get('net_mf_amount', 0))
        td = row.get('trade_date')
        # 报告一下其他字段看看量级
        buy_lg = float(row.get('buy_lg_amount', 0) or 0)
        sell_lg = float(row.get('sell_lg_amount', 0) or 0)
        buy_elg = float(row.get('buy_elg_amount', 0) or 0)
        sell_elg = float(row.get('sell_elg_amount', 0) or 0)
        buy_sm = float(row.get('buy_sm_amount', 0) or 0)
        sell_sm = float(row.get('sell_sm_amount', 0) or 0)
        print(f"{code}: 日期={td} net_mf={mf:.2f} (={mf/1e8:.4f}亿, {mf/1e4:.2f}万)")
        print(f"  大单买={buy_lg:.2f} 大单卖={sell_lg:.2f}")
        print(f"  特大买={buy_elg:.2f} 特大卖={sell_elg:.2f}")
        print(f"  小单买={buy_sm:.2f} 小单卖={sell_sm:.2f}")
    else:
        print(f"{code}: Tushare 返回空")
