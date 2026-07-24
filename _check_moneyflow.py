# -*- coding: utf-8 -*-
"""排查资金流向数据管道"""
from tools.stock_tools import stock_tool_instance
import warnings
warnings.filterwarnings("ignore")

code = "002371"
print(f"=== {code} 资金流向管道排查 ===")

# 1. 直接从 Tushare 拉取
print("\n1. Tushare moneyflow 原始数据:")
df = stock_tool_instance.tushare.moneyflow(code, trade_date='', start_date='2026-07-01', end_date='2026-07-22')
if df is not None and not df.empty:
    print(f"  列: {list(df.columns)}")
    row = df.iloc[0]
    print(f"  最新日: {row.get('trade_date')} net_mf_amount={row.get('net_mf_amount')}")
    # _y 格式化
    v = float(row.get('net_mf_amount', 0))
    av = abs(v)
    if av >= 1e8:
        print(f"  _y() 输出: {v/1e8:.2f}亿")
    elif av >= 1e4:
        print(f"  _y() 输出: {v/1e4:.2f}万")
    else:
        print(f"  _y() 输出: {v:.0f}")
else:
    print("  Tushare 返回空")

# 2. 通过 fetch_and_save 拉取并保存
print("\n2. fetch_and_save_stock_moneyflow():")
result = stock_tool_instance.fetch_and_save_stock_moneyflow(code)
if result is not None and not result.empty:
    row = result.iloc[0]
    print(f"  最新日: {row.get('trade_date')} net_mf_amount={row.get('net_mf_amount')}")
    v = float(row.get('net_mf_amount', 0))
    av = abs(v)
    if av >= 1e8:
        print(f"  _y() 输出: {v/1e8:.2f}亿")
    elif av >= 1e4:
        print(f"  _y() 输出: {v/1e4:.2f}万")
    else:
        print(f"  _y() 输出: {v:.0f}")
else:
    print("  fetch_and_save 返回空")
