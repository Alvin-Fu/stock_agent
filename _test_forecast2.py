import sys; sys.path.insert(0, '.')
import akshare as ak
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 300)
pd.set_option('display.max_colwidth', 50)

try:
    df = ak.stock_profit_forecast_em(symbol="002594")
    if df is not None and not df.empty:
        print(f"列: {list(df.columns)}")
        print(f"行数: {len(df)}")
        print(df.head(3).to_string())
except Exception as e:
    print(f"EM error: {e}")

print("\n---")

try:
    df2 = ak.stock_profit_forecast_ths(symbol="002594")
    if df2 is not None and not df2.empty:
        print(f"列(ths): {list(df2.columns)}")
        print(df2.head(3).to_string())
except Exception as e:
    print(f"THS error: {e}")
