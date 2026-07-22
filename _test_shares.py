import sys; sys.path.insert(0, '.')
import tushare as ts
pro = ts.pro_api()

# 查总股本
df = pro.daily_basic(ts_code="002594.SZ", trade_date="20260722", fields="ts_code,total_mv,circ_mv")
if df is not None and not df.empty:
    print(f"daily_basic: {df.to_string()}")

# 查 stock_basic
df2 = pro.stock_basic(ts_code="002594.SZ")
if df2 is not None and not df2.empty:
    print(f"stock_basic: {df2.to_string()}")

# 查股本
df3 = pro.top10_holders(ts_code="002594.SZ")
print(f"top10_holders: {list(df3.columns) if df3 is not None else 'None'}")
