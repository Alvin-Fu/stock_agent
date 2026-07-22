import sys; sys.path.insert(0, '.')
import tushare as ts
pro = ts.pro_api()

df = pro.stock_basic(ts_code="002594.SZ", fields="ts_code,total_share,float_share,market_cap,nmv")
if df is not None and not df.empty:
    print(f"stock_basic fields: {[c for c in df.columns]}")
    print(df.to_string())
else:
    print("stock_basic empty or no fields")

# Try fina_indicator for total shares
df2 = pro.fina_indicator(ts_code="002594.SZ", period="20251231", fields="ts_code,end_date,total_share")
if df2 is not None and not df2.empty:
    print(f"\nfina_indicator total_share: {df2.to_string()}")

# Check stk_holdernumber for shares
df3 = pro.daily_basic(ts_code="002594.SZ", trade_date="20260722")
if df3 is not None and not df3.empty:
    print(f"\ndaily_basic: {df3.to_string()}")
    print(f"\ntotal_mv 万元: {df3.iloc[0]['total_mv']}")
    print(f"circ_mv 万元: {df3.iloc[0]['circ_mv']}")
    try:
        close = df3.iloc[0].get('close')
        if close:
            total_shares = float(df3.iloc[0]['total_mv']) / float(close)  # 万股 / 元 = 万股
            print(f"close: {close} -> total shares: {total_shares} 万股 = {total_shares/10000:.2f}亿股")
    except:
        pass
