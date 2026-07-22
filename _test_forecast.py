import sys
sys.path.insert(0, '/Users/yikexiaobaicai/workspace/python/stock_agent')

# 查 Tushare 盈利预测接口
import tushare as ts
pro = ts.pro_api()

# 尝试 report_rc
for code in ["002594.SZ"]:
    df = pro.report_rc(ts_code=code)
    if df is not None and not df.empty:
        print(f"=== {code} report_rc ===")
        print(f"列: {list(df.columns)}")
        print(df.head(3).to_string())
        break
