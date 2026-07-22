"""触发迁移：调用 fetch_and_save_fina_indicator 触发旧数据归一化"""
import logging
logging.disable(logging.CRITICAL)

from tools.stock_tools import stock_tool_instance, _format_fina_indicator
from storage.sqlite.stock_storage import get_db

print("1. 调用 fetch_and_save_fina_indicator（触发迁移+获取数据）...")
df = stock_tool_instance.fetch_and_save_fina_indicator('002594')

if df is not None and not df.empty:
    print(f"   返回数据: {len(df)} 行")
    latest = df.iloc[0]
    print(f"   最新报告期: {latest.get('report_date')}")
    print(f"   roe: {latest.get('roe')} (应为 0.01646 左右)")
    print(f"   netprofit_margin: {latest.get('netprofit_margin')} (应为 0.0267 左右)")
    print(f"   gross_margin: {latest.get('gross_margin')} (应为 0.188 左右)")
    print(f"   debt_to_assets: {latest.get('debt_to_assets')} (应为 0.7094 左右)")
    print(f"   ar_turn: {latest.get('ar_turn')} (应为 3.70 左右，周转率非百分数不变)")
    
    print("\n2. 验证 _format_fina_indicator 输出...")
    text = _format_fina_indicator(df, '002594')
    for line in text.split('\n'):
        ls = line.strip()
        if any(kw in ls for kw in ['ROE', '毛利率', '净利率', '周转天数', '负债率', '营收增长', '净利润增长']):
            print(f"   {ls}")

print("\n3. 验证 DB 数据已被迁移...")
db = get_db()
db_df = db.get_stock_fina_indicator('002594')
if db_df is not None and not db_df.empty:
    r = db_df.iloc[0]
    print(f"   DB roe: {r.get('roe')} (应 < 1.0 表示已是小数)")
    print(f"   DB netprofit_margin: {r.get('netprofit_margin')} (应 < 1.0)")
else:
    print("   DB 无数据")
