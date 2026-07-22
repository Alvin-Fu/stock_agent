"""Use the actual migration code in stock_tools"""
import logging
logging.disable(logging.CRITICAL)
from tools.stock_tools import stock_tool_instance
from storage.sqlite.stock_storage import get_db

# Check BEFORE
db = get_db()
bef = db.get_stock_fina_indicator('002594')
print(f"BEFORE: roe={bef.iloc[0].get('roe')}, netprofit_margin={bef.iloc[0].get('netprofit_margin')}")

# Call the function (this triggers migration in stock_tools)
# But the API might fail - the migration runs inside the function before the API call
result = stock_tool_instance.fetch_and_save_fina_indicator('002594')

# Check AFTER
db2 = get_db()
aft = db2.get_stock_fina_indicator('002594')
print(f"AFTER:  roe={aft.iloc[0].get('roe')}, netprofit_margin={aft.iloc[0].get('netprofit_margin')}")

if result is not None and not result.empty:
    r = result.iloc[0]
    print(f"\nReturned: roe={r.get('roe')}, netprofit_margin={r.get('netprofit_margin')}")
