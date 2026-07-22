"""检查 fina_indicator 数据库数据"""
import logging, pandas as pd
logging.disable(logging.CRITICAL)
from storage.sqlite.stock_storage import get_db

db = get_db()
df = db.get_stock_fina_indicator('002594')
print(f"DB fina_indicator: {type(df).__name__}, empty={df is None or df.empty}")
if df is not None and not df.empty:
    print(f"  shape: {df.shape}")
    print(f"  columns: {list(df.columns)}")
    latest = df.iloc[0]
    print(f"  latest report_date: {latest.get('report_date')}")
    print(f"  roe: {latest.get('roe')}")
    print(f"  netprofit_margin: {latest.get('netprofit_margin')}")
    print(f"  gross_margin: {latest.get('gross_margin')}")
    print(f"  debt_to_assets: {latest.get('debt_to_assets')}")
    print(f"  mbrg: {latest.get('mbrg')}")
    print(f"  nprg: {latest.get('nprg')}")
    print(f"  ar_turn: {latest.get('ar_turn')}")
