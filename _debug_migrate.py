"""Debug migration step by step"""
import logging
logging.disable(logging.CRITICAL)
from storage.sqlite.stock_storage import get_db

db = get_db()

# Check old DB state
old_data = db.get_stock_fina_indicator('002594')
print(f"1. Before migration - roe={old_data.iloc[0].get('roe')}")

# Simulate migration
import pandas as pd
_PCT_FIELDS = {'roe', 'roe_waa', 'roe_dt', 'roa',
               'netprofit_margin', 'gross_margin',
               'debt_to_assets', 'debt_to_eqy', 'n_cashflow_to_liab',
               'profit_to_gr'}
_dirty = False
for col in _PCT_FIELDS & set(old_data.columns):
    _mask = old_data[col].notna() & (old_data[col].abs() >= 1.0)
    if _mask.any():
        print(f"  Migrating {col}: {old_data.loc[_mask, col].iloc[0]} -> {old_data.loc[_mask, col].iloc[0]/100}")
        old_data.loc[_mask, col] = old_data.loc[_mask, col].astype(float) / 100.0
        _dirty = True

print(f"2. Dirty={_dirty}, after migration - roe={old_data.iloc[0].get('roe')}")

# Save
if _dirty:
    save_count = db.save_stock_fina_indicator(old_data, '002594')
    print(f"3. Save count={save_count}")

# Verify DB
db2 = get_db()
verify = db2.get_stock_fina_indicator('002594')
print(f"4. After save - roe={verify.iloc[0].get('roe')}")
