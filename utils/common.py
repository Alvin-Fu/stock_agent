from datetime import datetime
import pandas as pd

TASK_NAME_DAILY_TASK = "daily_task"

def parse_row_date(row_date):
    if isinstance(row_date, str):
        date_str = row_date.strip()
        # 定义支持的格式列表（按常见程度排序）
        date_formats = [
            '%Y-%m-%d',  # 1991-04-03
            '%Y%m%d',  # 19910403
            '%Y/%m/%d',  # 1991/04/03
            '%Y.%m.%d',  # 1991.04.03
            '%d/%m/%Y',  # 03/04/1991
            '%m/%d/%Y',  # 04/03/1991
            '%Y年%m月%d日',  # 1991年04月03日
        ]
        # 尝试每种格式
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format).date()
            except ValueError:
                continue
    elif isinstance(row_date, datetime):
        row_date = row_date.date()
    elif isinstance(row_date, pd.Timestamp):
        row_date = row_date.date()

    return row_date