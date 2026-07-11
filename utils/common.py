import unicodedata
from datetime import datetime
import pandas as pd

TASK_NAME_DAILY_TASK = "daily_task"


def sanitize_text(text: str) -> str:
    """
    清理终端输入中的非法 Unicode（如 surrogate 半字符 \\udce5）。
    这类字符会让 logging 和 LLM 请求在 utf-8 编码时直接抛
    UnicodeEncodeError: surrogates not allowed。
    """
    if not text:
        return text
    # encode('utf-8', 'ignore') 会丢弃孤立的 surrogate 字符
    cleaned = text.encode("utf-8", "ignore").decode("utf-8", "ignore")
    try:
        cleaned = unicodedata.normalize("NFKC", cleaned)
    except Exception:
        pass
    return cleaned

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
        # 所有格式都解析失败，返回 None 由调用方处理（不再原样返回字符串）
        return None
    elif isinstance(row_date, datetime):
        row_date = row_date.date()
    elif isinstance(row_date, pd.Timestamp):
        row_date = row_date.date()

    return row_date