import unicodedata
from datetime import datetime
import pandas as pd

# pandas 3 兼容：新版默认字符串列用 pyarrow 后端，其正则走 RE2 引擎，
# 而 akshare 内部大量正则模式含 \u3000 这类 \u 转义的清洗代码 RE2 不支持，
# 会报 "Invalid regular expression: invalid escape sequence: \u"
# （东财新闻等一批接口都会炸）。这里强制回退 Python 字符串后端（走 re 引擎，
# 即 pandas 2 的老默认行为）。本模块被 utils 包入口加载，进程内先于 akshare 生效。
try:
    pd.set_option("mode.string_storage", "python")
except Exception:
    pass
try:
    # pandas 2.1~2.2 的实验开关（3.0 已移除，报错忽略即可）
    pd.set_option("future.infer_string", False)
except Exception:
    pass

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