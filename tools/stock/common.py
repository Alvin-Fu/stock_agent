from urllib.parse import urlparse
import os


def extract_last_segment_standard(url):
    # 解析URL，获取路径部分
    parsed_url = urlparse(url)
    # 拆分路径，取最后一个元素
    path_parts = parsed_url.path.rsplit('/', 1)
    if len(path_parts) < 2:
        return ""  # 路径异常时返回空
    last_part = path_parts[-1]
    # 移除.pdf后缀
    target_str = last_part.rsplit('.', 1)[0] if '.' in last_part else last_part
    return target_str


def _is_etf_code(stock_code: str) -> bool:
    """
    判断代码是否为 ETF 基金

    ETF 代码规则：
    - 上交所 ETF: 51xxxx, 52xxxx, 56xxxx, 58xxxx
    - 深交所 ETF: 15xxxx, 16xxxx, 18xxxx

    Args:
        stock_code: 股票/基金代码

    Returns:
        True 表示是 ETF 代码，False 表示是普通股票代码
    """
    etf_prefixes = ('51', '52', '56', '58', '15', '16', '18')
    return stock_code.startswith(etf_prefixes) and len(stock_code) == 6


def _is_hk_code(stock_code: str) -> bool:
    """
    判断代码是否为港股

    港股代码规则：
    - 5位数字代码，如 '00700' (腾讯控股)
    - 部分港股代码可能带有前缀，如 'hk00700', 'hk1810'

    Args:
        stock_code: 股票代码

    Returns:
        True 表示是港股代码，False 表示不是港股代码
    """
    # 去除可能的 'hk' 前缀并检查是否为纯数字
    code = stock_code.lower()
    if code.startswith('hk'):
        # 带 hk 前缀的一定是港股，去掉前缀后应为纯数字（1-5位）
        numeric_part = code[2:]
        return numeric_part.isdigit() and 1 <= len(numeric_part) <= 5
    # 无前缀时，5位纯数字才视为港股（避免误判 A 股代码）
    return code.isdigit() and len(code) == 5
