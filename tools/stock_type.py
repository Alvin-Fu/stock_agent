# -*- coding: utf-8 -*-
"""
标的类型分类器：根据代码/名称判断是 A 股 / ETF / 港股。
用于路由分流——不同标的类型走不同的分析管线。

用法：
    from tools.stock_type import classify
    t, label = classify(code_or_name)  # → ("a_stock", "A股")
"""

import re
from typing import Tuple, Optional


def classify(code: Optional[str] = None, name: Optional[str] = None) -> Tuple[str, str]:
    """
    判断标的类型，返回 (type_key, type_label)。

    type_key: "a_stock" / "etf" / "hk_stock" / "unknown"
    type_label: "A股" / "ETF" / "港股" / "未知"
    """
    if code:
        raw = str(code).strip().upper()
        # 先去掉 .HK 后缀再判断
        raw = raw.replace(".HK", "")
        # 6 位数字 → A 股/ETF 判断（保持前导零，不 lstrip）
        if re.match(r'^\d{6}$', raw):
            prefix = raw[:2]
            # ETF：上交所 51/52/56/58 开头，深交所 15/16/18 开头
            if raw.startswith(('51', '52', '56', '58', '15', '16', '18')):
                return ("etf", "ETF")
            # 沪市 A 股
            if prefix in ('60', '68'):
                return ("a_stock", "A股")
            # 深市 A 股
            if prefix in ('00', '30', '20'):
                return ("a_stock", "A股")
            return ("a_stock", "A股")  # 其余 6 位默认为 A 股

        # 港股：去前导零后为 1-5 位数字
        trimmed = raw.lstrip("0")
        if re.match(r'^\d{1,5}$', trimmed):
            return ("hk_stock", "港股")

    # 从名称推断
    if name:
        name = str(name)
        if any(kw in name for kw in ("ETF", "etf", "指数", "基金")):
            return ("etf", "ETF")
        if any(kw in name for kw in ("港股", "H股", "港交所")):
            return ("hk_stock", "港股")

    # 默认
    return ("a_stock", "A股")


def is_etf(code: Optional[str] = None, name: Optional[str] = None) -> bool:
    """快速判断是否为 ETF"""
    return classify(code=code, name=name)[0] == "etf"


def is_hk(code: Optional[str] = None, name: Optional[str] = None) -> bool:
    """快速判断是否为港股"""
    return classify(code=code, name=name)[0] == "hk_stock"
