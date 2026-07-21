# -*- coding: utf-8 -*-
"""
行业数据路由（Data Router）
==========================
策略：股票代码 → 查行业 → 若有行业专用API → 优先调用
                            → 若无 → 返回None，由Researcher走搜索兜底

当前已注册的行业专用数据源：
  汽车整车 / 汽车配件 / 汽车服务 / 摩托车 → 懂车帝车型月销量API

扩展方式：在 _INDUSTRY_API_MAP 中添加新的行业→数据源映射即可。
"""

from typing import Dict, Any, Optional, Tuple
from datetime import date, timedelta

from utils.logger import logger

# ---- 进程级内存缓存（同一次运行 + 6小时有效期） ----
# 懂车帝月销量数据每月才变一次，每次分析重复拉完全浪费
_INDUSTRY_CACHE: Dict[str, tuple] = {}  # stock_code → (result_dict, expiry_ts)
_CACHE_TTL_HOURS = 6


# ===== 行业与专用数据源映射表（扩展点） =====
# 格式: {行业名: (工具模块名, 工具函数名, 说明)}
_INDUSTRY_API_MAP: Dict[str, Tuple[str, str, str]] = {
    "汽车整车": ("vehicle_sales_fetcher", "call_fetch_vehicle_sales",
                 "懂车帝全国车型月销量，含车型明细/品牌归属/价格区间"),
    "汽车配件": ("vehicle_sales_fetcher", "call_fetch_vehicle_sales",
                 "懂车帝全国车型月销量（配件行业间接参考整车销量）"),
    "汽车服务": ("vehicle_sales_fetcher", "call_fetch_vehicle_sales",
                 "懂车帝全国车型月销量（服务行业间接参考整车销量）"),
    "摩托车": ("vehicle_sales_fetcher", "call_fetch_vehicle_sales",
               "懂车帝全国车型月销量（含摩托车类数据）"),
}

# 行业名模糊匹配规则：included in / includes
_INDUSTRY_ALIASES: Dict[str, list] = {
    "汽车整车": ["汽车", "新能源车", "新能源汽车", "整车", "乘用车", "商用车"],
}

# 备用：用行业名反向查
_INDUSTRY_LOOKUP: Dict[str, str] = {}
for _std_name, _aliases in _INDUSTRY_ALIASES.items():
    for _a in _aliases:
        _INDUSTRY_LOOKUP[_a] = _std_name
# 行业名本身也算
for _std_name in _INDUSTRY_API_MAP:
    _INDUSTRY_LOOKUP[_std_name] = _std_name


def normalize_industry(industry: str) -> Optional[str]:
    """行业名标准化：别名 → 标准名；找不到返回 None"""
    if not industry:
        return None
    if industry in _INDUSTRY_API_MAP:
        return industry
    if industry in _INDUSTRY_LOOKUP:
        return _INDUSTRY_LOOKUP[industry]
    # 包含匹配
    for std_name, aliases in _INDUSTRY_ALIASES.items():
        if any(a in industry for a in aliases):
            return std_name
        if std_name in industry:
            return std_name
    return None


def has_industry_api(industry: str) -> bool:
    """判断某行业是否有专用数据 API"""
    std = normalize_industry(industry)
    return std is not None and std in _INDUSTRY_API_MAP


def get_stock_industry(stock_code: str) -> Optional[str]:
    """根据股票代码从基础表查行业"""
    if not stock_code:
        return None
    try:
        from storage.sqlite.stock_storage import DatabaseManager
        db = DatabaseManager()
        basic = db.get_stock_basic(stock_code)
        if basic is not None:
            return getattr(basic, 'industry', None)
    except Exception as e:
        logger.debug(f"查询股票[{stock_code}]行业失败: {e}")
    return None


def fetch_industry_data(stock_code: str, stock_name: str = "") -> Dict[str, Any]:
    """
    主入口：智能路由获取行业特有数据
    返回: {
        "has_data": bool,
        "industry": "标准行业名",
        "data_text": "格式化后的数据文本（供LLM分析）",
        "source": "数据来源描述",
    }
    """
    if not stock_code:
        return {"has_data": False, "industry": None, "data_text": "", "source": ""}

    # ---- 内存缓存：6小时内同股票不再重复拉取 ----
    import time
    now = time.time()
    cache_key = f"industry:{stock_code}"
    if cache_key in _INDUSTRY_CACHE:
        data, expiry = _INDUSTRY_CACHE[cache_key]
        if now < expiry:
            logger.info(f"行业数据路由: [{stock_code}] 命中内存缓存（6h内），跳过API调用")
            return data

    # 1. 查行业
    industry = get_stock_industry(stock_code)
    if not industry:
        logger.info(f"股票[{stock_code}]未找到行业信息，无专用数据源")
        return {"has_data": False, "industry": None, "data_text": "", "source": ""}

    std_industry = normalize_industry(industry)
    if not std_industry:
        logger.info(f"行业[{industry}]无专用数据源，走通用搜索")
        return {"has_data": False, "industry": industry, "data_text": "", "source": ""}

    # 2. 有专用API → 调用
    api_info = _INDUSTRY_API_MAP.get(std_industry)
    if not api_info:
        return {"has_data": False, "industry": industry, "data_text": "", "source": ""}

    tool_name, func_name, description = api_info
    try:
        # 动态导入调用 - 避免循环依赖
        if tool_name == "vehicle_sales_fetcher":
            from tools.stock_tools import stock_tool_instance
            df = stock_tool_instance.fetch_and_save_vehicle_sales(stock_code=stock_code)
            if df is not None and not df.empty:
                from tools.stock_tools import _format_vehicle_sales
                text = _format_vehicle_sales(df, stock_code)
                logger.info(f"✅ 行业数据路由: [{stock_code}] 匹配到[{std_industry}]，调用 {func_name} 成功")
                result = {
                    "has_data": True,
                    "industry": industry,
                    "std_industry": std_industry,
                    "data_text": text,
                    "source": f"行业专用数据源({description})",
                }
                # 写入内存缓存
                _INDUSTRY_CACHE[cache_key] = (result, now + _CACHE_TTL_HOURS * 3600)
                return result
            else:
                logger.info(f"行业数据路由: [{stock_code}] {func_name} 返回空")
                return {"has_data": False, "industry": industry, "data_text": "",
                        "source": f"行业专用数据源无数据({description})"}
        else:
            logger.warning(f"行业数据路由: 未知工具 {tool_name}")
            return {"has_data": False, "industry": industry, "data_text": "", "source": ""}

    except Exception as e:
        logger.warning(f"行业数据路由: [{stock_code}] 调用 {func_name} 失败: {e}")
        return {"has_data": False, "industry": industry, "data_text": "",
                "source": f"行业专用数据源异常: {e}"}
