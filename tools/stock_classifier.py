# -*- coding: utf-8 -*-
"""
标的属性分类器：根据行业 + 财务特征判定周期股/成长股/防御股/价值股。

不同属性的标的，分析重点、估值方法、技术策略完全不同：
  - 周期股：PE逆向逻辑（高PE→景气底部买入，低PE→景气顶部风险）
  - 成长股：PEG/PS估值，趋势跟踪策略
  - 防御股：股息率/PE绝对值，均值回归
  - 价值股：PB/股息率，低位布局

用法：
    from tools.stock_classifier import classify_stock_attribute
    info = classify_stock_attribute("600519")  # → {"type": "defensive", "label": "防御股", ...}
"""

from typing import Dict, Any, Optional
from utils.logger import logger

# ======================================================================
# 行业 → 属性映射表（覆盖A股主要申万/东财行业）
# ======================================================================

_INDUSTRY_MAP = {
    # ---- 周期股 ----
    "钢铁": "cyclical", "有色金属": "cyclical", "煤炭": "cyclical",
    "化工": "cyclical", "基础化工": "cyclical", "建筑材料": "cyclical",
    "建筑装饰": "cyclical", "机械设备": "cyclical", "汽车": "cyclical",
    "汽车整车": "cyclical", "汽车零部件": "cyclical",
    "房地产": "cyclical", "房地产服务": "cyclical",
    "券商": "cyclical", "证券": "cyclical",
    "航空装备": "cyclical", "航运": "cyclical", "港口": "cyclical",
    "石油石化": "cyclical", "石油": "cyclical", "采掘": "cyclical",
    "国防军工": "cyclical", "军工电子": "cyclical",
    "化纤": "cyclical", "塑料": "cyclical", "橡胶": "cyclical",
    "造纸": "cyclical", "包装印刷": "cyclical",
    "有色金属冶炼": "cyclical", "小金属": "cyclical",

    # ---- 成长股 ----
    "半导体": "growth", "半导体及元件": "growth", "芯片": "growth",
    "电子": "growth", "消费电子": "growth", "光学光电子": "growth",
    "医疗器械": "growth", "生物医药": "growth", "医药生物": "growth",
    "化学制药": "growth", "生物制品": "growth", "医疗服务": "growth",
    "电池": "growth", "光伏设备": "growth", "风电设备": "growth",
    "新能源": "growth", "储能": "growth", "新能源车": "growth",
    "计算机": "growth", "软件开发": "growth", "IT服务": "growth",
    "人工智能": "growth", "云计算": "growth", "大数据": "growth",
    "通信": "growth", "通信设备": "growth", "通信服务": "growth",
    "传媒": "growth", "游戏": "growth", "互联网电商": "growth",
    "机器人": "growth", "工业母机": "growth", "航天装备": "growth",
    "专业服务": "growth", "环境治理": "growth",

    # ---- 防御股 ----
    "公用事业": "defensive", "电力": "defensive", "水务": "defensive",
    "燃气": "defensive", "环保": "defensive",
    "高速公路": "defensive", "铁路公路": "defensive", "公路": "defensive",
    "食品饮料": "defensive", "白酒": "defensive", "饮料乳品": "defensive",
    "调味发酵品": "defensive", "休闲食品": "defensive",
    "种植业": "defensive", "养殖业": "defensive", "农业综合": "defensive",

    # ---- 价值股 ----
    "银行": "value", "保险": "value", "多元金融": "value",
    "房地产开发": "value", "建材": "value",
    "物流": "value", "贸易": "value",
}

# 属性标签
_TYPE_LABELS = {
    "cyclical": "周期股",
    "growth": "成长股",
    "defensive": "防御股",
    "value": "价值股",
    "unknown": "未分类",
}

# ======================================================================
# 各属性的分析指导（注入到各Agent的prompt中）
# ======================================================================

_ANALYSIS_GUIDE = {
    "cyclical": {
        "label": "周期股",
        "valuation_method": "PE逆向逻辑 — 高PE（景气底部亏损/微利）→买入信号，低PE（景气顶部高盈利）→卖出风险。禁止用成长股的PE分位逻辑",
        "key_metrics": "产品价格周期、产能利用率、库存周期、ROE均值回归、固定资产周转率",
        "technical_strategy": "均值回归 — 超跌买入（PB<历史10%分位）、超涨卖出（PB>历史90%分位）；RSI极端超卖是周期底部的技术信号",
        "valuation_warning": "⚠️ 周期股估值陷阱：当前PE低不代表便宜（可能是景气顶部），PE高不代表贵（可能是景气底部）。必须结合产品价格周期位置判断",
    },
    "growth": {
        "label": "成长股",
        "valuation_method": "PEG/PS估值 — PE高但增速更快(PEG<1)为合理；亏损公司看PS和营收增速；禁止用周期股的PE逆向逻辑",
        "key_metrics": "渗透率、营收增速拐点、客户拓展、研发投入转化率、订单/出货量趋势、毛利率环比变化",
        "technical_strategy": "趋势跟踪 — 均线多头排列时持有，破位（如跌破MA60）时减仓；突破新高是加仓信号而非卖出信号",
        "valuation_warning": "⚠️ 成长股估值核心在增速持续性：高PE+高增速(PEG<1)=合理，高PE+增速放缓=泡沫。必须判断增速是一次性脉冲还是持续性增长",
    },
    "defensive": {
        "label": "防御股",
        "valuation_method": "股息率 + PE绝对值 — 股息率>4%为低估信号，PE<15倍为合理区间；PB意义不大",
        "key_metrics": "股息率、分红比率、营收稳定性（波动率<10%）、ROE稳定性、自由现金流",
        "technical_strategy": "区间操作 — 在PE历史低位区间买入，高位区间卖出；适合做大波段而非频繁交易",
        "valuation_warning": "⚠️ 防御股看绝对收益：股息率低于3%时估值偏贵，需关注分红可持续性（利润是否覆盖分红）",
    },
    "value": {
        "label": "价值股",
        "valuation_method": "PB + 股息率 — PB<1为深度低估，PB>1.5为偏贵；银行看PB和不良率，地产看NAV折价",
        "key_metrics": "PB、股息率、ROE、资产质量（银行不良率/地产土储质量）、NAV折价率",
        "technical_strategy": "低位布局 — PB历史低位时分批建仓，等待估值修复；破净（PB<1）是深度价值信号",
        "valuation_warning": "⚠️ 价值陷阱：低PB可能是资产质量恶化（银行不良暴露/地产存货减值），需结合资产质量指标判断",
    },
    "unknown": {
        "label": "未分类",
        "valuation_method": "通用PE分位估值",
        "key_metrics": "通用指标",
        "technical_strategy": "通用多周期分析",
        "valuation_warning": "",
    },
}


def _get_industry_from_db(code: str) -> str:
    """从数据库获取行业名称"""
    try:
        from storage.sqlite.stock_storage import get_db
        basic = get_db().get_stock_basic(code)
        if basic:
            return getattr(basic, "industry", "") or ""
    except Exception as e:
        logger.debug(f"[标的分类] {code} 行业获取失败: {e}")
    return ""


def _match_industry(industry: str) -> str:
    """模糊匹配行业名到属性类型"""
    if not industry:
        return "unknown"
    ind = industry.strip()
    # 精确匹配
    if ind in _INDUSTRY_MAP:
        return _INDUSTRY_MAP[ind]
    # 包含匹配（行业名包含关键词）
    for key, val in _INDUSTRY_MAP.items():
        if key in ind or ind in key:
            return val
    return "unknown"


def _validate_by_financials(code: str, industry_type: str) -> str:
    """用财务特征校验分类（补充行业映射的不足）
    返回可能修正后的类型"""
    if industry_type != "unknown":
        return industry_type  # 行业映射已命中，不做财务校验覆盖

    # 行业未命中时，用财务特征推断
    try:
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        fina = db.get_stock_fina_indicator(code)
        if fina is None or fina.empty:
            return "unknown"

        latest = fina.iloc[0]
        # 取近几期的ROE和营收增速
        recent = fina.head(4)
        roe_values = []
        rev_growth_values = []
        for _, row in recent.iterrows():
            try:
                roe = float(row.get("roe", 0) or 0)
                if roe != 0:
                    roe_values.append(roe)
                rev_g = float(row.get("revenue_growth", 0) or 0)
                if rev_g != 0:
                    rev_growth_values.append(rev_g)
            except (TypeError, ValueError):
                continue

        if len(rev_growth_values) >= 3:
            # 计算营收增速波动率（标准差/均值）
            import statistics
            mean_g = statistics.mean(rev_growth_values)
            if abs(mean_g) > 0.1:  # 避免除以接近0的数
                stdev_g = statistics.stdev(rev_growth_values) if len(rev_growth_values) > 1 else 0
                cv = stdev_g / abs(mean_g) if abs(mean_g) > 0.1 else 0
                # 高波动率（变异系数>1.5）→ 周期性特征
                if cv > 1.5 and mean_g > 0:
                    return "cyclical"

            # 持续高增长（均值>25%，且波动不大）→ 成长性特征
            if mean_g > 25 and cv < 1.0:
                return "growth"

            # 极低波动 + 正ROE → 防御性特征
            if len(roe_values) >= 3:
                roe_mean = statistics.mean(roe_values)
                roe_std = statistics.stdev(roe_values) if len(roe_values) > 1 else 0
                if roe_std < 3 and 5 < roe_mean < 20 and abs(mean_g) < 15:
                    return "defensive"

        return "unknown"
    except Exception as e:
        logger.debug(f"[标的分类] {code} 财务特征校验失败: {e}")
        return "unknown"


def classify_stock_attribute(code: str) -> Dict[str, Any]:
    """
    判定标的属性（周期股/成长股/防御股/价值股）。

    逻辑：行业映射为主 → 财务特征校验为辅 → 返回分析指导。

    Returns:
        {
            "type": "cyclical/growth/defensive/value/unknown",
            "label": "周期股/成长股/防御股/价值股/未分类",
            "industry": "行业名",
            "valuation_method": "估值方法指导",
            "key_metrics": "关键指标",
            "technical_strategy": "技术策略指导",
            "valuation_warning": "估值陷阱提示",
        }
    """
    if not code or "," in code:
        return _ANALYSIS_GUIDE["unknown"].copy() | {"type": "unknown", "industry": ""}

    # Step 1: 获取行业
    industry = _get_industry_from_db(code)

    # Step 2: 行业映射
    stock_type = _match_industry(industry)

    # Step 3: 行业未命中时，用财务特征校验
    if stock_type == "unknown":
        stock_type = _validate_by_financials(code, stock_type)

    guide = _ANALYSIS_GUIDE.get(stock_type, _ANALYSIS_GUIDE["unknown"]).copy()
    guide["type"] = stock_type
    guide["industry"] = industry
    return guide


def get_valuation_warning(code: str) -> str:
    """快捷接口：仅返回估值陷阱提示文本"""
    return classify_stock_attribute(code).get("valuation_warning", "")


def is_cyclical(code: str) -> bool:
    """快捷接口：是否为周期股"""
    return classify_stock_attribute(code).get("type") == "cyclical"
