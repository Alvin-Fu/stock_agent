"""
业务分析增强模块
提供估值分析、风险提示、时间序列分析等高级功能
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta
import statistics


class RiskLevel(Enum):
    """风险等级"""
    VERY_HIGH = "极高风险"
    HIGH = "高风险"
    MEDIUM = "中等风险"
    LOW = "低风险"
    VERY_LOW = "极低风险"


class ValuationLevel(Enum):
    """估值水平"""
    EXTREMELY_CHEAP = "极度低估"
    CHEAP = "低估"
    FAIR = "合理"
    EXPENSIVE = "偏高"
    EXTREMELY_EXPENSIVE = "极度高估"


@dataclass
class ValuationMetrics:
    """估值指标"""
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    peg_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    ev_ebitda: Optional[float] = None


@dataclass
class RiskIndicator:
    """风险指标"""
    indicator_name: str
    value: float
    threshold: float
    risk_level: RiskLevel
    description: str


class ValuationAnalyzer:
    """
    估值分析器
    评估股票估值水平和投资价值
    """

    INDUSTRY_PE_BENCHMARKS = {
        "科技": (30, 50),
        "医药": (25, 40),
        "消费": (20, 35),
        "金融": (8, 15),
        "制造业": (15, 25),
        "能源": (10, 20),
        "房地产": (5, 12),
    }

    @classmethod
    def analyze_pe(cls, pe_ratio: Optional[float], industry: str = "制造业") -> Tuple[str, str]:
        """
        分析市盈率
        Returns: (valuation_level, analysis_text)
        """
        if pe_ratio is None:
            return "无法评估", "缺乏市盈率数据"

        low, high = cls.INDUSTRY_PE_BENCHMARKS.get(industry, (15, 30))

        if pe_ratio < low * 0.5:
            return ValuationLevel.EXTREMELY_CHEAP.value, f"PE={pe_ratio:.1f}极度低于行业基准{low}-{high}"
        elif pe_ratio < low:
            return ValuationLevel.CHEAP.value, f"PE={pe_ratio:.1f}低于行业基准{low}-{high}"
        elif low <= pe_ratio <= high:
            return ValuationLevel.FAIR.value, f"PE={pe_ratio:.1f}处于行业合理区间{low}-{high}"
        elif pe_ratio <= high * 1.5:
            return ValuationLevel.EXPENSIVE.value, f"PE={pe_ratio:.1f}高于行业基准{low}-{high}"
        else:
            return ValuationLevel.EXTREMELY_EXPENSIVE.value, f"PE={pe_ratio:.1f}极度高于行业基准"

    @classmethod
    def analyze_pb(cls, pb_ratio: Optional[float]) -> Tuple[str, str]:
        """分析市净率"""
        if pb_ratio is None:
            return "无法评估", "缺乏市净率数据"

        if pb_ratio < 1:
            return "破净", f"PB={pb_ratio:.2f}低于1，资产价值被低估"
        elif 1 <= pb_ratio < 2:
            return "正常", f"PB={pb_ratio:.2f}处于合理水平"
        elif 2 <= pb_ratio < 5:
            return "偏高", f"PB={pb_ratio:.2f}偏高，市场给予较高溢价"
        else:
            return "极高", f"PB={pb_ratio:.2f}极高，需关注资产质量"

    @classmethod
    def comprehensive_valuation(
        cls,
        metrics: ValuationMetrics,
        industry: str = "制造业"
    ) -> Dict[str, Any]:
        """
        综合估值分析
        """
        pe_level, pe_analysis = cls.analyze_pe(metrics.pe_ratio, industry)
        pb_level, pb_analysis = cls.analyze_pe(metrics.pb_ratio, industry)

        overall_score = 0
        factors = []

        if metrics.pe_ratio:
            if metrics.pe_ratio < 20:
                overall_score += 2
            elif metrics.pe_ratio < 30:
                overall_score += 1
            factors.append({"指标": "PE", "分析": pe_analysis, "得分": overall_score})

        if metrics.dividend_yield:
            div_score = min(metrics.dividend_yield / 3, 3)
            overall_score += div_score
            factors.append({
                "指标": "股息率",
                "分析": f"股息率={metrics.dividend_yield:.2f}%",
                "得分": div_score
            })

        if metrics.peg_ratio:
            if metrics.peg_ratio < 1:
                overall_score += 1
            elif metrics.peg_ratio > 2:
                overall_score -= 1
            factors.append({"指标": "PEG", "分析": f"PEG={metrics.peg_ratio:.2f}", "得分": metrics.peg_ratio})

        investment_recommendation = "中性"
        if overall_score >= 4:
            investment_recommendation = "推荐买入"
        elif overall_score >= 2:
            investment_recommendation = "谨慎买入"
        elif overall_score <= -1:
            investment_recommendation = "建议回避"

        return {
            "综合评分": overall_score,
            "估值水平": pe_level if metrics.pe_ratio else "无法评估",
            "市净率分析": pb_level,
            "投资建议": investment_recommendation,
            "详细因子": factors,
            "时间戳": datetime.now().isoformat()
        }


class RiskAnalyzer:
    """
    风险分析器
    识别和量化投资风险
    """

    @classmethod
    def analyze_concentration_risk(
        cls,
        revenue_distribution: Dict[str, float],
        customer_concentration: Optional[float] = None
    ) -> List[RiskIndicator]:
        """
        分析集中度风险
        """
        indicators = []

        max_concentration = max(revenue_distribution.values()) if revenue_distribution else 0

        if max_concentration > 80:
            level = RiskLevel.VERY_HIGH
        elif max_concentration > 60:
            level = RiskLevel.HIGH
        elif max_concentration > 40:
            level = RiskLevel.MEDIUM
        else:
            level = RiskLevel.LOW

        indicators.append(RiskIndicator(
            indicator_name="收入集中度",
            value=max_concentration,
            threshold=60,
            risk_level=level,
            description=f"最大单一业务占比{max_concentration:.1f}%"
        ))

        if customer_concentration and customer_concentration > 30:
            indicators.append(RiskIndicator(
                indicator_name="客户集中度",
                value=customer_concentration,
                threshold=30,
                risk_level=RiskLevel.HIGH,
                description=f"前五大客户占比{customer_concentration:.1f}%"
            ))

        return indicators

    @classmethod
    def analyze_financial_risk(
        cls,
        debt_ratio: Optional[float] = None,
        current_ratio: Optional[float] = None,
        quick_ratio: Optional[float] = None
    ) -> List[RiskIndicator]:
        """
        分析财务风险
        """
        indicators = []

        if debt_ratio:
            if debt_ratio > 80:
                level = RiskLevel.VERY_HIGH
            elif debt_ratio > 60:
                level = RiskLevel.HIGH
            elif debt_ratio > 40:
                level = RiskLevel.MEDIUM
            else:
                level = RiskLevel.LOW

            indicators.append(RiskIndicator(
                indicator_name="资产负债率",
                value=debt_ratio,
                threshold=60,
                risk_level=level,
                description=f"资产负债率={debt_ratio:.1f}%"
            ))

        if current_ratio:
            if current_ratio < 1:
                level = RiskLevel.HIGH
            elif current_ratio < 1.5:
                level = RiskLevel.MEDIUM
            else:
                level = RiskLevel.LOW

            indicators.append(RiskIndicator(
                indicator_name="流动比率",
                value=current_ratio,
                threshold=1.5,
                risk_level=level,
                description=f"流动比率={current_ratio:.2f}"
            ))

        return indicators

    @classmethod
    def comprehensive_risk_assessment(
        cls,
        business_data: Dict[str, Any],
        financial_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        综合风险评估
        """
        all_indicators = []

        if "revenue_distribution" in business_data:
            concentration_indicators = cls.analyze_concentration_risk(
                business_data.get("revenue_distribution", {}),
                business_data.get("customer_concentration")
            )
            all_indicators.extend(concentration_indicators)

        if "debt_ratio" in financial_data or "current_ratio" in financial_data:
            financial_indicators = cls.analyze_financial_risk(
                financial_data.get("debt_ratio"),
                financial_data.get("current_ratio"),
                financial_data.get("quick_ratio")
            )
            all_indicators.extend(financial_indicators)

        overall_risk_score = sum(
            indicator.risk_level.value.count("高") * 2
            for indicator in all_indicators
        )

        risk_summary = {
            "风险指标数量": len(all_indicators),
            "综合风险评分": overall_risk_score,
            "风险等级": RiskLevel.VERY_HIGH.value if overall_risk_score > 10
                       else RiskLevel.HIGH.value if overall_risk_score > 5
                       else RiskLevel.MEDIUM.value if overall_risk_score > 2
                       else RiskLevel.LOW.value,
            "详细指标": [
                {
                    "名称": ind.indicator_name,
                    "数值": ind.value,
                    "阈值": ind.threshold,
                    "风险等级": ind.risk_level.value,
                    "描述": ind.description
                }
                for ind in all_indicators
            ],
            "风险提示": [
                ind.description
                for ind in all_indicators
                if ind.risk_level in [RiskLevel.HIGH, RiskLevel.VERY_HIGH]
            ],
            "时间戳": datetime.now().isoformat()
        }

        return risk_summary


class TimeSeriesAnalyzer:
    """
    时间序列分析器
    分析趋势、周期性和异常
    """

    @classmethod
    def detect_trend(
        cls,
        values: List[float],
        window: int = 5
    ) -> Dict[str, Any]:
        """
        检测趋势
        使用移动平均和线性回归
        """
        if len(values) < window:
            return {"趋势": "数据不足", "强度": 0}

        ma = statistics.mean(values[-window:])

        if len(values) >= window * 2:
            ma_prev = statistics.mean(values[-window*2:-window])
            ma_curr = statistics.mean(values[-window:])
            trend_strength = (ma_curr - ma_prev) / ma_prev if ma_prev != 0 else 0
        else:
            trend_strength = 0

        if trend_strength > 0.1:
            trend = "上升"
        elif trend_strength < -0.1:
            trend = "下降"
        else:
            trend = "震荡"

        return {
            "趋势": trend,
            "强度": abs(trend_strength),
            "方向": "多头" if trend_strength > 0 else "空头",
            "移动平均": ma,
            "最新值": values[-1] if values else None
        }

    @classmethod
    def detect_seasonality(
        cls,
        values: List[float],
        period: int = 4
    ) -> Dict[str, Any]:
        """
        检测季节性
        """
        if len(values) < period * 2:
            return {"季节性": "数据不足"}

        seasons = [[] for _ in range(period)]
        for i, value in enumerate(values):
            seasons[i % period].append(value)

        seasonal_strengths = []
        for i, season_values in enumerate(seasons):
            if len(season_values) > 1:
                season_mean = statistics.mean(season_values)
                overall_mean = statistics.mean(values)
                strength = (season_mean - overall_mean) / overall_mean if overall_mean != 0 else 0
                seasonal_strengths.append({
                    "周期": i + 1,
                    "均值": season_mean,
                    "偏离度": strength
                })

        return {
            "季节性": "存在" if seasonal_strengths else "不明显",
            "详细": seasonal_strengths
        }

    @classmethod
    def detect_anomalies(
        cls,
        values: List[float],
        threshold: float = 2.0
    ) -> List[Dict[str, Any]]:
        """
        检测异常值
        """
        if len(values) < 3:
            return []

        mean = statistics.mean(values)
        stdev = statistics.stdev(values)

        anomalies = []
        for i, value in enumerate(values):
            z_score = abs((value - mean) / stdev) if stdev != 0 else 0

            if z_score > threshold:
                anomalies.append({
                    "位置": i,
                    "值": value,
                    "Z分数": z_score,
                    "偏离度": (value - mean) / mean if mean != 0 else 0
                })

        return anomalies

    @classmethod
    def analyze_growth_momentum(
        cls,
        values: List[float],
        periods: List[int] = [1, 3, 6, 12]
    ) -> Dict[str, Any]:
        """
        分析增长动能
        """
        if len(values) < 2:
            return {"增长动能": "数据不足"}

        momentum_data = []

        for period in periods:
            if len(values) >= period + 1:
                current = values[-1]
                previous = values[-period - 1]
                growth_rate = (current - previous) / previous if previous != 0 else 0

                momentum_data.append({
                    "周期": f"{period}期",
                    "增长率": growth_rate,
                    "年化增长率": ((1 + growth_rate) ** (12 / period) - 1) if period <= 12 else growth_rate
                })

        avg_growth = statistics.mean([m["增长率"] for m in momentum_data])

        momentum_score = "强劲" if avg_growth > 0.2 else "良好" if avg_growth > 0.05 else "一般" if avg_growth > -0.05 else "疲软"

        return {
            "动能评分": momentum_score,
            "平均增长率": avg_growth,
            "各周期增长": momentum_data,
            "时间戳": datetime.now().isoformat()
        }


valuation_analyzer = ValuationAnalyzer()
risk_analyzer = RiskAnalyzer()
time_series_analyzer = TimeSeriesAnalyzer()
