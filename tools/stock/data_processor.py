"""
数据处理模块
提供数据验证、转换、清洗和历史对比功能
"""

import pandas as pd
import numpy as np
import statistics
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from enum import Enum
from utils.logger import logger


class DataQualityLevel(Enum):
    """数据质量等级"""
    EXCELLENT = "优秀"
    GOOD = "良好"
    FAIR = "一般"
    POOR = "较差"
    INVALID = "无效"


class ValidationResult:
    """验证结果"""
    def __init__(self, is_valid: bool, quality: DataQualityLevel, errors: List[str], warnings: List[str], metadata: Dict[str, Any] = None):
        self.is_valid = is_valid
        self.quality = quality
        self.errors = errors
        self.warnings = warnings
        self.metadata = metadata or {}


class DataValidator:
    """
    数据验证器
    确保数据的完整性、合法性和一致性
    """

    @staticmethod
    def validate_numeric(
        value: Any,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        allow_none: bool = False
    ) -> ValidationResult:
        """
        验证数值类型数据
        """
        errors = []
        warnings = []

        if value is None:
            if allow_none:
                return ValidationResult(
                    is_valid=True,
                    quality=DataQualityLevel.GOOD,
                    errors=[],
                    warnings=["值为空"],
                    metadata={"value": None}
                )
            else:
                return ValidationResult(
                    is_valid=False,
                    quality=DataQualityLevel.INVALID,
                    errors=["数值为空"],
                    warnings=[],
                    metadata={"original_value": value}
                )

        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                quality=DataQualityLevel.INVALID,
                errors=[f"无法转换为数值: {value}"],
                warnings=[],
                metadata={"original_value": value}
            )

        if min_val is not None and numeric_value < min_val:
            errors.append(f"值{numeric_value}低于最小值{min_val}")

        if max_val is not None and numeric_value > max_val:
            errors.append(f"值{numeric_value}超过最大值{max_val}")

        quality = DataQualityLevel.EXCELLENT if len(errors) == 0 else DataQualityLevel.POOR

        return ValidationResult(
            is_valid=len(errors) == 0,
            quality=quality,
            errors=errors,
            warnings=warnings,
            metadata={"value": numeric_value, "original_value": value}
        )

    @staticmethod
    def validate_percentage(
        value: Any,
        allow_none: bool = False
    ) -> ValidationResult:
        """
        验证百分比数据（0-100或0-1）
        """
        result = DataValidator.validate_numeric(value, allow_none=allow_none)

        if not result.is_valid:
            return result

        numeric_value = result.metadata.get("value")

        if numeric_value is not None:
            if 0 <= numeric_value <= 1:
                normalized_value = numeric_value * 100
                result.metadata["value"] = normalized_value
                result.metadata["normalized"] = True
            elif 0 <= numeric_value <= 100:
                result.metadata["normalized"] = False
            else:
                result.is_valid = False
                result.quality = DataQualityLevel.INVALID
                result.errors.append(f"百分比值{numeric_value}超出合理范围")

        return result

    @staticmethod
    def validate_financial_metrics(
        data: Dict[str, Any],
        required_fields: List[str]
    ) -> ValidationResult:
        """
        验证财务数据完整性
        """
        errors = []
        warnings = []

        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            errors.append(f"缺少必需字段: {missing_fields}")

        for field, value in data.items():
            if value is None:
                warnings.append(f"字段{field}值为空")

            if isinstance(value, (int, float)) and value < 0:
                if field not in ["debt", "loss", "liability"]:
                    warnings.append(f"字段{field}为负值，可能需要检查")

        return ValidationResult(
            is_valid=len(errors) == 0,
            quality=DataQualityLevel.GOOD if len(errors) == 0 else DataQualityLevel.FAIR,
            errors=errors,
            warnings=warnings,
            metadata={"missing_fields": missing_fields}
        )

    @staticmethod
    def validate_consistency(
        current: Dict[str, Any],
        historical: List[Dict[str, Any]],
        tolerance: float = 0.1
    ) -> ValidationResult:
        """
        验证数据一致性（与历史数据对比）
        """
        warnings = []

        for key in current.keys():
            if key in (historical[-1] if historical else {}):
                current_val = current[key]
                prev_val = historical[-1].get(key)

                if isinstance(current_val, (int, float)) and isinstance(prev_val, (int, float)):
                    if prev_val != 0:
                        change = abs((current_val - prev_val) / prev_val)

                        if change > tolerance:
                            warnings.append(
                                f"{key}较上期变化{change*100:.1f}%，超过容忍度{tolerance*100:.1f}%"
                            )

        return ValidationResult(
            is_valid=True,
            quality=DataQualityLevel.GOOD if len(warnings) == 0 else DataQualityLevel.FAIR,
            errors=[],
            warnings=warnings,
            metadata={"consistency_checked_fields": list(current.keys())}
        )


class DataCleaner:
    """
    数据清洗器
    处理缺失值、异常值和数据标准化
    """

    @staticmethod
    def fill_missing_values(
        data: Dict[str, Any],
        strategy: str = "forward"
    ) -> Dict[str, Any]:
        """
        填充缺失值
        strategy: forward(前向), backward(后向), mean(均值), zero(零值)
        """
        cleaned = data.copy()

        for key, value in data.items():
            if value is None or value == "":
                if strategy == "zero":
                    cleaned[key] = 0
                elif strategy == "mean" and isinstance(value, (int, float)):
                    cleaned[key] = statistics.mean([v for v in data.values() if isinstance(v, (int, float))])

        return cleaned

    @staticmethod
    def normalize_numeric_fields(
        data: Dict[str, Any],
        fields: List[str],
        method: str = "minmax"
    ) -> Dict[str, Any]:
        """
        标准化数值字段
        method: minmax(最大最小), zscore(Z分数)
        """
        normalized = data.copy()

        for field in fields:
            if field in data and isinstance(data[field], (int, float)):
                if method == "minmax":
                    pass
                elif method == "zscore":
                    values = [v for v in data.values() if isinstance(v, (int, float))]
                    if len(values) > 1:
                        mean = statistics.mean(values)
                        std = statistics.stdev(values)
                        normalized[field] = (data[field] - mean) / std if std != 0 else 0

        return normalized

    @staticmethod
    def remove_outliers(
        values: List[float],
        method: str = "iqr",
        threshold: float = 1.5
    ) -> List[float]:
        """
        移除异常值
        method: iqr(四分位距), zscore(Z分数)
        """
        if len(values) < 4:
            return values

        if method == "iqr":
            sorted_values = sorted(values)
            q1_idx = len(sorted_values) // 4
            q3_idx = 3 * len(sorted_values) // 4
            q1 = sorted_values[q1_idx]
            q3 = sorted_values[q3_idx]
            iqr = q3 - q1

            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr

            return [v for v in values if lower_bound <= v <= upper_bound]

        elif method == "zscore":
            mean = statistics.mean(values)
            std = statistics.stdev(values) if len(values) > 1 else 1

            return [v for v in values if abs((v - mean) / std) <= threshold]

        return values


class DataComparator:
    """
    数据对比器
    支持时间序列对比、同行对比等
    """

    @staticmethod
    def compare_periods(
        current: Dict[str, Any],
        previous: Dict[str, Any],
        metrics: List[str]
    ) -> Dict[str, Any]:
        """
        同期对比分析
        """
        comparison_results = {}

        for metric in metrics:
            current_val = current.get(metric)
            previous_val = previous.get(metric)

            if current_val is not None and previous_val is not None:
                if isinstance(current_val, (int, float)) and isinstance(previous_val, (int, float)):
                    change = current_val - previous_val
                    change_pct = (change / previous_val * 100) if previous_val != 0 else 0

                    comparison_results[metric] = {
                        "当前值": current_val,
                        "上期值": previous_val,
                        "变化量": change,
                        "变化率": change_pct,
                        "趋势": "上升" if change > 0 else "下降" if change < 0 else "持平"
                    }

        return comparison_results

    @staticmethod
    def compare_industry_peers(
        company_data: Dict[str, Any],
        peer_data: List[Dict[str, Any]],
        metrics: List[str]
    ) -> Dict[str, Any]:
        """
        同行对比分析
        """
        comparison_results = {}

        for metric in metrics:
            values = [peer.get(metric) for peer in peer_data if peer.get(metric) is not None]

            if values and metric in company_data:
                company_val = company_data[metric]

                if isinstance(company_val, (int, float)):
                    mean_val = statistics.mean(values)
                    median_val = statistics.median(values)
                    max_val = max(values)
                    min_val = min(values)

                    percentile = statistics.mean([
                        1 if company_val >= v else 0
                        for v in values
                    ]) * 100

                    comparison_results[metric] = {
                        "公司值": company_val,
                        "行业均值": mean_val,
                        "行业中位数": median_val,
                        "行业最高": max_val,
                        "行业最低": min_val,
                        "行业百分位": percentile,
                        "相对均值": (company_val - mean_val) / mean_val if mean_val != 0 else 0,
                        "相对中位数": (company_val - median_val) / median_val if median_val != 0 else 0
                    }

        return comparison_results

    @staticmethod
    def generate_comparison_report(
        company_data: Dict[str, Any],
        historical_data: List[Dict[str, Any]],
        peer_data: List[Dict[str, Any]],
        key_metrics: List[str]
    ) -> Dict[str, Any]:
        """
        生成综合对比报告
        """
        report = {
            "时间戳": datetime.now().isoformat(),
            "指标数量": len(key_metrics)
        }

        if historical_data:
            report["同期对比"] = DataComparator.compare_periods(
                company_data,
                historical_data[-1] if historical_data else {},
                key_metrics
            )

        if peer_data:
            report["同行对比"] = DataComparator.compare_industry_peers(
                company_data,
                peer_data,
                key_metrics
            )

        return report


# 全局实例
data_validator = DataValidator()
data_cleaner = DataCleaner()
data_comparator = DataComparator()
