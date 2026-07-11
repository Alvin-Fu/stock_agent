"""
数据质量保障模块
提供数据完整性检查、校验和版本管理功能
"""

import pandas as pd
import numpy as np
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


class ValidationError(Exception):
    """数据验证异常"""
    pass


class DataValidator:
    """
    数据完整性校验器
    
    校验内容：
    1. 必需列检查
    2. 数值范围检查
    3. 数据类型检查
    4. 时间连续性检查
    """

    # K线数据必需列
    KLINE_REQUIRED_COLUMNS = ['date', 'open', 'high', 'low', 'close', 'volume']
    
    # 数值范围限制
    VALUE_RANGES = {
        'close': {'min': 0.01, 'max': 10000},
        'open': {'min': 0.01, 'max': 10000},
        'high': {'min': 0.01, 'max': 10000},
        'low': {'min': 0.01, 'max': 10000},
        'volume': {'min': 0, 'max': 1e12},
        'amount': {'min': 0, 'max': 1e15},
        'pct_chg': {'min': -100, 'max': 100},
    }

    @classmethod
    def validate_kline_data(cls, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        校验K线数据完整性
        
        Returns:
            (is_valid: bool, errors: List[str])
        """
        errors = []
        
        if df is None or df.empty:
            return False, ["数据为空"]
        
        # 1. 检查必需列
        missing_cols = [col for col in cls.KLINE_REQUIRED_COLUMNS if col not in df.columns]
        if missing_cols:
            errors.append(f"缺少必需列: {', '.join(missing_cols)}")
        
        # 2. 检查日期列
        if 'date' in df.columns:
            # 检查日期格式
            try:
                pd.to_datetime(df['date'])
            except Exception as e:
                errors.append(f"日期列格式错误: {e}")
            
            # 检查日期连续性（允许周末和节假日缺口）
            dates = pd.to_datetime(df['date']).sort_values()
            date_diff = dates.diff().dt.days.dropna()
            # 正常交易日间隔应该是1天，超过5天可能有数据缺失
            if (date_diff > 5).any():
                max_gap = date_diff.max()
                errors.append(f"发现日期缺口，最大间隔{int(max_gap)}天")
        
        # 3. 检查数值范围
        for col, range_dict in cls.VALUE_RANGES.items():
            if col in df.columns:
                series = df[col]
                if series.min() < range_dict['min']:
                    errors.append(f"{col}最小值{series.min()}低于允许范围{range_dict['min']}")
                if series.max() > range_dict['max']:
                    errors.append(f"{col}最大值{series.max()}超出允许范围{range_dict['max']}")
                
                # 检查是否有负值（成交量除外）
                if col not in ['volume', 'amount', 'pct_chg']:
                    if (series < 0).any():
                        errors.append(f"{col}包含负值")
        
        # 4. 检查价格逻辑
        if all(col in df.columns for col in ['high', 'low', 'open', 'close']):
            # 最高价应大于等于最低价
            if (df['high'] < df['low']).any():
                errors.append("存在最高价小于最低价的异常数据")
            
            # 开盘价和收盘价应在高低价之间
            if ((df['open'] < df['low']) | (df['open'] > df['high'])).any():
                errors.append("存在开盘价超出高低价范围的异常数据")
            
            if ((df['close'] < df['low']) | (df['close'] > df['high'])).any():
                errors.append("存在收盘价超出高低价范围的异常数据")
        
        # 5. 检查成交量
        if 'volume' in df.columns:
            if (df['volume'] == 0).any():
                zero_count = (df['volume'] == 0).sum()
                errors.append(f"存在{zero_count}条成交量为0的记录")
        
        return len(errors) == 0, errors

    @classmethod
    def validate_financial_data(cls, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        校验财务数据完整性
        
        Returns:
            (is_valid: bool, errors: List[str])
        """
        errors = []
        
        if df is None or df.empty:
            return False, ["数据为空"]
        
        # 检查关键财务指标
        financial_cols = ['revenue', 'net_income', 'total_assets', 'total_liabilities']
        
        for col in financial_cols:
            if col in df.columns:
                # 检查负值（负债除外）
                if col != 'total_liabilities' and (df[col] < 0).any():
                    errors.append(f"{col}包含负值")
        
        # 检查资产=负债+权益的会计恒等式
        if all(col in df.columns for col in ['total_assets', 'total_liabilities', 'total_equity']):
            df['check_sum'] = df['total_liabilities'] + df['total_equity']
            diff = df['total_assets'] - df['check_sum']
            if (abs(diff) / df['total_assets'] > 0.01).any():
                errors.append("资产负债表不平衡（误差超过1%）")
        
        return len(errors) == 0, errors

    @classmethod
    def calculate_quality_score(cls, df: pd.DataFrame, data_type: str) -> DataQualityLevel:
        """
        计算数据质量分数
        
        Args:
            df: 数据DataFrame
            data_type: 数据类型（'kline', 'financial', 'research'）
        
        Returns:
            数据质量等级
        """
        if df is None or df.empty:
            return DataQualityLevel.INVALID
        
        if data_type == 'kline':
            is_valid, errors = cls.validate_kline_data(df)
        elif data_type == 'financial':
            is_valid, errors = cls.validate_financial_data(df)
        else:
            is_valid = True
            errors = []
        
        if not is_valid:
            # 检查错误严重程度（子串匹配，错误信息带具体上下文，精确相等永远匹配不上）
            critical_keywords = ['缺少必需列', '格式错误', '异常数据']
            has_critical = any(
                any(keyword in error for keyword in critical_keywords)
                for error in errors
            )
            
            if has_critical:
                return DataQualityLevel.INVALID
            elif len(errors) >= 3:
                return DataQualityLevel.POOR
            else:
                return DataQualityLevel.FAIR
        
        # 检查数据完整性
        completeness = cls._calculate_completeness(df)
        
        if completeness >= 95:
            return DataQualityLevel.EXCELLENT
        elif completeness >= 80:
            return DataQualityLevel.GOOD
        else:
            return DataQualityLevel.FAIR

    @classmethod
    def _calculate_completeness(cls, df: pd.DataFrame) -> float:
        """计算数据完整性百分比"""
        total_cells = df.size
        missing_cells = df.isnull().sum().sum()
        return ((total_cells - missing_cells) / total_cells) * 100


class DataVersionManager:
    """
    数据版本管理器
    
    功能：
    1. 记录数据版本信息
    2. 支持版本回溯
    3. 跟踪数据来源和更新时间
    """

    def __init__(self):
        self.versions: Dict[str, Dict[str, Dict]] = {}  # {stock_code: {data_type: version_info}}
    
    def record_version(
        self,
        stock_code: str,
        data_type: str,
        source_name: str,
        record_count: int,
        quality_level: DataQualityLevel
    ):
        """
        记录数据版本
        
        Args:
            stock_code: 股票代码
            data_type: 数据类型
            source_name: 数据源名称
            record_count: 记录条数
            quality_level: 数据质量等级
        """
        if stock_code not in self.versions:
            self.versions[stock_code] = {}
        
        self.versions[stock_code][data_type] = {
            'version': datetime.now().strftime('%Y%m%d%H%M%S'),
            'timestamp': datetime.now(),
            'source': source_name,
            'record_count': record_count,
            'quality': quality_level.value,
            'history': self._get_history(stock_code, data_type)
        }
        
        logger.info(f"[版本记录] {stock_code} {data_type} v{self.versions[stock_code][data_type]['version']}")

    def get_version_info(self, stock_code: str, data_type: str) -> Optional[Dict]:
        """获取版本信息"""
        return self.versions.get(stock_code, {}).get(data_type)

    def _get_history(self, stock_code: str, data_type: str) -> List[Dict]:
        """获取历史版本记录（保留最近5条）"""
        history = self.versions.get(stock_code, {}).get(data_type, {}).get('history', [])
        new_entry = {
            'timestamp': datetime.now(),
            'version': datetime.now().strftime('%Y%m%d%H%M%S')
        }
        history.append(new_entry)
        # 只保留最近5条历史
        return history[-5:]

    def get_stock_version_summary(self, stock_code: str) -> Dict[str, Dict]:
        """获取股票的所有数据类型版本摘要"""
        summary = {}
        if stock_code in self.versions:
            for data_type, version_info in self.versions[stock_code].items():
                summary[data_type] = {
                    'version': version_info['version'],
                    'updated_at': version_info['timestamp'].strftime('%Y-%m-%d %H:%M'),
                    'source': version_info['source'],
                    'quality': version_info['quality']
                }
        return summary


class DataCleaner:
    """
    数据清洗器
    
    功能：
    1. 处理缺失值
    2. 去除异常值
    3. 数据标准化
    """

    @classmethod
    def fill_missing_values(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        填充缺失值
        
        策略：
        - 价格类数据：使用前后值插值
        - 成交量：使用0或均值填充
        """
        df = df.copy()
        
        # 价格列使用线性插值
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                df[col] = df[col].interpolate(method='linear')
        
        # 成交量使用0填充
        if 'volume' in df.columns:
            df['volume'] = df['volume'].fillna(0)
        
        # 百分比变化使用0填充
        if 'pct_chg' in df.columns:
            df['pct_chg'] = df['pct_chg'].fillna(0)
        
        return df

    @classmethod
    def remove_outliers(cls, df: pd.DataFrame, method: str = 'iqr') -> pd.DataFrame:
        """
        去除异常值

        注意：K线数据不做统计学离群过滤（IQR/Z-score 会把趋势股的真实高低价区、
        重尾成交量当成"异常"整行删掉，导致均线/MACD 计算失真）。
        这里只删除物理上不可能的行：
        1. 价格列（open/high/low/close）<= 0
        2. 最高价 < 最低价

        Args:
            method: 保留参数以兼容旧调用方，当前不再使用
        """
        df = df.copy()

        # 价格必须为正数（NaN 保留，交给缺失值填充处理）
        price_cols = [col for col in ['open', 'high', 'low', 'close'] if col in df.columns]
        for col in price_cols:
            df = df[df[col].isna() | (df[col] > 0)]

        # 最高价不能低于最低价
        if 'high' in df.columns and 'low' in df.columns:
            df = df[df['high'].isna() | df['low'].isna() | (df['high'] >= df['low'])]

        return df

    @classmethod
    def standardize_data(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据标准化
        
        确保：
        1. 日期格式统一
        2. 数值类型正确
        3. 列名标准化
        """
        df = df.copy()
        
        # 标准化日期格式
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        # 确保数值类型正确
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df


# 全局实例
data_validator = DataValidator()
version_manager = DataVersionManager()
data_cleaner = DataCleaner()
