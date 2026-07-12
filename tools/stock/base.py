# -*- coding: utf-8 -*-
"""
===================================
数据源基类与管理器
===================================

设计模式：策略模式 (Strategy Pattern)
- BaseFetcher: 抽象基类，定义统一接口
- DataFetcherManager: 策略管理器，实现自动切换

防封禁策略：
1. 每个 Fetcher 内置流控逻辑
2. 失败自动切换到下一个数据源
3. 指数退避重试机制
"""

import random
import time
import sqlite3
import traceback

from utils import load_documents_from_dir
from utils.logger import logger
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from .common import _is_hk_code, _is_etf_code
from .cache_manager import cache_manager, incremental_updater
from .data_quality import data_validator, data_cleaner, version_manager, DataQualityLevel
from .monitor import monitor, performance_logger

import pandas as pd
import numpy as np
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# 配置日志

# === 标准化列名定义 ===
STANDARD_COLUMNS = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']


class DataFetchError(Exception):
    """数据获取异常基类"""
    pass


class RateLimitError(DataFetchError):
    """API 速率限制异常"""
    pass


class DataSourceUnavailableError(DataFetchError):
    """数据源不可用异常"""
    pass


class BaseFetcher(ABC):
    """
    数据源抽象基类

    职责：
    1. 定义统一的数据获取接口
    2. 提供数据标准化方法
    3. 实现通用的技术指标计算

    子类实现：
    - _fetch_raw_data(): 从具体数据源获取原始数据
    - _normalize_data(): 将原始数据转换为标准格式
    """

    name: str = "BaseFetcher"
    priority: int = 99  # 优先级数字越小越优先

    @abstractmethod
    def _fetch_raw_data(self, freq: str, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从数据源获取原始数据（子类必须实现）

        Args:
            freq: 数据频率，如 'daily', 'weekly', 'monthly'
            stock_code: 股票代码，如 '600519', '000001'
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'

        Returns:
            原始数据 DataFrame（列名因数据源而异）
        """
        pass

    @abstractmethod
    def _normalize_data(self, freq: str, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化数据列名（子类必须实现）

        将不同数据源的列名统一为：
        ['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        """
        pass

    def get_daily_data(
            self,
            stock_code: str,
            df_db: Optional[pd.DataFrame] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30,
            use_cache: bool = True
    ) -> pd.DataFrame:
        """
        获取日线数据（统一入口）

        流程：
        1. 检查缓存（可选）
        2. 检查是否需要更新
        3. 调用子类获取原始数据
        4. 数据质量检查
        5. 标准化列名
        6. 计算技术指标
        7. 更新缓存

        Args:
            stock_code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选，默认今天）
            days: 获取天数（当 start_date 未指定时使用）
            use_cache: 是否使用缓存

        Returns:
            标准化的 DataFrame，包含技术指标
        """
        # 计算日期范围
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"end date{end_date}, start date{start_date}")

        if start_date is None:
            start_date = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days * 2)
            logger.info(f"start data{start_date}")

        logger.info(f"[{self.name}] 获取 {stock_code} 数据: {start_date} ~ {end_date}")

        # 记录开始时间（性能监控）
        perf_start = performance_logger.start_timer(f"get_daily_data_{stock_code}")

        try:
            # Step 0: 检查缓存
            if use_cache:
                cached_data = cache_manager.get(
                    stock_code=stock_code,
                    data_type='daily',
                    start_date=start_date,
                    end_date=end_date
                )
                if cached_data is not None:
                    logger.info(f"[{self.name}] 从缓存获取 {stock_code}")
                    performance_logger.end_timer(f"get_daily_data_{stock_code}", perf_start)
                    return cached_data

            # Step 0.5: 检查是否需要更新
            needs_update, reason = incremental_updater.needs_update(stock_code, 'daily')
            if not needs_update:
                logger.info(f"[{self.name}] {stock_code} {reason}")
                # 如果不需要更新但缓存也没有，继续获取
                pass

            # Step 1: 获取原始数据
            raw_df = self._fetch_raw_data("daily", stock_code, start_date, end_date)

            if raw_df is None or raw_df.empty:
                logger.error(f"[{self.name}] 未获取到 {stock_code} 的数据")
                # 记录失败（监控）
                monitor.record_request(
                    source_name=self.name,
                    stock_code=stock_code,
                    success=False,
                    response_time=time.time() - perf_start,
                    data_type='daily',
                    error_message="未获取到数据"
                )
                raise DataFetchError(f"[{self.name}] 未获取到 {stock_code} 的数据")

            # Step 2: 标准化列名
            df = self._normalize_data("daily", raw_df, stock_code)

            df = merge_and_clean_data("date", df_db, df)

            # Step 3: 数据质量检查
            is_valid, errors = data_validator.validate_kline_data(df)
            if not is_valid:
                logger.warning(f"[{self.name}] {stock_code} 数据质量问题: {errors}")

            # Step 4: 数据清洗（增强版）
            df = data_cleaner.fill_missing_values(df)
            df = data_cleaner.remove_outliers(df)
            df = data_cleaner.standardize_data(df)

            # Step 5: 计算技术指标
            df = self._calculate_indicators(df, freq="daily")

            # Step 6: 更新缓存
            if use_cache:
                cache_manager.set(
                    stock_code=stock_code,
                    data_type='daily',
                    data=df,
                    start_date=start_date,
                    end_date=end_date
                )

            # Step 7: 记录更新
            incremental_updater.record_update(stock_code, 'daily')

            # Step 8: 记录数据质量和版本
            quality_level = data_validator.calculate_quality_score(df, 'kline')
            version_manager.record_version(
                stock_code=stock_code,
                data_type='daily',
                source_name=self.name,
                record_count=len(df),
                quality_level=quality_level
            )

            # Step 9: 记录成功（监控）
            monitor.record_request(
                source_name=self.name,
                stock_code=stock_code,
                success=True,
                response_time=time.time() - perf_start,
                data_type='daily'
            )

            logger.info(f"[{self.name}] {stock_code} 获取成功，共 {len(df)} 条数据，质量: {quality_level.value}")

            performance_logger.end_timer(f"get_daily_data_{stock_code}", perf_start)
            return df

        except Exception as e:
            # 记录失败（监控）
            monitor.record_request(
                source_name=self.name,
                stock_code=stock_code,
                success=False,
                response_time=time.time() - perf_start,
                data_type='daily',
                error_message=str(e)
            )
            logger.error(f"[{self.name}] 获取 {stock_code} 失败: {str(e)} {traceback.format_exc()}")
            performance_logger.end_timer(f"get_daily_data_{stock_code}", perf_start)
            raise DataFetchError(f"[{self.name}] {stock_code}: {str(e)} ") from e


    def get_stock_data(
            self,
            freq: str,
            stock_code: str,
            df_db: Optional[pd.DataFrame] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30,
            use_cache: bool = True
    ) -> pd.DataFrame:
        """
        获取周线或月线数据（统一入口）

        Args:
            freq: 数据频率 ('week', 'month')
            stock_code: 股票代码
            df_db: 数据库已有数据（用于增量更新）
            start_date: 开始日期
            end_date: 结束日期
            days: 获取天数
            use_cache: 是否使用缓存
        """
        # 计算日期范围
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"end date{end_date}, start date{start_date}")

        if start_date is None:
            start_date = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days * 2)
            logger.info(f"start data{start_date}")

        logger.info(f"[{self.name}] 获取 {stock_code} {freq}数据: {start_date} ~ {end_date}")

        # 记录开始时间（性能监控）
        perf_start = performance_logger.start_timer(f"get_{freq}_data_{stock_code}")

        try:
            # Step 0: 检查缓存
            if use_cache:
                cached_data = cache_manager.get(
                    stock_code=stock_code,
                    data_type=freq,
                    start_date=start_date,
                    end_date=end_date
                )
                if cached_data is not None:
                    logger.info(f"[{self.name}] 从缓存获取 {stock_code} {freq}数据")
                    performance_logger.end_timer(f"get_{freq}_data_{stock_code}", perf_start)
                    return cached_data

            # Step 1: 获取原始数据
            raw_df = self._fetch_raw_data(freq, stock_code, start_date, end_date)

            if raw_df is None or raw_df.empty:
                logger.error(f"[{self.name}] 未获取到 {stock_code} 的数据")
                # 记录失败（监控）
                monitor.record_request(
                    source_name=self.name,
                    stock_code=stock_code,
                    success=False,
                    response_time=time.time() - perf_start,
                    data_type=freq,
                    error_message="未获取到数据"
                )
                raise DataFetchError(f"[{self.name}] 未获取到 {stock_code} 的数据")

            # Step 2: 标准化列名
            df = self._normalize_data(freq, raw_df, stock_code)

            df = merge_and_clean_data("date", df_db, df)

            # Step 3: 数据质量检查
            is_valid, errors = data_validator.validate_kline_data(df)
            if not is_valid:
                logger.warning(f"[{self.name}] {stock_code} 数据质量问题: {errors}")

            # Step 4: 数据清洗（增强版）
            df = data_cleaner.fill_missing_values(df)
            df = data_cleaner.remove_outliers(df)
            df = data_cleaner.standardize_data(df)

            # Step 5: 计算技术指标（52周窗口按周期折算）
            df = self._calculate_indicators(df, freq=freq)

            # Step 6: 更新缓存
            if use_cache:
                cache_manager.set(
                    stock_code=stock_code,
                    data_type=freq,
                    data=df,
                    start_date=start_date,
                    end_date=end_date
                )

            # Step 7: 记录更新
            incremental_updater.record_update(stock_code, freq)

            # Step 8: 记录数据质量和版本
            quality_level = data_validator.calculate_quality_score(df, 'kline')
            version_manager.record_version(
                stock_code=stock_code,
                data_type=freq,
                source_name=self.name,
                record_count=len(df),
                quality_level=quality_level
            )

            # Step 9: 记录成功（监控）
            monitor.record_request(
                source_name=self.name,
                stock_code=stock_code,
                success=True,
                response_time=time.time() - perf_start,
                data_type=freq
            )

            logger.info(f"[{self.name}] {stock_code} {freq}数据获取成功，共 {len(df)} 条数据，质量: {quality_level.value}")

            performance_logger.end_timer(f"get_{freq}_data_{stock_code}", perf_start)
            return df

        except Exception as e:
            # 记录失败（监控）
            monitor.record_request(
                source_name=self.name,
                stock_code=stock_code,
                success=False,
                response_time=time.time() - perf_start,
                data_type=freq,
                error_message=str(e)
            )
            logger.error(f"[{self.name}] 获取 {stock_code} 失败: {str(e)} {traceback.format_exc()}")
            performance_logger.end_timer(f"get_{freq}_data_{stock_code}", perf_start)
            raise DataFetchError(f"[{self.name}] {stock_code}: {str(e)} ") from e


    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        数据清洗

        处理：
        1. 确保日期列格式正确
        2. 数值类型转换
        3. 去除空值行
        4. 按日期排序
        """
        df = df.copy()

        # 确保日期列为 datetime 类型
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        # 数值列类型转换
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 去除关键列为空的行
        df = df.dropna(subset=['close', 'volume'])

        # 按日期升序排序
        df = df.sort_values('date', ascending=True).reset_index(drop=True)
        logger.info(f"clean data success")
        return df

    # 52周（一年）位置窗口：日线约244个交易日，周线52根，月线12根
    _POS_52W_WINDOW = {"daily": 244, "week": 52, "weekly": 52, "month": 12, "monthly": 12}

    def _calculate_indicators(self, df: pd.DataFrame, freq: str = "daily") -> pd.DataFrame:
        """
        计算技术指标（全部在升序序列上计算，最后降序展示）

        数值指标：MA/EMA、MACD(DIF/DEA/MACD)、量比、RSI(6/12/24)、KDJ(9,3,3)、
                 BOLL(20,2)、ATR14、OBV、pos_52w（收盘价在近一年高低区间的位置0-100）
        信号列（代码判定，LLM 只做解读）：macd_signal、ma_pattern、ma_cross、vol_signal、gap_signal
        """
        df = df.copy()
        # 所有指标（MACD/均线/量比）必须在按日期升序的序列上计算，
        # 否则 rolling/ewm/shift 取到的是"未来"数据
        if 'date' in df.columns:
            df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
        df = self._calculate_macd_signal(df)
        df = self.calculate_ma_ema(df, "close")

        # 量比：当日成交量 / 前5日平均成交量（升序数据上 shift(1) 才是历史均量）
        avg_volume_5 = df['volume'].rolling(window=5, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / avg_volume_5.shift(1)
        df['volume_ratio'] = df['volume_ratio'].fillna(1.0).round(2)

        df = self._calculate_rsi(df)
        df = self._calculate_kdj(df)
        df = self._calculate_boll(df)
        df = self._calculate_atr(df)
        df = self._calculate_obv(df)
        df = self._calculate_pos_52w(df, self._POS_52W_WINDOW.get(freq, 244))
        df = self._calculate_signals(df)

        # 全部指标计算完成后再按日期降序排列（仅用于展示，最新数据在前）
        if 'date' in df.columns:
            df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
        logger.info(f"calculate indicators success")
        return df

    @staticmethod
    def _calculate_rsi(df: pd.DataFrame, periods=(6, 12, 24)) -> pd.DataFrame:
        """RSI（Wilder 平滑，与国内行情软件口径一致）"""
        diff = df['close'].diff()
        gain = diff.clip(lower=0)
        loss = -diff.clip(upper=0)
        for p in periods:
            avg_gain = gain.ewm(alpha=1 / p, adjust=False).mean()
            avg_loss = loss.ewm(alpha=1 / p, adjust=False).mean()
            rs = avg_gain / avg_loss.replace(0, np.nan)
            df[f'rsi{p}'] = (100 - 100 / (1 + rs)).fillna(100.0).round(2)
        return df

    @staticmethod
    def _calculate_kdj(df: pd.DataFrame, n: int = 9) -> pd.DataFrame:
        """KDJ(9,3,3)：K/D 用国内通行的 1/3 递归平滑"""
        low_n = df['low'].rolling(window=n, min_periods=1).min()
        high_n = df['high'].rolling(window=n, min_periods=1).max()
        span = (high_n - low_n).replace(0, np.nan)
        rsv = ((df['close'] - low_n) / span * 100).fillna(50.0)
        df['kdj_k'] = rsv.ewm(alpha=1 / 3, adjust=False).mean().round(2)
        df['kdj_d'] = df['kdj_k'].ewm(alpha=1 / 3, adjust=False).mean().round(2)
        df['kdj_j'] = (3 * df['kdj_k'] - 2 * df['kdj_d']).round(2)
        return df

    @staticmethod
    def _calculate_boll(df: pd.DataFrame, n: int = 20, k: float = 2.0) -> pd.DataFrame:
        """布林带(20,2)"""
        mid = df['close'].rolling(window=n, min_periods=n).mean()
        std = df['close'].rolling(window=n, min_periods=n).std()
        df['boll_mid'] = mid.round(2)
        df['boll_upper'] = (mid + k * std).round(2)
        df['boll_lower'] = (mid - k * std).round(2)
        return df

    @staticmethod
    def _calculate_atr(df: pd.DataFrame, n: int = 14) -> pd.DataFrame:
        """ATR14（Wilder 平滑真实波幅）"""
        prev_close = df['close'].shift(1)
        tr = pd.concat([
            df['high'] - df['low'],
            (df['high'] - prev_close).abs(),
            (df['low'] - prev_close).abs(),
        ], axis=1).max(axis=1)
        df['atr14'] = tr.ewm(alpha=1 / n, adjust=False).mean().round(3)
        return df

    @staticmethod
    def _calculate_obv(df: pd.DataFrame) -> pd.DataFrame:
        """OBV 能量潮：涨日加量、跌日减量的累积"""
        direction = np.sign(df['close'].diff()).fillna(0)
        df['obv'] = (direction * df['volume']).cumsum()
        return df

    @staticmethod
    def _calculate_pos_52w(df: pd.DataFrame, window: int) -> pd.DataFrame:
        """收盘价在近一年（按周期折算窗口）高低区间中的位置，0=年内最低 100=年内最高"""
        high_w = df['high'].rolling(window=window, min_periods=1).max()
        low_w = df['low'].rolling(window=window, min_periods=1).min()
        span = (high_w - low_w).replace(0, np.nan)
        df['pos_52w'] = ((df['close'] - low_w) / span * 100).fillna(50.0).round(1)
        return df

    @staticmethod
    def _calculate_signals(df: pd.DataFrame) -> pd.DataFrame:
        """
        信号列：由代码判定金叉死叉/均线形态/量价/缺口，LLM 只负责解读。
        全部在升序序列上用 shift(1) 对比前一根，无未来数据。
        """
        # 均线排列形态（逐行判定）
        def _pattern(row):
            vals = [row.get('ma5'), row.get('ma10'), row.get('ma20'), row.get('ma50')]
            if any(v is None or pd.isna(v) for v in vals):
                return '数据不足'
            if vals[0] > vals[1] > vals[2] > vals[3]:
                return '多头排列'
            if vals[0] < vals[1] < vals[2] < vals[3]:
                return '空头排列'
            return '缠绕'
        df['ma_pattern'] = df.apply(_pattern, axis=1)

        # 均线金叉/死叉（5x10、10x20）
        cross_parts = pd.Series([''] * len(df), index=df.index)
        for fast, slow in [(5, 10), (10, 20)]:
            f, s = df.get(f'ma{fast}'), df.get(f'ma{slow}')
            if f is None or s is None:
                continue
            golden = (f.shift(1) <= s.shift(1)) & (f > s)
            death = (f.shift(1) >= s.shift(1)) & (f < s)
            cross_parts = cross_parts.where(~golden, cross_parts + f'金叉{fast}x{slow} ')
            cross_parts = cross_parts.where(~death, cross_parts + f'死叉{fast}x{slow} ')
        df['ma_cross'] = cross_parts.str.strip()

        # 量价信号：量比 + 当日涨跌方向
        chg = df['close'].diff()
        vr = df['volume_ratio']
        df['vol_signal'] = ''
        df.loc[(vr >= 2.0), 'vol_signal'] = '异常放量'
        df.loc[(vr >= 1.5) & (vr < 2.0) & (chg > 0), 'vol_signal'] = '放量上涨'
        df.loc[(vr >= 1.5) & (vr < 2.0) & (chg < 0), 'vol_signal'] = '放量下跌'
        df.loc[(vr <= 0.7) & (chg < 0), 'vol_signal'] = '缩量回调'

        # 跳空缺口
        prev_high = df['high'].shift(1)
        prev_low = df['low'].shift(1)
        df['gap_signal'] = ''
        df.loc[df['low'] > prev_high, 'gap_signal'] = '向上跳空'
        df.loc[df['high'] < prev_low, 'gap_signal'] = '向下跳空'
        return df

    def calculate_ma_ema(
            self,
            df: pd.DataFrame,
            price_col: str = 'close',
            ma_periods: list = [5, 10, 20, 50, 120, 200],
            ema_periods: list = [5, 10, 20, 50, 120, 200]
    ) -> pd.DataFrame:
        """
        计算MA（均线）和EMA（指数均线）
        :param df: 周线/月线DataFrame（需包含price_col字段）
        :param price_col: 计算基准字段（默认收盘价close）
        :param ma_periods: 要计算的MA周期（如[5,10,20]周/月）
        :return: 新增MA/EMA列的DataFrame
        """
        df = df.copy()
        if df.empty or price_col not in df.columns:
            return df
        df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
        # ---------------------- 1. 计算MA（简单移动平均） ----------------------
        for period in ma_periods:
            # rolling(window=period)：min_periods=period，数据不足时为 NaN（不用短窗口造假）
            ma = df[price_col].rolling(window=period, min_periods=period).mean().round(2)
            df[f'ma{period}'] = ma

        # ---------------------- 2. 计算EMA（指数移动平均） ----------------------
        for period in ema_periods:
            # ewm(span=period)：指数加权窗口；adjust=False：使用递归公式（行业标准）
            ema = df[price_col].ewm(span=period, adjust=False).mean().round(2)
            key = f'ema{period}'
            df[key] = ema
        # 注意：这里保持升序返回，后续量比等指标仍需在升序序列上计算，
        # 统一由 _calculate_indicators 最后再做展示排序
        return df

    def _calculate_macd_signal(
            self, df: pd.DataFrame,
            short_window=12, long_window=26, signal_window=9) -> pd.DataFrame:
        """计算MACD的信号"""
        df = df.copy()

        df['EMA_short'] = df['close'].ewm(span=short_window, adjust=False).mean()
        df['EMA_long'] = df['close'].ewm(span=long_window, adjust=False).mean()
        df['DIF'] = df['EMA_short'] - df['EMA_long']  # 快线
        df['DEA'] = df['DIF'].ewm(span=signal_window, adjust=False).mean()
        df['MACD'] = df['DIF'] - df['DEA']
        # 计算交叉点
        df['macd_signal'] = 0
        df.loc[(df['DIF'].shift(1) <= df['DEA'].shift(1)) & (df['DIF'] > df['DEA']), 'macd_signal'] = 1
        df.loc[(df['DIF'].shift(1) >= df['DEA'].shift(1)) & (df['DIF'] < df['DEA']), 'macd_signal'] = -1
        return df

    @staticmethod
    def random_sleep(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
        """
        智能随机休眠（Jitter）

        防封禁策略：模拟人类行为的随机延迟
        在请求之间加入不规则的等待时间
        """
        sleep_time = random.uniform(min_seconds, max_seconds)
        logger.info(f"随机休眠 {sleep_time:.2f} 秒...")
        time.sleep(sleep_time)


class DataFetcherManager:
    """
    数据源策略管理器

    职责：
    1. 管理多个数据源（按优先级排序）
    2. 自动故障切换（Failover）
    3. 提供统一的数据获取接口

    切换策略：
    - 优先使用高优先级数据源
    - 失败后自动切换到下一个
    - 所有数据源都失败时抛出异常
    """

    def __init__(self, fetchers: Optional[List[BaseFetcher]] = None):
        """
        初始化管理器

        Args:
            fetchers: 数据源列表（可选，默认按优先级自动创建）
        """
        self._fetchers: List[BaseFetcher] = []

        if fetchers:
            # 按优先级排序
            self._fetchers = sorted(fetchers, key=lambda f: f.priority)
        else:
            # 默认数据源将在首次使用时延迟加载
            self._init_default_fetchers()

    def _init_default_fetchers(self) -> None:
        """
        初始化默认数据源列表

        按优先级排序：
        0. AkshareFetcher (Priority 0) - 主源
        1. TushareFetcher (Priority 1) - 备用
        """
        from .akshare_fetcher import AkshareFetcher
        from .tushare_fetcher import TushareFetcher

        self._fetchers = [
            AkshareFetcher(),  # 最高优先级
            TushareFetcher(),
        ]

        # 按优先级排序
        self._fetchers.sort(key=lambda f: f.priority)

        logger.info(f"已初始化 {len(self._fetchers)} 个数据源: " +
                    ", ".join([f.name for f in self._fetchers]))

    def add_fetcher(self, fetcher: BaseFetcher) -> None:
        """添加数据源并重新排序"""
        self._fetchers.append(fetcher)
        self._fetchers.sort(key=lambda f: f.priority)

    def get_daily_data(
            self,
            stock_code: str,
            df_db: Optional[pd.DataFrame] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30
    ) -> Tuple[pd.DataFrame, str]:
        """
        获取日线数据（自动切换数据源）

        故障切换策略：
        1. 从最高优先级数据源开始尝试
        2. 捕获异常后自动切换到下一个
        3. 记录每个数据源的失败原因
        4. 所有数据源失败后抛出详细异常

        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            days: 获取天数

        Returns:
            Tuple[DataFrame, str]: (数据, 成功的数据源名称)

        Raises:
            DataFetchError: 所有数据源都失败时抛出
        """
        errors = []
        for fetcher in self._fetchers:
            try:
                logger.info(f"尝试使用 [{fetcher.name}] 获取 {stock_code}...")
                df = fetcher.get_daily_data(
                    stock_code=stock_code,
                    df_db=df_db,
                    start_date=start_date,
                    end_date=end_date,
                    days=days
                )

                if df is not None and not df.empty:
                    logger.info(f"[{fetcher.name}] 成功获取 {stock_code}")
                    return df, fetcher.name

            except Exception as e:
                error_msg = f"[{fetcher.name}] 失败: {str(e)} 开始时间{start_date} 结束时间{end_date}"
                logger.error(error_msg)
                errors.append(error_msg)
                # 继续尝试下一个数据源
                continue

        # 所有数据源都失败
        error_summary = f"所有数据源获取 {stock_code} 失败:\n" + "\n".join(errors)
        logger.error(error_summary)
        raise DataFetchError(error_summary)

    def get_weekly_data(
            self,
            stock_code: str,
            df_db: Optional[pd.DataFrame] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30
    ) -> Tuple[pd.DataFrame, str]:
        """
        获取周线数据（自动切换数据源）
        """
        errors = []
        for fetcher in self._fetchers:
            try:
                logger.info(f"尝试使用 [{fetcher.name}] 获取 {stock_code}...")
                df = fetcher.get_stock_data(
                    freq="week",
                    stock_code=stock_code,
                    df_db=df_db,
                    start_date=start_date,
                    end_date=end_date,
                    days=days
                )

                if df is not None and not df.empty:
                    logger.info(f"[{fetcher.name}] 成功获取 {stock_code}")
                    return df, fetcher.name

            except Exception as e:
                error_msg = f"[{fetcher.name}] 失败: {str(e)} 开始时间{start_date} 结束时间{end_date}"
                logger.error(error_msg)
                errors.append(error_msg)
                # 继续尝试下一个数据源
                continue

        # 所有数据源都失败
        error_summary = f"所有数据源获取 {stock_code} 失败:\n" + "\n".join(errors)
        logger.error(error_summary)
        raise DataFetchError(error_summary)


    def get_monthly_data(
            self,
            stock_code: str,
            df_db: Optional[pd.DataFrame] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30
    ) -> Tuple[pd.DataFrame, str]:
        """
        获取月线数据（自动切换数据源）
        """
        errors = []
        for fetcher in self._fetchers:
            try:
                logger.info(f"尝试使用 [{fetcher.name}] 获取 {stock_code}...")
                df = fetcher.get_stock_data(
                    freq="month",
                    stock_code=stock_code,
                    df_db=df_db,
                    start_date=start_date,
                    end_date=end_date,
                    days=days
                )

                if df is not None and not df.empty:
                    logger.info(f"[{fetcher.name}] 成功获取 {stock_code}")
                    return df, fetcher.name

            except Exception as e:
                error_msg = f"[{fetcher.name}] 失败: {str(e)} 开始时间{start_date} 结束时间{end_date}"
                logger.warning(error_msg)
                errors.append(error_msg)
                # 继续尝试下一个数据源
                continue

        # 所有数据源都失败
        error_summary = f"所有数据源获取 {stock_code} 失败:\n" + "\n".join(errors)
        logger.error(error_summary)
        raise DataFetchError(error_summary)

def merge_and_clean_data(date_field: str, df_db, df_new):
    """
        核心逻辑：
        1. 合并存量+增量数据
        2. 统一日期类型（DB/akshare 是 date 对象，tushare 是 Timestamp——
           不统一会导致重叠日去重失效、混合类型排序抛异常、复权漂移检测失灵）
        3. 按日期去重（保留增量数据，即最后一条）
        4. 按日期升序排序
    """
    from utils.common import parse_row_date

    # 步骤1：合并数据
    df_merged = pd.concat([df_db, df_new], ignore_index=True)

    # 步骤2：统一日期类型为 datetime.date
    if date_field in df_merged.columns:
        df_merged[date_field] = df_merged[date_field].apply(parse_row_date)

    # 步骤3：按日期去重（关键！保留最后一行=增量数据覆盖存量重复数据）
    # 若想保留存量数据，把 keep="last" 改为 keep="first"
    df_dedup = df_merged.drop_duplicates(
        subset=[date_field],  # 按日期去重（股票数据核心去重维度）
        keep="last"  # 重复时保留最后一行（增量数据）
    )

    # 步骤3：按日期升序排序（时间序列数据必备）
    df_sorted = df_dedup.sort_values(
        by=date_field,
        ascending=True  # 升序=从早到晚排序
    ).reset_index(drop=True)  # 重置索引，避免混乱
    return df_sorted


