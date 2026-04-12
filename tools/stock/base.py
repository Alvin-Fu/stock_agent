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

import logging
import random
import time
import sqlite3
import traceback
import utils.logger as logger

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
from .common import _is_hk_code, _is_etf_code

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
            days: int = 30
    ) -> pd.DataFrame:
        """
        获取日线数据（统一入口）

        流程：
        1. 计算日期范围
        2. 调用子类获取原始数据
        3. 标准化列名
        4. 计算技术指标

        Args:
            stock_code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选，默认今天）
            days: 获取天数（当 start_date 未指定时使用）

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

        try:
            # Step 1: 获取原始数据
            raw_df = self._fetch_raw_data("daily", stock_code, start_date, end_date)

            if raw_df is None or raw_df.empty:
                logger.error(f"[{self.name}] 未获取到 {stock_code} 的数据")
                raise DataFetchError(f"[{self.name}] 未获取到 {stock_code} 的数据")

            # Step 2: 标准化列名
            df = self._normalize_data(raw_df, stock_code)

            df = merge_and_clean_data("date", df_db, df)

            # Step 3: 数据清洗
            df = self._clean_data(df)

            # Step 4: 计算技术指标
            df = self._calculate_indicators(df)

            logger.info(f"[{self.name}] {stock_code} 获取成功，共 {len(df)} 条数据")
            return df

        except Exception as e:
            logger.error(f"[{self.name}] 获取 {stock_code} 失败: {str(e)} {traceback.format_exc()}")
            raise DataFetchError(f"[{self.name}] {stock_code}: {str(e)} ") from e


    def get_stock_data(
            self,
            freq: str,
            stock_code: str,
            df_db: Optional[pd.DataFrame] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30
    ) -> pd.DataFrame:
        """
        获取周线或月线数据（统一入口）
        """
        # 计算日期范围
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"end date{end_date}, start date{start_date}")

        if start_date is None:
            start_date = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days * 2)
            logger.info(f"start data{start_date}")

        logger.info(f"[{self.name}] 获取 {stock_code} 数据: {start_date} ~ {end_date}")

        try:
            # Step 1: 获取原始数据
            raw_df = self._fetch_raw_data(freq, stock_code, start_date, end_date)

            if raw_df is None or raw_df.empty:
                logger.error(f"[{self.name}] 未获取到 {stock_code} 的数据")
                raise DataFetchError(f"[{self.name}] 未获取到 {stock_code} 的数据")

            # Step 2: 标准化列名
            df = self._normalize_data(raw_df, stock_code)

            df = merge_and_clean_data("date", df_db, df)

            # Step 3: 数据清洗
            df = self._clean_data(df)

            # Step 4: 计算技术指标
            df = self._calculate_indicators(df)

            logger.info(f"[{self.name}] {stock_code} 获取成功，共 {len(df)} 条数据")
            return df

        except Exception as e:
            logger.error(f"[{self.name}] 获取 {stock_code} 失败: {str(e)} {traceback.format_exc()}")
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

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标

        计算指标：
        - MA5, MA10, MA20: 移动平均线
        - Volume_Ratio: 量比（今日成交量 / 5日平均成交量）
        """
        df = df.copy()
        df = self._calculate_macd_signal(df)
        df = self.calculate_ma_ema(df, "close")
        logging.warning(f"{df['ma200'][-10:]}200天线")
        logging.warning(f"{df['ma5'][-10:]}均线")

        # 量比：当日成交量 / 5日平均成交量
        avg_volume_5 = df['volume'].rolling(window=5, min_periods=1).mean()
        df['volume_ratio'] = df['volume'] / avg_volume_5.shift(1)
        df['volume_ratio'] = df['volume_ratio'].fillna(1.0).round(2)
        logger.info(f"calculate indicators success")
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
            # rolling(window=period)：固定周期窗口；min_periods=1：数据不足时也计算
            ma = df[price_col].rolling(window=period, min_periods=1).mean().round(2)
            key = f'ma{period}'
            logger.warning(f"ma key: [{key}]")
            df[key] = ma

        # ---------------------- 2. 计算EMA（指数移动平均） ----------------------
        for period in ema_periods:
            # ewm(span=period)：指数加权窗口；adjust=False：使用递归公式（行业标准）
            ema = df[price_col].ewm(span=period, adjust=False, min_periods=1).mean().round(2)
            key = f'ema{period}'
            logger.warning(f"ma key: [{key}]")
            df[key] = ema
        df = df.sort_values(by='date', ascending=False).reset_index(drop=True)
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
        logger.debug(f"随机休眠 {sleep_time:.2f} 秒...")
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
        self.fetcher_map = None
        self._fetchers: List[BaseFetcher] = []

        if fetchers:
            # 按优先级排序
            self._fetchers = sorted(fetchers, key=lambda f: f.priority)
        else:
            # 默认数据源将在首次使用时延迟加载
            self._init_default_fetchers()

        for fetcher in self._fetchers:
            self.fetcher_map[fetcher.name] = fetcher

    def _init_default_fetchers(self) -> None:
        """
        初始化默认数据源列表

        按优先级排序：
        0. EfinanceFetcher (Priority 0) - 最高优先级
        1. AkshareFetcher (Priority 1)
        2. TushareFetcher (Priority 2)
        3. BaostockFetcher (Priority 3)
        4. YfinanceFetcher (Priority 4)
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

    def get_fetcher(self, name: str) -> BaseFetcher:
        """根据名称获取数据源"""
        if name in self.fetcher_map:
            return self.fetcher_map[name]
        else:
            raise ValueError(f"未找到数据源: {name}")

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
                logger.warning(error_msg)
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
                logger.warning(error_msg)
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
        2. 按日期去重（保留增量数据，即最后一条）
        3. 按日期升序排序
        """
    # 步骤1：合并数据
    df_merged = pd.concat([df_db, df_new], ignore_index=True)

    # 步骤2：按日期去重（关键！保留最后一行=增量数据覆盖存量重复数据）
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


