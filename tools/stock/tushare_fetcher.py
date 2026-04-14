# -*- coding: utf-8 -*-
"""
===================================
TushareFetcher - 备用数据源 1 (Priority 2)
===================================

数据来源：Tushare Pro API（挖地兔）
特点：需要 Token、有请求配额限制
优点：数据质量高、接口稳定

流控策略：
1. 实现"每分钟调用计数器"
2. 超过免费配额（80次/分）时，强制休眠到下一分钟
3. 使用 tenacity 实现指数退避重试
"""

import logging
import time
import traceback
from datetime import datetime
from typing import Optional, Tuple
import tushare as ts
from utils.config import get_stock_tools_config

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS
from utils.logger import logger

class TushareFetcher(BaseFetcher):
    """
    Tushare Pro 数据源实现
    
    优先级：2
    数据来源：Tushare Pro API
    
    关键策略：
    - 每分钟调用计数器，防止超出配额
    - 超过 80 次/分钟时强制等待
    - 失败后指数退避重试
    
    配额说明（Tushare 免费用户）：
    - 每分钟最多 80 次请求
    - 每天最多 500 次请求
    """
    
    name = "TushareFetcher"
    priority = 0
    
    def __init__(self, rate_limit_per_minute: int = 200):
        """
        初始化 TushareFetcher
        
        Args:
            rate_limit_per_minute: 每分钟最大请求数（默认80，Tushare免费配额）
        """
        self.rate_limit_per_minute = rate_limit_per_minute
        self._call_count = 0  # 当前分钟内的调用次数
        self._minute_start: Optional[float] = None  # 当前计数周期开始时间
        self._api: Optional[object] = None  # Tushare API 实例
        
        # 尝试初始化 API
        self._init_api()
    
    def _init_api(self) -> None:
        """
        初始化 Tushare API
        
        如果 Token 未配置，此数据源将不可用
        """
        
        if not get_stock_tools_config().get("tushare_token"):
            logger.warning("Tushare Token 未配置，此数据源不可用")
            return
        
        try:
            import tushare as ts
            
            # 设置 Token
            ts.set_token(get_stock_tools_config().get("tushare_token"))
            
            # 获取 API 实例
            self._api = ts.pro_api()
            
            logger.info("Tushare API 初始化成功")
            
        except Exception as e:
            logger.error(f"Tushare API 初始化失败: {e}")
            self._api = None
    
    def _check_rate_limit(self) -> None:
        """
        检查并执行速率限制
        
        流控策略：
        1. 检查是否进入新的一分钟
        2. 如果是，重置计数器
        3. 如果当前分钟调用次数超过限制，强制休眠
        """
        current_time = time.time()
        
        # 检查是否需要重置计数器（新的一分钟）
        if self._minute_start is None:
            self._minute_start = current_time
            self._call_count = 0
        elif current_time - self._minute_start >= 60:
            # 已经过了一分钟，重置计数器
            self._minute_start = current_time
            self._call_count = 0
            logger.debug("速率限制计数器已重置")
        
        # 检查是否超过配额
        if self._call_count >= self.rate_limit_per_minute:
            # 计算需要等待的时间（到下一分钟）
            elapsed = current_time - self._minute_start
            sleep_time = max(0, 60 - elapsed) + 1  # +1 秒缓冲
            
            logger.warning(
                f"Tushare 达到速率限制 ({self._call_count}/{self.rate_limit_per_minute} 次/分钟)，"
                f"等待 {sleep_time:.1f} 秒..."
            )
            
            time.sleep(sleep_time)
            
            # 重置计数器
            self._minute_start = time.time()
            self._call_count = 0
        
        # 增加调用计数
        self._call_count += 1
        logger.debug(f"Tushare 当前分钟调用次数: {self._call_count}/{self.rate_limit_per_minute}")
    
    def _convert_stock_code(self, stock_code: str) -> str:
        """
        转换股票代码为 Tushare 格式
        
        Tushare 要求的格式：
        - 沪市：600519.SH
        - 深市：000001.SZ
        
        Args:
            stock_code: 原始代码，如 '600519', '000001'
            
        Returns:
            Tushare 格式代码，如 '600519.SH', '000001.SZ'
        """
        logger.info(f"stock code[{stock_code}]")
        code = stock_code.strip()
        logger.info(f"stock code[{code}]")
        # 已经包含后缀的情况
        if '.' in code:
            return code.upper()
        
        # 根据代码前缀判断市场
        # 沪市：600xxx, 601xxx, 603xxx, 688xxx (科创板)
        # 深市：000xxx, 002xxx, 300xxx (创业板)
        if code.startswith(('600', '601', '603', '688')):
            return f"{code}.SH"
        elif code.startswith(('000', '002', '300')):
            return f"{code}.SZ"
        else:
            # 默认尝试深市
            logger.warning(f"无法确定股票 {code} 的市场，默认使用深市")
            return f"{code}.SZ"
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, freq: str, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Tushare 获取原始数据
        
        使用 daily() 接口获取日线数据
        
        流程：
        1. 检查 API 是否可用
        2. 执行速率限制检查
        3. 转换股票代码格式
        4. 调用 API 获取数据
        """
        logger.info("使用tushare")
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        if freq != "daily":
            return self.fetch_raw_weekly_month_data(stock_code, start_date, end_date, freq)

        return self.pro_bar(stock_code, start_date, end_date)
    
    def _normalize_data(self, freq: str, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 Tushare 数据
        
        Tushare daily 返回的列名：
        ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
        
        需要映射到标准列名：
        date, open, high, low, close, volume, amount, pct_chg
        """
        if freq != 'daily':
            return self.clean_month_weekly_data( df)

        df = df.copy()
        
        # 列名映射
        column_mapping = {
            'trade_date': 'date',
            'vol': 'volume',
            # open, high, low, close, amount, pct_chg 列名相同
        }
        
        df = df.rename(columns=column_mapping)
        
        # 转换日期格式（YYYYMMDD -> YYYY-MM-DD）
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')



        # 成交量单位转换（Tushare 的 vol 单位是手，需要转换为股）
        if 'volume' in df.columns:
            df['volume'] = df['volume'] * 100
        
        # 成交额单位转换（Tushare 的 amount 单位是千元，转换为元）
        if 'amount' in df.columns:
            df['amount'] = df['amount'] * 1000
        
        # 添加股票代码列
        df['code'] = stock_code
        
        # 只保留需要的列
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        logger.info(f"thshare _normalize_data")
        return df

    def pro_bar(
            self,  stock_code: str, start_date: str, end_date: str,
            adj: str = "qfq",
            freq: str = "D"
    ) -> pd.DataFrame:
        """
        获取复权行情数据
        """
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        try:
            df = ts.pro_bar(ts_code=ts_code, adj=adj, start_date=ts_start, end_date=ts_end,
                            freq=freq)
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            logger.error(f"tushare 获取数据失败[{e}] {traceback.format_exc()}")


    def get_stock_basic(self) -> pd.DataFrame:
        """获取股票基础信息"""
        # 转换代码格式
        try:
            # 获取股票的基础数据
            df = ts.pro_api().stock_basic()
            return df

        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare 获取数据失败: {e}") from e

    def fetch_common(self, stock_code: str, start_date: str, end_date: str):
        # 速率限制检查
        self._check_rate_limit()

        # 转换代码格式
        ts_code = self._convert_stock_code(stock_code)
        logger.info(f"ts code: [{ts_code}, {start_date}, {end_date}, {type(start_date)}]")

        # 转换日期格式（Tushare 要求 YYYYMMDD）
        ts_start = start_date.replace('-', '')
        ts_end = end_date.replace('-', '')

        logger.info(f"调用 Tushare daily[{ts_code}, {ts_start}, {ts_end}]")
        return ts_code, ts_start, ts_end

    def fetch_raw_weekly_month_data(
            self,
            stock_code: str,
            start_date: str,
            end_date: str,
            freq: str
    ) -> pd.DataFrame:
        """
        获取周和月线数据（复权--每日更新）
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)

        logger.debug(f"调用 Tushare stk_week_month_adj({ts_code}, {ts_start}, {ts_end})")
        try:
            # 获取周线或者月线
            df = ts.pro_api().stk_week_month_adj(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
                freq=freq,
            )
            return df

        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare 获取数据失败: {e}") from e

    def clean_month_weekly_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗周数据，确保date字段非空"""
        # 1. 映射Tushare字段到表字段
        df = df.copy()
        # 列名映射
        column_mapping = {
            'trade_date': 'date',
            'vol': 'volume',
            'ts_code': 'code',
            'close_qfq': 'close',
            'open_qfq': 'open',
            'high_qfq': 'high',
            'low_qfq': 'low',
        }
        df = df.drop(columns = ['close', 'high', 'low', 'open'])

        df = df.rename(columns=column_mapping)
        # 2. 清理date字段
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        # 成交量单位转换（Tushare 的 vol 单位是手，需要转换为股）
        if 'vol' in df.columns:
            df['volume'] = df['vol'] * 100

        # 成交额单位转换（Tushare 的 amount 单位是千元，转换为元）
        if 'amount' in df.columns:
            df['amount'] = df['amount'] * 1000

        return df


    def stock_daily_basic(
            self,
            start_date: str,
            end_date: str,
            stock_code =None,
            trade_date = None,
    ) -> pd.DataFrame:
        """每日指标
            stock_code 和 trade_date二选一
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        if stock_code is None and trade_date is None:
            raise DataFetchError("请求参数错误请检查 stock code 和 trade date")
        t_date = None
        if trade_date is not None:
            t_date = trade_date.replace('-', '')

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        try:
            df = ts.pro_api().daily_basic(
                ts_code=ts_code,
                trade_date=t_date,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare stk daily basic err: {e}") from e

    def stk_holdertrade(self, stock_code: str, ann_date, start_date: str, end_date: str)  -> pd.DataFrame:
        """
        获取股东增减持数据
        args:
            ann_date: 公告日期
        trade_type: 交易类型IN增持DE减持
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        a_date = ann_date.replace('-', '')
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk holdertrade({ts_code}, {ts_start}, {ts_end}, {ann_date})")
        try:
            df = ts.pro_api().stk_holdertrade(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
                ann_date=a_date,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare stk holdertrade err: {e}") from e

    def forecast(self, stock_code: str, ann_date: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        业绩预告
        args:
            stock_code: 股票代码（二选一）
            ann_date: 公告日期（二选一）
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        a_date = ann_date.replace('-', '')
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk forecast({ts_code}, {ts_start}, {ts_end}, {ann_date})")
        try:
            df = ts.pro_api().forecast(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
                ann_date=a_date,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare forecast err: {e}") from e

    def express(self, stock_code: str, ann_date: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        业绩快报
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        a_date = ann_date.replace('-', '')
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk express({ts_code}, {ts_start}, {ts_end}, {ann_date})")
        try:
            df = ts.pro_api().express(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
                ann_date=a_date,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare express err: {e}") from e

    # 两融数据
    def margin(self, trade_date, start_date, end_date: str, exchange_id: str) -> pd.DataFrame:
        """
        融资融券每日交易汇总数据
        args:
            exchange_id: SSE上交所SZSE深交所BSE北交所
        """
        t_date = trade_date.replace('-', '')
        ts_start = start_date.replace('-', '')
        ts_end = end_date.replace('-', '')
        logger.info(f"stk margin({ts_start}, {ts_end}, {trade_date})")
        try:
            df = ts.pro_api().margin(
                trade_date=trade_date,
                start_date=ts_start,
                end_date=ts_end,
                exchange_id=exchange_id,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare margin err: {e}") from e

    def margin_detail(self, stock_code, trade_date, start_date, end_date: str) -> pd.DataFrame:
        """
        融资融券交易明细
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        t_date = trade_date.replace('-', '')
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk mergin detail{ts_code}, {ts_start}, {ts_end}, {t_date}")
        try:
            df = ts.pro_api().mergin_detail(
                trade_date=trade_date,
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare mergin detail err: {e}") from e

    # 资金流向数据
    def moneyflow(self, stock_code, trade_date, start_date, end_date: str) -> pd.DataFrame:
        """
        个股资金流向
        args:
            股票和时间参数至少输入一个
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        t_date = trade_date.replace('-', '')
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk moneyflow{ts_code}, {ts_start}, {ts_end}, {t_date}")
        try:
            df = ts.pro_api().moneyflow(
                trade_date=trade_date,
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare moneyflow err: {e}") from e

    def moneyflow_hsgt(self, trade_date, start_date, end_date: str) -> pd.DataFrame:
        """
        个股资金流向
        args:
            交易日期和开始日期二选一
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        t_date = trade_date.replace('-', '')
        ts_start = start_date.replace('-', '')
        ts_end = end_date.replace('-', '')
        logger.info(f"stk moneyflow hsgt, {ts_start}, {ts_end}, {t_date}")
        try:
            df = ts.pro_api().moneyflow_hsgt(
                trade_date=trade_date,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()

            # 检测配额超限
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e

            raise DataFetchError(f"Tushare moneyflow hsgt err: {e}") from e

    # ETF数据，需要的积分都比较高暂不实现
    def etf_basic(self, stock_code, index_code, list_date, list_status, exchange, mgr: str)->pd.DataFrame:
        """
        ETF基础数据
        args:
            list_status: L上市 D退市 P待上市
        """


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = TushareFetcher()
    
    try:
        df = fetcher.get_daily_data('600519')  # 茅台
        print(f"获取成功，共 {len(df)} 条数据")
        print(df.tail())
    except Exception as e:
        print(f"获取失败: {e}")
