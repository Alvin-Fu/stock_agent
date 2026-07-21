# -*- coding: utf-8 -*-
"""
===================================
TushareFetcher - 备用数据源 (Priority 1)
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
from datetime import datetime, date, timedelta
from typing import Optional, Tuple
import requests
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

    优先级：1（备用数据源）
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
    priority = 0  # 主数据源（Akshare 为备用）

    def __init__(self, rate_limit_per_minute: int = 80):
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
        retry=retry_if_exception_type((
            ConnectionError,
            TimeoutError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )),
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
            # 非配额异常也要抛出，让上层 DataFetcherManager 记录根因并降级
            logger.error(f"tushare 获取数据失败[{e}] {traceback.format_exc()}")
            raise DataFetchError(f"Tushare 获取数据失败: {e}") from e


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
        # 注意：上面已经把 vol rename 成 volume，这里必须判断/取 volume 列
        if 'volume' in df.columns:
            df['volume'] = df['volume'] * 100

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

    def income(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取利润表数据
        args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk income({ts_code}, {ts_start}, {ts_end})")
        
        try:
            # 显式指定 fields：默认返回不保证包含四项费用（sell_exp/admin_exp/rd_exp/fin_exp），
            # 需要显式列出才能拿到，供下游分析"利润下滑是费用吃掉的还是毛利掉了"
            df = ts.pro_api().income(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
                fields=(
                    "ts_code,ann_date,end_date,total_revenue,operate_profit,n_income,"
                    "basic_eps,oper_cost,sell_exp,admin_exp,rd_exp,fin_exp,update_flag"
                ),
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare income err: {e}") from e

    def stock_cashflow(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取现金流量表数据（报告期累计口径，单位：元）
        args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk cashflow({ts_code}, {ts_start}, {ts_end})")

        try:
            # 显式指定 fields：经营/投资/筹资活动现金流净额、购建固定资产等支付的现金（资本开支）、
            # 自由现金流（tushare 计算值，可能为空）；update_flag 用于同报告期多条时取最新
            df = ts.pro_api().cashflow(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
                fields=(
                    "ts_code,ann_date,end_date,n_cashflow_act,n_cashflow_inv_act,"
                    "n_cash_flows_fnc_act,c_pay_acq_const_fids,free_cashflow,update_flag"
                ),
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

            raise DataFetchError(f"Tushare cashflow err: {e}") from e

    def balancesheet(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取资产负债表数据
        args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")
        
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"stk balancesheet({ts_code}, {ts_start}, {ts_end})")
        
        try:
            df = ts.pro_api().balancesheet(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare balancesheet err: {e}") from e

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

    def fina_indicator(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取财务指标数据
        args:
            stock_code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
        返回:
            DataFrame: 财务指标数据（eps, roe, roa, gross_margin, inv_turn 等）
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"fina_indicator({ts_code}, {ts_start}, {ts_end})")

        try:
            df = ts.pro_api().fina_indicator(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare fina_indicator err: {e}") from e

    def fina_mainbz(self, stock_code: str, start_date: str, end_date: str,
                    bz_type: str = "P") -> pd.DataFrame:
        """
        获取主营业务构成数据
        args:
            stock_code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            bz_type: 业务类型 P按产品 D按地区 I按行业
        返回:
            DataFrame: 主营业务构成数据
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"fina_mainbz({ts_code}, {ts_start}, {ts_end}, type={bz_type})")

        try:
            df = ts.pro_api().fina_mainbz(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
                type=bz_type,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare fina_mainbz err: {e}") from e

    def holdernumber(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股东户数数据
        args:
            stock_code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
        返回:
            DataFrame: 股东户数数据
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"holdernumber({ts_code}, {ts_start}, {ts_end})")

        try:
            df = ts.pro_api().stk_holdernumber(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare holdernumber err: {e}") from e

    def hk_hold(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取沪深港通持股数据（北向持股）
        args:
            stock_code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
        返回:
            DataFrame: 北向持股数据
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"hk_hold({ts_code}, {ts_start}, {ts_end})")

        try:
            df = ts.pro_api().hk_hold(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare hk_hold err: {e}") from e

    def top10_holders(self, stock_code: str, start_date: str, end_date: str,
                      holder_type: str = "top10") -> pd.DataFrame:
        """
        获取十大股东数据
        args:
            stock_code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            holder_type: top10十大股东 / top10_float十大流通股东
        返回:
            DataFrame: 十大股东数据
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"top10_holders({ts_code}, {ts_start}, {ts_end}, type={holder_type})")

        try:
            api_method = "top10_holders" if holder_type == "top10" else "top10_floatholders"
            df = getattr(ts.pro_api(), api_method)(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare top10_holders err: {e}") from e

    def sw_daily(self, trade_date: str = None, start_date: str = None,
                 end_date: str = None) -> pd.DataFrame:
        """
        获取申万行业日线行情（含PE/PB等估值指标）
        注意：2000积分档无权限，返回空DataFrame，由上层通过成分股聚合计算
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        t_date = trade_date.replace('-', '') if trade_date else None
        ts_start = start_date.replace('-', '') if start_date else None
        ts_end = end_date.replace('-', '') if end_date else None
        logger.info(f"sw_daily(trade_date={t_date}, start={ts_start}, end={ts_end})")

        try:
            df = ts.pro_api().sw_daily(
                trade_date=t_date,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare sw_daily 无权限，将通过成分股聚合计算: {e}")
                return pd.DataFrame()
            raise DataFetchError(f"Tushare sw_daily err: {e}") from e

    def index_member(self, index_code: str) -> pd.DataFrame:
        """
        获取指数成分股（含申万行业成分）
        args:
            index_code: 指数代码，如 801080.SI（申万电子行业）
        返回:
            DataFrame: 成分股列表
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        logger.info(f"index_member({index_code})")

        try:
            df = ts.pro_api().index_member(index_code=index_code)
            if df.empty:
                return pd.DataFrame()
            df = df[df['out_date'].isna()] if 'out_date' in df.columns else df
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare index_member err: {e}") from e

    def daily_basic(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取股票每日基本面指标（PE/PB等）
        args:
            stock_code: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
        返回:
            DataFrame: 每日基本面数据
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"daily_basic({ts_code}, {ts_start}, {ts_end})")

        try:
            df = ts.pro_api().daily_basic(
                ts_code=ts_code,
                start_date=ts_start,
                end_date=ts_end,
            )
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare daily_basic err: {e}") from e

    def index_classify(self, level: str = 'L1', src: str = 'SW2021') -> pd.DataFrame:
        """
        获取行业分类列表
        args:
            level: 行业级别 L1/L2/L3
            src: 分类来源 SW2021/SW2014
        返回:
            DataFrame: 行业分类列表
        """
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化，请检查 Token 配置")

        logger.info(f"index_classify(level={level}, src={src})")

        try:
            df = ts.pro_api().index_classify(level=level, src=src)
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['quota', '配额', 'limit', '权限']):
                logger.warning(f"Tushare 配额可能超限: {e}")
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare index_classify err: {e}") from e

    def repurchase(self, stock_code: str) -> pd.DataFrame:
        """获取股票回购数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code, _, _ = self.fetch_common(stock_code, "", "")
        logger.info(f"repurchase({ts_code})")
        try:
            df = ts.pro_api().repurchase(ts_code=ts_code)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare repurchase err: {e}") from e

    def share_float(self, stock_code: str = None, ann_date: str = None) -> pd.DataFrame:
        """获取限售解禁数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code = None
        if stock_code:
            ts_code, _, _ = self.fetch_common(stock_code, "", "")
        t_ann = ann_date.replace('-', '') if ann_date else None
        logger.info(f"share_float(ts_code={ts_code}, ann_date={t_ann})")
        try:
            kwargs = {}
            if ts_code: kwargs['ts_code'] = ts_code
            if t_ann: kwargs['ann_date'] = t_ann
            df = ts.pro_api().share_float(**kwargs)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare share_float err: {e}") from e

    def broker_recommend(self, month: str) -> pd.DataFrame:
        """获取分析师月度评级数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info(f"broker_recommend({month})")
        try:
            df = ts.pro_api().broker_recommend(month=month)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare broker_recommend err: {e}") from e

    def pledge_stat(self, stock_code: str) -> pd.DataFrame:
        """获取股权质押统计数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code, _, _ = self.fetch_common(stock_code, "", "")
        logger.info(f"pledge_stat({ts_code})")
        try:
            df = ts.pro_api().pledge_stat(ts_code=ts_code)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare pledge_stat err: {e}") from e

    def block_trade(self, stock_code: str = None, start_date: str = None,
                    end_date: str = None) -> pd.DataFrame:
        """获取大宗交易数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code = None
        if stock_code:
            ts_code, _, _ = self.fetch_common(stock_code, "", "")
        ts_start = start_date.replace('-', '') if start_date else None
        ts_end = end_date.replace('-', '') if end_date else None
        logger.info(f"block_trade(ts_code={ts_code}, {ts_start}, {ts_end})")
        try:
            kwargs = {}
            if ts_code: kwargs['ts_code'] = ts_code
            if ts_start: kwargs['start_date'] = ts_start
            if ts_end: kwargs['end_date'] = ts_end
            df = ts.pro_api().block_trade(**kwargs)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare block_trade err: {e}") from e

    def top_list(self, trade_date: str) -> pd.DataFrame:
        """获取龙虎榜每日明细"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        t_date = trade_date.replace('-', '') if trade_date else None
        logger.info(f"top_list({t_date})")
        try:
            df = ts.pro_api().top_list(trade_date=t_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare top_list err: {e}") from e

    def top_inst(self, trade_date: str) -> pd.DataFrame:
        """获取龙虎榜机构席位追踪"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        t_date = trade_date.replace('-', '') if trade_date else None
        logger.info(f"top_inst({t_date})")
        try:
            df = ts.pro_api().top_inst(trade_date=t_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare top_inst err: {e}") from e

    def pledge_detail(self, stock_code: str) -> pd.DataFrame:
        """获取股权质押明细数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code, _, _ = self.fetch_common(stock_code, "", "")
        logger.info(f"pledge_detail({ts_code})")
        try:
            df = ts.pro_api().pledge_detail(ts_code=ts_code)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare pledge_detail err: {e}") from e

    def report_rc(self, stock_code: str, start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """获取券商卖方盈利预测数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code, ts_start, ts_end = self.fetch_common(stock_code, start_date, end_date)
        logger.info(f"report_rc({ts_code}, {ts_start}, {ts_end})")
        try:
            df = ts.pro_api().report_rc(ts_code=ts_code, start_date=ts_start, end_date=ts_end)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare report_rc err: {e}") from e

    # ------------------------------------------------------------------
    # 以下为新增 Tushare 接口（分红送股/财务审计意见/财报披露计划）
    # ------------------------------------------------------------------

    def dividend(self, stock_code: str) -> pd.DataFrame:
        """分红送股数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code, _, _ = self.fetch_common(stock_code, "", "")
        logger.info(f"dividend({ts_code})")
        try:
            df = ts.pro_api().dividend(ts_code=ts_code)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare dividend err: {e}") from e

    def fina_audit(self, stock_code: str) -> pd.DataFrame:
        """财务审计意见数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code, _, _ = self.fetch_common(stock_code, "", "")
        logger.info(f"fina_audit({ts_code})")
        try:
            df = ts.pro_api().fina_audit(ts_code=ts_code)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare fina_audit err: {e}") from e

    def disclosure_date(self, stock_code: str) -> pd.DataFrame:
        """财报披露计划日期"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        ts_code, _, _ = self.fetch_common(stock_code, "", "")
        logger.info(f"disclosure_date({ts_code})")
        try:
            df = ts.pro_api().disclosure_date(ts_code=ts_code)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare disclosure_date err: {e}") from e

    # ====== 融资融券系列 ======

    def margin_secs(self) -> pd.DataFrame:
        """融资融券标的（盘前更新）"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("margin_secs()")
        try:
            df = ts.pro_api().margin_secs()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare margin_secs err: {e}") from e

    def slb_sec(self) -> pd.DataFrame:
        """转融券交易汇总"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("slb_sec()")
        try:
            df = ts.pro_api().slb_sec()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare slb_sec err: {e}") from e

    def slb_len(self) -> pd.DataFrame:
        """转融资交易汇总"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("slb_len()")
        try:
            df = ts.pro_api().slb_len()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare slb_len err: {e}") from e

    def moneyflow_mkt_dc(self, trade_date: str = "") -> pd.DataFrame:
        """大盘资金流向（东方财富DC）"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info(f"moneyflow_mkt_dc({trade_date})")
        try:
            df = ts.pro_api().moneyflow_mkt_dc(trade_date=trade_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare moneyflow_mkt_dc err: {e}") from e

    def fund_adj(self, ts_code: str) -> pd.DataFrame:
        """基金复权因子"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info(f"fund_adj({ts_code})")
        try:
            df = ts.pro_api().fund_adj(ts_code=ts_code)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare fund_adj err: {e}") from e

    # ====== 经济日历 & 利率 ======

    def cn_schedule(self, date: str = "") -> pd.DataFrame:
        """中国经济数据发布日程"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info(f"cn_schedule({date})")
        try:
            df = ts.pro_api().cn_schedule(date=date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare cn_schedule err: {e}") from e

    def shibor(self) -> pd.DataFrame:
        """Shibor 利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("shibor()")
        try:
            import time as _t
            start = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            end = date.today().strftime("%Y%m%d")
            df = ts.pro_api().shibor(start_date=start, end_date=end)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare shibor err: {e}") from e

    def shibor_quote(self) -> pd.DataFrame:
        """Shibor 报价数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("shibor_quote()")
        try:
            df = ts.pro_api().shibor_quote()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare shibor_quote err: {e}") from e

    def shibor_lpr(self) -> pd.DataFrame:
        """LPR 贷款基础利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("shibor_lpr()")
        try:
            import time as _t
            start = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            end = date.today().strftime("%Y%m%d")
            df = ts.pro_api().shibor_lpr(start_date=start, end_date=end)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare shibor_lpr err: {e}") from e

    def libor(self) -> pd.DataFrame:
        """Libor 拆借利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("libor()")
        try:
            import time as _t
            start = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            end = date.today().strftime("%Y%m%d")
            df = ts.pro_api().libor(start_date=start, end_date=end)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare libor err: {e}") from e

    def hibor(self) -> pd.DataFrame:
        """Hibor 拆借利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("hibor()")
        try:
            import time as _t
            start = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            end = date.today().strftime("%Y%m%d")
            df = ts.pro_api().hibor(start_date=start, end_date=end)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare hibor err: {e}") from e

    def wz_index(self) -> pd.DataFrame:
        """温州民间借贷利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("wz_index()")
        try:
            df = ts.pro_api().wz_index()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare wz_index err: {e}") from e

    def gz_index(self) -> pd.DataFrame:
        """广州民间借贷利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("gz_index()")
        try:
            df = ts.pro_api().gz_index()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            raise DataFetchError(f"Tushare gz_index err: {e}") from e

    def cn_gdp(self) -> pd.DataFrame:
        """国民经济之GDP数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("cn_gdp()")
        try:
            df = ts.pro_api().cn_gdp()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare cn_gdp err: {e}") from e

    def cn_cpi(self) -> pd.DataFrame:
        """国民经济之CPI数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("cn_cpi()")
        try:
            df = ts.pro_api().cn_cpi()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare cn_cpi err: {e}") from e

    def cn_ppi(self) -> pd.DataFrame:
        """国民经济之PPI数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("cn_ppi()")
        try:
            df = ts.pro_api().cn_ppi()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare cn_ppi err: {e}") from e

    def cn_m(self) -> pd.DataFrame:
        """货币供应量数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("cn_m()")
        try:
            df = ts.pro_api().cn_m()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare cn_m err: {e}") from e

    def sf_month(self) -> pd.DataFrame:
        """社会融资规模增量月度数据"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("sf_month()")
        try:
            df = ts.pro_api().sf_month()
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare sf_month err: {e}") from e

    def us_tycr(self, start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """美国每日国债收益率曲线利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("us_tycr()")
        try:
            if not start_date:
                start_date = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            if not end_date:
                end_date = date.today().strftime("%Y%m%d")
            df = ts.pro_api().us_tycr(start_date=start_date, end_date=end_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare us_tycr err: {e}") from e

    def us_trycr(self, start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """美国国债实际收益率曲线利率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("us_trycr()")
        try:
            if not start_date:
                start_date = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            if not end_date:
                end_date = date.today().strftime("%Y%m%d")
            df = ts.pro_api().us_trycr(start_date=start_date, end_date=end_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare us_trycr err: {e}") from e

    def us_tbr(self, start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """美国短期国债收益率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("us_tbr()")
        try:
            if not start_date:
                start_date = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            if not end_date:
                end_date = date.today().strftime("%Y%m%d")
            df = ts.pro_api().us_tbr(start_date=start_date, end_date=end_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare us_tbr err: {e}") from e

    def us_tltr(self, start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """美国长期国债收益率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("us_tltr()")
        try:
            if not start_date:
                start_date = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            if not end_date:
                end_date = date.today().strftime("%Y%m%d")
            df = ts.pro_api().us_tltr(start_date=start_date, end_date=end_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare us_tltr err: {e}") from e

    def us_trltr(self, start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """美国实际长期国债平均收益率"""
        if self._api is None:
            raise DataFetchError("Tushare API 未初始化")
        logger.info("us_trltr()")
        try:
            if not start_date:
                start_date = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
            if not end_date:
                end_date = date.today().strftime("%Y%m%d")
            df = ts.pro_api().us_trltr(start_date=start_date, end_date=end_date)
            if df.empty: return pd.DataFrame()
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(kw in error_msg for kw in ['quota', '配额', 'limit', '权限']):
                raise RateLimitError(f"Tushare 配额超限: {e}") from e
            raise DataFetchError(f"Tushare us_trltr err: {e}") from e


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
