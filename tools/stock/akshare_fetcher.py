# -*- coding: utf-8 -*-
"""
===================================
AkshareFetcher - 主数据源 (Priority 1)
===================================

数据来源：东方财富爬虫（通过 akshare 库）
特点：免费、无需 Token、数据全面
风险：爬虫机制易被反爬封禁

防封禁策略：
1. 每次请求前随机休眠 2-5 秒
2. 随机轮换 User-Agent
3. 使用 tenacity 实现指数退避重试

增强数据：
- 实时行情：量比、换手率、市盈率、市净率、总市值、流通市值
- 筹码分布：获利比例、平均成本、筹码集中度

数据存储策略：
1. 增量获取：首次全量获取，后续根据数据库最新日期增量获取
2. 自动存储：获取数据后自动保存到数据库
3. 主键设计：使用日期+股票代码作为复合主键，避免重复数据
4. 智能更新：数据库已有数据时跳过，只获取新数据

增量获取流程：
  开始
    ↓
  查询数据库中该股票的最新日期
    ↓
  如果无历史数据 → 全量获取（从start_date到end_date）
    ↓
  如果有历史数据 → 从最新日期+1天开始增量获取
    ↓
  调用akshare API获取数据
    ↓
  数据标准化和技术指标计算
    ↓
  保存到数据库（UPSERT操作）
    ↓
  返回DataFrame
"""
import logging
from utils.logger import logger
import random
import time
import threading
from dataclasses import dataclass
from typing import Optional, Dict, Any, Union
from .common import extract_last_segment_standard, _is_etf_code, _is_hk_code

import pandas as pd
from pandas import DataFrame
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS


@dataclass
class RealtimeQuote:
    """
    实时行情数据
    
    包含当日实时交易数据和估值指标
    """
    code: str
    name: str = ""
    price: float = 0.0           # 最新价
    change_pct: float = 0.0      # 涨跌幅(%)
    change_amount: float = 0.0   # 涨跌额
    
    # 量价指标
    volume_ratio: float = 0.0    # 量比（当前成交量/过去5日平均成交量）
    turnover_rate: float = 0.0   # 换手率(%)
    amplitude: float = 0.0       # 振幅(%)
    
    # 估值指标
    pe_ratio: float = 0.0        # 市盈率(动态)
    pb_ratio: float = 0.0        # 市净率
    total_mv: float = 0.0        # 总市值(元)
    circ_mv: float = 0.0         # 流通市值(元)
    
    # 其他
    change_60d: float = 0.0      # 60日涨跌幅(%)
    high_52w: float = 0.0        # 52周最高
    low_52w: float = 0.0         # 52周最低
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'name': self.name,
            'price': self.price,
            'change_pct': self.change_pct,
            'volume_ratio': self.volume_ratio,
            'turnover_rate': self.turnover_rate,
            'amplitude': self.amplitude,
            'pe_ratio': self.pe_ratio,
            'pb_ratio': self.pb_ratio,
            'total_mv': self.total_mv,
            'circ_mv': self.circ_mv,
            'change_60d': self.change_60d,
        }


@dataclass  
class ChipDistribution:
    """
    筹码分布数据
    
    反映持仓成本分布和获利情况
    """
    code: str
    date: str = ""
    
    # 获利情况
    profit_ratio: float = 0.0     # 获利比例(0-1)
    avg_cost: float = 0.0         # 平均成本
    
    # 筹码集中度
    cost_90_low: float = 0.0      # 90%筹码成本下限
    cost_90_high: float = 0.0     # 90%筹码成本上限
    concentration_90: float = 0.0  # 90%筹码集中度（越小越集中）
    
    cost_70_low: float = 0.0      # 70%筹码成本下限
    cost_70_high: float = 0.0     # 70%筹码成本上限
    concentration_70: float = 0.0  # 70%筹码集中度
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'date': self.date,
            'profit_ratio': self.profit_ratio,
            'avg_cost': self.avg_cost,
            'cost_90_low': self.cost_90_low,
            'cost_90_high': self.cost_90_high,
            'concentration_90': self.concentration_90,
            'concentration_70': self.concentration_70,
        }
    
    def get_chip_status(self, current_price: float) -> str:
        """
        获取筹码状态描述
        
        Args:
            current_price: 当前股价
            
        Returns:
            筹码状态描述
        """
        status_parts = []
        
        # 获利比例分析
        if self.profit_ratio >= 0.9:
            status_parts.append("获利盘极高(>90%)")
        elif self.profit_ratio >= 0.7:
            status_parts.append("获利盘较高(70-90%)")
        elif self.profit_ratio >= 0.5:
            status_parts.append("获利盘中等(50-70%)")
        elif self.profit_ratio >= 0.3:
            status_parts.append("套牢盘较多(>30%)")
        else:
            status_parts.append("套牢盘极重(>70%)")
        
        # 筹码集中度分析 (90%集中度 < 10% 表示集中)
        if self.concentration_90 < 0.08:
            status_parts.append("筹码高度集中")
        elif self.concentration_90 < 0.15:
            status_parts.append("筹码较集中")
        elif self.concentration_90 < 0.25:
            status_parts.append("筹码分散度中等")
        else:
            status_parts.append("筹码较分散")
        
        # 成本与现价关系
        if current_price > 0 and self.avg_cost > 0:
            cost_diff = (current_price - self.avg_cost) / self.avg_cost * 100
            if cost_diff > 20:
                status_parts.append(f"现价高于平均成本{cost_diff:.1f}%")
            elif cost_diff > 5:
                status_parts.append(f"现价略高于成本{cost_diff:.1f}%")
            elif cost_diff > -5:
                status_parts.append("现价接近平均成本")
            else:
                status_parts.append(f"现价低于平均成本{abs(cost_diff):.1f}%")
        
        return "，".join(status_parts)



# User-Agent 池，用于随机轮换
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# 当前生效的 User-Agent（_set_random_user_agent 每次轮换更新）
_current_user_agent: str = USER_AGENTS[0]
_ua_patch_installed: bool = False


def _install_user_agent_patch() -> None:
    """一次性 patch requests 的默认 UA，使 akshare 等基于 requests 的库
    实际带上我们轮换的 User-Agent。

    akshare 内部用 requests.get / requests.Session 发请求，自身不暴露
    session 注入口；requests 的默认 UA 由 requests.utils.default_user_agent()
    产生（Session.__init__ 经 default_headers() 调用，模块级 requests.get
    每次新建 Session 也会调用）。patch 该函数即可让所有新建会话带上我们的 UA。
    已在请求 headers 里显式设置 UA 的调用不受影响（请求级 headers 优先）。
    """
    global _ua_patch_installed
    if _ua_patch_installed:
        return
    try:
        import requests as _requests

        def _patched_default_user_agent():
            return _current_user_agent

        _requests.utils.default_user_agent = _patched_default_user_agent
        _ua_patch_installed = True
        logger.debug("已安装 requests User-Agent 补丁")
    except Exception as e:
        logger.debug(f"安装 User-Agent 补丁失败: {e}")


# 缓存实时行情数据（避免重复请求）
_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 60  # 60秒缓存有效期
}

# ETF 实时行情缓存
_etf_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 60  # 60秒缓存有效期
}

# 线程锁（保护实时行情缓存的并发读写）
_realtime_cache_lock = threading.Lock()


class AkshareFetcher(BaseFetcher):
    """
    Akshare 数据源实现
    
    优先级：1（最高）
    数据来源：东方财富网爬虫
    
    关键策略：
    - 每次请求前随机休眠 10.0-50.0 秒
    - 随机 User-Agent 轮换
    - 失败后指数退避重试（最多3次）
    """
    
    name = "AkshareFetcher"
    priority = 1
    
    def __init__(self, sleep_min: float = 2.0, sleep_max: float = 8.0):
        """
        初始化 AkshareFetcher
        
        Args:
            sleep_min: 最小休眠时间（秒）
            sleep_max: 最大休眠时间（秒）
        """
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self._last_request_time: Optional[float] = None
    
    def _set_random_user_agent(self) -> None:
        """
        设置随机 User-Agent

        通过 patch requests 的默认 User-Agent（requests.utils.default_user_agent）
        实际生效，使 akshare 内部的 requests.get / Session 调用都带上轮换后的 UA。
        这是关键的反爬策略之一。
        """
        global _current_user_agent
        try:
            ua = random.choice(USER_AGENTS)
            _current_user_agent = ua
            _install_user_agent_patch()
            logger.debug(f"设置 User-Agent: {ua[:50]}...")
        except Exception as e:
            logger.debug(f"设置 User-Agent 失败: {e}")
    
    def _enforce_rate_limit(self) -> None:
        """
        强制执行速率限制
        
        策略：
        1. 检查距离上次请求的时间间隔
        2. 如果间隔不足，补充休眠时间
        3. 然后再执行随机 jitter 休眠
        """
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            min_interval = self.sleep_min
            if elapsed < min_interval:
                additional_sleep = min_interval - elapsed
                logger.debug(f"补充休眠 {additional_sleep:.2f} 秒")
                time.sleep(additional_sleep)
        
        # 执行随机 jitter 休眠
        self.random_sleep(self.sleep_min, self.sleep_max)
        self._last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),  # 最多重试3次
        wait=wait_exponential(multiplier=1, min=2, max=30),  # 指数退避：2, 4, 8... 最大30秒
        retry=retry_if_exception_type((ConnectionError, TimeoutError, DataFetchError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, freq: str, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Akshare 获取原始数据
        
        根据代码类型自动选择 API：
        - 普通股票：使用 ak.stock_zh_a_hist()
        - ETF 基金：使用 ak.fund_etf_hist_em()
        
        流程：
        1. 判断代码类型（股票/ETF）
        2. 设置随机 User-Agent
        3. 执行速率限制（随机休眠）
        4. 调用对应的 akshare API
        5. 处理返回数据
        """
        if freq not in ("daily", "weekly", "monthly"):
            raise ValueError(f"不支持的频率: {freq}")

        # 周线/月线降级：Akshare 无直接周/月线接口，用日线 resample 生成
        if freq in ("weekly", "monthly"):
            daily_df = self._fetch_raw_data("daily", stock_code, start_date, end_date)
            if daily_df is None or daily_df.empty:
                raise DataFetchError(f"Akshare 周月线降级失败: 日线数据为空 {stock_code}")
            return self._resample_kline(daily_df, freq)

        # 根据代码类型选择不同的获取方法
        if _is_hk_code(stock_code):
            return self._fetch_hk_data(stock_code, start_date, end_date)
        elif _is_etf_code(stock_code):
            return self._fetch_etf_data(stock_code, start_date, end_date)
        else:
            return self._fetch_stock_data(stock_code, start_date, end_date)

    @staticmethod
    def _resample_kline(daily_df: pd.DataFrame, freq: str) -> pd.DataFrame:
        """将日线 DataFrame resample 为周线/月线（Akshare 降级方案）"""
        import pandas as pd
        df = daily_df.copy()
        # 确保有 date 列且为 datetime
        if 'date' not in df.columns:
            return daily_df
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        rule = 'W-FRI' if freq == 'weekly' else 'M'
        agg = {}
        for col in ('open', 'high', 'low', 'close', 'volume', 'amount'):
            if col in df.columns:
                agg[col] = 'last' if col == 'close' else ('sum' if col in ('volume', 'amount') else 'first')
            # high/low 需要特殊处理
        if 'high' in df.columns:
            agg['high'] = 'max'
        if 'low' in df.columns:
            agg['low'] = 'min'
        if 'open' in df.columns:
            agg['open'] = 'first'
        resampled = df.resample(rule).agg(agg).dropna(subset=['close'])
        resampled = resampled.reset_index()
        resampled['date'] = resampled['date'].dt.strftime('%Y-%m-%d')
        if 'pct_chg' in resampled.columns:
            resampled['pct_chg'] = resampled['close'].pct_change() * 100
        return resampled

    def _fetch_stock_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取普通 A 股历史数据
        
        数据来源：主=ak.stock_zh_a_hist() (东方财富), 备=ak.stock_zh_a_hist_tx() (腾讯)
        """
        import akshare as ak
        import time as _time

        # 防封禁策略 1: 随机 User-Agent
        self._set_random_user_agent()
        # 防封禁策略 2: 强制休眠
        self._enforce_rate_limit()

        # ---- 主数据源：东方财富 stock_zh_a_hist ----
        logger.info(f"[API调用] ak.stock_zh_a_hist(symbol={stock_code}, period=daily, "
                   f"start_date={start_date.replace('-', '')}, end_date={end_date.replace('-', '')}, adjust=qfq)")

        primary_ok = False
        df = None
        try:
            api_start = _time.time()
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"  # 前复权
            )

            api_elapsed = _time.time() - api_start

            if df is not None and not df.empty:
                primary_ok = True
                logger.info(f"[API返回] ak.stock_zh_a_hist 成功: 返回 {len(df)} 行数据, 耗时 {api_elapsed:.2f}s")
                logger.info(f"[API返回] 列名: {list(df.columns)}")
                logger.info(f"[API返回] 日期范围: {df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}")
                logger.debug(f"[API返回] 最新3条数据:\n{df.tail(3).to_string()}")
            else:
                logger.warning(f"[API返回] ak.stock_zh_a_hist 返回空数据, 耗时 {api_elapsed:.2f}s")

        except Exception as e:
            logger.warning(f"[API异常] ak.stock_zh_a_hist 失败 ({type(e).__name__}), 尝试备用数据源...")

        if primary_ok:
            return df

        # ---- 备用数据源：腾讯 stock_zh_a_hist_tx ----
        # 腾讯接口需要市场前缀：sh/sz/bj
        code = stock_code.strip()
        # 沪市：600xxx, 601xxx, 603xxx, 605xxx (主板), 688xxx (科创板)
        # 深市：000xxx (主板), 001xxx (主板), 002xxx (中小板/主板), 300xxx (创业板)
        # 北交所：920xxx, 8xxxxx, 4xxxxx
        if code.startswith(('600', '601', '603', '605', '688')):
            tx_symbol = f"sh{code}"
        elif code.startswith(('000', '001', '002', '300')):
            tx_symbol = f"sz{code}"
        elif code.startswith(('920', '8', '4')):
            tx_symbol = f"bj{code}"
        else:
            tx_symbol = f"sz{code}"  # 默认深市

        # 备用源也需要休眠防封禁
        self._enforce_rate_limit()

        logger.info(f"[API调用] ak.stock_zh_a_hist_tx(symbol={tx_symbol}, "
                   f"start_date={start_date.replace('-', '')}, end_date={end_date.replace('-', '')}, adjust=qfq)")

        try:
            api_start = _time.time()
            df_tx = ak.stock_zh_a_hist_tx(
                symbol=tx_symbol,
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"
            )
            api_elapsed = _time.time() - api_start

            if df_tx is not None and not df_tx.empty:
                logger.info(f"[API返回] ak.stock_zh_a_hist_tx 成功: 返回 {len(df_tx)} 行数据, 耗时 {api_elapsed:.2f}s")
                logger.info(f"[API返回] 列名: {list(df_tx.columns)}")
                return df_tx
            else:
                logger.warning(f"[API返回] ak.stock_zh_a_hist_tx 返回空数据, 耗时 {api_elapsed:.2f}s")

        except Exception as e2:
            logger.warning(f"[API异常] ak.stock_zh_a_hist_tx 也失败 ({type(e2).__name__}): {e2}")
            # 检测反爬封禁（以最近一次异常为准）
            error_msg = str(e2).lower()
            if any(kw in error_msg for kw in ['banned', 'blocked', '频率', 'rate', '限制']):
                raise RateLimitError(f"Akshare 可能被限流: {e2}") from e2

        # 全部失败
        raise DataFetchError(f"Akshare 获取数据失败: 主(东方财富)和备(腾讯)数据源均失败")
    
    def _fetch_etf_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取 ETF 基金历史数据
        
        数据来源：ak.fund_etf_hist_em()
        
        Args:
            stock_code: ETF 代码，如 '512400', '159883'
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
            
        Returns:
            ETF 历史数据 DataFrame
        """
        import akshare as ak
        
        # 防封禁策略 1: 随机 User-Agent
        self._set_random_user_agent()
        
        # 防封禁策略 2: 强制休眠
        self._enforce_rate_limit()
        
        logger.info(f"[API调用] ak.fund_etf_hist_em(symbol={stock_code}, period=daily, "
                   f"start_date={start_date.replace('-', '')}, end_date={end_date.replace('-', '')}, adjust=qfq)")
        
        try:
            import time as _time
            api_start = _time.time()
            
            # 调用 akshare 获取 ETF 日线数据
            df = ak.fund_etf_hist_em(
                symbol=stock_code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"  # 前复权
            )
            
            api_elapsed = _time.time() - api_start
            
            # 记录返回数据摘要
            if df is not None and not df.empty:
                logger.info(f"[API返回] ak.fund_etf_hist_em 成功: 返回 {len(df)} 行数据, 耗时 {api_elapsed:.2f}s")
                logger.info(f"[API返回] 列名: {list(df.columns)}")
                logger.info(f"[API返回] 日期范围: {df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}")
                logger.debug(f"[API返回] 最新3条数据:\n{df.tail(3).to_string()}")
            else:
                logger.warning(f"[API返回] ak.fund_etf_hist_em 返回空数据, 耗时 {api_elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # 检测反爬封禁
            if any(keyword in error_msg for keyword in ['banned', 'blocked', '频率', 'rate', '限制']):
                logger.warning(f"检测到可能被封禁: {e}")
                raise RateLimitError(f"Akshare 可能被限流: {e}") from e
            
            raise DataFetchError(f"Akshare 获取 ETF 数据失败: {e}") from e
    
    def _fetch_hk_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取港股历史数据
        
        数据来源：ak.stock_hk_hist()
        
        Args:
            stock_code: 港股代码，如 '00700', '01810'
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
            
        Returns:
            港股历史数据 DataFrame
        """
        import akshare as ak
        
        # 防封禁策略 1: 随机 User-Agent
        self._set_random_user_agent()
        
        # 防封禁策略 2: 强制休眠
        self._enforce_rate_limit()
        
        # 确保代码格式正确（5位数字）
        code = stock_code.lower().replace('hk', '').zfill(5)
        
        logger.info(f"[API调用] ak.stock_hk_hist(symbol={code}, period=daily, "
                   f"start_date={start_date.replace('-', '')}, end_date={end_date.replace('-', '')}, adjust=qfq)")
        
        try:
            import time as _time
            api_start = _time.time()
            
            # 调用 akshare 获取港股日线数据
            df = ak.stock_hk_hist(
                symbol=code,
                period="daily",
                start_date=start_date.replace('-', ''),
                end_date=end_date.replace('-', ''),
                adjust="qfq"  # 前复权
            )
            
            api_elapsed = _time.time() - api_start
            
            # 记录返回数据摘要
            if df is not None and not df.empty:
                logger.info(f"[API返回] ak.stock_hk_hist 成功: 返回 {len(df)} 行数据, 耗时 {api_elapsed:.2f}s")
                logger.info(f"[API返回] 列名: {list(df.columns)}")
                logger.info(f"[API返回] 日期范围: {df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}")
                logger.debug(f"[API返回] 最新3条数据:\n{df.tail(3).to_string()}")
            else:
                logger.warning(f"[API返回] ak.stock_hk_hist 返回空数据, 耗时 {api_elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # 检测反爬封禁
            if any(keyword in error_msg for keyword in ['banned', 'blocked', '频率', 'rate', '限制']):
                logger.warning(f"检测到可能被封禁: {e}")
                raise RateLimitError(f"Akshare 可能被限流: {e}") from e
            
            raise DataFetchError(f"Akshare 获取港股数据失败: {e}") from e
    
    def _normalize_data(self, freq: str, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 Akshare 数据
        
        Akshare 返回的列名（中文）：
        日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
        
        需要映射到标准列名：
        date, open, high, low, close, volume, amount, pct_chg
        """
        if freq not in ("daily", "weekly", "monthly"):
            raise ValueError(f"不支持的频率: {freq}")

        df = df.copy()
        
        # 列名映射（Akshare 中文列名 -> 标准英文列名）
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'pct_chg',
            '换手率': 'turnover_rate',
        }
        
        # 重命名列
        df = df.rename(columns=column_mapping)
        
        # 添加股票代码列
        df['code'] = stock_code
        
        # 只保留需要的列（额外保留换手率）
        keep_cols = ['code'] + STANDARD_COLUMNS + ['turnover_rate']
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df
    
    def get_realtime_quote(self, stock_code: str) -> Optional[RealtimeQuote]:
        """
        获取实时行情数据
        
        根据代码类型自动选择数据源：
        - 普通股票：ak.stock_zh_a_spot_em()
        - ETF 基金：ak.fund_etf_spot_em()
        
        Args:
            stock_code: 股票/ETF代码
            
        Returns:
            RealtimeQuote 对象，获取失败返回 None
        """
        # 根据代码类型选择不同的获取方法
        if _is_hk_code(stock_code):
            return self._get_hk_realtime_quote(stock_code)
        elif _is_etf_code(stock_code):
            return self._get_etf_realtime_quote(stock_code)
        else:
            return self._get_stock_realtime_quote(stock_code)
    
    def _fallback_realtime_quote(self, stock_code: str) -> Optional[RealtimeQuote]:
        """AkShare 实时行情全市场快照失败时的降级方案：
        1. 尝试 Tushare daily_basic 获取单只股票的 PE/PB/市值/换手率
        2. 如果 Tushare 也失败，返回过期缓存数据（即使已过期）
        3. 如果缓存也没有，返回 None"""
        # ---- 1. Tushare daily_basic 降级 ----
        try:
            from .tushare_fetcher import TushareFetcher
            ts_fetcher = TushareFetcher()
            if ts_fetcher._api is not None:
                from datetime import date, timedelta
                end_date = date.today().strftime("%Y-%m-%d")
                start_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
                ts_df = ts_fetcher.daily_basic(stock_code, start_date=start_date, end_date=end_date)
                if ts_df is not None and not ts_df.empty:
                    row = ts_df.iloc[0]

                    def _safe(v, d=0.0):
                        try:
                            f = float(v)
                            return f if not pd.isna(f) else d
                        except (TypeError, ValueError):
                            return d

                    quote = RealtimeQuote(
                        code=stock_code,
                        name=stock_code,  # daily_basic 不返回名称
                        price=_safe(row.get('close')),
                        pe_ratio=_safe(row.get('pe_ttm') or row.get('pe')),
                        pb_ratio=_safe(row.get('pb')),
                        total_mv=_safe(row.get('total_mv')) * 10000,  # Tushare total_mv 单位万元 → 元
                        circ_mv=_safe(row.get('circ_mv')) * 10000,
                        turnover_rate=_safe(row.get('turnover_rate')),
                    )
                    logger.info(f"[降级] {stock_code} 使用 Tushare daily_basic 获取估值数据: "
                                f"价格={quote.price}, PE={quote.pe_ratio}, PB={quote.pb_ratio}")
                    return quote
        except Exception as e:
            logger.warning(f"[降级] Tushare daily_basic 也失败: {e}")

        # ---- 2. 过期缓存降级 ----
        with _realtime_cache_lock:
            if _realtime_cache['data'] is not None:
                cached_df = _realtime_cache['data']
                row = cached_df[cached_df['代码'] == stock_code]
                if not row.empty:
                    row = row.iloc[0]

                    def _safe(v, d=0.0):
                        try:
                            f = float(v)
                            return f if not pd.isna(f) else d
                        except (TypeError, ValueError):
                            return d

                    quote = RealtimeQuote(
                        code=stock_code,
                        name=str(row.get('名称', '')),
                        price=_safe(row.get('最新价')),
                        change_pct=_safe(row.get('涨跌幅')),
                        pe_ratio=_safe(row.get('市盈率-动态')),
                        pb_ratio=_safe(row.get('市净率')),
                        total_mv=_safe(row.get('总市值')),
                        circ_mv=_safe(row.get('流通市值')),
                        turnover_rate=_safe(row.get('换手率')),
                    )
                    cache_age = int(time.time() - _realtime_cache['timestamp'])
                    logger.warning(f"[降级] {stock_code} 使用过期缓存数据"
                                   f"（缓存于 {cache_age} 秒前）")
                    return quote

        return None

    def _get_stock_realtime_quote(self, stock_code: str) -> Optional[RealtimeQuote]:
        """
        获取普通 A 股实时行情数据
        
        数据来源：ak.stock_zh_a_spot_em()
        包含：量比、换手率、市盈率、市净率、总市值、流通市值等
        """
        import akshare as ak
        
        try:
            # 检查缓存
            current_time = time.time()
            with _realtime_cache_lock:
                if (_realtime_cache['data'] is not None and 
                    current_time - _realtime_cache['timestamp'] < _realtime_cache['ttl']):
                    df = _realtime_cache['data']
                    logger.debug(f"[缓存命中] 使用缓存的A股实时行情数据")
                else:
                    df = None  # 标记需要重新获取
            
            if df is None:
                last_error: Optional[Exception] = None
                for attempt in range(1, 3):
                    try:
                        # 防封禁策略
                        self._set_random_user_agent()
                        self._enforce_rate_limit()

                        logger.info(f"[API调用] ak.stock_zh_a_spot_em() 获取A股实时行情... (attempt {attempt}/2)")
                        import time as _time
                        api_start = _time.time()

                        df = ak.stock_zh_a_spot_em()

                        api_elapsed = _time.time() - api_start
                        logger.info(f"[API返回] ak.stock_zh_a_spot_em 成功: 返回 {len(df)} 只股票, 耗时 {api_elapsed:.2f}s")
                        break
                    except Exception as e:
                        last_error = e
                        logger.warning(f"[API错误] ak.stock_zh_a_spot_em 获取失败 (attempt {attempt}/2): {e}")
                        time.sleep(min(2 ** attempt, 5))

                # 不缓存失败结果，避免瞬时网络抖动阻塞全部行情60秒
                if df is None:
                    logger.error(f"[API错误] ak.stock_zh_a_spot_em 最终失败: {last_error}")
                    return self._fallback_realtime_quote(stock_code)
                # 只有成功获取数据时才缓存（双重检查：避免覆盖其他线程已刷新的缓存）
                if not df.empty:
                    with _realtime_cache_lock:
                        if (_realtime_cache['data'] is None or
                                time.time() - _realtime_cache['timestamp'] >= _realtime_cache['ttl']):
                            _realtime_cache['data'] = df
                            _realtime_cache['timestamp'] = time.time()
                else:
                    logger.warning(f"[API错误] ak.stock_zh_a_spot_em 返回空数据，不缓存")
                    return self._fallback_realtime_quote(stock_code)

            if df is None or df.empty:
                logger.warning(f"[实时行情] A股实时行情数据为空，跳过 {stock_code}")
                return self._fallback_realtime_quote(stock_code)
            
            # 查找指定股票
            row = df[df['代码'] == stock_code]
            if row.empty:
                logger.warning(f"[API返回] 未找到股票 {stock_code} 的实时行情")
                return self._fallback_realtime_quote(stock_code)
            
            row = row.iloc[0]
            
            # 安全获取字段值
            def safe_float(val, default=0.0):
                try:
                    if pd.isna(val):
                        return default
                    return float(val)
                except:
                    return default
            
            quote = RealtimeQuote(
                code=stock_code,
                name=str(row.get('名称', '')),
                price=safe_float(row.get('最新价')),
                change_pct=safe_float(row.get('涨跌幅')),
                change_amount=safe_float(row.get('涨跌额')),
                volume_ratio=safe_float(row.get('量比')),
                turnover_rate=safe_float(row.get('换手率')),
                amplitude=safe_float(row.get('振幅')),
                pe_ratio=safe_float(row.get('市盈率-动态')),
                pb_ratio=safe_float(row.get('市净率')),
                total_mv=safe_float(row.get('总市值')),
                circ_mv=safe_float(row.get('流通市值')),
                change_60d=safe_float(row.get('60日涨跌幅')),
                high_52w=safe_float(row.get('52周最高')),
                low_52w=safe_float(row.get('52周最低')),
            )
            
            logger.info(f"[实时行情] {stock_code} {quote.name}: 价格={quote.price}, 涨跌={quote.change_pct}%, "
                       f"量比={quote.volume_ratio}, 换手率={quote.turnover_rate}%, "
                       f"PE={quote.pe_ratio}, PB={quote.pb_ratio}")
            return quote
            
        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 实时行情失败: {e}")
            return self._fallback_realtime_quote(stock_code)
    
    def _get_etf_realtime_quote(self, stock_code: str) -> Optional[RealtimeQuote]:
        """
        获取 ETF 基金实时行情数据
        
        数据来源：ak.fund_etf_spot_em()
        包含：最新价、涨跌幅、成交量、成交额、换手率等
        
        Args:
            stock_code: ETF 代码
            
        Returns:
            RealtimeQuote 对象，获取失败返回 None
        """
        import akshare as ak
        
        try:
            # 检查缓存
            current_time = time.time()
            with _realtime_cache_lock:
                if (_etf_realtime_cache['data'] is not None and 
                    current_time - _etf_realtime_cache['timestamp'] < _etf_realtime_cache['ttl']):
                    df = _etf_realtime_cache['data']
                    logger.debug(f"[缓存命中] 使用缓存的ETF实时行情数据")
                else:
                    df = None  # 标记需要重新获取
            
            if df is None:
                last_error: Optional[Exception] = None
                for attempt in range(1, 3):
                    try:
                        # 防封禁策略
                        self._set_random_user_agent()
                        self._enforce_rate_limit()

                        logger.info(f"[API调用] ak.fund_etf_spot_em() 获取ETF实时行情... (attempt {attempt}/2)")
                        import time as _time
                        api_start = _time.time()

                        df = ak.fund_etf_spot_em()

                        api_elapsed = _time.time() - api_start
                        logger.info(f"[API返回] ak.fund_etf_spot_em 成功: 返回 {len(df)} 只ETF, 耗时 {api_elapsed:.2f}s")
                        break
                    except Exception as e:
                        last_error = e
                        logger.warning(f"[API错误] ak.fund_etf_spot_em 获取失败 (attempt {attempt}/2): {e}")
                        time.sleep(min(2 ** attempt, 5))

                # 不缓存失败结果，避免瞬时网络抖动阻塞全部行情60秒
                if df is None:
                    logger.error(f"[API错误] ak.fund_etf_spot_em 最终失败: {last_error}")
                    return None
                # 只有成功获取数据时才缓存（双重检查：避免覆盖其他线程已刷新的缓存）
                if not df.empty:
                    with _realtime_cache_lock:
                        if (_etf_realtime_cache['data'] is None or
                                time.time() - _etf_realtime_cache['timestamp'] >= _etf_realtime_cache['ttl']):
                            _etf_realtime_cache['data'] = df
                            _etf_realtime_cache['timestamp'] = time.time()
                else:
                    logger.warning(f"[API错误] ak.fund_etf_spot_em 返回空数据，不缓存")
                    return None

            if df is None or df.empty:
                logger.warning(f"[实时行情] ETF实时行情数据为空，跳过 {stock_code}")
                return None
            
            # 查找指定 ETF
            row = df[df['代码'] == stock_code]
            if row.empty:
                logger.warning(f"[API返回] 未找到 ETF {stock_code} 的实时行情")
                return None
            
            row = row.iloc[0]
            
            # 安全获取字段值
            def safe_float(val, default=0.0):
                try:
                    if pd.isna(val):
                        return default
                    return float(val)
                except:
                    return default
            
            # ETF 行情数据构建（部分字段 ETF 可能不支持，使用默认值）
            quote = RealtimeQuote(
                code=stock_code,
                name=str(row.get('名称', '')),
                price=safe_float(row.get('最新价')),
                change_pct=safe_float(row.get('涨跌幅')),
                change_amount=safe_float(row.get('涨跌额')),
                volume_ratio=safe_float(row.get('量比', 0)),  # ETF 可能无量比
                turnover_rate=safe_float(row.get('换手率')),
                amplitude=safe_float(row.get('振幅')),
                pe_ratio=0.0,  # ETF 通常无市盈率
                pb_ratio=0.0,  # ETF 通常无市净率
                total_mv=safe_float(row.get('总市值', 0)),
                circ_mv=safe_float(row.get('流通市值', 0)),
                change_60d=0.0,  # ETF 接口可能不提供
                high_52w=safe_float(row.get('52周最高', 0)),
                low_52w=safe_float(row.get('52周最低', 0)),
            )
            
            logger.info(f"[ETF实时行情] {stock_code} {quote.name}: 价格={quote.price}, 涨跌={quote.change_pct}%, "
                       f"换手率={quote.turnover_rate}%")
            return quote
            
        except Exception as e:
            logger.error(f"[API错误] 获取 ETF {stock_code} 实时行情失败: {e}")
            return None
    
    def _get_hk_realtime_quote(self, stock_code: str) -> Optional[RealtimeQuote]:
        """
        获取港股实时行情数据
        
        数据来源：ak.stock_hk_spot_em()
        包含：最新价、涨跌幅、成交量、成交额等
        
        Args:
            stock_code: 港股代码
            
        Returns:
            RealtimeQuote 对象，获取失败返回 None
        """
        import akshare as ak
        
        try:
            # 防封禁策略
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            # 确保代码格式正确（5位数字）
            code = stock_code.lower().replace('hk', '').zfill(5)
            
            logger.info(f"[API调用] ak.stock_hk_spot_em() 获取港股实时行情...")
            import time as _time
            api_start = _time.time()
            
            df = ak.stock_hk_spot_em()
            
            api_elapsed = _time.time() - api_start
            logger.info(f"[API返回] ak.stock_hk_spot_em 成功: 返回 {len(df)} 只港股, 耗时 {api_elapsed:.2f}s")
            
            # 查找指定港股
            row = df[df['代码'] == code]
            if row.empty:
                logger.warning(f"[API返回] 未找到港股 {code} 的实时行情")
                return None
            
            row = row.iloc[0]
            
            # 安全获取字段值
            def safe_float(val, default=0.0):
                try:
                    if pd.isna(val):
                        return default
                    return float(val)
                except:
                    return default
            
            # 港股行情数据构建
            quote = RealtimeQuote(
                code=stock_code,
                name=str(row.get('名称', '')),
                price=safe_float(row.get('最新价')),
                change_pct=safe_float(row.get('涨跌幅')),
                change_amount=safe_float(row.get('涨跌额')),
                volume_ratio=safe_float(row.get('量比', 0)),  # 港股可能无量比
                turnover_rate=safe_float(row.get('换手率', 0)),
                amplitude=safe_float(row.get('振幅', 0)),
                pe_ratio=safe_float(row.get('市盈率', 0)),  # 港股可能有市盈率
                pb_ratio=safe_float(row.get('市净率', 0)),  # 港股可能有市净率
                total_mv=safe_float(row.get('总市值', 0)),
                circ_mv=safe_float(row.get('流通市值', 0)),
                change_60d=0.0,  # 港股接口可能不提供
                high_52w=safe_float(row.get('52周最高', 0)),
                low_52w=safe_float(row.get('52周最低', 0)),
            )
            
            logger.info(f"[港股实时行情] {stock_code} {quote.name}: 价格={quote.price}, 涨跌={quote.change_pct}%, "
                       f"换手率={quote.turnover_rate}%")
            return quote
            
        except Exception as e:
            logger.error(f"[API错误] 获取港股 {stock_code} 实时行情失败: {e}")
            return None
    
    def get_chip_distribution(self, stock_code: str) -> Optional[ChipDistribution]:
        """
        获取筹码分布数据
        
        数据来源：ak.stock_cyq_em()
        包含：获利比例、平均成本、筹码集中度
        
        注意：ETF/指数没有筹码分布数据，会直接返回 None
        
        Args:
            stock_code: 股票代码
            
        Returns:
            ChipDistribution 对象（最新一天的数据），获取失败返回 None
        """
        import akshare as ak
        
        # ETF/指数没有筹码分布数据
        if _is_etf_code(stock_code):
            logger.debug(f"[API跳过] {stock_code} 是 ETF/指数，无筹码分布数据")
            return None
        
        try:
            # 防封禁策略
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            logger.info(f"[API调用] ak.stock_cyq_em(symbol={stock_code}) 获取筹码分布...")
            import time as _time
            api_start = _time.time()
            
            df = ak.stock_cyq_em(symbol=stock_code)
            
            api_elapsed = _time.time() - api_start
            
            if df.empty:
                logger.warning(f"[API返回] ak.stock_cyq_em 返回空数据, 耗时 {api_elapsed:.2f}s")
                return None
            
            logger.info(f"[API返回] ak.stock_cyq_em 成功: 返回 {len(df)} 天数据, 耗时 {api_elapsed:.2f}s")
            logger.debug(f"[API返回] 筹码数据列名: {list(df.columns)}")
            
            # 取最新一天的数据
            latest = df.iloc[-1]
            
            def safe_float(val, default=0.0):
                try:
                    if pd.isna(val):
                        return default
                    return float(val)
                except:
                    return default
            
            chip = ChipDistribution(
                code=stock_code,
                date=str(latest.get('日期', '')),
                profit_ratio=safe_float(latest.get('获利比例')),
                avg_cost=safe_float(latest.get('平均成本')),
                cost_90_low=safe_float(latest.get('90成本-低')),
                cost_90_high=safe_float(latest.get('90成本-高')),
                concentration_90=safe_float(latest.get('90集中度')),
                cost_70_low=safe_float(latest.get('70成本-低')),
                cost_70_high=safe_float(latest.get('70成本-高')),
                concentration_70=safe_float(latest.get('70集中度')),
            )
            
            logger.info(f"[筹码分布] {stock_code} 日期={chip.date}: 获利比例={chip.profit_ratio:.1%}, "
                       f"平均成本={chip.avg_cost}, 90%集中度={chip.concentration_90:.2%}, "
                       f"70%集中度={chip.concentration_70:.2%}")
            return chip
            
        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 筹码分布失败: {e}")
            return None

    def stock_research_report_em(self, stock_code: str) -> Union[DataFrame, None]:
        """
        获取股票研究报告数据

        数据来源：ak.stock_research_report_em()
        包含：报告标题、报告内容、报告日期
        """
        import akshare as ak

        try:
            # 防封禁策略
            self._set_random_user_agent()
            self._enforce_rate_limit()

            logger.info(f"[API调用] ak.stock_research_report_em(symbol={stock_code}) 获取研究报告...")
            import time as _time
            api_start = _time.time()

            df = ak.stock_research_report_em(symbol=stock_code)

            api_elapsed = _time.time() - api_start

            if df.empty:
                logger.warning(f"[API返回] ak.stock_research_report_em 返回空数据, 耗时 {api_elapsed:.2f}s")
                return None

            # 如果启用了PDF链接分解功能
            if '报告PDF链接' in df.columns:
                df['pdf_name'] = df['报告PDF链接'].apply(extract_last_segment_standard)
                logger.info(f"[数据处理] 已提取PDF链接的最后一段内容，新增'PDF文件名'列")

            logger.info(f"[API返回] ak.stock_research_report_em 成功: 返回 {len(df)} 条数据, 耗时 {api_elapsed:.2f}s")
            df = self._normalize_research_report_data(df, stock_code)

            return df

        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 研究报告失败: {e}")
            return None

    def _normalize_research_report_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化股票研究报告数据

        数据来源：ak.stock_research_report_em()
        包含：报告标题、报告内容、报告日期

        Args:
            df: 输入数据
            stock_code: 股票代码

        Returns:
            DataFrame 包含标准化后的数据
        """
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        column_mapping = {
            '日期': 'date',
            '股票代码': 'code',
            '股票简介': 'stock_intro',
            '报告名称': 'report_name',
            '东财评级': 'east_rating',
            '机构': 'rating_agency',
            '报告PDF链接': 'report_pdf_link',
            '行业': 'industry',
            '近一个月个股研报数':  'month_research_count',
        }

        # 获取所有列名
        columns = df.columns.tolist()
        # 查找包含"每股收益"的字段
        index_share = 0
        index_ratio = 0
        for col in columns:
            if '盈利预测-收益' in col:
                # 尝试提取年份
                import re
                index_share += 1
                year_match = re.search(r'(\d{4})', col)
                df[f'forecasting_earning_per_share{index_share}'] = df[col]
                if year_match:
                    year = year_match.group(1)
                    df[f'share_year{index_share}'] = year
                else:
                    logger.warning(f"[数据处理] 未找到年份信息，将使用默认值 '2023'")
            if '盈利预测-市盈率' in col:
                # 尝试提取年份
                import re
                index_ratio += 1
                df[f'Predicted_price_earnings_ratio{index_ratio}'] = df[col]
                year_match = re.search(r'(\d{4})', col)
                if year_match:
                    year = year_match.group(1)
                    df[f'ratio_year{index_ratio}'] = year
                else:
                    logger.warning(f"[数据处理] 未找到年份信息，将使用默认值 '2023'")

        df = df.rename(columns=column_mapping)

        return df

    def get_enhanced_data(self, stock_code: str, days: int = 60) -> Dict[str, Any]:
        """
        获取增强数据（历史K线 + 实时行情 + 筹码分布）
        
        Args:
            stock_code: 股票代码
            days: 历史数据天数
            
        Returns:
            包含所有数据的字典
        """
        result = {
            'code': stock_code,
            'daily_data': None,
            'realtime_quote': None,
            'chip_distribution': None,
        }
        
        # 获取日线数据
        try:
            df = self.get_daily_data(stock_code, days=days)
            result['daily_data'] = df
        except Exception as e:
            logger.error(f"获取 {stock_code} 日线数据失败: {e}")
        
        # 获取实时行情
        result['realtime_quote'] = self.get_realtime_quote(stock_code)
        
        # 获取筹码分布
        result['chip_distribution'] = self.get_chip_distribution(stock_code)
        
        return result

    def new_energy_penetration(self) -> pd.DataFrame:
        """
        获取新能源车月度销量及渗透率数据（行业宏观数据）
        数据来源：Akshare 乘联会 CPCA 数据
        返回:
            DataFrame: 包含月份、总销量、新能源车销量、渗透率等字段
        """
        try:
            import akshare as ak
            self._enforce_rate_limit()
            logger.info("获取新能源车渗透率数据")

            try:
                total_df = ak.car_market_total_cpca()
                fuel_df = ak.car_market_fuel_cpca()

                if total_df.empty or fuel_df.empty:
                    logger.warning("未获取到汽车销量数据")
                    return pd.DataFrame()

                result_rows = []
                for year_col in [col for col in total_df.columns if '年' in col]:
                    year = year_col.replace('年', '')
                    for _, row in total_df.iterrows():
                        month_str = str(row['月份'])
                        month_num = int(month_str.replace('月', ''))
                        month_date = f"{year}-{month_num:02d}"

                        total_val = row.get(year_col)
                        fuel_row = fuel_df[fuel_df['月份'] == row['月份']]
                        fuel_val = fuel_row.iloc[0][year_col] if not fuel_row.empty else None

                        if total_val is not None and fuel_val is not None and pd.notna(total_val) and pd.notna(fuel_val):
                            new_energy = total_val - fuel_val
                            penetration = (new_energy / total_val * 100) if total_val > 0 else None
                            result_rows.append({
                                'month': month_date,
                                'total_sales': float(total_val) * 10000,
                                'new_energy_sales': float(new_energy) * 10000,
                                'penetration_rate': float(penetration) if penetration else None,
                            })

                if not result_rows:
                    logger.warning("未获取到有效新能源车渗透率数据")
                    return pd.DataFrame()

                result_df = pd.DataFrame(result_rows)
                result_df['month'] = pd.to_datetime(result_df['month'])
                result_df = result_df.sort_values('month', ascending=False).reset_index(drop=True)

                logger.info(f"获取新能源车渗透率数据成功，共 {len(result_df)} 条记录")
                return result_df
            except Exception as e:
                logger.warning(f"乘联会数据获取失败，尝试其他接口: {e}")

            try:
                df = ak.energy_car_sales_yearly_em()
                if df is not None and not df.empty:
                    return df
            except Exception:
                pass

            try:
                df = ak.new_energy_vehicle_sales_rank()
                if df is not None and not df.empty:
                    return df
            except Exception:
                pass

            logger.warning("未获取到新能源车渗透率数据")
            return pd.DataFrame()
        except ImportError:
            logger.error("akshare 未安装，无法获取新能源车渗透率数据")
            raise DataSourceUnavailableError("akshare 未安装")
        except Exception as e:
            logger.error(f"获取新能源车渗透率数据失败: {e}")
            raise DataFetchError(f"akshare 获取新能源车渗透率失败: {e}") from e

    # ===== 懂车帝 API - 车型级月销量数据 =====
    # 数据来源：懂车帝（dongchedi.com）全国车型销量排行榜
    # 说明：此接口非 Akshare，是本模块直接调用的 HTTP API
    # 可以获取到指定月份全国各车型的销量数据，精确到车型级别

    def get_vehicle_sales(self, month: str = None) -> pd.DataFrame:
        """
        通过懂车帝API获取全国车型月销量排行数据
        Args:
            month: 月份，格式 YYYY-MM，默认取最近完整月份
        Returns:
            DataFrame: 车型销量数据（车型名、品牌、月销量、指导价等）
        """
        from datetime import date, timedelta
        from calendar import monthrange

        try:
            import requests
        except ImportError:
            logger.error("requests 未安装")
            raise DataSourceUnavailableError("requests 未安装")

        self._enforce_rate_limit()

        # 默认取上个月
        if not month:
            today = date.today()
            first_of_month = today.replace(day=1)
            last_month = first_of_month - timedelta(days=1)
            month = last_month.strftime("%Y-%m")
        logger.info(f"get_vehicle_sales(month={month})")

        try:
            session = requests.Session()
            session.headers.update({
                "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/120.0.0.0 Safari/537.36"),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Referer": "https://www.dongchedi.com/sales/rank",
            })

            # 先访问首页种 Cookie
            session.get("https://www.dongchedi.com/sales/rank", timeout=10)

            all_models = []
            offset = 0
            while True:
                params = {
                    "city_name": "全国",
                    "rank_data_type": "2",
                    "count": "50",
                    "offset": str(offset),
                    "month": month.replace('-', ''),
                    "aid": "1839",
                    "app_name": "auto_web_pc",
                }
                resp = session.get(
                    "https://www.dongchedi.com/motor/pc/car/rank_data",
                    params=params, timeout=15
                )
                data = resp.json()
                lst = data.get('data', {}).get('list', [])
                if not lst:
                    break
                all_models.extend(lst)
                has_more = data.get('data', {}).get('paging', {}).get('has_more', False)
                if not has_more:
                    break
                offset += 50

            if not all_models:
                logger.warning(f"未获取到 {month} 月份车型销量数据")
                return pd.DataFrame()

            rows = []
            for item in all_models:
                rows.append({
                    'month': month,
                    'series_name': item.get('series_name', ''),
                    'brand_name': item.get('brand_name', ''),
                    'sales_volume': item.get('count', 0) or 0,
                    'min_price': item.get('min_price'),
                    'max_price': item.get('max_price'),
                    'price_range': item.get('price', ''),
                    'rank': item.get('rank'),
                    'series_id': item.get('series_id'),
                })

            result_df = pd.DataFrame(rows)
            logger.info(f"获取 {month} 车型销量数据成功，共 {len(result_df)} 条")
            return result_df

        except Exception as e:
            error_msg = str(e)
            logger.error(f"获取懂车帝车型销量数据失败: {e}")
            raise DataFetchError(f"懂车帝车型销量获取失败: {error_msg}") from e


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = AkshareFetcher()
    
    # 测试普通股票
    print("=" * 50)
    print("测试普通股票数据获取")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('600519')  # 茅台
        print(f"[股票] 获取成功，共 {len(df)} 条数据")
        print(df.tail())
    except Exception as e:
        print(f"[股票] 获取失败: {e}")
    
    # 测试 ETF 基金
    print("\n" + "=" * 50)
    print("测试 ETF 基金数据获取")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('512400')  # 有色龙头ETF
        print(f"[ETF] 获取成功，共 {len(df)} 条数据")
        print(df.tail())
    except Exception as e:
        print(f"[ETF] 获取失败: {e}")
    
    # 测试 ETF 实时行情
    print("\n" + "=" * 50)
    print("测试 ETF 实时行情获取")
    print("=" * 50)
    try:
        quote = fetcher.get_realtime_quote('512880')  # 证券ETF
        if quote:
            print(f"[ETF实时] {quote.name}: 价格={quote.price}, 涨跌幅={quote.change_pct}%")
        else:
            print("[ETF实时] 未获取到数据")
    except Exception as e:
        print(f"[ETF实时] 获取失败: {e}")
    
    # 测试港股历史数据
    print("\n" + "=" * 50)
    print("测试港股历史数据获取")
    print("=" * 50)
    try:
        df = fetcher.get_daily_data('00700')  # 腾讯控股
        print(f"[港股] 获取成功，共 {len(df)} 条数据")
        print(df.tail())
    except Exception as e:
        print(f"[港股] 获取失败: {e}")
    
    # 测试港股实时行情
    print("\n" + "=" * 50)
    print("测试港股实时行情获取")
    print("=" * 50)
    try:
        quote = fetcher.get_realtime_quote('00700')  # 腾讯控股
        if quote:
            print(f"[港股实时] {quote.name}: 价格={quote.price}, 涨跌幅={quote.change_pct}%")
        else:
            print("[港股实时] 未获取到数据")
    except Exception as e:
        print(f"[港股实时] 获取失败: {e}")
