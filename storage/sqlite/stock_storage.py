# -*- coding: utf-8 -*-
"""
数据库管理
"""
import os
import io
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Mapping
from utils.common import parse_row_date

from PIL.PdfParser import pdf_repr
from pandas.core.computation.expressions import where
from sqlalchemy import create_engine, Column, Integer, String, Text, JSON

import pandas as pd
import requests


from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Date,
    DateTime,
    Integer,
    Index,
    UniqueConstraint,
    select,
    and_,
    desc,
)
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)

from utils.config import get_db_config
from utils.logger import logger


# SQLAlchemy ORM 基类
Base = declarative_base()


# === 数据模型定义 ===

class StockDaily(Base):
    """
    股票日线数据模型 - ORM映射类

    数据库表: stock_daily
    功能：映射数据库表到Python对象，存储每日行情数据和技术指标

    设计原则：
    1. 完整性：包含股票分析所需的全部核心数据
    2. 唯一性：同一股票同一日期只能有一条记录
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：建立复合索引优化查询

    字段分类说明：
    • 标识字段：id, code, date - 唯一标识一条记录
    • 价格数据：open, high, low, close - OHLC价格数据
    • 成交数据：volume, amount, pct_chg - 市场活跃度指标
    • 技术指标：ma5-ma200, volume_ratio - 趋势和量能分析
    • 元数据：data_source, created_at, updated_at - 数据审计

    技术指标解释：
    • MA5/MA10/MA20: 短期趋势判断（5/10/20日移动平均线）
    • MA50/MA120/MA200: 中长期趋势判断（50/120/200日移动平均线）
    • volume_ratio: 量比，当日成交量/5日平均成交量，反映市场活跃度

    索引设计：
    • code字段单独索引：快速按股票代码查询
    • date字段单独索引：快速按日期查询
    • (code, date)复合索引：优化按股票和日期组合查询
    • (code, date)唯一约束：确保数据唯一性
    """
    __tablename__ = 'stock_daily'

    # ===== 标识字段 =====
    # 主键：自增整数，用于数据库内部标识
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码：A股6位数字代码，如600519(茅台)、000001(平安银行)
    # 建立索引优化按代码查询的性能
    code = Column(String(10), nullable=False, index=True)

    # 交易日期：格式YYYY-MM-DD，建立索引优化按日期查询
    date = Column(Date, nullable=False, index=True)

    # ===== 价格数据 (OHLC) =====
    # 开盘价：交易日开始时的第一笔成交价格
    open = Column(Float)
    # 最高价：交易日内的最高成交价格
    high = Column(Float)
    # 最低价：交易日内的最低成交价格
    low = Column(Float)
    # 收盘价：交易日结束时的最后一笔成交价格，最重要的价格指标
    close = Column(Float)

    # ===== 成交数据 =====
    # 成交量：当日成交的股票数量（单位：股），反映市场活跃度
    volume = Column(Float)
    # 成交额：当日成交的总金额（单位：元），成交量 × 平均价格
    amount = Column(Float)
    # 涨跌幅：当日收盘价相对于前一日收盘价的变化百分比
    # 正数表示上涨，负数表示下跌
    pct_chg = Column(Float)

    # ===== 技术指标 =====
    # 移动平均线 (Moving Average) - 不同周期的趋势指标
    ma5 = Column(Float)  # 5日移动平均线：短期趋势
    ma10 = Column(Float)  # 10日移动平均线：短期趋势
    ma20 = Column(Float)  # 20日移动平均线：中期趋势
    ma50 = Column(Float)  # 50日移动平均线：中期趋势
    ma120 = Column(Float)  # 120日移动平均线：长期趋势
    ma200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）
    ema5 = Column(Float)  # 5日移动平均线：短期趋势
    ema10 = Column(Float)  # 10日移动平均线：短期趋势
    ema20 = Column(Float)  # 20日移动平均线：中期趋势
    ema50 = Column(Float)  # 50日移动平均线：中期趋势
    ema120 = Column(Float)  # 120日移动平均线：长期趋势
    ema200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）

    # 量比：当日成交量与过去5日平均成交量的比值
    # >1.0: 放量，市场活跃； <1.0: 缩量，市场冷清
    volume_ratio = Column(Float)

    # ===== 元数据 =====
    # 数据来源：记录数据是从哪个数据源获取的
    # 示例值："AkshareFetcher"、"TushareFetcher"
    data_source = Column(String(50))

    # 创建时间：记录首次插入数据库的时间（自动设置）
    created_at = Column(DateTime, default=datetime.now)
    # 更新时间：记录最后一次修改的时间（自动更新）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date'),
    )

    def __repr__(self):
        """
        对象字符串表示，用于调试和日志输出

        示例：<StockDaily(code=600519, date=2026-01-15, close=1820.0)>
        """
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"

    def to_dict(self) -> Dict[str, Any]:
        """
        将数据库记录转换为字典格式

        使用场景：
        1. 将数据传递给其他模块（如AI分析器）
        2. JSON序列化，用于API响应
        3. 数据导出和备份

        Returns:
            Dict[str, Any]: 包含所有字段的字典
        """
        return {
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma50': self.ma50,
            'ma120': self.ma120,
            'ma200': self.ma200,
            'ema5': self.ema5,
            'ema10': self.ema10,
            'ema20': self.ema20,
            'ema50': self.ema50,
            'ema120': self.ema120,
            'ema200': self.ema200,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }

class StockWeekly(Base):
    """
    股票周线数据模型 - ORM映射类

    数据库表: stock_weekly
    功能：映射数据库表到Python对象，存储每日行情数据和技术指标

    设计原则：
    1. 完整性：包含股票分析所需的全部核心数据
    2. 唯一性：同一股票同一日期只能有一条记录
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：建立复合索引优化查询

    字段分类说明：
    • 标识字段：id, code, trade_date - 唯一标识一条记录
    • 价格数据：open, high, low, close - OHLC价格数据
    • 成交数据：volume, amount, pct_chg - 市场活跃度指标
    • 技术指标：ma5-ma200, volume_ratio - 趋势和量能分析
    • 元数据：data_source, created_at, updated_at - 数据审计

    技术指标解释：
    • MA5/MA10/MA20: 短期趋势判断（5/10/20日移动平均线）
    • MA50/MA120/MA200: 中长期趋势判断（50/120/200日移动平均线）
    • volume_ratio: 量比，当日成交量/5日平均成交量，反映市场活跃度

    索引设计：
    • code字段单独索引：快速按股票代码查询
    • date字段单独索引：快速按日期查询
    • (code, date)复合索引：优化按股票和日期组合查询
    • (code, date)唯一约束：确保数据唯一性
    """
    __tablename__ = 'stock_weekly'

    # ===== 标识字段 =====
    # 主键：自增整数，用于数据库内部标识
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码：A股6位数字代码，如600519(茅台)、000001(平安银行)
    # 建立索引优化按代码查询的性能
    code = Column(String(10), nullable=False, index=True)

    # 交易日期：格式YYYY-MM-DD，建立索引优化按日期查询
    date = Column(Date, nullable=False, index=True)
    # 计算截止日期
    end_date = Column(Date, nullable=False, index=True)

    # ===== 价格数据 (OHLC) =====
    # 开盘价：交易日开始时的第一笔成交价格
    open = Column(Float)
    # 最高价：交易日内的最高成交价格
    high = Column(Float)
    # 最低价：交易日内的最低成交价格
    low = Column(Float)
    # 收盘价：交易日结束时的最后一笔成交价格，最重要的价格指标
    close = Column(Float)

    # ===== 成交数据 =====
    # 成交量：当日成交的股票数量（单位：股），反映市场活跃度
    volume = Column(Float)
    # 成交额：当日成交的总金额（单位：元），成交量 × 平均价格
    amount = Column(Float)
    # 涨跌幅：当日收盘价相对于前一日收盘价的变化百分比
    # 正数表示上涨，负数表示下跌
    pct_chg = Column(Float)
    # 涨跌额
    change = Column(Float)

    # ===== 技术指标 =====
    # 移动平均线 (Moving Average) - 不同周期的趋势指标
    ma5 = Column(Float)  # 5日移动平均线：短期趋势
    ma10 = Column(Float)  # 10日移动平均线：短期趋势
    ma20 = Column(Float)  # 20日移动平均线：中期趋势
    ma50 = Column(Float)  # 50日移动平均线：中期趋势
    ma120 = Column(Float)  # 120日移动平均线：长期趋势
    ma200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）
    ema5 = Column(Float)  # 5日移动平均线：短期趋势
    ema10 = Column(Float)  # 10日移动平均线：短期趋势
    ema20 = Column(Float)  # 20日移动平均线：中期趋势
    ema50 = Column(Float)  # 50日移动平均线：中期趋势
    ema120 = Column(Float)  # 120日移动平均线：长期趋势
    ema200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）

    # 量比：当日成交量与过去5日平均成交量的比值
    # >1.0: 放量，市场活跃； <1.0: 缩量，市场冷清
    volume_ratio = Column(Float)

    # ===== 元数据 =====
    # 数据来源：记录数据是从哪个数据源获取的
    # 示例值："AkshareFetcher"、"TushareFetcher"
    data_source = Column(String(50))

    # 创建时间：记录首次插入数据库的时间（自动设置）
    created_at = Column(DateTime, default=datetime.now)
    # 更新时间：记录最后一次修改的时间（自动更新）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_week_code_date'),
        Index('ix_week_code_date_end', 'code', 'date', 'end_date'),
    )

    def __repr__(self):
        """
        对象字符串表示，用于调试和日志输出

        示例：<StockDaily(code=600519, date=2026-01-15, close=1820.0)>
        """
        return f"<StockWeekly(code={self.code}, date={self.date}, close={self.close})>"

    def to_dict(self) -> Dict[str, Any]:
        """
        将数据库记录转换为字典格式

        使用场景：
        1. 将数据传递给其他模块（如AI分析器）
        2. JSON序列化，用于API响应
        3. 数据导出和备份

        Returns:
            Dict[str, Any]: 包含所有字段的字典
        """
        return {
            'code': self.code,
            'date': self.date,
            'end_date': self.end_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'change': self.change,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma50': self.ma50,
            'ma120': self.ma120,
            'ma200': self.ma200,
            'ema5': self.ema5,
            'ema10': self.ema10,
            'ema20': self.ema20,
            'ema50': self.ema50,
            'ema120': self.ema120,
            'ema200': self.ema200,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }

class StockMonth(Base):
    """
    股票月线数据模型 - ORM映射类

    数据库表: stock_month
    功能：映射数据库表到Python对象，存储每日行情数据和技术指标

    设计原则：
    1. 完整性：包含股票分析所需的全部核心数据
    2. 唯一性：同一股票同一日期只能有一条记录
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：建立复合索引优化查询

    字段分类说明：
    • 标识字段：id, code, trade_date - 唯一标识一条记录
    • 价格数据：open, high, low, close - OHLC价格数据
    • 成交数据：volume, amount, pct_chg - 市场活跃度指标
    • 技术指标：ma5-ma200, volume_ratio - 趋势和量能分析
    • 元数据：data_source, created_at, updated_at - 数据审计

    技术指标解释：
    • MA5/MA10/MA20: 短期趋势判断（5/10/20日移动平均线）
    • MA50/MA120/MA200: 中长期趋势判断（50/120/200日移动平均线）
    • volume_ratio: 量比，当日成交量/5日平均成交量，反映市场活跃度

    索引设计：
    • code字段单独索引：快速按股票代码查询
    • date字段单独索引：快速按日期查询
    • (code, date)复合索引：优化按股票和日期组合查询
    • (code, date)唯一约束：确保数据唯一性
    """
    __tablename__ = 'stock_month'

    # ===== 标识字段 =====
    # 主键：自增整数，用于数据库内部标识
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码：A股6位数字代码，如600519(茅台)、000001(平安银行)
    # 建立索引优化按代码查询的性能
    code = Column(String(10), nullable=False, index=True)

    # 交易日期：格式YYYY-MM-DD，建立索引优化按日期查询
    date = Column(Date, nullable=False, index=True)
    # 计算截止日期
    end_date = Column(Date, nullable=False, index=True)

    # ===== 价格数据 (OHLC) =====
    # 开盘价：交易日开始时的第一笔成交价格
    open = Column(Float)
    # 最高价：交易日内的最高成交价格
    high = Column(Float)
    # 最低价：交易日内的最低成交价格
    low = Column(Float)
    # 收盘价：交易日结束时的最后一笔成交价格，最重要的价格指标
    close = Column(Float)

    # ===== 成交数据 =====
    # 成交量：当日成交的股票数量（单位：股），反映市场活跃度
    volume = Column(Float)
    # 成交额：当日成交的总金额（单位：元），成交量 × 平均价格
    amount = Column(Float)
    # 涨跌幅：当日收盘价相对于前一日收盘价的变化百分比
    # 正数表示上涨，负数表示下跌
    pct_chg = Column(Float)
    # 涨跌额
    change = Column(Float)

    # ===== 技术指标 =====
    # 移动平均线 (Moving Average) - 不同周期的趋势指标
    ma5 = Column(Float)  # 5日移动平均线：短期趋势
    ma10 = Column(Float)  # 10日移动平均线：短期趋势
    ma20 = Column(Float)  # 20日移动平均线：中期趋势
    ma50 = Column(Float)  # 50日移动平均线：中期趋势
    ma120 = Column(Float)  # 120日移动平均线：长期趋势
    ma200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）
    ema5 = Column(Float)  # 5日移动平均线：短期趋势
    ema10 = Column(Float)  # 10日移动平均线：短期趋势
    ema20 = Column(Float)  # 20日移动平均线：中期趋势
    ema50 = Column(Float)  # 50日移动平均线：中期趋势
    ema120 = Column(Float)  # 120日移动平均线：长期趋势
    ema200 = Column(Float)  # 200日移动平均线：长期趋势（牛熊分界线）

    # 量比：当日成交量与过去5日平均成交量的比值
    # >1.0: 放量，市场活跃； <1.0: 缩量，市场冷清
    volume_ratio = Column(Float)

    # ===== 元数据 =====
    # 数据来源：记录数据是从哪个数据源获取的
    # 示例值："AkshareFetcher"、"TushareFetcher"
    data_source = Column(String(50))

    # 创建时间：记录首次插入数据库的时间（自动设置）
    created_at = Column(DateTime, default=datetime.now)
    # 更新时间：记录最后一次修改的时间（自动更新）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_month_code_date'),
        Index('ix_month_code_date', 'code', 'date', 'end_date'),
    )

    def __repr__(self):
        """
        对象字符串表示，用于调试和日志输出

        示例：<StockMonth(code=600519, date=2026-01-15, close=1820.0)>
        """
        return f"<StockMonth(code={self.code}, date={self.date}, close={self.close})>"

    def to_dict(self) -> Dict[str, Any]:
        """
        将数据库记录转换为字典格式

        使用场景：
        1. 将数据传递给其他模块（如AI分析器）
        2. JSON序列化，用于API响应
        3. 数据导出和备份

        Returns:
            Dict[str, Any]: 包含所有字段的字典
        """
        return {
            'code': self.code,
            'date': self.date,
            'end_date': self.end_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'change': self.change,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'ma50': self.ma50,
            'ma120': self.ma120,
            'ma200': self.ma200,
            'ema5': self.ema5,
            'ema10': self.ema10,
            'ema20': self.ema20,
            'ema50': self.ema50,
            'ema120': self.ema120,
            'ema200': self.ema200,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }

# === 新增表1：股票基本信息表 ===
class StockBasic(Base):
    """
    股票基本信息模型 - ORM映射类

    数据库表: stock_basic
    功能：存储股票基础属性（非行情类静态/低频更新数据）

    设计原则：
    1. 完整性：包含股票分析所需的核心基本信息
    2. 唯一性：股票代码唯一标识一条记录
    3. 可追溯：记录更新时间，便于数据审计
    4. 高性能：code字段索引优化查询

    字段说明：
    • 核心标识：code（股票代码，唯一）
    • 基础信息：name（股票名称）、industry（所属行业）、area（所属地域）
    • 上市信息：list_date（上市日期）、market（市场类型：沪A/深A/创业板等）
    • 财务简讯：total_share（总股本）、circulating_share（流通股本）
    • 元数据：updated_at（最后更新时间）
    """
    __tablename__ = 'stock_basic'

    # 字段定义
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, unique=True, index=True)  # 股票代码（唯一）
    name = Column(String(50), nullable=False)  # 股票名称（如：贵州茅台）
    industry = Column(String(50))  # 所属行业（如：白酒、半导体）
    list_date = Column(Date)  # 上市日期（YYYY-MM-DD）
    market = Column(String(10))  # 市场类型（沪A/深A/创业板/科创板）
    list_status = Column(String(10))  # 上市状态，L上市，D退市，G过会未交易，P暂停上市
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)  # 最后更新时间
    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', name='uix_base_code'),
        Index('ix_base_industry', 'industry'),
    )


    def __repr__(self):
        return f"<StockBasic(code={self.code}, name={self.name}, industry={self.industry})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'name': self.name,
            'industry': self.industry,
            'list_date': self.list_date,
            'market': self.market,
            'updated_at': self.updated_at,
            'list_status': self.list_status,
        }

class StockDailyBasic(Base):
    """股票每日指标数据"""
    __tablename__ = 'stock_daily_basic'

    # ===== 标识字段 =====
    # 主键：自增整数，用于数据库内部标识
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码：A股6位数字代码，如600519(茅台)、000001(平安银行)
    # 建立索引优化按代码查询的性能
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    close = Column(Float, nullable=True)
    turnover_rate = Column(Float, nullable=True) # 换手率
    turnover_rate_f = Column(Float, nullable=True)  # 换手率（自由流通股）
    volume_ratio =  Column(Float, nullable=True)   # 量比
    pe = Column(Float, nullable=True)   # 市盈率
    pe_ttm = Column(Float, nullable=True)  # 静态市盈率
    pb = Column(Float, nullable=True)  # 市净率
    ps = Column(Float, nullable=True)  # 市销率
    ps_ttm = Column(Float, nullable=True)  #
    dv_ratio = Column(Float, nullable=True)  # 股息率
    dv_ttm = Column(Float, nullable=True)  # ttm
    total_share = Column(Float, nullable=True)  # 总股本（万股）
    float_share = Column(Float, nullable=True)  # 流通股本
    free_share = Column(Float, nullable=True)  # 自由流通股本
    total_mv = Column(Float, nullable=True)  # 总市值（万元）
    circ_mv = Column(Float, nullable=True)  # 流通市值
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)  # 最后更新时间
    # ===== 数据库约束和索引 =====
    # 唯一约束：确保同一股票同一日期只有一条记录，防止数据重复
    # 复合索引：优化按股票代码和日期组合查询的性能
    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_stock_daily_basic'),
        Index('idx_daily_basic_code_date', 'code', 'trade_date'),
    )

    def __repr__(self):
        return f"<StockDailyBasic(code={self.code}, trade_date={self.trade_date})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'trade_date': self.trade_date,
            'close': self.close,
            'turnover_rate': self.turnover_rate,
            'turnover_rate_f': self.turnover_rate_f,
            'volume_ratio': self.volume_ratio,
            'pe': self.pe,
            'pe_ttm': self.pe_ttm,
            'pb': self.pb,
            'ps': self.ps,
            'ps_ttm': self.ps_ttm,
            'dv_ratio': self.dv_ratio,
            'dv_ttm': self.dv_ttm,
            'total_share': self.total_share,
            'float_share': self.float_share,
            'total_mv': self.total_mv,
            'circ_mv': self.circ_mv,
        }

# === 当天预测的数据
class DailyForecast(Base):
    __tablename__ = 'daily_forecast'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, unique=True, index=True)
    forecast_date = Column(Date, nullable=False, unique=True, index=True)
    forecast_rue = Column(String, nullable=False)
    practice_rue = Column(String, nullable=False)
    forecast_model = Column(String, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # 复合约束与索引（核心：股票+日期唯一）
    __table_args__ = (
        UniqueConstraint('code', 'forecast_date', name='uix_daily_forecast_code_date'),
        Index('ix_daily_forecast_code_date', 'code', 'forecast_date'),
    )

    def __repr__(self):
        return f"<DailyForecast(code={self.code}, forecast_date={self.forecast_date})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'forecast_date': self.forecast_date,
            'forecast_rue': self.forecast_rue,
            'practice_rue': self.practice_rue,
            'forecast_model': self.forecast_model,
        }

# === 新增表2：股票资金流向表 ===
class StockMoneyFlow(Base):
    """
    股票资金流向模型 - ORM映射类

    数据库表: stock_money_flow
    功能：存储每日资金流向数据（主力/散户/北向资金等）

    设计原则：
    1. 完整性：包含资金分析核心维度
    2. 唯一性：(code, date)复合唯一约束
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：(code, date)复合索引优化查询

    字段说明：
    • 标识字段：code（股票代码）、date（交易日期）
    • 资金数据：main_inflow（主力净流入）、retail_inflow（散户净流入）、north_inflow（北向资金净流入）
    • 占比数据：main_ratio（主力资金占比）、retail_ratio（散户资金占比）
    • 元数据：data_source（数据来源）、updated_at（更新时间）
    """
    __tablename__ = 'stock_money_flow'

    # 字段定义
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)  # 股票代码
    date = Column(Date, nullable=False, index=True)  # 交易日期
    main_inflow = Column(Float)  # 主力资金净流入（万元）
    retail_inflow = Column(Float)  # 散户资金净流入（万元）
    north_inflow = Column(Float)  # 北向资金净流入（万元）
    main_ratio = Column(Float)  # 主力资金占比（%）
    retail_ratio = Column(Float)  # 散户资金占比（%）
    data_source = Column(String(50))  # 数据来源（如：EastMoneyFetcher）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 复合约束与索引（核心：股票+日期唯一）
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_money_flow_code_date'),
        Index('ix_money_flow_code_date', 'code', 'date'),
    )

    def __repr__(self):
        return f"<StockMoneyFlow(code={self.code}, date={self.date}, main_inflow={self.main_inflow})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'date': self.date,
            'main_inflow': self.main_inflow,
            'retail_inflow': self.retail_inflow,
            'north_inflow': self.north_inflow,
            'main_ratio': self.main_ratio,
            'retail_ratio': self.retail_ratio,
            'data_source': self.data_source
        }

class StockResearchReport(Base):
    """
    股票调研报告模型 - ORM映射类

    数据库表: stock_research_report
    功能：存储股票调研报告数据

    设计原则：
    1. 完整性：包含调研报告核心维度
    2. 唯一性：(code, date, pdf_name)复合唯一约束
    3. 可追溯：记录数据来源和更新时间
    4. 高性能：(code, date)复合索引优化查询

    字段说明：
    • 标识字段：code（股票代码）、date（调研日期）
    • 调研数据：title（调研标题）、content（调研内容）
    """
    __tablename__ = 'stock_research_report'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)  # 日期
    pdf_name = Column(String(100), nullable=False, index=True)  # PDF文件名
    report_name = Column(String(200), nullable=False)  # 报告名称
    east_rating = Column(String(10))   # 评级
    rating_agency = Column(String(20))  # 评级机构
    month_research_count = Column(Integer)  # 近一个月研报数
    industry = Column(String(200))  # 行业
    share_year1 = Column(String(10))
    ratio_yaar1 = Column(String(10))
    forecasting_earning_per_share1 = Column(Float) # 每股收益
    Predicted_price_earnings_ratio1 = Column(Float)
    share_year2 = Column(String(10))
    ratio_yaar2 = Column(String(10))
    forecasting_earning_per_share2 = Column(Float)
    Predicted_price_earnings_ratio2 = Column(Float)
    share_year3 = Column(String(10))
    ratio_yaar3 = Column(String(10))
    forecasting_earning_per_share3 = Column(Float)
    Predicted_price_earnings_ratio3 = Column(Float)
    downloaded_path = Column(String(200))  # 下载路径
    report_pdf_link = Column(String(200))  # 报告PDF链接
    data_source = Column(String(50))  # 数据来源（如：EastMoneyFetcher）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'pdf_name', name='uix_research_report_code_pdf'),
        Index('ix_research_report_code_date_pdf', 'code', 'date', 'pdf_name'),
    )

    def __repr__(self):
        return f"<StockResearchReport(code={self.code}, date={self.date}, pdf_name={self.pdf_name}, title={self.title})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'date': self.date,
            'pdf_name': self.pdf_name,
            'title': self.title,
            'east_rating': self.east_rating,
            'rating_agency': self.rating_agency,
            'month_research_count': self.month_research_count,
            'industry': self.industry,
            'share_year1': self.share_year1,
            'ratio_yaar1': self.ratio_yaar1,
            'forecasting_earning_per_share1': self.forecasting_earning_per_share1,
            'Predicted_price_earnings_ratio1': self.Predicted_price_earnings_ratio1,
            'share_year2': self.share_year2,
            'ratio_yaar2': self.ratio_yaar2,
            'forecasting_earning_per_share2': self.forecasting_earning_per_share2,
            'Predicted_price_earnings_ratio2': self.Predicted_price_earnings_ratio2,
            'share_year3': self.share_year3,
            'ratio_yaar3': self.ratio_yaar3,
            'forecasting_earning_per_share3': self.forecasting_earning_per_share3,
            'Predicted_price_earnings_ratio3': self.Predicted_price_earnings_ratio3,
            'downloaded_path': self.downloaded_path,
            'data_source': self.data_source,

        }

class StockResearchReportAnalyze(Base):
    """
    股票调研报告分析模型 - ORM映射类
    """
    __tablename__ = 'stock_research_report_analyze'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)  # 日期
    pdf_name = Column(String(100), nullable=False, index=True)  # PDF文件名
    analyze_content = Column(Text)  # 分析内容
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    __table_args__ = (
        UniqueConstraint('code', 'pdf_name', name='uix_research_report_analyze_code_pdf'),
        Index('ix_research_report_analyze_code_date_pdf', 'code', 'date', 'pdf_name'),
    )

    def __repr__(self):
        return f"<StockResearchReportAnalyze(code={self.code}, date={self.date}, pdf_name={self.pdf_name}, analyze_content={self.analyze_content})>"


    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'date': self.date,
            'pdf_name': self.pdf_name,
            'analyze_content': self.analyze_content,
        }


class FinancialReportAnalyze(Base):
    """
    财务报表分析模型 - ORM映射类
    """
    __tablename__ = 'financial_report_analyze'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)  # 日期
    pdf_name = Column(String(100), nullable=False, index=True)  # PDF文件名
    report_type = Column(String(20), nullable=False)  # 报告类型：机构研报、年报、季报
    analyze_content = Column(Text)  # 分析内容
    ratios = Column(JSON)  # 财务比率
    confidence = Column(String(10))  # 可信度
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    __table_args__ = (
        UniqueConstraint('code', 'pdf_name', name='uix_financial_report_analyze_code_pdf'),
        Index('ix_financial_report_analyze_code_date_pdf', 'code', 'date', 'pdf_name'),
        Index('ix_financial_report_analyze_type', 'report_type'),
    )

    def __repr__(self):
        return f"<FinancialReportAnalyze(code={self.code}, date={self.date}, pdf_name={self.pdf_name}, report_type={self.report_type})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'date': self.date,
            'pdf_name': self.pdf_name,
            'report_type': self.report_type,
            'analyze_content': self.analyze_content,
            'ratios': self.ratios,
            'confidence': self.confidence,
        }

class DailyTask(Base):
    """
    每日任务状态状态模型 - ORM映射类
    """
    __tablename__ = 'daily_task'
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    task_name = Column(String(50), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)  # 日期
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    __table_args__ = (
        UniqueConstraint('code', 'task_name', name='uix_daily_task_code_task_name'),
        Index('ix_daily_task_code_task_name_date', 'code', 'task_name', 'date'),
    )

    def __repr__(self):
        return f"<DailyTask(code={self.code}, task_name={self.task_name}, date={self.date})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'task_name': self.task_name,
            'date': self.date,
        }

class DatabaseManager:
    """
    数据库管理器
    """
    # 单例模式类变量：存储唯一的实例
    _instance: Optional['DatabaseManager'] = None

    def __new__(cls, *args, **kwargs):
        """
        单例模式实现 - 重写 __new__ 方法

        设计原理：
        1. __new__ 方法在 __init__ 之前调用，负责创建对象实例
        2. 检查类变量 _instance 是否已存在
        3. 如果不存在，调用父类的 __new__ 创建新实例
        4. 标记实例为未初始化状态（通过 _initialized 标志）
        5. 返回单例实例

        这样确保整个应用生命周期内只有一个 DatabaseManager 实例

        Returns:
            DatabaseManager: 单例实例
        """
        if cls._instance is None:
            # 创建新实例
            cls._instance = super().__new__(cls)
            # 标记为未初始化，防止 __init__ 重复初始化
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_url: Optional[str] = None):
        """
        初始化数据库管理器

        注意：由于单例模式，__init__ 方法可能被多次调用
        使用 _initialized 标志确保只初始化一次

        初始化流程：
        1. 获取数据库连接URL（从参数或配置文件）
        2. 创建SQLAlchemy引擎（配置连接池）
        3. 创建会话工厂（配置会话行为）
        4. 创建数据库表（如果不存在）

        Args:
            db_url: 数据库连接URL
                格式：sqlite:///path/to/database.db
                示例：sqlite:///./data/stock_analysis.db
                如果为None，则从配置文件中读取
        """
        # 单例初始化保护：如果已经初始化，直接返回
        if self._initialized:
            return

        # 步骤1：获取数据库连接URL
        if db_url is None:
            config = get_db_config()
            db_url = config.get("sqlite_path", "sqlite:///./data/sqlite/stock.db")

        # 步骤2：创建SQLAlchemy引擎（连接池管理器）
        # 参数说明：
        # - echo=False: 生产环境关闭SQL语句日志（调试时可设为True）
        # - pool_pre_ping=True: 连接健康检查，避免使用失效连接
        # - 其他参数使用SQLAlchemy默认值，适合大多数场景
        self._engine = create_engine(
            db_url,
            echo=False,  # 设为 True 可查看 SQL 语句（调试用）
            pool_pre_ping=True,  # 连接健康检查（推荐开启）
        )

        # 步骤3：创建会话工厂
        # sessionmaker 是一个工厂函数，用于创建新的Session对象
        # 配置说明：
        # - bind=self._engine: 绑定到上面创建的引擎
        # - autocommit=False: 手动控制事务提交（推荐）
        # - autoflush=False: 手动控制数据刷新（提高性能）
        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autocommit=False,  # 手动提交事务，确保数据一致性
            autoflush=False,  # 手动刷新数据，提高性能
        )

        # 步骤4：创建所有表（如果不存在）
        # Base.metadata.create_all 会检查表是否存在，不存在则创建
        # 这是SQLAlchemy的便利功能，避免手动编写CREATE TABLE语句
        Base.metadata.create_all(self._engine)

        # 标记为已初始化，防止重复初始化
        self._initialized = True
        logger.info(f"数据库初始化完成: {db_url}")

    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        """
        获取数据库管理器单例实例（推荐使用此方法）

        这是访问 DatabaseManager 的标准方式，优于直接实例化。

        设计优势：
        1. 延迟初始化：首次调用时才创建实例，节省资源
        2. 线程安全：确保多线程环境下只有一个实例
        3. 简化调用：隐藏单例实现的复杂性
        4. 类型安全：返回类型明确的DatabaseManager实例

        使用场景：
        from storage import get_db  # 推荐使用这个便捷函数
        db = get_db()  # 内部调用此方法

        或者：
        db = DatabaseManager.get_instance()

        Returns:
            DatabaseManager: 数据库管理器单例实例
        """
        if cls._instance is None:
            cls._instance = cls()  # 创建新实例（触发__init__）
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """
        重置单例实例（主要用于测试）

        使用场景：
        1. 单元测试：每个测试用例需要干净的数据库状态
        2. 配置变更：重新加载数据库配置（如切换数据库）
        3. 连接故障：强制重新建立数据库连接
        4. 内存管理：释放数据库连接资源

        工作原理：
        1. 如果存在实例，调用 _engine.dispose() 释放所有连接
        2. 将类变量 _instance 设为 None
        3. 下次调用 get_instance() 时会创建新实例

        注意事项：
        • 生产环境慎用：释放连接可能导致正在进行的操作失败
        • 线程安全：调用此方法时确保没有其他线程在使用数据库
        • 数据一致性：确保所有事务已提交或回滚

        示例：
            # 在测试开始时重置数据库
            DatabaseManager.reset_instance()
            db = DatabaseManager.get_instance()  # 创建新实例
        """
        if cls._instance is not None:
            cls._instance._engine.dispose()  # 释放数据库连接
            cls._instance = None  # 清除单例实例

    def get_session(self) -> Session:
        """
        获取数据库会话（上下文管理器）

        设计模式：工作单元模式 (Unit of Work Pattern)

        核心概念：
        • Session（会话）：一组相关的数据库操作集合
        • 事务：确保一组操作要么全部成功，要么全部失败
        • 上下文管理器：使用 with 语句自动管理资源

        设计优势：
        1. 自动资源管理：确保会话正确关闭，避免连接泄漏
        2. 异常安全：异常时自动回滚事务，保证数据一致性
        3. 代码简洁：无需手动 try-finally，代码更清晰
        4. 事务控制：支持嵌套事务和保存点

        工作流程：
        1. 创建新会话（从连接池获取连接）
        2. 执行数据库操作（查询、插入、更新、删除）
        3. 提交事务（如果所有操作成功）
        4. 回滚事务（如果任何操作失败）
        5. 关闭会话（释放连接回连接池）

        使用示例：
            # 基本用法
            with db.get_session() as session:
                # 查询数据
                stock = session.query(StockDaily).filter_by(code='600519').first()

                # 修改数据
                stock.close = 1850.0

                # 提交事务（重要！）
                session.commit()

            # 事务自动回滚示例
            try:
                with db.get_session() as session:
                    # 操作1：成功
                    session.add(StockDaily(...))

                    # 操作2：失败，触发异常
                    raise ValueError("模拟错误")

                    # 这行不会执行
                    session.commit()
            except Exception:
                # 事务已自动回滚，操作1不会保存到数据库
                print("事务已回滚")

        Returns:
            Session: SQLAlchemy 会话对象，支持上下文管理器协议

        Raises:
            Exception: 创建会话失败时抛出原始异常
        """
        session = self._SessionLocal()
        try:
            return session
        except Exception:
            # 创建会话失败时，确保关闭会话
            session.close()
            raise  # 重新抛出异常

    def is_date_exist(self, code, freq: str, target_date: Optional[date] = None, )-> bool:
        """当前日期的数据是否存在
        args:
            freq: 频率(日：daily，周：week，月：month)
        """
        if target_date is None:
            target_date = date.today()

        t = StockDaily
        if freq == "week":
            t = StockWeekly
        elif freq == "month":
            t = StockMonth
        with self.get_session() as session:
            # 构建查询：查找指定股票和日期的记录
            # select(StockDaily): 选择 StockDaily 表的所有列
            # .where(): 添加查询条件
            # and_(): 逻辑与，两个条件必须同时满足
            # scalar_one_or_none(): 返回单个结果或None
            result = session.execute(
                select(t).where(
                    and_(
                        t.code == code, t.date == target_date
                    )
                )
            ).scalar_one_or_none()
            return result is not None

    def get_latest_daily_data(self, code: str, days: int = 2) -> List[StockDaily]:
        """
        获取N天的日线数据（按日期降序排列）
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()

            # 将SQLAlchemy的Scalar序列转换为Python列表
            return list(results)

    def get_all_daily_data(self, code: str) -> pd.DataFrame:
        """获取全部的数据"""
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            # 核心：利用to_dict()转为字典列表（关键简化步骤）
            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            # 4. 核心：将datetime.date转为pd.Timestamp（和Tushare统一类型）
            if "date" in data_list.columns:
                # datetime.date → pd.Timestamp（关键兼容步骤）
                data_list["date"] = data_list["date"].apply(lambda x: pd.Timestamp(x))

                # 确保code字段格式统一（字符串类型）
                data_list["code"] = data_list["code"].astype(str)

            return data_list

    def get_daily_data_range(self, code: str, start_date: date, end_date: date) -> List[StockDaily]:
        """获取一段时间的日线数据(按日期降序排列)"""
        if start_date > end_date:
            logger.error(f"start_date {start_date} > end_date {end_date}")
            raise ValueError(f"{start_date}, {end_date} err")
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(desc(StockDaily.date))
            ).scalars().all()
            return list(results)

    def get_latest_weekly_data(self, code: str, days: int = 2) -> List[StockWeekly]:
        """获取N天的周线数据（按日期降序排列）"""
        with self.get_session() as session:
            results = session.execute(
                select(StockWeekly)
                .where(StockWeekly.code == code)
                .order_by(desc(StockWeekly.date))
                .limit(days)
            ).scalars().all()
            logger.warning(f"result count: [{len(results)}]")
            return list(results)

    def get_all_weekly_data(self, code: str) -> pd.DataFrame:
        """获取全部的周数据"""
        with self.get_session() as session:
            results = session.execute(
                select(StockWeekly)
                .where(StockWeekly.code == code)
                .order_by(desc(StockWeekly.date))
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            # 核心：利用to_dict()转为字典列表（关键简化步骤）
            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            # 4. 核心：将datetime.date转为pd.Timestamp（和Tushare统一类型）
            if "date" in data_list.columns:
                # datetime.date → pd.Timestamp（关键兼容步骤）
                data_list["date"] = data_list["date"].apply(lambda x: pd.Timestamp(x))
                data_list["end_date"] = data_list["end_date"].apply(lambda x: pd.Timestamp(x))
                # 确保code字段格式统一（字符串类型）
                data_list["code"] = data_list["code"].astype(str)

            return data_list

    def get_weekly_data_range(self, code: str, start_date: date, end_date: date) -> List[StockWeekly]:
        """获取一段时间的周线数据(按日期降序排列)"""
        if start_date > end_date:
            logger.error(f"start_date {start_date} > end_date {end_date}")
            raise ValueError(f"{start_date}, {end_date} err")
        with self.get_session() as session:
            results = session.execute(
                select(StockWeekly)
                .where(
                    and_(
                        StockWeekly.code == code,
                        StockWeekly.date >= start_date,
                        StockWeekly.date <= end_date
                    )
                )
                .order_by(desc(StockWeekly.date))
            ).scalars().all()
            return list(results)

    def get_latest_month_data(self, code: str, days: int = 2) -> List[StockMonth]:
        """获取N天的月线数据（按照日期降序排列）"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMonth)
                .where(StockMonth.code == code)
                .order_by(desc(StockMonth.date))
                .limit(days)
            ).scalars().all()
            return list(results)

    def get_all_month_data(self, code: str) -> pd.DataFrame:
        """获取全部的月数据"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMonth)
                .where(StockMonth.code == code)
                .order_by(desc(StockMonth.date))
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            # 核心：利用to_dict()转为字典列表（关键简化步骤）
            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            # 4. 核心：将datetime.date转为pd.Timestamp（和Tushare统一类型）
            if "date" in data_list.columns:
                # datetime.date → pd.Timestamp（关键兼容步骤）
                data_list["date"] = data_list["date"].apply(lambda x: pd.Timestamp(x))
                data_list["end_date"] = data_list["end_date"].apply(lambda x: pd.Timestamp(x))
                # 确保code字段格式统一（字符串类型）
                data_list["code"] = data_list["code"].astype(str)

            return data_list

    def get_month_data_range(self, code: str, start_date: date, end_date: date) -> List[StockMonth]:
        """获取N天的月线数据（按照日期降序排列）"""
        if start_date > end_date:
            logger.error(f"start_date {start_date} > end_date {end_date}")
            raise ValueError(f"{start_date}, {end_date} err")
        with self.get_session() as session:
            results = session.execute(
                select(StockMonth)
                .where(
                    and_(
                        StockMonth.code == code,
                        StockMonth.date >= start_date,
                        StockMonth.date <= end_date
                    )
                )
                .order_by(desc(StockMonth.date))
            ).scalars().all()
            return list(results)

    def get_latest_daily_forecast(self, code: str, days: int = 2) -> List[DailyForecast]:
        """获取N天的预测数据"""
        with self.get_session() as session:
            results = session.execute(
                select(DailyForecast)
                .where(DailyForecast.code == code)
                .order_by(desc(DailyForecast.forecast_date))
                .limit(days)
            ).scalars().all()
            return list(results)

    def is_pdf_analyzed(self, code: str, pdf_name: str) -> bool:
        """
        检查PDF文件是否已经分析过
        
        Args:
            code: 股票代码
            pdf_name: PDF文件名
            
        Returns:
            bool: True表示已经分析过，False表示未分析过
        """
        with self.get_session() as session:
            result = session.execute(
                select(FinancialReportAnalyze)
                .where(
                    and_(
                        FinancialReportAnalyze.code == code,
                        FinancialReportAnalyze.pdf_name == pdf_name
                    )
                )
            ).scalar_one_or_none()
            return result is not None

    def save_financial_analyze(self, code: str, date: date, pdf_name: str, report_type: str, 
                             analyze_content: str, ratios: Dict[str, Any], confidence: str = "high") -> bool:
        """
        保存财务分析结果
        
        Args:
            code: 股票代码
            date: 分析日期
            pdf_name: PDF文件名
            report_type: 报告类型：机构研报、年报、季报
            analyze_content: 分析内容
            ratios: 财务比率
            confidence: 可信度
            
        Returns:
            bool: True表示保存成功，False表示保存失败
        """
        try:
            with self.get_session() as session:
                # 检查是否已经存在
                existing = session.execute(
                    select(FinancialReportAnalyze)
                    .where(
                        and_(
                            FinancialReportAnalyze.code == code,
                            FinancialReportAnalyze.pdf_name == pdf_name
                        )
                    )
                ).scalar_one_or_none()
                
                if existing:
                    # 更新现有记录
                    existing.analyze_content = analyze_content
                    existing.ratios = ratios
                    existing.confidence = confidence
                    existing.updated_at = datetime.now()
                else:
                    # 创建新记录
                    new_analyze = FinancialReportAnalyze(
                        code=code,
                        date=date,
                        pdf_name=pdf_name,
                        report_type=report_type,
                        analyze_content=analyze_content,
                        ratios=ratios,
                        confidence=confidence
                    )
                    session.add(new_analyze)
                
                session.commit()
                return True
        except Exception as e:
            logger.error(f"保存财务分析结果失败: {e}")
            return False

    def get_financial_analyze(self, code: str) -> (List[FinancialReportAnalyze], Mapping[str, bool]):
        """
        获取财务分析结果
        
        Args:
            code: 股票代码

        Returns:
            List[FinancialReportAnalyze]: 分析结果列表
            Mapping[str, bool]: 所有PDF文件的映射，键为PDF文件名，值为True表示已分析
        """
        with self.get_session() as session:
            query = select(FinancialReportAnalyze).where(FinancialReportAnalyze.code == code)

            results = session.execute(
                query.order_by(desc(FinancialReportAnalyze.date))
            ).scalars().all()
            data_list = list(results)
            return data_list, self.get_all_financial_analyze_map(data_list)

    def get_all_financial_analyze_map(self, res: List[FinancialReportAnalyze]) -> Mapping[str, bool]:
        """获取所有财务分析结果"""
        return {obj.pdf_name: True for obj in res}

    def get_daily_forecast_range(self, code: str, start_date, end_date: str) -> List[DailyForecast]:
        """获取每日预测数据"""
        if start_date > end_date:
            logger.error(f"start_date {start_date} > end_date {end_date}")
            raise ValueError(f"{start_date}, {end_date} err")

        with self.get_session() as session:
            results = session.execute(
                select(DailyForecast).where(
                    and_(
                        DailyForecast.code == code,
                        DailyForecast.forecast_date >= start_date,
                        DailyForecast.forecast_date <= end_date
                    )
                )
                .order_by(desc(DailyForecast.forecast_date))
            ).scalars().all()
            return list(results)

    def get_stock_basic(self, code: str) -> Optional[StockBasic]:
        """获取股票的基本信息"""
        if code is None:
            logger.error(f"code is null")
            return None

        with self.get_session() as session:
            result = session.execute(
                select(StockBasic).where(
                    StockBasic.code == code
                )
            ).scalar_one_or_none()
            return result

    def get_latest_daily_basic_data(self, code: str, days: int = 2) -> pd.DataFrame:
        """获取每日指标数据"""
        with self.get_session() as session:
            results = session.execute(
                select(StockDailyBasic)
                .where(StockDailyBasic.code == code)
                .order_by(desc(StockDailyBasic.trade_date))
                .limit(days)
            ).scalars().all()
            # 核心：利用to_dict()转为字典列表（关键简化步骤）
            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            # 4. 核心：将datetime.date转为pd.Timestamp（和Tushare统一类型）
            if "date" in data_list.columns:
                # datetime.date → pd.Timestamp（关键兼容步骤）
                data_list["date"] = data_list["date"].apply(lambda x: pd.Timestamp(x))
                data_list["end_date"] = data_list["end_date"].apply(lambda x: pd.Timestamp(x))
                # 确保code字段格式统一（字符串类型）
                data_list["code"] = data_list["code"].astype(str)

            return data_list

    def get_daily_basic_data(self, code: str, start_date: str, end_date: str) -> List[StockDailyBasic]:
        """获取一段时间的每日指标数据"""
        if start_date > end_date:
            logger.error(f"start_date {start_date} > end_date {end_date}")
            raise ValueError(f"{start_date}, {end_date} err")
        with self.get_session() as session:
            results = session.execute(
                select(StockDailyBasic)
                .where(
                    and_(
                        StockDailyBasic.code == code,
                        StockDailyBasic.trade_date >= start_date,
                        StockDailyBasic.trade_date <= end_date
                    )
                )
                .order_by(desc(StockDailyBasic.trade_date))
            ).scalars().all()
            return list(results)

    def get_stock_daily_task(self, code: str) -> Dict[str, date]:
        """
        获取股票的任务状态信息

        Args:
            code: 股票代码

        Returns:
            Dict[str, date]: 任务名称到执行日期的映射字典
            示例：{'daily_data': datetime.date(2026, 3, 29), 'weekly_data': datetime.date(2026, 3, 28)}

        使用场景:
            1. 检查各个任务的最后执行时间
            2. 判断哪些任务今天已执行，哪些需要执行
            3. 配合每日只执行一次的设计模式
        """
        result = {}
        with self.get_session() as session:
            results = session.execute(
                select(DailyTask).where(
                    DailyTask.code == code
                ).order_by(
                    desc(DailyTask.date)
                )
            ).scalars().all()

            # 构建任务名称到最新执行日期的映射字典
            for task in results:
                # 如果任务名不存在，则添加（保证是最新的日期）
                result[task.task_name] = task.date

        return result

    def get_stock_research_report_days(self, code: str, days: int = 30):
        """
        获取最近一段时间的研报
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockResearchReport)
                .where(
                    and_(
                        StockResearchReport.code == code,

                    )
                )
                .order_by(desc(StockResearchReport.date))
            )

    def save_stock_basic(self, df: pd.DataFrame) -> int:
        """存储股票基本数据"""
        logger.info(f"save stock basic")
        if df is None or df.empty:
            logger.warning(f"保存的数据为空")
            return 0

        saved_count = 0
        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    code = row.get('symbol')
                    list_date = parse_row_date(row.get('list_date'))
                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockBasic).where(
                            and_(
                                StockBasic.code == code,  # 股票代码匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.name = row.get('name')
                        existing.industry = row.get('industry')
                        existing.list_date = list_date
                        existing.list_status = row.get('list_status')
                        existing.market = row.get('market')
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockBasic对象，填充所有字段
                        record = StockBasic(
                            # 标识字段
                            code=code,  # 股票代码
                            name=row.get('name'),
                            industry=row.get('industry'),
                            list_date=list_date,
                            list_status=row.get('list_status'),
                            market=row.get('market'),

                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_stock_daily_basic(self, df: pd.DataFrame, code: str) -> int:
        """获取每日指标数据"""
        if df is None or df.empty:
            logger.warning(f"保存的数据为空，跳过{code}")
            return 0
        saved_count = 0
        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    trade_date = parse_row_date(row.get('trade_date'))
                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockDailyBasic).where(
                            and_(
                                StockDailyBasic.code == code,  # 股票代码匹配
                                StockDailyBasic.trade_date == trade_date
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.close = row.get('close')
                        existing.turnover_rate = row.get('turnover_rate')
                        existing.turnover_rate_f = row.get('turnover_rate_f')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.pe = row.get('pe')
                        existing.pe_ttm = row.get('pe_ttm')
                        existing.pb = row.get('pb')
                        existing.ps = row.get('ps')
                        existing.ps_ttm = row.get('ps_ttm')
                        existing.dv_ratio = row.get('dv_ratio')
                        existing.dv_ttm = row.get('dv_ttm')
                        existing.total_share = row.get('total_share')
                        existing.float_share = row.get('float_share')
                        existing.free_share = row.get('free_share')
                        existing.total_mv = row.get('total_mv')
                        existing.circ_mv = row.get('circ_mv')
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockBasic对象，填充所有字段
                        record = StockDailyBasic(
                            # 标识字段
                            code=code,
                            trade_date=trade_date,
                            close=row.get('close'),
                            turnover_rate=row.get('turnover_rate'),
                            turnover_rate_f=row.get('turnover_rate_f'),
                            volume_ratio=row.get('volume_ratio'),
                            pe=row.get('pe'),
                            pe_ttm=row.get('pe_ttm'),
                            pb=row.get('pb'),
                            ps=row.get('ps'),
                            ps_ttm=row.get('ps_ttm'),
                            dv_ratio=row.get('dv_ratio'),
                            dv_ttm=row.get('dv_ttm'),
                            total_share=row.get('total_share'),
                            float_share=row.get('float_share'),
                            free_share=row.get('free_share'),
                            total_mv=row.get('total_mv'),
                            circ_mv=row.get('circ_mv'),
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise
        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_daily_data(
            self,
            df: pd.DataFrame,
            code: str,
            start_date: Optional[Date] = None,
            data_source: str = "Unknown"
    ) -> int:
        """
        保存日线数据到数据库（支持UPSERT操作）

        设计模式：UPSERT (Update or Insert) + 批处理 (Batch Processing)

        核心功能：
        1. 将Pandas DataFrame中的数据保存到数据库
        2. 智能更新：存在则更新，不存在则插入
        3. 事务安全：保证数据一致性
        4. 性能优化：批处理减少数据库交互

        技术实现：手动实现UPSERT逻辑
        1. 遍历DataFrame的每一行
        2. 对每一行检查是否已存在（通过code+date唯一标识）
        3. 如果存在：更新现有记录
        4. 如果不存在：插入新记录
        5. 所有操作在一个事务中提交

        为什么手动实现UPSERT？
        1. SQLite不支持原生UPSERT语法（INSERT ... ON CONFLICT）
        2. 需要更细粒度的控制（更新部分字段而非全部）
        3. 需要记录数据来源和更新时间
        4. 需要统计新增记录数

        数据流：
        Pandas DataFrame → 数据清洗 → 逐行处理 → 数据库

        性能优化策略：
        1. 批量提交：所有操作在一个事务中提交，减少IO
        2. 索引优化：利用(code, date)索引快速检查存在性
        3. 内存优化：逐行处理避免一次性加载所有数据到内存
        4. 连接复用：使用同一个数据库会话

        错误处理：
        1. 空数据检查：如果DataFrame为空，直接返回0
        2. 事务回滚：任何异常都回滚整个事务
        3. 详细日志：记录成功和失败信息
        4. 异常传播：抛出原始异常供调用者处理

        使用场景：
            从API获取数据后保存：
            data = fetch_stock_data('600519', '2026-01-01', '2026-01-15')
            saved_count = db.save_daily_data(data, '600519', 'AkshareFetcher')

        数据格式要求：
        DataFrame必须包含以下列（名称需匹配）：
        • date: 日期（支持str/datetime/pd.Timestamp格式）
        • open, high, low, close: OHLC价格
        • volume, amount: 成交量和成交额
        • pct_chg: 涨跌幅
        • ma5, ma10, ma20, ma50, ma120, ma200: 移动平均线
        • volume_ratio: 量比

        Args:
            df: 包含日线数据的Pandas DataFrame
                不能为None或空，否则直接返回0
                支持多种日期格式：str、datetime、pd.Timestamp
            code: 股票代码，如 '600519'
                用于标识数据所属的股票
            data_source: 数据来源名称，如 'AkshareFetcher'
                用于数据质量追踪和问题排查
                默认值：'Unknown'

        Returns:
            int: 新增的记录数（不包括更新的记录）
                返回0表示：1) 数据为空 2) 所有数据已存在（只更新不新增）

        时间复杂度：O(n × log m)，n为DataFrame行数，m为表中记录数
        空间复杂度：O(1)（除了输入DataFrame）

        Raises:
            Exception: 保存过程中任何错误都会抛出，事务自动回滚
        """
        # 前置检查：确保输入数据有效
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存

        saved_count = 0  # 计数器：记录新增（非更新）的记录数
        start_date_str = parse_row_date(start_date)
        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = row.get('date')

                    # 情况1：字符串格式，如 "2026-01-15"
                    if isinstance(row_date, str):
                        # datetime.strptime: 字符串解析为datetime对象
                        # .date(): 提取日期部分（去除时间）
                        row_date = datetime.strptime(row_date, '%Y-%m-%d').date()

                    # 情况2：datetime对象（直接使用日期部分）
                    elif isinstance(row_date, datetime):
                        row_date = row_date.date()

                    # 情况3：Pandas Timestamp对象（转换为datetime再提取日期）
                    elif isinstance(row_date, pd.Timestamp):
                        row_date = row_date.date()

                    if  row_date < start_date_str:
                        continue

                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == code,  # 股票代码匹配
                                StockDaily.date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.open = row.get('open')
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.ma50 = row.get('ma50')
                        existing.ma120 = row.get('ma120')
                        existing.ma200 = row.get('ma200')
                        existing.ema5 = row.get('ema5')
                        existing.ema10 = row.get('ema10')
                        existing.ema20 = row.get('ema20')
                        existing.ema50 = row.get('ema50')
                        existing.ema120 = row.get('ema120')
                        existing.ema200 = row.get('ema200')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source  # 更新数据来源
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = StockDaily(
                            # 标识字段
                            code=code,  # 股票代码
                            date=row_date,  # 交易日期

                            # OHLC价格数据
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),

                            # 成交数据
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),

                            # 技术指标（移动平均线）
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            ma50=row.get('ma50'),
                            ma120=row.get('ma120'),
                            ma200=row.get('ma200'),
                            ema5=row.get('ema5'),
                            ema10=row.get('ema10'),
                            ema20=row.get('ema20'),
                            ema50=row.get('ema50'),
                            ema120=row.get('ema120'),
                            ema200=row.get('ema200'),

                            # 量能指标
                            volume_ratio=row.get('volume_ratio'),

                            # 元数据
                            data_source=data_source,  # 数据来源
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_week_data(
            self,
            df: pd.DataFrame,
            code: str,
            start_date: Optional[date] = None,
            data_source: str = 'Unknown'
    ) -> int:
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存

        saved_count = 0  # 计数器：记录新增（非更新）的记录数
        start_date_str = parse_row_date(start_date)
        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = parse_row_date(row.get('date'))
                    end_date = parse_row_date(row.get('end_date'))
                    if end_date < start_date_str:
                        continue

                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockWeekly).where(
                            and_(
                                StockWeekly.code == code,  # 股票代码匹配
                                StockWeekly.date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.open = row.get('open')
                        existing.end_date = end_date
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.change = row.get('change')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.ma50 = row.get('ma50')
                        existing.ma120 = row.get('ma120')
                        existing.ma200 = row.get('ma200')
                        existing.ema5 = row.get('ema5')
                        existing.ema10 = row.get('ema10')
                        existing.ema20 = row.get('ema20')
                        existing.ema50 = row.get('ema50')
                        existing.ema120 = row.get('ema120')
                        existing.ema200 = row.get('ema200')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source  # 更新数据来源
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = StockWeekly(
                            # 标识字段
                            code=code,  # 股票代码
                            date=row_date,  # 交易日期
                            end_date = end_date,

                            # OHLC价格数据
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),

                            # 成交数据
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),
                            change=row.get('change'),

                            # 技术指标（移动平均线）
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            ma50=row.get('ma50'),
                            ma120=row.get('ma120'),
                            ma200=row.get('ma200'),
                            ema5=row.get('ema5'),
                            ema10=row.get('ema10'),
                            ema20=row.get('ema20'),
                            ema50=row.get('ema50'),
                            ema120=row.get('ema120'),
                            ema200=row.get('ema200'),

                            # 量能指标
                            volume_ratio=row.get('volume_ratio'),

                            # 元数据
                            data_source=data_source,  # 数据来源
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_month_data(
            self,
            df: pd.DataFrame,
            code: str,
            start_date: Optional[date] = None,
            data_source: str = 'Unknown'
    ) -> int:
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存

        saved_count = 0  # 计数器：记录新增（非更新）的记录数
        start_date_str = parse_row_date(start_date)
        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = parse_row_date(row.get('date'))
                    e_date = parse_row_date(row.get('end_date'))


                    if e_date < start_date_str:
                        continue

                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(StockMonth).where(
                            and_(
                                StockMonth.code == code,  # 股票代码匹配
                                StockMonth.date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.open = row.get('open')
                        existing.end_date = e_date
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.change = row.get('change')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.ma50 = row.get('ma50')
                        existing.ma120 = row.get('ma120')
                        existing.ma200 = row.get('ma200')
                        existing.ema5 = row.get('ema5')
                        existing.ema10 = row.get('ema10')
                        existing.ema20 = row.get('ema20')
                        existing.ema50 = row.get('ema50')
                        existing.ema120 = row.get('ema120')
                        existing.ema200 = row.get('ema200')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source  # 更新数据来源
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = StockMonth(
                            # 标识字段
                            code=code,  # 股票代码
                            date=row_date,  # 交易日期
                            end_date = e_date,

                            # OHLC价格数据
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),

                            # 成交数据
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),
                            change=row.get('change'),

                            # 技术指标（移动平均线）
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            ma50=row.get('ma50'),
                            ma120=row.get('ma120'),
                            ma200=row.get('ma200'),
                            ema5=row.get('ema5'),
                            ema10=row.get('ema10'),
                            ema20=row.get('ema20'),
                            ema50=row.get('ema50'),
                            ema120=row.get('ema120'),
                            ema200=row.get('ema200'),

                            # 量能指标
                            volume_ratio=row.get('volume_ratio'),

                            # 元数据
                            data_source=data_source,  # 数据来源
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        # === 步骤6：返回结果 ===
        # 只返回新增记录数（更新的记录不计入）
        return saved_count

    def save_daily_forecast(
            self,
            df: pd.DataFrame,
            code: str,
            forecast_model: str = 'Unknown'
    ) -> int:
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0  # 无数据可保存
        saved_count = 0  # 计数器：记录新增（非更新）的记录数

        # 使用数据库会话（工作单元模式）
        with self.get_session() as session:
            try:
                # 遍历DataFrame的每一行（批处理中的逐行处理）
                # df.iterrows(): 返回(index, row)元组，_表示忽略索引
                for _, row in df.iterrows():
                    # === 步骤1：解析日期（支持多种格式）===
                    # 数据可能来自不同来源，日期格式不统一，需要标准化
                    row_date = parse_row_date(row.get('forecast_date'))

                    # === 步骤2：检查记录是否已存在（UPSERT核心）===
                    # 查询条件：相同的股票代码 + 相同的交易日期
                    # 利用(code, date)复合索引快速查找
                    existing = session.execute(
                        select(DailyForecast).where(
                            and_(
                                DailyForecast.code == code,  # 股票代码匹配
                                DailyForecast.forecast_date == row_date  # 交易日期匹配
                            )
                        )
                    ).scalar_one_or_none()  # 返回单个结果或None

                    # === 步骤3：根据存在性执行更新或插入 ===
                    if existing:
                        # 情况A：记录已存在 → 执行UPDATE（更新）
                        # 更新所有字段，确保数据最新
                        existing.practice_rue = row.get('practice_rue')
                        forecast_model = forecast_model
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = DailyForecast(
                            # 标识字段
                            code=code,  # 股票代码
                            foreccst_date=row_date,  # 交易日期
                            forecast_rue = row.get('forecast_rue'),
                            practice_rue=row.get('practice_rue'),
                            forecast_model=row.get('forecast_model'),
                            # created_at和updated_at由SQLAlchemy自动设置
                        )
                        session.add(record)  # 添加到会话（延迟插入）
                        saved_count += 1  # 新增记录计数+1

                # === 步骤4：提交事务 ===
                # 所有行处理完成后，一次性提交到数据库
                # 优点：1) 原子性 2) 性能优化（减少IO）3) 数据一致性
                session.commit()

                # 记录成功日志（区分新增和更新）
                if saved_count > 0:
                    logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 数据成功，所有数据已存在（只更新不新增）")

            except Exception as e:
                # === 步骤5：错误处理（事务回滚）===
                # 任何异常都触发事务回滚，保证数据一致性
                # 回滚会撤销本次事务中的所有操作
                session.rollback()

                # 记录错误日志（包含详细上下文）
                logger.error(f"保存 {code} 数据失败: {e}")

                # 重新抛出异常，让调用者处理
                # 这是重要的设计：不吞没异常，让上层决定如何处理
                raise

        return saved_count

    def save_stock_research_report_analysis(
        self,
        report_analysis: List[Dict[str, Any]],
        code: str,
    ) -> int:
        """
        保存股票研究报告分析数据到数据库
        """
        saved_count = 0
        with self.get_session() as session:
            try:
                for item in report_analysis:
                    pdf_name = item["pdf_name"]
                    existing = session.execute(
                        select(StockResearchReportAnalyze).where(
                            and_(
                                StockResearchReportAnalyze.code == code,
                                StockResearchReportAnalyze.pdf_name == pdf_name
                            )
                        )
                    ).scalar_one_or_none()
                    if not existing:
                        an = StockResearchReportAnalyze(
                            code=code,
                            pdf_name=pdf_name,
                            date = item["date"],
                            analyze_content = item["analyze_content"],
                        )
                        an.updated_at = datetime.now()
                        session.add(an)
                        saved_count += 1
                session.commit()
                logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 数据失败: {e}")
                raise
        return saved_count

    def get_stock_research_report_analysis_pdf_names(
        self,
        code: str,
    ) -> Dict[str, int]:
        """
        获取股票研究报告分析数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockResearchReportAnalyze.pdf_name).where(
                    StockResearchReportAnalyze.code == code
                )
            ).scalars().all()
            pdf_name_count = {}
            for pdf_name in results:
                pdf_name_count[pdf_name] = True
            return pdf_name_count

    def get_stock_research_report_analysis(self, code: str, days: int = 30)->  List[str]:
        """
        获取最近一段时间的研究报告分析数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockResearchReportAnalyze.analyze_content)
                .where(
                    and_(
                        StockResearchReportAnalyze.code == code,
                        StockResearchReportAnalyze.date >= (date.today() - timedelta(days=days))
                    )
                )
            ).scalars().all()
            analyze_contents = [result for result in results]
            return analyze_contents


    # 获取最近N天的数据
    def get_stock_research_report_last_days(self, code: str, days: int = 30) -> List[str]:
        """
        获取股票N天的研究报告数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockResearchReportAnalyze.analyze_content).where(
                    and_(
                        StockResearchReportAnalyze.code == code,
                        StockResearchReportAnalyze.date >= (date.today() - timedelta(days=days))
                    )
                )
            ).scalars().all()
            analyze_content = [result for result in results]
            return analyze_content

    def save_stock_research_report(
        self,
        df: pd.DataFrame,
        code: str,
    ) -> int:
        """保存股票研究报告数据到数据库"""
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0
        save_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    date = parse_row_date(row.get("date"))
                    pdf_name = row.get("pdf_name")

                    half_year_ago = date.today() - timedelta(days=90)

                    # 如果研报日期早于半年前，跳过
                    if date < half_year_ago:
                        logger.debug(
                            f"[{code}] 研报 {pdf_name} 日期 ({date}) 早于半年前 ({half_year_ago})，已忽略")
                        continue

                    existing = session.execute(
                        select(StockResearchReport).where(
                            and_(
                                StockResearchReport.code == code,
                                StockResearchReport.date == date,
                                StockResearchReport.pdf_name == pdf_name
                            )
                        )
                    ).scalar_one_or_none()
                    if not existing:
                        record = StockResearchReport(
                            code=code,
                            date=date,
                            pdf_name=pdf_name,
                            report_name=row.get("report_name"),
                            east_rating=row.get("east_rating"),
                            rating_agency=row.get("rating_agency"),
                            month_research_count=row.get("month_research_count"),
                            industry= row.get("industry"),
                            report_pdf_link=row.get("report_pdf_link"),
                            share_year1=row.get("share_year1"),
                            ratio_year1=row.get("ratio_year1"),
                            forecasting_earning_per_share1 = row.get("forecasting_earning_per_share1"),
                            Predicted_price_earnings_ratio1 = row.get("Predicted_price_earnings_ratio1"),
                            share_year2=row.get("share_year2"),
                            ratio_year2=row.get("ratio_year2"),
                            forecasting_earning_per_share2 = row.get("forecasting_earning_per_share2"),
                            Predicted_price_earnings_ratio2 = row.get("Predicted_price_earnings_ratio2"),
                            share_year3=row.get("share_year3"),
                            ratio_year3=row.get("ratio_year3"),
                            forecasting_earning_per_share3 = row.get("forecasting_earning_per_share3"),
                            Predicted_price_earnings_ratio3 = row.get("Predicted_price_earnings_ratio3"),
                            updated_at=datetime.now(),
                        )
                        session.add(record)
                        save_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 数据失败: {e}")

        return save_count

    def download_research_report(self, url: str, filename: str = None, stock_code: str = None) -> Dict[str, Any]:
        """
        下载PDF文件

        参数:
            url: PDF链接
            filename: 自定义文件名（可选）
            stock_code: 股票代码（可选，用于组织文件）
            report_date: 报告日期（可选，用于组织文件）

        返回:
            Dict: 包含下载结果的字典
        """
        result = {
            'success': False,
            'file_path': None,
            'error': None,
            'file_size': 0
        }

        try:
            # 验证URL
            if not url or not url.startswith(('http://', 'https://')):
                result['error'] = f"无效的URL: {url}"
                logger.error(result['error'])
                return result

            # 生成文件名
            if not filename:
                result['error'] = f"无效的文件名: {filename}"
                logger.error(result['error'])
                return result

            if not self.is_valid_pdf_filename(filename):  # 验证文件名是否有效
                filename = f"{filename}.pdf"

            pdf_path = ().get_pdf_dir()
            # 创建子目录（如果提供了股票代码）
            if stock_code:
                stock_dir = pdf_path/stock_code
                stock_dir.mkdir(exist_ok=True)
                file_path = stock_dir / filename
            else:
                file_path = pdf_path / filename

            # 检查文件是否已存在
            if file_path.exists():
                result['success'] = True
                result['file_path'] = str(file_path)
                result['file_size'] = file_path.stat().st_size
                logger.info(f"PDF文件已存在: {file_path}")
                return result

            # 下载PDF
            logger.info(f"开始下载PDF: {url}")

            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            # 检查内容类型
            content_type = response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and 'application/pdf' not in content_type:
                logger.warning(f"URL可能不是PDF文件，内容类型: {content_type}")

            # 保存文件
            file_content = bytearray()
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        file_content.extend(chunk)

            # 验证文件
            file_size = file_path.stat().st_size
            if file_size == 0:
                file_path.unlink()  # 删除空文件
                result['error'] = "下载的文件为空"
                logger.error(result['error'])
                return result
            content = self.extract_text_from_pdf_content(file_content)
            result['success'] = True
            result['file_path'] = str(file_path)
            result['file_size'] = file_size
            result['file_content'] = content

            logger.info(f"PDF下载成功: {file_path} {len(content)}({file_size} bytes)")
        except Exception as e:
            result['error'] = f"下载失败: {e}"
            logger.error(result['error'])

        return result

    def extract_text_from_pdf_content(self, file_content: bytes) -> str | None:
        """
        从 PDF 二进制内容中提取文本

        参数:
            file_content: PDF 文件的二进制内容（bytes 或 bytearray）

        返回:
            str: 提取的文本内容
        """
        try:
            import PyPDF2

            # 将 bytes 转换为文件对象
            pdf_file = io.BytesIO(bytes(file_content))

            # 读取 PDF
            reader = PyPDF2.PdfReader(pdf_file)
            print(f"[PDF 提取] 共 {len(reader.pages)} 页")

            # 提取前几页的内容（避免 token 超限）
            max_pages = min(len(reader.pages), 10)
            text_content = []

            for i, page in enumerate(reader.pages[:max_pages]):
                text = page.extract_text()
                if text:
                    text_content.append(f"=== 第 {i + 1} 页 ===\n{text}")
                    print(f"[PDF 提取] 第 {i + 1} 页提取成功，{len(text)} 字符")
                else:
                    print(f"[PDF 提取] 第 {i + 1} 页无法提取文本（可能是图片或扫描版）")

            # 合并为字符串
            full_text = "\n\n".join(text_content)
            print(f"[PDF 提取] 总共提取 {len(full_text)} 字符")

            return full_text

        except ImportError:
            print("[PDF 提取] 未安装 PyPDF2，请运行：pip install PyPDF2")
            return None
        except Exception as e:
            print(f"[PDF 提取] 提取失败：{e}")
            return None

    def  is_valid_pdf_filename(self, filename):
        """
        验证是否为有效的PDF文件名
        规则：以.pdf结尾，且.pdf前必须有文件名
        """
        # 转换为小写处理
        filename_lower = filename.lower()

        # 检查是否以.pdf结尾
        if not filename_lower.endswith('.pdf'):
            return False

        # 检查.pdf前是否有文件名（不能只是".pdf"）
        if filename_lower == '.pdf':
            return False

        # 检查是否包含路径分隔符（可选）
        if os.path.sep in filename:
            # 提取纯文件名
            basename = os.path.basename(filename)
            # 检查纯文件名是否有效
            return len(basename) > 4  # 至少"x.pdf"

        # 纯文件名：长度至少为5（如"a.pdf"）
        return len(filename) >= 5

    def download_research_report_pdf(self, research_report: 'StockResearchReport'):
        """
        下载研究报告的PDF文件

        参数:
            company-research-consensus-analyzer: StockResearchReport实例

        返回:
            Dict: 包含下载结果的字典
        """
        if not research_report.report_pdf_link:
            logger.warning(f"无效的PDF URL")
            return

        # 生成文件名
        filename = f"{research_report.code}_{research_report.date}_{research_report.pdf_name}.pdf"

        # 下载PDF
        result = self.download_research_report(
            url=research_report.report_pdf_link,
            filename=filename,
            stock_code=research_report.code,
            report_date=str(research_report.date)
        )

        # 如果下载成功，更新数据库中的下载路径
        if result['success']:
            research_report.downloaded_path = result['file_path']

        return result

    def batch_download_pdfs(self, research_reports: List['StockResearchReport']) -> Dict[str, Any]:
        """
        批量下载PDF文件

        参数:
            research_reports: StockResearchReport列表

        返回:
            Dict: 包含批量下载结果的字典
        """
        results = {
            'total': len(research_reports),
            'success': 0,
            'failed': 0,
            'details': []
        }

        for report in research_reports:
            result = self.download_research_report_pdf(report)
            result['report'] = {
                'code': report.code,
                'date': str(report.date),
                'pdf_name': report.pdf_name
            }

            results['details'].append(result)

            if result['success']:
                results['success'] += 1
            else:
                results['failed'] += 1

        logger.info(f"批量下载完成: 总计 {results['total']}, 成功 {results['success']}, 失败 {results['failed']}")

        return results

    def get_downloaded_files(self, stock_code: str = None) -> List[Dict[str, Any]]:
        """
        获取已下载的PDF文件列表

        参数:
            stock_code: 股票代码（可选，用于筛选）

        返回:
            List[Dict]: 文件信息列表
        """
        files_info = []

        search_dir = self.download_dir
        if stock_code:
            search_dir = search_dir / stock_code

        if not search_dir.exists():
            return files_info

        for file_path in search_dir.rglob('*.pdf'):
            if file_path.is_file():
                stat = file_path.stat()
                files_info.append({
                    'file_path': str(file_path),
                    'file_name': file_path.name,
                    'file_size': stat.st_size,
                    'modified_time': datetime.fromtimestamp(stat.st_mtime),
                    'stock_code': file_path.parent.name if file_path.parent != self.download_dir else None
                })

        return files_info

    def save_daily_task_data(self, code: str, task_names: List[str]) -> int:
        """
        保存每日任务信息（UPSERT 模式）

        设计模式：UPSERT (Update or Insert)

        核心功能：
        1. 检查数据库中是否已存在今日的任务记录
        2. 如果存在：更新日期和状态
        3. 如果不存在：创建新的任务记录
        4. 批量操作，统一提交，提高性能

        Args:
            code: 股票代码
            task_names: 任务名称列表，如 ['daily_data', 'weekly_data']

        Returns:
            int: 新增的记录数（不包括更新的记录）

        使用场景:
            1. 数据抓取完成后，批量更新任务状态
            2. 初始化每日任务列表
            3. 标记某些任务为已完成

        示例:
            # 标记日常任务为已完成
            db.save_daily_task_data('600519', ['daily_data', 'weekly_data'])

            # 初始化任务为待执行状态
            db.save_daily_task_data('000001', ['morning_check'], status='pending')
        """
        if task_names is None or len(task_names) == 0:
            logger.warning(f"保存的任务名为空，跳过 {code}")
            return 0

        saved_count = 0  # 新增记录计数器
        today = date.today()

        with self.get_session() as session:
            try:
                # 步骤 1: 查询数据库中该股票的所有任务记录
                db_results = session.execute(
                    select(DailyTask).where(
                        DailyTask.code == code
                    )
                ).scalars().all()

                # 步骤 2: 将查询结果转换为字典 (key: task_name, value: DailyTaskStatus)
                # 这样可以 O(1) 时间复杂度查找，而不是 O(n)
                task_map: Dict[str, DailyTask] = {}
                for task in db_results:
                    task_map[task.task_name] = task

                # 步骤 3: 遍历任务列表，执行 UPSERT 操作
                for task_name in task_names:
                    # 从字典中获取现有记录
                    existing_task = task_map.get(task_name)

                    if existing_task:
                        # 情况 A: 记录已存在 → 执行 UPDATE
                        # 只在日期或状态不同时才更新，避免不必要的写操作
                        if existing_task.date != today:
                            existing_task.date = today
                            existing_task.updated_at = datetime.now()
                            logger.debug(f"更新任务状态：{code} - {task_name}")
                        # 注意：更新操作不计入 saved_count
                    else:
                        # 情况 B: 记录不存在 → 执行 INSERT
                        record = DailyTask(
                            code=code,
                            task_name=task_name,
                            date=today,
                            updated_at=datetime.now()
                        )
                        session.add(record)
                        saved_count += 1  # 新增计数
                        logger.debug(f"新增任务记录：{code} - {task_name}")

                # 步骤 4: 统一提交所有更改（重要！）
                # 优点：1) 原子性 2) 性能优化 3) 数据一致性
                session.commit()

                # 记录成功日志
                if saved_count > 0:
                    logger.info(f"保存 {code} 任务数据成功，新增 {saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 任务数据成功，所有任务已存在（只更新不新增）")

            except Exception as e:
                # 步骤 5: 错误处理（事务回滚）
                session.rollback()
                logger.error(f"保存 {code} 任务数据失败：{e}")
                raise

        # 返回新增记录数
        return saved_count

    def get_analysis_context(
        self,
        code: str,
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取股票分析所需的上下文数据（为AI分析器准备）
        """

        if target_date is None:
            target_date = date.today()
        daily_recent_data = self.get_latest_daily_data(code, 2)
        if not daily_recent_data:
            logger.warning(f"daily data is null for {code}")
            return None
        daily_today_data = daily_recent_data[0]
        daily_yesterday_data = daily_recent_data[1] if len(daily_recent_data) > 1 else None
        context = {
            'code': code,
            'date': daily_today_data.date.isoformat(),
            'today': daily_today_data.to_dict(),
        }

        analysis_contents = self.get_stock_research_report_analysis(code, 30)
        context['analysis_contents'] = analysis_contents

        if daily_yesterday_data:
            context['yesterday'] = daily_yesterday_data.to_dict()
            # 计算成交量变化(今日成交量 / 昨日成交量）
            # > 1.0: 放量（市场活跃） < 1.0: 缩量（市场冷清） = 1.0: 平量（市场平稳）
            if daily_yesterday_data.volume and daily_yesterday_data.volume > 0:
                volume_ratio = daily_today_data.volume / daily_yesterday_data.volume
                context['daily_volume_change_ratio'] = round(volume_ratio, 2)  # 保留2位小数
            # 计算价格变化百分比 公式：(今日收盘价 - 昨日收盘价) / 昨日收盘价 × 100%
            if daily_yesterday_data.close and daily_yesterday_data.close > 0:
                price_change_pct = (daily_today_data.close - daily_yesterday_data.close) / daily_yesterday_data.close * 100
                context['daily_price_change_ratio'] = round(price_change_pct, 2)  # 保留2位小数
            # 分析均线形态
            context['daily_ma_status'] = self._analyze_ma_status("ma",
                daily_today_data.close, daily_today_data.ma5, daily_today_data.ma10,
                daily_today_data.ma20, daily_today_data.ma50, daily_today_data.ma120,
                daily_today_data.ma200,
            )
            context['daily_ema_status'] = self._analyze_ma_status("ema",
                daily_today_data.close, daily_today_data.ema5, daily_today_data.ema10,
                daily_today_data.ema20, daily_today_data.ema50, daily_today_data.ema120,
                daily_today_data.ema200,
            )

        week_recent_data = self.get_latest_weekly_data(code, 3)
        if week_recent_data:
            week_today_data = week_recent_data[0]
            context["week_today_data"] = week_today_data.to_dict()
            if len(week_recent_data) >= 2:
                week_yesterday_data = week_recent_data[1]
                if week_yesterday_data:
                    context['week_yesterday'] = week_yesterday_data.to_dict()
                    # 计算成交量变化(今日成交量 / 昨日成交量）
                    # > 1.0: 放量（市场活跃） < 1.0: 缩量（市场冷清） = 1.0: 平量（市场平稳）
                    if week_yesterday_data.volume and week_yesterday_data.volume > 0:
                        volume_ratio = week_today_data.volume / week_yesterday_data.volume
                        context['weekly_volume_change_ratio'] = round(volume_ratio, 2)  # 保留2位小数
                    # 计算价格变化百分比 公式：(今日收盘价 - 昨日收盘价) / 昨日收盘价 × 100%
                    if week_yesterday_data.close and week_yesterday_data.close > 0:
                        price_change_pct = (
                           week_today_data.close - week_yesterday_data.close) / week_yesterday_data.close * 100
                        context['weekly_price_change_ratio'] = round(price_change_pct, 2)  # 保留2位小数
                    # 分析均线形态
                    context['weekly_ma_status'] = self._analyze_ma_status("ma",
                        week_today_data.close, week_today_data.ma5,week_today_data.ma10,week_today_data.ma20,
                        week_today_data.ma50,week_today_data.ma120,week_today_data.ma200,)
                    context['weekly_ema_status'] = self._analyze_ma_status("ema",
                        week_today_data.close, week_today_data.ema5,
                        week_today_data.ema10,week_today_data.ema20,
                        week_today_data.ema50,week_today_data.ema120,week_today_data.ema200,)
        else :
            logger.warning(f"daily data is null for {code}")

        month_recent_data = self.get_latest_month_data(code, 3)
        if month_recent_data:
            month_today_data = month_recent_data[0]
            context["month_today_data"] = month_today_data.to_dict()
            if len(month_recent_data) >= 2:
                month_yesterday_data = month_recent_data[1]
                if month_yesterday_data:
                    context['month_yesterday'] = month_yesterday_data.to_dict()
                    # 计算成交量变化(今日成交量 / 昨日成交量）
                    # > 1.0: 放量（市场活跃） < 1.0: 缩量（市场冷清） = 1.0: 平量（市场平稳）
                    if month_yesterday_data.volume and month_yesterday_data.volume > 0:
                        volume_ratio = month_today_data.volume / month_yesterday_data.volume
                        context['month_volume_change_ratio'] = round(volume_ratio, 2)  # 保留2位小数
                    # 计算价格变化百分比 公式：(今日收盘价 - 昨日收盘价) / 昨日收盘价 × 100%
                    if month_yesterday_data.close and month_yesterday_data.close > 0:
                        price_change_pct = (
                             month_today_data.close - month_yesterday_data.close) / month_yesterday_data.close * 100
                        context['month_price_change_ratio'] = round(price_change_pct, 2)  # 保留2位小数
                    # 分析均线形态
                    context['month_ma_status'] = self._analyze_ma_status("ma",
                        month_today_data.close, month_today_data.ma5,month_today_data.ma10,
                        month_today_data.ma20,month_today_data.ma50, month_today_data.ma120,month_today_data.ma200)
                    context['month_ema_status'] = self._analyze_ma_status("ema",
                        month_today_data.close, month_today_data.ema5,month_today_data.ema10,
                        month_today_data.ema20,month_today_data.ema50,month_today_data.ema120, month_today_data.ema200)
        else:
            logger.warning(f"month data is null for {code}")
        return context

    def _analyze_ma_status(self, ma_type: str, close, ma5, ma10, ma20, ma50, ma120, ma200: Any)->str:
        """
        分析移动平均线形态（技术分析核心方法）
        设计模式：技术指标分析 (Technical Indicator Analysis)
        核心功能：
        1. 分析股票价格的短期、中期、长期趋势
        2. 判断均线排列形态（多头/空头/震荡）
        3. 为交易决策提供技术面依据
        技术分析原理：
        移动平均线 (Moving Average, MA) 是趋势跟踪指标：
        • MA5: 5日移动平均线 → 短期趋势（1周）
        • MA10: 10日移动平均线 → 短期趋势（2周）
        • MA20: 20日移动平均线 → 中期趋势（1个月）
        • 价格在均线之上：支撑作用
        • 价格在均线之下：压力作用

        均线排列形态分类：
        1. 多头排列 (Bullish Alignment): 价格 > MA5 > MA10 > MA20
            - 强烈看涨信号，上升趋势确立
            - 均线呈发散状，趋势强度递增
            - 适合买入或持有

        2. 空头排列 (Bearish Alignment): 价格 < MA5 < MA10 < MA20
            - 强烈看跌信号，下降趋势确立
            - 均线呈发散状，下跌趋势强劲
            - 适合卖出或观望

        3. 短期向好 (Short-term Bullish): 价格 > MA5 且 MA5 > MA10
            - 短期趋势向上，但中长期不确定
            - 可能处于上升初期或反弹阶段
            - 谨慎乐观，需要更多确认

        4. 短期走弱 (Short-term Bearish): 价格 < MA5 且 MA5 < MA10
            - 短期趋势向下，但中长期不确定
            - 可能处于下跌初期或回调阶段
            - 谨慎对待，防范风险

        5. 震荡整理 (Consolidation): 其他情况
            - 趋势不明，均线缠绕
            - 市场处于盘整阶段
            - 适合观望，等待方向选择

        判断逻辑优先级：
        1. 先检查多头排列（最强看涨信号）
        2. 再检查空头排列（最强看跌信号）
        3. 然后检查短期趋势
        4. 最后默认震荡整理

        使用场景：
        1. 自动交易系统：作为买入/卖出信号
        2. 分析报告：提供技术面分析结论
        3. 风险控制：判断市场趋势，调整仓位
        4. AI分析：为机器学习模型提供特征

        注意事项：
        1. 均线分析是滞后指标，反映历史趋势
        2. 需要结合其他指标（成交量、MACD等）综合判断
        3. 在震荡市中均线可能频繁交叉，产生虚假信号
        4. 不同周期的均线组合可以提供多时间框架分析

        Args:
            data: StockDaily 对象
                必须包含close、ma5、ma10、ma20字段
                如果字段为None，会转换为0（避免TypeError）

        Returns:
            str: 均线形态描述字符串（包含表情符号增强可读性）
                可能返回值：
                - "多头排列 📈"    (强烈看涨)
                - "空头排列 📉"    (强烈看跌)
                - "短期向好 🔼"    (短期看涨)
                - "短期走弱 🔽"    (短期看跌)
                - "震荡整理 ↔️"    (趋势不明)
        """
        # 步骤1：提取价格和均线值（处理None值）
        # 使用or 0将None转换为0，避免条件判断时的TypeError
        close = close or 0  # 当前收盘价
        ma5 = ma5 or 0  # 5日移动平均线
        ma10 = ma10 or 0  # 10日移动平均线
        ma20 = ma20 or 0  # 20日移动平均线
        ma50 = ma50 or 0  # 50日移动平均线
        ma120 = ma120 or 0  # 120日移动平均线
        ma200 = ma200 or 0  # 200日移动平均线

        # 调试日志：记录均线值（用于问题排查）
        # 注意：这里使用warning级别，生产环境可改为debug
        logger.debug(f"_analyze_ma_status - ma type{ma_type},Close:{close}, MA5:{ma5}, MA10:{ma10}, MA20:{ma20} "
                     f"MA50:{ma50} MA120:{ma120} MA200:{ma200}")

        # 步骤2：判断均线形态（按优先级）

        # 条件1：多头排列（最强看涨信号）
        # 标准：价格 > MA5 > MA10 > MA20 > 0
        # > 0 检查确保均线值为正数（避免除零或无效数据）
        if ma200 > 0 and close > ma5 > ma10 > ma20 > ma50 > ma120 > ma200 > 0:
            return "多头排列 📈长期看涨"
        if ma120 > 0 and close > ma5 > ma10 > ma20 > ma50 > ma120 > 0:
            return "多头排列 📈中长期看涨"
        if ma50 > 0 and close > ma5 > ma10 > ma20 > ma50 > 0:
            return "多头排列 📈中长期看涨"
        if close > ma5 > ma10 > ma20  > 0:
            return "多头排列 📈中期看涨"  # 强烈看涨，趋势明确

        # 条件2：空头排列（最强看跌信号）
        # 标准：价格 < MA5 < MA10 < MA20 且 MA20 > 0
        # MA20 > 0 确保是有效的空头排列（不是数据缺失）
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "空头排列 📉"  # 强烈看跌，趋势明确

        # 条件3：短期向好（价格在MA5之上，且MA5在MA10之上）
        # 标准：close > ma5 and ma5 > ma10
        # 表示短期趋势向上，但中长期趋势不确定
        elif close > ma5 and ma5 > ma10:
            return "短期向好 🔼"  # 短期看涨，需要确认

        # 条件4：短期走弱（价格在MA5之下，且MA5在MA10之下）
        # 标准：close < ma5 and ma5 < ma10
        # 表示短期趋势向下，但中长期趋势不确定
        elif close < ma5 and ma5 < ma10:
            return "短期走弱 🔽"  # 短期看跌，防范风险

        # 条件5：其他情况（震荡整理）
        # 均线缠绕，趋势不明，处于盘整阶段
        else:
            return "震荡整理 ↔️"  # 趋势不明，观望为主



# ===== 便捷函数 (Convenience Function) ====================================

def get_db() -> DatabaseManager:
    """
    获取数据库管理器单例实例的便捷函数

    设计目的：
    1. 简化数据库访问：一行代码获取数据库管理器
    2. 统一访问入口：确保所有模块使用相同的获取方式
    3. 隐藏实现细节：调用者无需了解单例模式的实现
    4. 类型安全：明确的返回类型注解，便于IDE提示和类型检查

    使用示例：
        # 导入便捷函数
        from storage import get_db

        # 获取数据库管理器
        db = get_db()

        # 使用数据库功能
        has_data = db.has_today_data('600519')
        context = db.get_analysis_context('600519')

    实现原理：
    内部调用 DatabaseManager.get_instance() 方法
    该方法实现单例模式，确保全局只有一个数据库连接实例

    为什么推荐使用此函数？
    1. 更简洁：get_db() 比 DatabaseManager.get_instance() 更短
    2. 更直观：函数名明确表达其功能
    3. 更稳定：如果实现方式改变，只需修改此函数

    Returns:
        DatabaseManager: 数据库管理器单例实例
    """
    return DatabaseManager().get_instance()

if __name__ == '__main__':

    dbM = get_db("")
    print("=" * 60)
    print("  存储模块 (storage.py) 功能测试")
    print("=" * 60)
    print(f"✓ 数据库初始化成功")

    # ========== 测试用例1：检查今日数据（断点续传逻辑测试）==========
    is_exist = dbM.is_date_exist('600519', "week")
    print(f"600519 week数据是否存在 {is_exist}")

    print("\n 验证upsert操作")
    # 数据结构与真实股票数据一致，包含所有必需字段
    test_df = pd.DataFrame({
        'date': [date.today()],  # 交易日期：今天
        'open': [1800.0],  # 开盘价：1800元
        'high': [1850.0],  # 最高价：1850元
        'low': [1780.0],  # 最低价：1780元
        'close': [1820.0],  # 收盘价：1820元（最重要指标）
        'volume': [10000000],  # 成交量：1000万股
        'amount': [18200000000],  # 成交额：182亿元
        'pct_chg': [1.5],  # 涨跌幅：+1.5%
        'ma5': [1810.0],  # 5日移动平均线
        'ma10': [1800.0],  # 10日移动平均线
        'ma20': [1790.0],  # 20日移动平均线
        'volume_ratio': [1.2],  # 量比：1.2（放量）
    })
    saved = dbM.save_daily_forecast(test_df, "600519", "test")
    print(f"保存测试数据结果： {saved}")

