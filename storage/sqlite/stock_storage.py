# -*- coding: utf-8 -*-
"""
数据库管理
"""
import os
import io
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Mapping
from utils.common import parse_row_date

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
    Text,
    JSON,
    Index,
    UniqueConstraint,
    select,
    and_,
    desc,
    delete,
    inspect,
    text,
    or_,
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
    # 注意：code/forecast_date 不能单列 unique（否则一只股票只能有一条预测），
    # 唯一性由下面的 (code, forecast_date) 复合唯一约束保证
    code = Column(String(10), nullable=False, index=True)
    forecast_date = Column(Date, nullable=False, index=True)
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

# === 利润表模型 ===
class StockIncome(Base):
    """
    股票利润表模型 - ORM映射类

    数据库表: stock_income
    功能：存储公司利润表数据

    字段说明：
    • 标识字段：code（股票代码）、report_date（报告日期）
    • 利润数据：total_revenue（营业收入，单位：元）、operating_profit（营业利润，单位：元）、net_profit（净利润，单位：元）
    • 费用数据：sell_exp（销售费用）、admin_exp（管理费用）、rd_exp（研发费用）、fin_exp（财务费用），单位均为元
    • 元数据：data_source（数据来源）、updated_at（更新时间）
    """
    __tablename__ = 'stock_income'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    total_revenue = Column(Float)  # 营业收入（单位：元）
    operating_profit = Column(Float)  # 营业利润（单位：元）
    net_profit = Column(Float)  # 净利润（单位：元）
    basic_eps = Column(Float)  # 基本每股收益（单位：元）
    sell_exp = Column(Float)  # 销售费用（单位：元）
    admin_exp = Column(Float)  # 管理费用（单位：元）
    rd_exp = Column(Float)  # 研发费用（单位：元）
    fin_exp = Column(Float)  # 财务费用（单位：元）
    revenue_growth = Column(Float)  # 营业收入同比增长率（单位：%）
    profit_growth = Column(Float)  # 净利润同比增长率（单位：%）
    gross_margin = Column(Float)  # 毛利率（单位：%）
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', name='uix_income_code_date'),
        Index('ix_income_code_date', 'code', 'report_date'),
    )

    def __repr__(self):
        return f"<StockIncome(code={self.code}, report_date={self.report_date}, net_profit={self.net_profit})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'report_date': self.report_date,
            'total_revenue': self.total_revenue,
            'operating_profit': self.operating_profit,
            'net_profit': self.net_profit,
            'basic_eps': self.basic_eps,
            'sell_exp': self.sell_exp,
            'admin_exp': self.admin_exp,
            'rd_exp': self.rd_exp,
            'fin_exp': self.fin_exp,
            'revenue_growth': self.revenue_growth,
            'profit_growth': self.profit_growth,
            'gross_margin': self.gross_margin,
            'data_source': self.data_source,
        }

# === 分红送股模型 ===
class StockDividend(Base):
    """
    股票分红送股模型 - ORM映射类

    数据库表: stock_dividend
    功能：存储分红送股数据

    字段说明：
    • 标识字段：code（股票代码）、end_date（报告期）
    • 分红数据：div_procf（每股分红）、stk_bo_rate（送股比例）、stk_co_rate（转增比例）
    • 金额数据：cash_div（现金分红总额）
    • 日期数据：ex_date（除权除息日）、pay_date（派息日）
    • 元数据：updated_at（更新时间）
    """
    __tablename__ = 'stock_dividend'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    end_date = Column(Date, nullable=False, index=True)
    div_procf = Column(Float)  # 每股分红
    stk_bo_rate = Column(Float)  # 送股比例
    stk_co_rate = Column(Float)  # 转赠比例
    cash_div = Column(Float)  # 现金分红总额
    ex_date = Column(Date)  # 除权除息日
    pay_date = Column(Date)  # 派息日
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'end_date', name='uix_dividend_code_date'),
        Index('ix_dividend_code_date', 'code', 'end_date'),
    )

    def __repr__(self):
        return f"<StockDividend(code={self.code}, end_date={self.end_date}, div_procf={self.div_procf})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'end_date': self.end_date,
            'div_procf': self.div_procf,
            'stk_bo_rate': self.stk_bo_rate,
            'stk_co_rate': self.stk_co_rate,
            'cash_div': self.cash_div,
            'ex_date': self.ex_date,
            'pay_date': self.pay_date,
        }

# === 财务审计意见模型 ===
class StockFinaAudit(Base):
    """
    股票财务审计意见模型 - ORM映射类

    数据库表: stock_fina_audit
    功能：存储财务审计意见数据

    字段说明：
    • 标识字段：code（股票代码）、end_date（报告期）
    • 审计数据：audit_opinion（审计意见）、opinions（审计意见全文基数）、auditor（审计事务所）
    • 金额数据：audit_fee（审计费用）
    • 元数据：updated_at（更新时间）
    """
    __tablename__ = 'stock_fina_audit'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    end_date = Column(Date, nullable=False, index=True)
    audit_opinion = Column(String(100))  # 审计意见
    opinions = Column(Text)  # 审计意见全文基数
    auditor = Column(String(100))  # 审计事务所
    audit_fee = Column(Float)  # 审计费用
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'end_date', name='uix_fina_audit_code_date'),
        Index('ix_fina_audit_code_date', 'code', 'end_date'),
    )

    def __repr__(self):
        return f"<StockFinaAudit(code={self.code}, end_date={self.end_date}, audit_opinion={self.audit_opinion})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'end_date': self.end_date,
            'audit_opinion': self.audit_opinion,
            'opinions': self.opinions,
            'auditor': self.auditor,
            'audit_fee': self.audit_fee,
        }

# === 财报披露计划模型 ===
class StockDisclosureDate(Base):
    """
    股票财报披露计划模型 - ORM映射类

    数据库表: stock_disclosure_date
    功能：存储财报披露计划数据

    字段说明：
    • 标识字段：code（股票代码）、end_date（报告期）
    • 日期数据：stm_issue_date（首次披露日）、stm_comm_date（董事会公告日）、actual_diss_date（实际披露日）
    • 元数据：updated_at（更新时间）
    """
    __tablename__ = 'stock_disclosure_date'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    end_date = Column(Date, nullable=False, index=True)
    stm_issue_date = Column(Date)  # 首次披露日
    stm_comm_date = Column(Date)  # 董事会公告日
    actual_diss_date = Column(Date)  # 实际披露日
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'end_date', name='uix_disclosure_date_code_date'),
        Index('ix_disclosure_date_code_date', 'code', 'end_date'),
    )

    def __repr__(self):
        return f"<StockDisclosureDate(code={self.code}, end_date={self.end_date}, stm_issue_date={self.stm_issue_date})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'end_date': self.end_date,
            'stm_issue_date': self.stm_issue_date,
            'stm_comm_date': self.stm_comm_date,
            'actual_diss_date': self.actual_diss_date,
        }

# === 资产负债表模型 ===
class StockBalanceSheet(Base):
    """
    股票资产负债表模型 - ORM映射类

    数据库表: stock_balance_sheet
    功能：存储公司资产负债表数据

    字段说明：
    • 标识字段：code（股票代码）、report_date（报告日期）
    • 资产数据：total_assets（总资产，单位：元）、current_assets（流动资产，单位：元）
    • 负债数据：total_liabilities（总负债，单位：元）、current_liabilities（流动负债，单位：元）
    • 权益数据：total_equity（所有者权益，单位：元）
    • 元数据：data_source（数据来源）、updated_at（更新时间）
    """
    __tablename__ = 'stock_balance_sheet'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    total_assets = Column(Float)  # 总资产（单位：元）
    current_assets = Column(Float)  # 流动资产（单位：元）
    non_current_assets = Column(Float)  # 非流动资产（单位：元）
    total_liabilities = Column(Float)  # 总负债（单位：元）
    current_liabilities = Column(Float)  # 流动负债（单位：元）
    non_current_liabilities = Column(Float)  # 非流动负债（单位：元）
    total_equity = Column(Float)  # 所有者权益（单位：元）
    asset_liability_ratio = Column(Float)  # 资产负债率（单位：%）
    current_ratio = Column(Float)  # 流动比率（倍数）
    accounts_receivable = Column(Float)  # 应收账款（单位：元，营运资本趋势用）
    inventory = Column(Float)  # 存货（单位：元，营运资本趋势用）
    fixed_assets = Column(Float)  # 固定资产（单位：元，产能扩张分析用）
    construction_in_progress = Column(Float)  # 在建工程（单位：元，产能扩张分析用）
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', name='uix_balance_code_date'),
        Index('ix_balance_code_date', 'code', 'report_date'),
    )

    def __repr__(self):
        return f"<StockBalanceSheet(code={self.code}, report_date={self.report_date}, total_assets={self.total_assets})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'report_date': self.report_date,
            'total_assets': self.total_assets,
            'current_assets': self.current_assets,
            'non_current_assets': self.non_current_assets,
            'total_liabilities': self.total_liabilities,
            'current_liabilities': self.current_liabilities,
            'non_current_liabilities': self.non_current_liabilities,
            'total_equity': self.total_equity,
            'asset_liability_ratio': self.asset_liability_ratio,
            'current_ratio': self.current_ratio,
            'data_source': self.data_source,
        }

# === 现金流量表模型 ===
class StockCashflow(Base):
    """
    股票现金流量表模型 - ORM映射类

    数据库表: stock_cashflow
    功能：存储公司现金流量表数据（报告期累计口径）

    字段说明：
    • 标识字段：code（股票代码）、report_date（报告日期）
    • 现金流数据（单位均为元）：
      operating_cashflow（经营活动现金流净额，tushare n_cashflow_act）
      investing_cashflow（投资活动现金流净额，tushare n_cashflow_inv_act）
      financing_cashflow（筹资活动现金流净额，tushare n_cash_flows_fnc_act）
      capex（购建固定资产、无形资产和其他长期资产支付的现金，tushare c_pay_acq_const_fids）
      free_cashflow（自由现金流，tushare 计算值，可能为空）
    • 元数据：data_source（数据来源）、updated_at（更新时间）
    """
    __tablename__ = 'stock_cashflow'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    operating_cashflow = Column(Float)  # 经营活动现金流净额（单位：元）
    investing_cashflow = Column(Float)  # 投资活动现金流净额（单位：元）
    financing_cashflow = Column(Float)  # 筹资活动现金流净额（单位：元）
    capex = Column(Float)  # 购建固定资产无形资产等支付的现金（资本开支，单位：元）
    free_cashflow = Column(Float)  # 自由现金流（单位：元，可能为空）
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', name='uix_cashflow_code_date'),
        Index('ix_cashflow_code_date', 'code', 'report_date'),
    )

    def __repr__(self):
        return f"<StockCashflow(code={self.code}, report_date={self.report_date}, operating_cashflow={self.operating_cashflow})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'report_date': self.report_date,
            'operating_cashflow': self.operating_cashflow,
            'investing_cashflow': self.investing_cashflow,
            'financing_cashflow': self.financing_cashflow,
            'capex': self.capex,
            'free_cashflow': self.free_cashflow,
            'data_source': self.data_source,
        }

class WatchTarget(Base):
    """
    监控清单模型：用户要求重点监控的公司/行业

    target_type: company=个股（code 必填）/ industry=行业（用 name+keywords 做新闻搜索）
    """
    __tablename__ = 'watch_target'

    id = Column(Integer, primary_key=True, autoincrement=True)
    target_type = Column(String(10), nullable=False, default='company')  # company / industry
    code = Column(String(10), index=True)  # 股票代码（行业类为空）
    name = Column(String(50), nullable=False)  # 公司名/行业名
    keywords = Column(String(200))  # 额外搜索关键词（可选）
    enabled = Column(Integer, nullable=False, default=1)  # 1=启用 0=停用
    source = Column(String(10), nullable=False, default='manual')  # manual=用户手动 / auto=分析流程自动
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('target_type', 'name', name='uix_watch_type_name'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'target_type': self.target_type,
            'code': self.code,
            'name': self.name,
            'keywords': self.keywords,
            'enabled': bool(self.enabled),
            'source': self.source,
        }


class MonitorEvent(Base):
    """
    监控事件模型：已产生/已推送的监控事件，dedup_key 唯一约束防重复推送
    """
    __tablename__ = 'monitor_event'

    id = Column(Integer, primary_key=True, autoincrement=True)
    target = Column(String(50), nullable=False, index=True)  # 关联的监控标的（名字或代码）
    event_type = Column(String(20), nullable=False)  # signal / news / policy
    dedup_key = Column(String(128), nullable=False, unique=True)  # 去重键
    title = Column(String(300))
    content = Column(Text)
    importance = Column(String(10))  # 高 / 中 / 低
    pushed_at = Column(DateTime)  # 实际推送时间（未推送为空）
    created_at = Column(DateTime, default=datetime.now, index=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'target': self.target,
            'event_type': self.event_type,
            'title': self.title,
            'importance': self.importance,
            'pushed_at': self.pushed_at,
        }


class AnnouncementText(Base):
    """
    公告正文缓存：产销快报等公告 PDF 抽出的文本，按 (code, title) 唯一。
    同一份公告只从巨潮下载一次，之后直接读库。
    """
    __tablename__ = 'announcement_text'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    title = Column(String(300), nullable=False)
    ann_time = Column(String(30))   # 公告时间（原文格式）
    url = Column(String(500))
    content = Column(Text)          # PDF 抽取的正文
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'title', name='uix_ann_code_title'),
    )


class StockFinaIndicator(Base):
    """
    股票财务指标模型
    数据库表: stock_fina_indicator
    功能：存储Tushare fina_indicator 108项财务指标的核心字段
    """
    __tablename__ = 'stock_fina_indicator'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    ann_date = Column(Date)
    eps = Column(Float)
    dt_eps = Column(Float)
    total_revenue_ps = Column(Float)
    revenue_ps = Column(Float)
    capital_rese_ps = Column(Float)
    undist_profit_ps = Column(Float)
    extra_item = Column(Float)
    profit_dedt = Column(Float)
    gross_margin = Column(Float)
    current_ratio = Column(Float)
    quick_ratio = Column(Float)
    cash_ratio = Column(Float)
    ar_turn = Column(Float)
    ca_turn = Column(Float)
    fa_turn = Column(Float)
    assets_turn = Column(Float)
    inv_turn = Column(Float)
    roe = Column(Float)
    roe_waa = Column(Float)
    roe_dt = Column(Float)
    roa = Column(Float)
    rop = Column(Float)
    netprofit_margin = Column(Float)
    grossprofit_margin = Column(Float)
    profit_to_gr = Column(Float)
    saleexp_to_gr = Column(Float)
    adminexp_of_gr = Column(Float)
    finaexp_of_gr = Column(Float)
    impai_ttm = Column(Float)
    op_of_gr = Column(Float)
    ebit_of_gr = Column(Float)
    debt_to_assets = Column(Float)
    debt_to_eqy = Column(Float)
    n_cashflow_to_liab = Column(Float)
    mbrg = Column(Float)
    nprg = Column(Float)
    seg = Column(Float)
    profit_yoy = Column(Float)
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', name='uix_fina_code_date'),
        Index('ix_fina_code_date', 'code', 'report_date'),
    )

    def __repr__(self):
        return f"<StockFinaIndicator(code={self.code}, report_date={self.report_date}, roe={self.roe})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'report_date': self.report_date,
            'ann_date': self.ann_date,
            'eps': self.eps,
            'dt_eps': self.dt_eps,
            'total_revenue_ps': self.total_revenue_ps,
            'revenue_ps': self.revenue_ps,
            'gross_margin': self.gross_margin,
            'current_ratio': self.current_ratio,
            'quick_ratio': self.quick_ratio,
            'cash_ratio': self.cash_ratio,
            'ar_turn': self.ar_turn,
            'ca_turn': self.ca_turn,
            'fa_turn': self.fa_turn,
            'assets_turn': self.assets_turn,
            'inv_turn': self.inv_turn,
            'roe': self.roe,
            'roe_waa': self.roe_waa,
            'roe_dt': self.roe_dt,
            'roa': self.roa,
            'rop': self.rop,
            'netprofit_margin': self.netprofit_margin,
            'grossprofit_margin': self.grossprofit_margin,
            'debt_to_assets': self.debt_to_assets,
            'debt_to_eqy': self.debt_to_eqy,
            'n_cashflow_to_liab': self.n_cashflow_to_liab,
            'mbrg': self.mbrg,
            'nprg': self.nprg,
            'seg': self.seg,
            'profit_yoy': self.profit_yoy,
            'data_source': self.data_source,
        }


class StockMainBusiness(Base):
    """
    股票主营业务构成模型
    数据库表: stock_main_business
    功能：存储按产品/地区拆分的收入、成本、毛利
    """
    __tablename__ = 'stock_main_business'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    bz_type = Column(String(5), nullable=False)
    bz_item = Column(String(100), nullable=False)
    bz_sales = Column(Float)
    bz_profit = Column(Float)
    bz_cost = Column(Float)
    gross_margin = Column(Float)
    sales_ratio = Column(Float)
    curr_type = Column(String(10))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', 'bz_type', 'bz_item', name='uix_mb_code_date_type_item'),
        Index('ix_mb_code_date', 'code', 'report_date'),
    )

    def __repr__(self):
        return f"<StockMainBusiness(code={self.code}, report_date={self.report_date}, item={self.bz_item})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'report_date': self.report_date,
            'bz_type': self.bz_type,
            'bz_item': self.bz_item,
            'bz_sales': self.bz_sales,
            'bz_profit': self.bz_profit,
            'bz_cost': self.bz_cost,
            'gross_margin': self.gross_margin,
            'sales_ratio': self.sales_ratio,
            'curr_type': self.curr_type,
            'data_source': self.data_source,
        }


class StockHolderNumber(Base):
    """
    股东户数模型
    数据库表: stock_holder_number
    功能：存储每期末股东户数变化
    """
    __tablename__ = 'stock_holder_number'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    ann_date = Column(Date)
    holder_num = Column(Float)
    holder_num_change = Column(Float)
    holder_num_change_ratio = Column(Float)
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', name='uix_holder_num_code_date'),
        Index('ix_holder_num_code_date', 'code', 'report_date'),
    )

    def __repr__(self):
        return f"<StockHolderNumber(code={self.code}, report_date={self.report_date}, holder_num={self.holder_num})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'report_date': self.report_date,
            'ann_date': self.ann_date,
            'holder_num': self.holder_num,
            'holder_num_change': self.holder_num_change,
            'holder_num_change_ratio': self.holder_num_change_ratio,
            'data_source': self.data_source,
        }


class StockNorthboundHold(Base):
    """
    北向持股模型
    数据库表: stock_northbound_hold
    功能：存储沪深港通每日持股数据
    """
    __tablename__ = 'stock_northbound_hold'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    name = Column(String(50))
    vol = Column(Float)
    ratio = Column(Float)
    exchange = Column(String(10))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_north_code_date'),
        Index('ix_north_code_date', 'code', 'trade_date'),
    )

    def __repr__(self):
        return f"<StockNorthboundHold(code={self.code}, trade_date={self.trade_date}, ratio={self.ratio})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'trade_date': self.trade_date,
            'name': self.name,
            'vol': self.vol,
            'ratio': self.ratio,
            'exchange': self.exchange,
            'data_source': self.data_source,
        }


class StockTop10Holder(Base):
    """
    十大股东模型
    数据库表: stock_top10_holder
    功能：存储定期报告披露的十大股东/十大流通股东数据
    """
    __tablename__ = 'stock_top10_holder'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)
    ann_date = Column(Date)
    holder_type = Column(String(20), nullable=False)
    holder_name = Column(String(200), nullable=False)
    hold_amount = Column(Float)
    hold_ratio = Column(Float)
    hold_float_ratio = Column(Float)
    hold_change = Column(String(20))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', 'holder_type', 'holder_name', name='uix_top10_code_date_type_name'),
        Index('ix_top10_code_date', 'code', 'report_date'),
    )

    def __repr__(self):
        return f"<StockTop10Holder(code={self.code}, report_date={self.report_date}, name={self.holder_name})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'report_date': self.report_date,
            'ann_date': self.ann_date,
            'holder_type': self.holder_type,
            'holder_name': self.holder_name,
            'hold_amount': self.hold_amount,
            'hold_ratio': self.hold_ratio,
            'hold_float_ratio': self.hold_float_ratio,
            'hold_change': self.hold_change,
            'data_source': self.data_source,
        }


class IndustryValuation(Base):
    """
    行业估值缓存模型
    数据库表: industry_valuation
    功能：存储申万行业每日估值（PE/PB/股息率等），用于同业对比
    """
    __tablename__ = 'industry_valuation'

    id = Column(Integer, primary_key=True, autoincrement=True)
    industry_code = Column(String(20), nullable=False, index=True)
    industry_name = Column(String(50))
    trade_date = Column(Date, nullable=False, index=True)
    pe_static = Column(Float)
    pe_ttm = Column(Float)
    pb = Column(Float)
    dividend_ratio = Column(Float)
    stock_count = Column(Integer)
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('industry_code', 'trade_date', name='uix_iv_code_date'),
        Index('ix_iv_code_date', 'industry_code', 'trade_date'),
    )

    def __repr__(self):
        return f"<IndustryValuation(industry={self.industry_name}, date={self.trade_date}, pe_ttm={self.pe_ttm})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'industry_code': self.industry_code,
            'industry_name': self.industry_name,
            'trade_date': self.trade_date,
            'pe_static': self.pe_static,
            'pe_ttm': self.pe_ttm,
            'pb': self.pb,
            'dividend_ratio': self.dividend_ratio,
            'stock_count': self.stock_count,
            'data_source': self.data_source,
        }


class NewEnergyPenetration(Base):
    """
    新能源车渗透率模型
    数据库表: new_energy_penetration
    功能：存储新能源车月度销量及渗透率数据
    """
    __tablename__ = 'new_energy_penetration'

    id = Column(Integer, primary_key=True, autoincrement=True)
    month = Column(Date, nullable=False, index=True)
    total_sales = Column(Float)
    new_energy_sales = Column(Float)
    penetration_rate = Column(Float)
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('month', name='uix_nep_month'),
    )

    def __repr__(self):
        return f"<NewEnergyPenetration(month={self.month}, rate={self.penetration_rate})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'month': self.month,
            'total_sales': self.total_sales,
            'new_energy_sales': self.new_energy_sales,
            'penetration_rate': self.penetration_rate,
            'data_source': self.data_source,
        }


class VehicleMonthlySales(Base):
    """懂车帝-全国车型月销量数据"""
    __tablename__ = 'vehicle_monthly_sales'

    id = Column(Integer, primary_key=True, autoincrement=True)
    month = Column(String(7), nullable=False, index=True)
    series_name = Column(String(50), nullable=False)
    brand_name = Column(String(50))
    sales_volume = Column(Integer)
    min_price = Column(Float)
    max_price = Column(Float)
    price_range = Column(String(50))
    rank = Column(Integer)
    series_id = Column(Integer)
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('month', 'series_name', name='uix_vms_month_series'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockRepurchase(Base):
    """股票回购"""
    __tablename__ = 'stock_repurchase'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    ann_date = Column(Date, nullable=False)
    end_date = Column(Date)
    proc = Column(String(50))
    exp_date = Column(Date)
    vol = Column(Float)
    amount = Column(Float)
    high_limit = Column(Float)
    low_limit = Column(Float)
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'ann_date', name='uix_rep_code_date'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockShareFloat(Base):
    """限售解禁"""
    __tablename__ = 'stock_share_float'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    ann_date = Column(Date)
    float_date = Column(Date, nullable=False, index=True)
    float_share = Column(Float)
    float_ratio = Column(Float)
    holder_name = Column(String(200))
    share_type = Column(String(50))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'float_date', 'holder_name', name='uix_sf_code_date_holder'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockBrokerReco(Base):
    """分析师月度评级"""
    __tablename__ = 'stock_broker_reco'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    month = Column(String(6), nullable=False, index=True)
    broker = Column(String(100), nullable=False)
    name = Column(String(50))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'month', 'broker', name='uix_br_code_month_broker'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockPledge(Base):
    """股权质押统计"""
    __tablename__ = 'stock_pledge'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    end_date = Column(Date, nullable=False, index=True)
    pledge_count = Column(Integer)
    unrest_pledge = Column(Float)
    rest_pledge = Column(Float)
    total_share = Column(Float)
    pledge_ratio = Column(Float)
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'end_date', name='uix_pg_code_date'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockBlockTrade(Base):
    """大宗交易"""
    __tablename__ = 'stock_block_trade'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    price = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    buyer = Column(String(200))
    seller = Column(String(200))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', 'price', 'vol', name='uix_bt_code_date_price'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockPledgeDetail(Base):
    """股权质押明细数据"""
    __tablename__ = 'stock_pledge_detail'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    end_date = Column(Date, nullable=False, index=True)
    pledger = Column(String(100))       # 出质人
    pledge_amount = Column(Float)       # 质押数量（万股）
    pledge_ratio = Column(Float)        # 占所持股份比例(%)
    pledge_total_ratio = Column(Float)  # 占总股本比例(%)
    pledge_start_date = Column(Date)    # 质押开始日期
    pledge_end_date = Column(Date)      # 质押到期日期
    pledge_status = Column(String(20))  # 质押状态
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'end_date', 'pledger', name='uix_pledge_detail_code_date'),
        Index('ix_pledge_detail_code_date', 'code', 'end_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockHolderTrade(Base):
    """股东增减持数据"""
    __tablename__ = 'stock_holder_trade'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    ann_date = Column(Date, nullable=False, index=True)
    holder_name = Column(String(100))   # 股东名称
    trade_type = Column(String(10))     # 交易类型 IN增持 DE减持
    trade_volume = Column(Float)        # 变动数量（万股）
    trade_ratio = Column(Float)         # 变动比例(%)
    after_ratio = Column(Float)         # 变动后持股比例(%)
    avg_price = Column(Float)           # 均价（元）
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'ann_date', 'holder_name', name='uix_holder_trade_code_date'),
        Index('ix_holder_trade_code_date', 'code', 'ann_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockReportRc(Base):
    """卖方盈利预测数据"""
    __tablename__ = 'stock_report_rc'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    report_date = Column(Date, nullable=False, index=True)        # 研报日期
    forecast_type = Column(String(20))                           # 预测类型（营收/净利润/EPS等）
    forecast_value = Column(Float)                               # 预测值（亿元/元）
    forecast_org = Column(String(100))                           # 机构名称
    analyst = Column(String(100))                                # 分析师
    rating = Column(String(20))                                  # 评级（买入/增持/中性等）
    rating_change = Column(String(20))                           # 评级变动
    target_price = Column(Float)                                 # 目标价（元）
    period = Column(String(20))                                  # 预测年份/期间
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'report_date', 'forecast_org', 'forecast_type', name='uix_report_rc_code_date'),
        Index('ix_report_rc_code_date', 'code', 'report_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockMargin(Base):
    """融资融券交易汇总"""
    __tablename__ = 'stock_margin'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    margin_balance = Column(Float)           # 融资余额
    margin_buy = Column(Float)               # 融资买入额
    short_sell_balance = Column(Float)       # 融券余额
    short_sell_volume = Column(Float)        # 融券卖出量
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_margin_code_date'),
        Index('ix_margin_code_date', 'code', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockMarginDetail(Base):
    """融资融券交易明细"""
    __tablename__ = 'stock_margin_detail'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    margin_buy = Column(Float)               # 融资买入额
    rzye = Column(Float)                     # 融资余额
    rqye = Column(Float)                     # 融券余额
    rzmre = Column(Float)                    # 融资买入额
    rqyl = Column(Float)                     # 融券余量
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_margin_detail_code_date'),
        Index('ix_margin_detail_code_date', 'code', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockMoneyflow(Base):
    """个股资金流"""
    __tablename__ = 'stock_moneyflow'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    buy_sm_vol = Column(Float)               # 小单买入量
    buy_sm_amount = Column(Float)            # 小单买入金额
    sell_sm_vol = Column(Float)              # 小单卖出量
    sell_sm_amount = Column(Float)           # 小单卖出金额
    buy_md_vol = Column(Float)               # 中单买入量
    buy_md_amount = Column(Float)            # 中单买入金额
    sell_md_vol = Column(Float)              # 中单卖出量
    sell_md_amount = Column(Float)           # 中单卖出金额
    buy_lg_vol = Column(Float)               # 大单买入量
    buy_lg_amount = Column(Float)            # 大单买入金额
    sell_lg_vol = Column(Float)              # 大单卖出量
    sell_lg_amount = Column(Float)           # 大单卖出金额
    buy_elg_vol = Column(Float)              # 特大单买入量
    buy_elg_amount = Column(Float)           # 特大单买入金额
    sell_elg_vol = Column(Float)             # 特大单卖出量
    sell_elg_amount = Column(Float)          # 特大单卖出金额
    net_mf_amount = Column(Float)            # 主力净流入
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_moneyflow_code_date'),
        Index('ix_moneyflow_code_date', 'code', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockHsgtMoneyflow(Base):
    """沪深港通资金流"""
    __tablename__ = 'stock_hsgt_moneyflow'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    ggt_ss = Column(Float)                   # 港股通（沪）
    ggt_sz = Column(Float)                   # 港股通（深）
    hgt = Column(Float)                      # 沪股通
    sgt = Column(Float)                      # 深股通
    north_money = Column(Float)              # 北向资金
    south_money = Column(Float)              # 南向资金
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('trade_date', name='uix_hsgt_moneyflow_date'),
        Index('ix_hsgt_moneyflow_date', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockMktMoneyflowDC(Base):
    """大盘资金流（日频）"""
    __tablename__ = 'stock_mkt_moneyflow_dc'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, nullable=False, index=True)
    close_sh = Column(Float)                 # 上证收盘
    change_pct = Column(Float)               # 涨跌幅
    main_net = Column(Float)                 # 主力净流入
    retail_net = Column(Float)               # 散户净流入
    total_net = Column(Float)                # 总净流入
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('trade_date', name='uix_mkt_moneyflow_dc_date'),
        Index('ix_mkt_moneyflow_dc_date', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockMacroRate(Base):
    """宏观利率数据（Shibor/Libor/Hibor等）"""
    __tablename__ = 'stock_macro_rate'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rate_type = Column(String(20), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    rate_value = Column(Float)               # 利率值
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('rate_type', 'date', name='uix_macro_rate_type_date'),
        Index('ix_macro_rate_type_date', 'rate_type', 'date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockWzIndex(Base):
    """温州指数"""
    __tablename__ = 'stock_wz_index'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    index_value = Column(Float)              # 指数值
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('date', name='uix_wz_index_date'),
        Index('ix_wz_index_date', 'date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockGzIndex(Base):
    """贵阳指数"""
    __tablename__ = 'stock_gz_index'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, index=True)
    index_value = Column(Float)              # 指数值
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('date', name='uix_gz_index_date'),
        Index('ix_gz_index_date', 'date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockFundAdj(Base):
    """复权因子"""
    __tablename__ = 'stock_fund_adj'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(30), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    adj_factor = Column(Float)               # 复权因子
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_fund_adj_code_date'),
        Index('ix_fund_adj_code_date', 'ts_code', 'trade_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockMarginSecs(Base):
    """融资融券标的列表（含ETF）"""
    __tablename__ = 'stock_margin_secs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String(30), nullable=False, index=True)
    name = Column(String(30))
    trade_date = Column(Date, index=True)
    is_etf = Column(String(1))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('ts_code', 'trade_date', name='uix_margin_secs_code_date'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockMacroIndicator(Base):
    """宏观指标数据缓存"""
    __tablename__ = 'stock_macro_indicator'

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_name = Column(String(30), nullable=False, index=True)
    period = Column(String(20), nullable=False)
    value_json = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('indicator_name', 'period', name='uq_macro_indicator_period'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockTopList(Base):
    """龙虎榜每日明细"""
    __tablename__ = 'stock_top_list'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    name = Column(String(50))
    close = Column(Float)
    pct_change = Column(Float)
    turnover_rate = Column(Float)
    amount = Column(Float)
    l_sell = Column(Float)
    l_buy = Column(Float)
    l_amount = Column(Float)
    net_amount = Column(Float)
    net_rate = Column(Float)
    amount_rate = Column(Float)
    float_values = Column(Float)
    reason = Column(String(200))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_tl_code_date'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class StockTopInst(Base):
    """龙虎榜机构席位追踪"""
    __tablename__ = 'stock_top_inst'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    exalter = Column(String(50))
    buy = Column(Float)
    buy_rate = Column(Float)
    sell = Column(Float)
    sell_rate = Column(Float)
    net_buy = Column(Float)
    side = Column(String(10))
    reason = Column(String(100))
    data_source = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', 'trade_date', name='uix_ti_code_date'),
    )

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class AnalysisSnapshot(Base):
    """
    分析快照：每次个股分析完成后留档的「可检验判断」，复盘的对账依据
    JSON 字段（support/resistance/key_reasons/indicators）以 json.dumps 文本存储
    """
    __tablename__ = 'analysis_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    question = Column(String(500))  # 当时的用户问题
    price_at_analysis = Column(Float)  # 分析时点收盘价（前复权）
    short_term_view = Column(String(10))  # 短期方向判断：偏多/中性/偏空
    mid_term_view = Column(String(10))  # 中期方向判断
    support = Column(Text)  # 支撑位列表 JSON
    resistance = Column(Text)  # 压力位列表 JSON
    key_reasons = Column(Text)  # 核心理由列表 JSON
    indicators = Column(Text)  # 当时关键指标快照 JSON（ma_pattern/rsi6/pos_52w等）
    trade_plan = Column(Text)  # 当时的程序操作参考 JSON（方向/观察区/止损/目标），条件触发提醒用
    moat_view = Column(String(100))      # 当时的护城河评级及依据（定性判断延续用）
    flywheel_view = Column(String(100))  # 当时的飞轮判断及依据（定性判断延续用）
    fundamental_outlook = Column(Text)   # 基本面前瞻 JSON：下期财报方向判断+依据+基准期（可证伪）
    fundamental_verdict = Column(String(10))   # 前瞻对账结果：正确/错误/未验证（新财报披露后程序判定）
    fundamental_note = Column(String(300))     # 对账依据（新旧同比数字）
    review_done = Column(Integer, nullable=False, default=0)  # 是否已自动复盘
    created_at = Column(DateTime, default=datetime.now, index=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'code': self.code,
            'name': self.name,
            'question': self.question,
            'price_at_analysis': self.price_at_analysis,
            'short_term_view': self.short_term_view,
            'mid_term_view': self.mid_term_view,
            'support': self.support,
            'resistance': self.resistance,
            'key_reasons': self.key_reasons,
            'indicators': self.indicators,
            'trade_plan': self.trade_plan,
            'moat_view': self.moat_view,
            'flywheel_view': self.flywheel_view,
            'fundamental_outlook': self.fundamental_outlook,
            'fundamental_verdict': self.fundamental_verdict,
            'fundamental_note': self.fundamental_note,
            'review_done': bool(self.review_done),
            'created_at': self.created_at,
        }


class AnalysisReview(Base):
    """
    复盘记录：快照到期后与实际走势对账的结果
    """
    __tablename__ = 'analysis_review'

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(Integer, nullable=False, index=True)
    code = Column(String(10), nullable=False, index=True)
    days_elapsed = Column(Integer)  # 距分析的自然日数
    price_now = Column(Float)
    pct_change = Column(Float)  # 区间涨跌幅 %
    error_pattern = Column(String(30))  # 误判主要类别：技术面权重失衡/基本面利空高估/资金面驱动低估/无明显误判/其他
    direction_verdict = Column(String(10))  # 方向对账：正确/错误/未验证
    review_content = Column(Text)  # LLM 生成的复盘卡片全文
    created_at = Column(DateTime, default=datetime.now, index=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'snapshot_id': self.snapshot_id,
            'code': self.code,
            'days_elapsed': self.days_elapsed,
            'price_now': self.price_now,
            'pct_change': self.pct_change,
            'error_pattern': self.error_pattern,
            'direction_verdict': self.direction_verdict,
            'review_content': self.review_content,
            'created_at': self.created_at,
        }


class ImprovementRule(Base):
    """
    从复盘教训中提炼的可复用改进规则（跨标的共享）。
    is_active 用于软删除，effectiveness 由后续复盘自动更新。
    """
    __tablename__ = 'improvement_rule'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_text = Column(Text, nullable=False)  # 具体的改进规则
    error_pattern = Column(String(30))  # 对应误判类别
    source_snapshot_id = Column(Integer)  # 产生该规则的复盘快照id
    code = Column(String(10), index=True)  # 适用标的代码（NULL=通用规则）
    source_stock_name = Column(String(50))  # 来源股票名称，便于追溯
    is_active = Column(Integer, nullable=False, default=1)  # 1=有效 0=停用
    hit_count = Column(Integer, nullable=False, default=0)  # 被引用次数
    effectiveness = Column(Float)  # 0-1: 按此规则操作后方向正确的比例（None=尚未评估）
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'rule_text': self.rule_text,
            'error_pattern': self.error_pattern,
            'source_snapshot_id': self.source_snapshot_id,
            'code': self.code,
            'source_stock_name': self.source_stock_name,
            'is_active': self.is_active,
            'hit_count': self.hit_count,
            'effectiveness': self.effectiveness,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }


class IndustrySnapshot(Base):
    """
    产业链分析快照：候选清单（含当时价与综合排名）、技术面首选、行业判断、基准点位
    """
    __tablename__ = 'industry_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    industry_name = Column(String(100), nullable=False, index=True)
    question = Column(String(500))
    candidates = Column(Text)  # JSON: [{"code","name","price","rank"}]，rank 从 1 开始
    top_pick = Column(String(10))  # 技术面最强的股票代码
    industry_view = Column(String(10))  # 行业判断：偏多/中性/偏空
    valuation = Column(Text)  # 行业估值与位置指标 JSON（含 overall 标签，复盘对账用）
    excluded = Column(Text)  # JSON: 被阶段门槛剔除的公司 [{"code","name","price"}]，门槛有效性对账用
    watch = Column(Text)  # JSON: 观察备选池（距门槛≤0.5分）[{"code","name","price"}]
    benchmark_price = Column(Float)  # 沪深300 当时点位
    review_done = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now, index=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'industry_name': self.industry_name,
            'question': self.question,
            'candidates': self.candidates,
            'top_pick': self.top_pick,
            'industry_view': self.industry_view,
            'valuation': self.valuation,
            'excluded': self.excluded,
            'watch': self.watch,
            'benchmark_price': self.benchmark_price,
            'review_done': bool(self.review_done),
            'created_at': self.created_at,
        }


class IndustryReview(Base):
    """
    产业链复盘记录：组合超额/排名区分度/首选命中/方向 四维对账结果
    """
    __tablename__ = 'industry_review'

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(Integer, nullable=False, index=True)
    industry_name = Column(String(100), nullable=False, index=True)
    days_elapsed = Column(Integer)
    portfolio_return = Column(Float)  # 候选等权收益 %
    benchmark_return = Column(Float)  # 沪深300 同期收益 %
    excess_return = Column(Float)  # 超额 %
    portfolio_verdict = Column(String(10))  # 跑赢/跑输/持平
    rank_effective = Column(String(10))  # 排名区分度：有效/无区分/反向
    top_pick_rank = Column(String(20))  # 首选实际涨幅名次，如 "2/8"
    direction_verdict = Column(String(10))  # 行业方向：正确/错误/未验证
    excluded_avg_return = Column(Float)  # 门槛剔除组等权收益 %（与 portfolio_return 对照验证门槛有效性）
    review_content = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'snapshot_id': self.snapshot_id,
            'industry_name': self.industry_name,
            'days_elapsed': self.days_elapsed,
            'portfolio_return': self.portfolio_return,
            'benchmark_return': self.benchmark_return,
            'excess_return': self.excess_return,
            'portfolio_verdict': self.portfolio_verdict,
            'rank_effective': self.rank_effective,
            'top_pick_rank': self.top_pick_rank,
            'direction_verdict': self.direction_verdict,
            'excluded_avg_return': self.excluded_avg_return,
            'review_content': self.review_content,
            'created_at': self.created_at,
        }


class UserFeedback(Base):
    """
    用户纠错记录：使用者对分析报告指出的错误（飞书「纠错 XX 内容」命令写入）。
    用途：①下次分析同一标的时注入 prompt「严禁再犯」；②复盘时对账该错误是否复发。
    个股用 code 关联，行业/产业链只有 target_name。
    """
    __tablename__ = 'user_feedback'

    id = Column(Integer, primary_key=True, autoincrement=True)
    target_name = Column(String(50), nullable=False, index=True)  # 用户说的对象名（公司/行业）
    code = Column(String(10), index=True)  # 解析到的股票代码（行业为空）
    content = Column(String(500), nullable=False)  # 纠错内容原文
    snapshot_id = Column(Integer)  # 关联的最近一次分析快照 id（可空）
    created_at = Column(DateTime, default=datetime.now, index=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'target_name': self.target_name,
            'code': self.code,
            'content': self.content,
            'snapshot_id': self.snapshot_id,
            'created_at': self.created_at,
        }


class IndustryReevalTrigger(Base):
    """
    行业重估触发条件：产业链分析给出"不参与/暂不参与"结论时留下的重估钩子。
    trigger_type=news 由 news_monitor 用 LLM 对照行业新闻判定；
    trigger_type=valuation 由 condition_watcher 按候选池 PE 分位程序判定。
    命中后置 status=hit 并推送提醒，让"不参与"变成"暂不参与+自动盯"。
    """
    __tablename__ = 'industry_reeval_trigger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    industry = Column(String(50), nullable=False, index=True)
    trigger_type = Column(String(10), nullable=False, default='news')  # news / valuation
    description = Column(String(300), nullable=False)  # 触发条件描述（可被公开信息验证）
    keywords = Column(String(200))  # news 型：盯梢关键词
    pool_codes = Column(Text)  # valuation 型：候选池代码列表 JSON
    pe_percentile_below = Column(Float)  # valuation 型：分位回落阈值
    status = Column(String(10), nullable=False, default='active')  # active / hit / expired
    hit_note = Column(String(300))  # 命中依据
    created_at = Column(DateTime, default=datetime.now, index=True)
    hit_at = Column(DateTime)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'industry': self.industry,
            'trigger_type': self.trigger_type,
            'description': self.description,
            'keywords': self.keywords,
            'pool_codes': self.pool_codes,
            'pe_percentile_below': self.pe_percentile_below,
            'status': self.status,
            'hit_note': self.hit_note,
            'created_at': self.created_at,
            'hit_at': self.hit_at,
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
    ratio_year1 = Column(String(10))
    forecasting_earning_per_share1 = Column(Float) # 每股收益
    Predicted_price_earnings_ratio1 = Column(Float)
    share_year2 = Column(String(10))
    ratio_year2 = Column(String(10))
    forecasting_earning_per_share2 = Column(Float)
    Predicted_price_earnings_ratio2 = Column(Float)
    share_year3 = Column(String(10))
    ratio_year3 = Column(String(10))
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
        return f"<StockResearchReport(code={self.code}, date={self.date}, pdf_name={self.pdf_name}, title={self.report_name})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，便于数据交互"""
        return {
            'code': self.code,
            'date': self.date,
            'pdf_name': self.pdf_name,
            'title': self.report_name,
            'east_rating': self.east_rating,
            'rating_agency': self.rating_agency,
            'month_research_count': self.month_research_count,
            'industry': self.industry,
            'share_year1': self.share_year1,
            'ratio_year1': self.ratio_year1,
            'forecasting_earning_per_share1': self.forecasting_earning_per_share1,
            'Predicted_price_earnings_ratio1': self.Predicted_price_earnings_ratio1,
            'share_year2': self.share_year2,
            'ratio_year2': self.ratio_year2,
            'forecasting_earning_per_share2': self.forecasting_earning_per_share2,
            'Predicted_price_earnings_ratio2': self.Predicted_price_earnings_ratio2,
            'share_year3': self.share_year3,
            'ratio_year3': self.ratio_year3,
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


class CompanySocialAccount(Base):
    """
    公司社交媒体官方账号缓存：微博 UID / 公众号名称等。
    每次分析时先查表，有缓存直接复用；没有则搜索并存入。
    """
    __tablename__ = 'company_social_account'

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), nullable=False, index=True)  # 股票代码
    company_name = Column(String(50), nullable=False)
    weibo_uid = Column(String(20))           # 微博 UID（数字ID）
    weibo_name = Column(String(100))         # 微博官方账号名
    wechat_name = Column(String(100))        # 公众号名称
    wechat_id = Column(String(100))          # 微信号（gh_xxx 或英文ID）
    weibo_posts = Column(Text)               # 最近微博缓存 JSON
    wechat_articles = Column(Text)           # 最近公众号文章缓存 JSON
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint('code', name='uix_social_account_code'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class PeerConsCache(Base):
    """
    同业对标板块成分股缓存。
    同一板块每24小时只拉一次成分股 list，减少对 akshare 的重复调用和网络故障。
    """
    __tablename__ = 'peer_cons_cache'

    id = Column(Integer, primary_key=True, autoincrement=True)
    industry = Column(String(50), nullable=False, index=True)
    data_json = Column(Text)          # 成分股数据的 JSON 序列化（{code, name} list）
    cached_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint('industry', name='uix_peer_cons_industry'),
    )


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
        # 兼容配置里写纯文件路径的情况（如 ./data/sqlite/stock.db），自动补 sqlite:/// 前缀
        if "://" not in str(db_url):
            db_path = Path(db_url)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite:///{db_url}"

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

        # SQLite 并发保护：飞书工作线程/监控调度线程/快照异步线程会同时写库，
        # WAL 允许读写并发，busy_timeout 让写锁竞争时等待而不是立刻报 database is locked
        if db_url.startswith("sqlite"):
            from sqlalchemy import event as _sa_event

            @_sa_event.listens_for(self._engine, "connect")
            def _set_sqlite_pragma(dbapi_conn, _record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=5000")
                cursor.close()

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

        # 步骤3.5：历史 schema 修复（必须在 create_all 之前执行）
        self._migrate_legacy_tables()

        # 步骤4：创建所有表（如果不存在）
        # Base.metadata.create_all 会检查表是否存在，不存在则创建
        # 这是SQLAlchemy的便利功能，避免手动编写CREATE TABLE语句
        Base.metadata.create_all(self._engine)

        # PDF 研报下载目录（统一目录，供下载与文件列举使用）
        self.download_dir = Path("./data/pdf")
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # 标记为已初始化，防止重复初始化
        self._initialized = True
        logger.info(f"数据库初始化完成: {db_url}")

    def _migrate_legacy_tables(self) -> None:
        """
        小迁移：修复历史 schema 错误。
        这两张表因字段/约束错误从未成功写入过（表为空），直接 DROP 后由
        create_all 按新模型重建，安全无数据丢失：
        1. stock_research_report：ratio_yaar1/2/3 列名 typo → ratio_year1/2/3
        2. daily_forecast：code/forecast_date 错误的单列 unique 约束 → 只保留复合唯一约束
        3. stock_income：历史表缺四项费用列（sell_exp/admin_exp/rd_exp/fin_exp），
           create_all 对已存在的表不会加列，用 ALTER TABLE ADD COLUMN 补齐（幂等，存量数据保留）
        """
        try:
            inspector = inspect(self._engine)
            table_names = inspector.get_table_names()
            with self._engine.connect() as conn:
                # stock_research_report：存在 typo 列 ratio_yaar1 说明是旧表
                if 'stock_research_report' in table_names:
                    cols = [c['name'] for c in inspector.get_columns('stock_research_report')]
                    if 'ratio_yaar1' in cols:
                        conn.execute(text('DROP TABLE stock_research_report'))
                        conn.commit()
                        logger.info("检测到 stock_research_report 旧表（ratio_yaar1 列名 typo），已删除待重建")

                # daily_forecast：存在 code 或 forecast_date 的单列唯一索引说明是旧表
                if 'daily_forecast' in table_names:
                    idx_rows = conn.execute(text("PRAGMA index_list('daily_forecast')")).fetchall()
                    for idx in idx_rows:
                        # 行结构: (seq, name, unique, origin, partial)
                        if not idx[2]:
                            continue
                        col_rows = conn.execute(text(f"PRAGMA index_info('{idx[1]}')")).fetchall()
                        col_names = [r[2] for r in col_rows]
                        if len(col_names) == 1 and col_names[0] in ('code', 'forecast_date'):
                            conn.execute(text('DROP TABLE daily_forecast'))
                            conn.commit()
                            logger.info("检测到 daily_forecast 旧表（单列 unique 约束），已删除待重建")
                            break

                # industry_snapshot：旧表缺列时补齐
                if 'industry_snapshot' in table_names:
                    snap_cols = [c['name'] for c in inspector.get_columns('industry_snapshot')]
                    for col_name, col_desc in (('valuation', '行业估值指标 JSON'),
                                               ('excluded', '门槛剔除公司 JSON'),
                                               ('watch', '观察备选池 JSON')):
                        if col_name not in snap_cols:
                            conn.execute(text(f'ALTER TABLE industry_snapshot ADD COLUMN {col_name} TEXT'))
                            conn.commit()
                            logger.info(f"industry_snapshot 表补充 {col_name} 列（{col_desc}）")

                # watch_target：旧表缺 source 列时补齐（区分手动/自动加入，自动过期只碰 auto）
                if 'watch_target' in table_names:
                    wt_cols = [c['name'] for c in inspector.get_columns('watch_target')]
                    if 'source' not in wt_cols:
                        conn.execute(text("ALTER TABLE watch_target ADD COLUMN source VARCHAR(10) "
                                          "NOT NULL DEFAULT 'manual'"))
                        conn.commit()
                        logger.info("watch_target 表补充 source 列（manual/auto，自动过期只处理 auto）")

                # industry_review：旧表缺剔除组收益列时补齐（门槛有效性对账用）
                if 'industry_review' in table_names:
                    rev_cols = [c['name'] for c in inspector.get_columns('industry_review')]
                    if 'excluded_avg_return' not in rev_cols:
                        conn.execute(text('ALTER TABLE industry_review ADD COLUMN excluded_avg_return FLOAT'))
                        conn.commit()
                        logger.info("industry_review 表补充 excluded_avg_return 列（剔除组平均收益）")

                # stock_balance_sheet：旧表缺应收/存货列时补齐（营运资本趋势用）
                if 'stock_balance_sheet' in table_names:
                    bs_cols = [c['name'] for c in inspector.get_columns('stock_balance_sheet')]
                    for col_name in ('accounts_receivable', 'inventory', 'fixed_assets', 'construction_in_progress'):
                        if col_name not in bs_cols:
                            conn.execute(text(f'ALTER TABLE stock_balance_sheet ADD COLUMN {col_name} FLOAT'))
                            conn.commit()
                            logger.info(f"stock_balance_sheet 表补充 {col_name} 列（{'营运资本趋势用' if col_name in ('accounts_receivable', 'inventory') else '产能扩张分析用'}）")

                # analysis_snapshot：旧表缺列时补齐（trade_plan=条件触发；moat/flywheel=定性延续；
                # fundamental_*=基本面前瞻对账闭环）
                if 'analysis_snapshot' in table_names:
                    asnap_cols = [c['name'] for c in inspector.get_columns('analysis_snapshot')]
                    for col_name, col_type, col_desc in (
                            ('trade_plan', 'TEXT', '程序操作参考 JSON'),
                            ('moat_view', 'VARCHAR(100)', '护城河评级'),
                            ('flywheel_view', 'VARCHAR(100)', '飞轮判断'),
                            ('fundamental_outlook', 'TEXT', '基本面前瞻 JSON'),
                            ('fundamental_verdict', 'VARCHAR(10)', '前瞻对账结果'),
                            ('fundamental_note', 'VARCHAR(300)', '前瞻对账依据')):
                        if col_name not in asnap_cols:
                            conn.execute(text(f'ALTER TABLE analysis_snapshot ADD COLUMN {col_name} {col_type}'))
                            conn.commit()
                            logger.info(f"analysis_snapshot 表补充 {col_name} 列（{col_desc}）")

                # stock_income：旧表缺四项费用列时用 ALTER TABLE 补齐（有数据不能 DROP 重建）
                if 'stock_income' in table_names:
                    income_cols = [c['name'] for c in inspector.get_columns('stock_income')]
                    # 四项费用列，单位：元
                    expense_cols = {
                        'sell_exp': '销售费用',
                        'admin_exp': '管理费用',
                        'rd_exp': '研发费用',
                        'fin_exp': '财务费用',
                    }
                    for col_name, col_desc in expense_cols.items():
                        if col_name not in income_cols:
                            conn.execute(text(f'ALTER TABLE stock_income ADD COLUMN {col_name} FLOAT'))
                            conn.commit()
                            logger.info(f"stock_income 表补充费用列 {col_name}（{col_desc}，单位：元）")

                # analysis_review：旧表缺 error_pattern / direction_verdict 列时补齐
                if 'analysis_review' in table_names:
                    review_cols = [c['name'] for c in inspector.get_columns('analysis_review')]
                    for col_name, col_type, col_desc in (
                            ('error_pattern', 'VARCHAR(30)', '误判主要类别'),
                            ('direction_verdict', 'VARCHAR(10)', '方向对账结果')):
                        if col_name not in review_cols:
                            conn.execute(text(f'ALTER TABLE analysis_review ADD COLUMN {col_name} {col_type}'))
                            conn.commit()
                            logger.info(f"analysis_review 表补充 {col_name} 列（{col_desc}）")
        except Exception as e:
            # 迁移失败不阻塞启动，但要暴露出来便于排查
            logger.error(f"历史表结构迁移失败: {e}")

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

    def get_peers_by_industry(self, industry: str, exclude_code: str = None,
                              limit: int = 12) -> List[Dict[str, Any]]:
        """按行业取同行公司（同行对比表用），返回 [{code, name}]"""
        if not industry:
            return []
        with self.get_session() as session:
            query = select(StockBasic.code, StockBasic.name).where(
                StockBasic.industry == industry)
            if exclude_code:
                query = query.where(StockBasic.code != exclude_code)
            rows = session.execute(query.limit(limit)).all()
            return [{"code": r[0], "name": r[1]} for r in rows]

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

    def get_stock_research_report_days(self, code: str, days: int = 30) -> List[StockResearchReport]:
        """
        获取最近一段时间的研报
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockResearchReport)
                .where(
                    and_(
                        StockResearchReport.code == code,
                        StockResearchReport.date >= (date.today() - timedelta(days=days))
                    )
                )
                .order_by(desc(StockResearchReport.date))
            ).scalars().all()
            return list(results)

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

                    # 日期缺失/解析失败的行直接跳过
                    if not isinstance(row_date, date):
                        logger.warning(f"[{code}] 日线数据行日期无效，跳过: {row.get('date')}")
                        continue

                    # start_date 为 None 时不做区间过滤，保存全部行
                    if start_date_str is not None and row_date < start_date_str:
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
                    if not isinstance(row_date, date):
                        logger.warning(f"[{code}] 周线数据行日期无效，跳过: {row.get('date')}")
                        continue
                    end_date = parse_row_date(row.get('end_date'))
                    # end_date 缺失时用 date 字段兜底（tushare 周/月线接口不返回 end_date）
                    if not isinstance(end_date, date):
                        end_date = row_date
                    # start_date 为 None 时不做区间过滤
                    if start_date_str is not None and end_date < start_date_str:
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
                    if not isinstance(row_date, date):
                        logger.warning(f"[{code}] 月线数据行日期无效，跳过: {row.get('date')}")
                        continue
                    e_date = parse_row_date(row.get('end_date'))
                    # end_date 缺失时用 date 字段兜底（tushare 周/月线接口不返回 end_date）
                    if not isinstance(e_date, date):
                        e_date = row_date
                    # start_date 为 None 时不做区间过滤
                    if start_date_str is not None and e_date < start_date_str:
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

    def _delete_stock_data(self, model, code: str) -> int:
        """按股票代码删除某张行情表的全部数据（内部通用实现）"""
        if code is None:
            logger.error(f"code is null")
            return 0
        with self.get_session() as session:
            try:
                result = session.execute(
                    delete(model).where(model.code == code)
                )
                session.commit()
                deleted_count = result.rowcount or 0
                logger.info(f"删除 {model.__tablename__} 中 {code} 的 {deleted_count} 条数据")
                return deleted_count
            except Exception as e:
                session.rollback()
                logger.error(f"删除 {code} 数据失败: {e}")
                raise

    def delete_daily_data(self, code: str) -> int:
        """删除某股票的全部日线数据（前复权基准漂移时全量重拉前调用）"""
        return self._delete_stock_data(StockDaily, code)

    def delete_week_data(self, code: str) -> int:
        """删除某股票的全部周线数据（前复权基准漂移时全量重拉前调用）"""
        return self._delete_stock_data(StockWeekly, code)

    def delete_month_data(self, code: str) -> int:
        """删除某股票的全部月线数据（前复权基准漂移时全量重拉前调用）"""
        return self._delete_stock_data(StockMonth, code)

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
                        existing.forecast_rue = row.get('forecast_rue')
                        existing.practice_rue = row.get('practice_rue')
                        existing.forecast_model = row.get('forecast_model')
                        existing.updated_at = datetime.now()  # 更新修改时间
                        # 注意：更新操作不增加saved_count（只统计新增）
                    else:
                        # 情况B：记录不存在 → 执行INSERT（插入）
                        # 创建新的StockDaily对象，填充所有字段
                        record = DailyForecast(
                            # 标识字段
                            code=code,  # 股票代码
                            forecast_date=row_date,  # 交易日期
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

                    # 日期解析失败的行直接跳过
                    if date is None:
                        logger.warning(f"[{code}] 研报 {pdf_name} 日期无效，跳过: {row.get('date')}")
                        continue

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
                # 与其他 save 方法一致：重新抛出异常，让调用者处理（不静默吞掉）
                raise

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

            # 使用统一的 PDF 下载目录（初始化时已创建）
            pdf_path = self.download_dir
            # 创建子目录（如果提供了股票代码）
            if stock_code:
                stock_dir = pdf_path/stock_code
                stock_dir.mkdir(parents=True, exist_ok=True)
                file_path = stock_dir / filename
            else:
                file_path = pdf_path / filename

            # 检查文件是否已存在
            if file_path.exists():
                # 已有文件尝试提取文本
                existing_content = self.extract_text_from_pdf_content(file_path.read_bytes()) or ""
                if existing_content:
                    result['success'] = True
                    result['file_path'] = str(file_path)
                    result['file_size'] = file_path.stat().st_size
                    result['file_content'] = existing_content
                    logger.info(f"PDF文件已存在: {file_path}")
                    return result
                # 文件损坏或扫描版PDF → 删掉重新下载
                logger.warning(f"文件已存在但提取文本为空（可能损坏或非PDF无文本内容），重新下载: {file_path}")
                file_path.unlink()

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
            # 文本提取失败（如未装 PyPDF2、扫描版 PDF）不影响下载结果本身
            content = self.extract_text_from_pdf_content(file_content) or ""
            result['success'] = True
            result['file_path'] = str(file_path)
            result['file_size'] = file_size
            result['file_content'] = content

            logger.info(f"PDF下载成功: {file_path} 提取文本{len(content)}字符({file_size} bytes)")
        except Exception as e:
            result['error'] = f"下载失败: {e}"
            logger.error(result['error'])

        return result

    def extract_text_from_pdf_content(self, file_content: bytes) -> Optional[str]:
        """
        从 PDF 二进制内容中提取文本（自动降级处理非PDF内容）

        - 真实 PDF（%PDF 文件头）→ PyPDF2 提取
        - HTML 页面 → BeautifulSoup 提取纯文本
        - 纯文本 → 直接返回

        参数:
            file_content: 文件的二进制内容（bytes 或 bytearray）

        返回:
            str: 提取的文本内容
        """
        content_bytes = bytes(file_content) if isinstance(file_content, bytearray) else file_content

        # === 非 PDF 内容：直接解码为文本 ===
        if not content_bytes.startswith(b'%PDF'):
            try:
                raw_text = content_bytes.decode('utf-8', errors='replace')
            except Exception:
                return None

            # 尝试用 BeautifulSoup 剥离 HTML 标签
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(raw_text, 'html.parser')
                # 如果有明显的 HTML 结构，提取纯文本
                if soup.find(['html', 'body', 'div', 'p', 'span']):
                    texts = soup.get_text(separator='\n', strip=True)
                    if texts:
                        logger.info(f"[非PDF提取] HTML 内容，提取 {len(texts)} 字符")
                        return texts
            except Exception:
                pass

            # 纯文本：去除空行后返回
            stripped = '\n'.join(line for line in raw_text.splitlines() if line.strip())
            if stripped.strip():
                logger.info(f"[非PDF提取] 纯文本内容，共 {len(stripped)} 字符")
                return stripped
            return None

        # === 真实 PDF：用 PyPDF2 提取 ===
        try:
            import PyPDF2

            # 将 bytes 转换为文件对象
            pdf_file = io.BytesIO(content_bytes)

            # 读取 PDF
            reader = PyPDF2.PdfReader(pdf_file)
            logger.info(f"[PDF 提取] 共 {len(reader.pages)} 页")

            # 提取前几页的内容（避免 token 超限）
            max_pages = min(len(reader.pages), 10)
            text_content = []

            for i, page in enumerate(reader.pages[:max_pages]):
                text = page.extract_text()
                if text:
                    text_content.append(f"=== 第 {i + 1} 页 ===\n{text}")
                else:
                    logger.debug(f"[PDF 提取] 第 {i + 1} 页无法提取文本（可能是图片或扫描版）")

            # 合并为字符串
            full_text = "\n\n".join(text_content)
            logger.info(f"[PDF 提取] 总共提取 {len(full_text)} 字符")

            return full_text

        except ImportError:
            logger.error("[PDF 提取] 未安装 PyPDF2，请运行：pip install PyPDF2（文本提取跳过，不影响 PDF 下载）")
            return None
        except Exception as e:
            logger.error(f"[PDF 提取] 提取失败：{e}")
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

        # 下载PDF（download_research_report 没有 report_date 参数，日期已编入文件名）
        result = self.download_research_report(
            url=research_report.report_pdf_link,
            filename=filename,
            stock_code=research_report.code,
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

    def get_stocks_by_industry(self, industry: str) -> List[Dict[str, Any]]:
        """获取指定行业的所有上市股票，按代码排序"""
        with self.get_session() as session:
            stmt = (
                select(StockBasic)
                .where(
                    StockBasic.industry == industry,
                    StockBasic.list_status == 'L',
                )
                .order_by(StockBasic.code)
            )
            stocks = session.execute(stmt).scalars().all()
            return [s.to_dict() for s in stocks]

    def get_top_stocks_by_industry(self, industry: str, top_n: int = 2) -> List[Dict[str, Any]]:
        """
        获取行业内市值排名前N的股票（近似龙一龙二）
        通过最新日线数据的总市值排名，fallback 到代码排序
        """
        stocks = self.get_stocks_by_industry(industry)
        if not stocks:
            return []

        codes = [s['code'] for s in stocks]

        # 尝试用最新日线 basic 数据里的总市值排名
        code_mv = {}
        with self.get_session() as session:
            from sqlalchemy import func
            for code in codes:
                row = session.execute(
                    select(StockDailyBasic.total_mv)
                    .where(StockDailyBasic.code == code)
                    .order_by(desc(StockDailyBasic.trade_date))
                    .limit(1)
                ).scalar_one_or_none()
                code_mv[code] = row or 0

        # 按市值降序，取 top_n
        stocks.sort(key=lambda s: code_mv.get(s['code'], 0), reverse=True)
        top = stocks[:top_n]

        # 补上名称，方便下游用
        for s in top:
            s['rank'] = top.index(s) + 1

        return top

    def save_stock_income(self, df: pd.DataFrame, code: str) -> int:
        """
        保存利润表数据到数据库（支持UPSERT操作）
        Args:
            df: 包含利润表数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存利润表数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                report_dates = [parse_row_date(d) for d in df['report_date'].tolist()]
                existing_records = session.execute(
                    select(StockIncome).where(
                        and_(
                            StockIncome.code == code,
                            StockIncome.report_date.in_(report_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.report_date: r for r in existing_records}

                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    existing = existing_map.get(report_date)

                    if existing:
                        existing.total_revenue = row.get('total_revenue')
                        existing.operating_profit = row.get('operating_profit')
                        existing.net_profit = row.get('net_profit')
                        existing.basic_eps = row.get('basic_eps')
                        existing.sell_exp = row.get('sell_exp')
                        existing.admin_exp = row.get('admin_exp')
                        existing.rd_exp = row.get('rd_exp')
                        existing.fin_exp = row.get('fin_exp')
                        existing.revenue_growth = row.get('revenue_growth')
                        existing.profit_growth = row.get('profit_growth')
                        existing.gross_margin = row.get('gross_margin')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockIncome(
                            code=code,
                            report_date=report_date,
                            total_revenue=row.get('total_revenue'),
                            operating_profit=row.get('operating_profit'),
                            net_profit=row.get('net_profit'),
                            basic_eps=row.get('basic_eps'),
                            sell_exp=row.get('sell_exp'),
                            admin_exp=row.get('admin_exp'),
                            rd_exp=row.get('rd_exp'),
                            fin_exp=row.get('fin_exp'),
                            revenue_growth=row.get('revenue_growth'),
                            profit_growth=row.get('profit_growth'),
                            gross_margin=row.get('gross_margin'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 利润表数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 利润表数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 利润表数据失败: {e}")
                raise

        return saved_count

    def save_stock_balance_sheet(self, df: pd.DataFrame, code: str) -> int:
        """
        保存资产负债表数据到数据库（支持UPSERT操作）
        Args:
            df: 包含资产负债表数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存资产负债表数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                report_dates = [parse_row_date(d) for d in df['report_date'].tolist()]
                existing_records = session.execute(
                    select(StockBalanceSheet).where(
                        and_(
                            StockBalanceSheet.code == code,
                            StockBalanceSheet.report_date.in_(report_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.report_date: r for r in existing_records}

                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    existing = existing_map.get(report_date)

                    if existing:
                        existing.total_assets = row.get('total_assets')
                        existing.current_assets = row.get('current_assets')
                        existing.non_current_assets = row.get('non_current_assets')
                        existing.total_liabilities = row.get('total_liabilities')
                        existing.current_liabilities = row.get('current_liabilities')
                        existing.non_current_liabilities = row.get('non_current_liabilities')
                        existing.total_equity = row.get('total_equity')
                        existing.asset_liability_ratio = row.get('asset_liability_ratio')
                        existing.current_ratio = row.get('current_ratio')
                        existing.accounts_receivable = row.get('accounts_receivable')
                        existing.inventory = row.get('inventory')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockBalanceSheet(
                            code=code,
                            report_date=report_date,
                            total_assets=row.get('total_assets'),
                            current_assets=row.get('current_assets'),
                            non_current_assets=row.get('non_current_assets'),
                            total_liabilities=row.get('total_liabilities'),
                            current_liabilities=row.get('current_liabilities'),
                            non_current_liabilities=row.get('non_current_liabilities'),
                            total_equity=row.get('total_equity'),
                            asset_liability_ratio=row.get('asset_liability_ratio'),
                            current_ratio=row.get('current_ratio'),
                            accounts_receivable=row.get('accounts_receivable'),
                            inventory=row.get('inventory'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 资产负债表数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 资产负债表数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 资产负债表数据失败: {e}")
                raise

        return saved_count

    def get_stock_income(self, code: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取利润表数据
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 利润表数据
        """
        with self.get_session() as session:
            query = select(StockIncome).where(StockIncome.code == code)

            if start_date:
                query = query.where(StockIncome.report_date >= start_date)
            if end_date:
                query = query.where(StockIncome.report_date <= end_date)

            results = session.execute(query.order_by(desc(StockIncome.report_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def get_stock_balance_sheet(self, code: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取资产负债表数据
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 资产负债表数据
        """
        with self.get_session() as session:
            query = select(StockBalanceSheet).where(StockBalanceSheet.code == code)

            if start_date:
                query = query.where(StockBalanceSheet.report_date >= start_date)
            if end_date:
                query = query.where(StockBalanceSheet.report_date <= end_date)

            results = session.execute(query.order_by(desc(StockBalanceSheet.report_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_cashflow(self, df: pd.DataFrame, code: str) -> int:
        """
        保存现金流量表数据到数据库（支持UPSERT操作）
        Args:
            df: 包含现金流量表数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存现金流量表数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                report_dates = [parse_row_date(d) for d in df['report_date'].tolist()]
                existing_records = session.execute(
                    select(StockCashflow).where(
                        and_(
                            StockCashflow.code == code,
                            StockCashflow.report_date.in_(report_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.report_date: r for r in existing_records}

                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    existing = existing_map.get(report_date)

                    if existing:
                        existing.operating_cashflow = row.get('operating_cashflow')
                        existing.investing_cashflow = row.get('investing_cashflow')
                        existing.financing_cashflow = row.get('financing_cashflow')
                        existing.capex = row.get('capex')
                        existing.free_cashflow = row.get('free_cashflow')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockCashflow(
                            code=code,
                            report_date=report_date,
                            operating_cashflow=row.get('operating_cashflow'),
                            investing_cashflow=row.get('investing_cashflow'),
                            financing_cashflow=row.get('financing_cashflow'),
                            capex=row.get('capex'),
                            free_cashflow=row.get('free_cashflow'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 现金流量表数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 现金流量表数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 现金流量表数据失败: {e}")
                raise

        return saved_count

    def get_stock_cashflow(self, code: str, start_date: Optional[date] = None, end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取现金流量表数据（按报告期降序）
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 现金流量表数据
        """
        with self.get_session() as session:
            query = select(StockCashflow).where(StockCashflow.code == code)

            if start_date:
                query = query.where(StockCashflow.report_date >= start_date)
            if end_date:
                query = query.where(StockCashflow.report_date <= end_date)

            results = session.execute(query.order_by(desc(StockCashflow.report_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_fina_indicator(self, df: pd.DataFrame, code: str) -> int:
        """
        保存财务指标数据到数据库（支持UPSERT操作）
        Args:
            df: 包含财务指标数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存财务指标数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                report_dates = [parse_row_date(d) for d in df['report_date'].tolist()]
                existing_records = session.execute(
                    select(StockFinaIndicator).where(
                        and_(
                            StockFinaIndicator.code == code,
                            StockFinaIndicator.report_date.in_(report_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.report_date: r for r in existing_records}

                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    ann_date = parse_row_date(row.get('ann_date')) if row.get('ann_date') else None
                    existing = existing_map.get(report_date)

                    if existing:
                        existing.ann_date = ann_date
                        existing.eps = row.get('eps')
                        existing.dt_eps = row.get('dt_eps')
                        existing.total_revenue_ps = row.get('total_revenue_ps')
                        existing.revenue_ps = row.get('revenue_ps')
                        existing.gross_margin = row.get('gross_margin')
                        existing.current_ratio = row.get('current_ratio')
                        existing.quick_ratio = row.get('quick_ratio')
                        existing.cash_ratio = row.get('cash_ratio')
                        existing.ar_turn = row.get('ar_turn')
                        existing.ca_turn = row.get('ca_turn')
                        existing.fa_turn = row.get('fa_turn')
                        existing.assets_turn = row.get('assets_turn')
                        existing.inv_turn = row.get('inv_turn')
                        existing.roe = row.get('roe')
                        existing.roe_waa = row.get('roe_waa')
                        existing.roe_dt = row.get('roe_dt')
                        existing.roa = row.get('roa')
                        existing.rop = row.get('rop')
                        existing.netprofit_margin = row.get('netprofit_margin')
                        existing.grossprofit_margin = row.get('grossprofit_margin')
                        existing.debt_to_assets = row.get('debt_to_assets')
                        existing.debt_to_eqy = row.get('debt_to_eqy')
                        existing.n_cashflow_to_liab = row.get('n_cashflow_to_liab')
                        existing.mbrg = row.get('mbrg')
                        existing.nprg = row.get('nprg')
                        existing.seg = row.get('seg')
                        existing.profit_yoy = row.get('profit_yoy')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockFinaIndicator(
                            code=code,
                            report_date=report_date,
                            ann_date=ann_date,
                            eps=row.get('eps'),
                            dt_eps=row.get('dt_eps'),
                            total_revenue_ps=row.get('total_revenue_ps'),
                            revenue_ps=row.get('revenue_ps'),
                            gross_margin=row.get('gross_margin'),
                            current_ratio=row.get('current_ratio'),
                            quick_ratio=row.get('quick_ratio'),
                            cash_ratio=row.get('cash_ratio'),
                            ar_turn=row.get('ar_turn'),
                            ca_turn=row.get('ca_turn'),
                            fa_turn=row.get('fa_turn'),
                            assets_turn=row.get('assets_turn'),
                            inv_turn=row.get('inv_turn'),
                            roe=row.get('roe'),
                            roe_waa=row.get('roe_waa'),
                            roe_dt=row.get('roe_dt'),
                            roa=row.get('roa'),
                            rop=row.get('rop'),
                            netprofit_margin=row.get('netprofit_margin'),
                            grossprofit_margin=row.get('grossprofit_margin'),
                            debt_to_assets=row.get('debt_to_assets'),
                            debt_to_eqy=row.get('debt_to_eqy'),
                            n_cashflow_to_liab=row.get('n_cashflow_to_liab'),
                            mbrg=row.get('mbrg'),
                            nprg=row.get('nprg'),
                            seg=row.get('seg'),
                            profit_yoy=row.get('profit_yoy'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 财务指标数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 财务指标数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 财务指标数据失败: {e}")
                raise

        return saved_count

    def get_stock_fina_indicator(self, code: str, start_date: Optional[date] = None,
                                 end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取财务指标数据
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 财务指标数据
        """
        with self.get_session() as session:
            query = select(StockFinaIndicator).where(StockFinaIndicator.code == code)

            if start_date:
                query = query.where(StockFinaIndicator.report_date >= start_date)
            if end_date:
                query = query.where(StockFinaIndicator.report_date <= end_date)

            results = session.execute(query.order_by(desc(StockFinaIndicator.report_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_main_business(self, df: pd.DataFrame, code: str) -> int:
        """
        保存主营业务构成数据到数据库（支持UPSERT操作）
        Args:
            df: 包含主营业务数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存主营业务数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    bz_type = row.get('bz_type', 'P')
                    bz_item = row.get('bz_item', '')

                    existing = session.execute(
                        select(StockMainBusiness).where(
                            and_(
                                StockMainBusiness.code == code,
                                StockMainBusiness.report_date == report_date,
                                StockMainBusiness.bz_type == bz_type,
                                StockMainBusiness.bz_item == bz_item,
                            )
                        )
                    ).scalar_one_or_none()

                    if existing:
                        existing.bz_sales = row.get('bz_sales')
                        existing.bz_profit = row.get('bz_profit')
                        existing.bz_cost = row.get('bz_cost')
                        existing.gross_margin = row.get('gross_margin')
                        existing.sales_ratio = row.get('sales_ratio')
                        existing.curr_type = row.get('curr_type')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockMainBusiness(
                            code=code,
                            report_date=report_date,
                            bz_type=bz_type,
                            bz_item=bz_item,
                            bz_sales=row.get('bz_sales'),
                            bz_profit=row.get('bz_profit'),
                            bz_cost=row.get('bz_cost'),
                            gross_margin=row.get('gross_margin'),
                            sales_ratio=row.get('sales_ratio'),
                            curr_type=row.get('curr_type'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 主营业务数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 主营业务数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 主营业务数据失败: {e}")
                raise

        return saved_count

    def get_stock_main_business(self, code: str, start_date: Optional[date] = None,
                                end_date: Optional[date] = None,
                                bz_type: Optional[str] = None) -> pd.DataFrame:
        """
        获取主营业务构成数据
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
            bz_type: 业务类型 P产品/D地区/I行业（可选）
        Returns:
            pd.DataFrame: 主营业务数据
        """
        with self.get_session() as session:
            query = select(StockMainBusiness).where(StockMainBusiness.code == code)

            if start_date:
                query = query.where(StockMainBusiness.report_date >= start_date)
            if end_date:
                query = query.where(StockMainBusiness.report_date <= end_date)
            if bz_type:
                query = query.where(StockMainBusiness.bz_type == bz_type)

            results = session.execute(query.order_by(desc(StockMainBusiness.report_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_holder_number(self, df: pd.DataFrame, code: str) -> int:
        """
        保存股东户数数据到数据库（支持UPSERT操作）
        Args:
            df: 包含股东户数数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存股东户数数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                report_dates = [parse_row_date(d) for d in df['report_date'].tolist()]
                existing_records = session.execute(
                    select(StockHolderNumber).where(
                        and_(
                            StockHolderNumber.code == code,
                            StockHolderNumber.report_date.in_(report_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.report_date: r for r in existing_records}

                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    ann_date = parse_row_date(row.get('ann_date')) if row.get('ann_date') else None
                    existing = existing_map.get(report_date)

                    if existing:
                        existing.ann_date = ann_date
                        existing.holder_num = row.get('holder_num')
                        existing.holder_num_change = row.get('holder_num_change')
                        existing.holder_num_change_ratio = row.get('holder_num_change_ratio')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockHolderNumber(
                            code=code,
                            report_date=report_date,
                            ann_date=ann_date,
                            holder_num=row.get('holder_num'),
                            holder_num_change=row.get('holder_num_change'),
                            holder_num_change_ratio=row.get('holder_num_change_ratio'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 股东户数数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 股东户数数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 股东户数数据失败: {e}")
                raise

        return saved_count

    def get_stock_holder_number(self, code: str, start_date: Optional[date] = None,
                                end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取股东户数数据
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 股东户数数据
        """
        with self.get_session() as session:
            query = select(StockHolderNumber).where(StockHolderNumber.code == code)

            if start_date:
                query = query.where(StockHolderNumber.report_date >= start_date)
            if end_date:
                query = query.where(StockHolderNumber.report_date <= end_date)

            results = session.execute(query.order_by(desc(StockHolderNumber.report_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_northbound_hold(self, df: pd.DataFrame, code: str) -> int:
        """
        保存北向持股数据到数据库（支持UPSERT操作）
        Args:
            df: 包含北向持股数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存北向持股数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                trade_dates = [parse_row_date(d) for d in df['trade_date'].tolist()]
                existing_records = session.execute(
                    select(StockNorthboundHold).where(
                        and_(
                            StockNorthboundHold.code == code,
                            StockNorthboundHold.trade_date.in_(trade_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.trade_date: r for r in existing_records}

                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = existing_map.get(trade_date)

                    if existing:
                        existing.name = row.get('name')
                        existing.vol = row.get('vol')
                        existing.ratio = row.get('ratio')
                        existing.exchange = row.get('exchange')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockNorthboundHold(
                            code=code,
                            trade_date=trade_date,
                            name=row.get('name'),
                            vol=row.get('vol'),
                            ratio=row.get('ratio'),
                            exchange=row.get('exchange'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 北向持股数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 北向持股数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 北向持股数据失败: {e}")
                raise

        return saved_count

    def get_stock_northbound_hold(self, code: str, start_date: Optional[date] = None,
                                  end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取北向持股数据
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 北向持股数据
        """
        with self.get_session() as session:
            query = select(StockNorthboundHold).where(StockNorthboundHold.code == code)

            if start_date:
                query = query.where(StockNorthboundHold.trade_date >= start_date)
            if end_date:
                query = query.where(StockNorthboundHold.trade_date <= end_date)

            results = session.execute(query.order_by(desc(StockNorthboundHold.trade_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'trade_date' in data_list.columns:
                data_list['trade_date'] = data_list['trade_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_top10_holder(self, df: pd.DataFrame, code: str) -> int:
        """
        保存十大股东数据到数据库（支持UPSERT操作）
        Args:
            df: 包含十大股东数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存十大股东数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    ann_date = parse_row_date(row.get('ann_date')) if row.get('ann_date') else None
                    holder_type = row.get('holder_type', 'top10')
                    holder_name = row.get('holder_name', '')

                    existing = session.execute(
                        select(StockTop10Holder).where(
                            and_(
                                StockTop10Holder.code == code,
                                StockTop10Holder.report_date == report_date,
                                StockTop10Holder.holder_type == holder_type,
                                StockTop10Holder.holder_name == holder_name,
                            )
                        )
                    ).scalar_one_or_none()

                    if existing:
                        existing.ann_date = ann_date
                        existing.hold_amount = row.get('hold_amount')
                        existing.hold_ratio = row.get('hold_ratio')
                        existing.hold_float_ratio = row.get('hold_float_ratio')
                        existing.hold_change = row.get('hold_change')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockTop10Holder(
                            code=code,
                            report_date=report_date,
                            ann_date=ann_date,
                            holder_type=holder_type,
                            holder_name=holder_name,
                            hold_amount=row.get('hold_amount'),
                            hold_ratio=row.get('hold_ratio'),
                            hold_float_ratio=row.get('hold_float_ratio'),
                            hold_change=row.get('hold_change'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 十大股东数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 十大股东数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 十大股东数据失败: {e}")
                raise

        return saved_count

    def get_stock_top10_holder(self, code: str, start_date: Optional[date] = None,
                               end_date: Optional[date] = None,
                               holder_type: Optional[str] = None) -> pd.DataFrame:
        """
        获取十大股东数据
        Args:
            code: 股票代码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
            holder_type: top10十大股东 / top10_float十大流通股东（可选）
        Returns:
            pd.DataFrame: 十大股东数据
        """
        with self.get_session() as session:
            query = select(StockTop10Holder).where(StockTop10Holder.code == code)

            if start_date:
                query = query.where(StockTop10Holder.report_date >= start_date)
            if end_date:
                query = query.where(StockTop10Holder.report_date <= end_date)
            if holder_type:
                query = query.where(StockTop10Holder.holder_type == holder_type)

            results = session.execute(query.order_by(desc(StockTop10Holder.report_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_industry_valuation(self, df: pd.DataFrame) -> int:
        """
        保存行业估值数据到数据库（支持UPSERT操作）
        Args:
            df: 包含行业估值数据的DataFrame
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning("保存行业估值数据为空，跳过")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    industry_code = row.get('industry_code', '')

                    existing = session.execute(
                        select(IndustryValuation).where(
                            and_(
                                IndustryValuation.industry_code == industry_code,
                                IndustryValuation.trade_date == trade_date,
                            )
                        )
                    ).scalar_one_or_none()

                    if existing:
                        existing.industry_name = row.get('industry_name')
                        existing.pe_static = row.get('pe_static')
                        existing.pe_ttm = row.get('pe_ttm')
                        existing.pb = row.get('pb')
                        existing.dividend_ratio = row.get('dividend_ratio')
                        existing.stock_count = row.get('stock_count')
                        existing.data_source = row.get('data_source', 'Tushare')
                        existing.updated_at = datetime.now()
                    else:
                        record = IndustryValuation(
                            industry_code=industry_code,
                            industry_name=row.get('industry_name'),
                            trade_date=trade_date,
                            pe_static=row.get('pe_static'),
                            pe_ttm=row.get('pe_ttm'),
                            pb=row.get('pb'),
                            dividend_ratio=row.get('dividend_ratio'),
                            stock_count=row.get('stock_count'),
                            data_source=row.get('data_source', 'Tushare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存行业估值数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存行业估值数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存行业估值数据失败: {e}")
                raise

        return saved_count

    def get_industry_valuation(self, industry_code: Optional[str] = None,
                               trade_date: Optional[date] = None,
                               start_date: Optional[date] = None,
                               end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取行业估值数据
        Args:
            industry_code: 行业代码（可选）
            trade_date: 交易日期（可选）
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 行业估值数据
        """
        with self.get_session() as session:
            query = select(IndustryValuation)

            if industry_code:
                query = query.where(IndustryValuation.industry_code == industry_code)
            if trade_date:
                query = query.where(IndustryValuation.trade_date == trade_date)
            if start_date:
                query = query.where(IndustryValuation.trade_date >= start_date)
            if end_date:
                query = query.where(IndustryValuation.trade_date <= end_date)

            results = session.execute(query.order_by(desc(IndustryValuation.trade_date))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'trade_date' in data_list.columns:
                data_list['trade_date'] = data_list['trade_date'].apply(lambda x: pd.Timestamp(x))

            return data_list

    def save_new_energy_penetration(self, df: pd.DataFrame) -> int:
        """
        保存新能源车渗透率数据到数据库（支持UPSERT操作）
        Args:
            df: 包含新能源车渗透率数据的DataFrame
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning("保存新能源车渗透率数据为空，跳过")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    month = parse_row_date(row.get('month'))

                    existing = session.execute(
                        select(NewEnergyPenetration).where(
                            NewEnergyPenetration.month == month
                        )
                    ).scalar_one_or_none()

                    if existing:
                        existing.total_sales = row.get('total_sales')
                        existing.new_energy_sales = row.get('new_energy_sales')
                        existing.penetration_rate = row.get('penetration_rate')
                        existing.data_source = row.get('data_source', 'Akshare')
                        existing.updated_at = datetime.now()
                    else:
                        record = NewEnergyPenetration(
                            month=month,
                            total_sales=row.get('total_sales'),
                            new_energy_sales=row.get('new_energy_sales'),
                            penetration_rate=row.get('penetration_rate'),
                            data_source=row.get('data_source', 'Akshare'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存新能源车渗透率数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存新能源车渗透率数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存新能源车渗透率数据失败: {e}")
                raise

        return saved_count

    def get_new_energy_penetration(self, start_date: Optional[date] = None,
                                    end_date: Optional[date] = None) -> pd.DataFrame:
        """
        获取新能源车渗透率数据
        Args:
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        Returns:
            pd.DataFrame: 新能源车渗透率数据
        """
        with self.get_session() as session:
            query = select(NewEnergyPenetration)

            if start_date:
                query = query.where(NewEnergyPenetration.month >= start_date)
            if end_date:
                query = query.where(NewEnergyPenetration.month <= end_date)

            results = session.execute(query.order_by(desc(NewEnergyPenetration.month))).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'month' in data_list.columns:
                data_list['month'] = data_list['month'].apply(lambda x: pd.Timestamp(x))

            return data_list

    # ===== 车型月销量数据 (懂车帝) =========================================

    def save_vehicle_sales(self, df: pd.DataFrame, month: str) -> int:
        """保存车型月销量数据"""
        if df is None or df.empty:
            return 0
        from sqlalchemy import delete
        saved_count = 0
        with self.get_session() as session:
            try:
                session.execute(
                    delete(VehicleMonthlySales).where(VehicleMonthlySales.month == month)
                )
                for _, row in df.iterrows():
                    session.add(VehicleMonthlySales(
                        month=month,
                        series_name=row.get('series_name', ''),
                        brand_name=row.get('brand_name', ''),
                        sales_volume=int(row.get('sales_volume', 0) or 0),
                        min_price=row.get('min_price'),
                        max_price=row.get('max_price'),
                        price_range=row.get('price_range', ''),
                        rank=int(row.get('rank', 0) or 0) if row.get('rank') else None,
                        series_id=int(row.get('series_id', 0)) if row.get('series_id') else None,
                        data_source='Dongchedi',
                    ))
                    saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存车型销量数据失败: {e}")
                raise
        return saved_count

    def get_vehicle_sales(self, month: str = None, brand: str = None) -> pd.DataFrame:
        """获取车型月销量数据"""
        with self.get_session() as session:
            query = select(VehicleMonthlySales)
            if month:
                query = query.where(VehicleMonthlySales.month == month)
            if brand:
                query = query.where(VehicleMonthlySales.brand_name.like(f'%{brand}%'))
            results = session.execute(
                query.order_by(VehicleMonthlySales.sales_volume.desc())
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            return pd.DataFrame([r.to_dict() for r in results])

    # ===== 第一梯队新增 save/get =========================================

    def save_stock_repurchase(self, df: pd.DataFrame, code: str) -> int:
        """保存股票回购数据"""
        if df is None or df.empty: return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    ann_date = parse_row_date(row.get('ann_date'))
                    existing = session.execute(
                        select(StockRepurchase).where(
                            and_(StockRepurchase.code == code, StockRepurchase.ann_date == ann_date)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.proc = row.get('proc')
                        existing.vol = row.get('vol')
                        existing.amount = row.get('amount')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockRepurchase(
                            code=code, ann_date=ann_date,
                            end_date=parse_row_date(row.get('end_date')),
                            proc=row.get('proc'), exp_date=parse_row_date(row.get('exp_date')),
                            vol=row.get('vol'), amount=row.get('amount'),
                            high_limit=row.get('high_limit'), low_limit=row.get('low_limit'),
                            data_source='Tushare',
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存股票回购数据失败: {e}")
                raise
        return saved_count

    def get_stock_repurchase(self, code: str) -> pd.DataFrame:
        with self.get_session() as session:
            results = session.execute(
                select(StockRepurchase).where(StockRepurchase.code == code)
                .order_by(desc(StockRepurchase.ann_date))
            ).scalars().all()
            if not results: return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'ann_date' in df.columns: df['ann_date'] = pd.to_datetime(df['ann_date'])
            return df

    def save_stock_share_float(self, df: pd.DataFrame, code: str) -> int:
        """保存限售解禁数据"""
        if df is None or df.empty: return 0
        saved_count = 0
        # 同一批 dedup：去重 (float_date, holder_name)，防止事务内互不可见导致的 UNIQUE 冲突
        seen = set()
        rows = []
        for _, row in df.iterrows():
            fd = parse_row_date(row.get('float_date'))
            hn = row.get('holder_name', '')
            key = (fd, hn)
            if key not in seen:
                seen.add(key)
                rows.append(row)
        with self.get_session() as session:
            try:
                for _, row in enumerate(rows):
                    float_date = parse_row_date(row.get('float_date'))
                    holder_name = row.get('holder_name', '')
                    existing = session.execute(
                        select(StockShareFloat).where(
                            and_(StockShareFloat.code == code,
                                 StockShareFloat.float_date == float_date,
                                 StockShareFloat.holder_name == holder_name)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.float_share = row.get('float_share')
                        existing.float_ratio = row.get('float_ratio')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockShareFloat(
                            code=code, ann_date=parse_row_date(row.get('ann_date')),
                            float_date=float_date,
                            float_share=row.get('float_share'), float_ratio=row.get('float_ratio'),
                            holder_name=holder_name, share_type=row.get('share_type'),
                            data_source='Tushare',
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存限售解禁数据失败: {e}")
                raise
        return saved_count

    def get_stock_share_float(self, code: str, future_only: bool = True) -> pd.DataFrame:
        with self.get_session() as session:
            query = select(StockShareFloat).where(StockShareFloat.code == code)
            if future_only:
                query = query.where(StockShareFloat.float_date >= date.today())
            results = session.execute(query.order_by(StockShareFloat.float_date.asc())).scalars().all()
            if not results: return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            for c in ['ann_date', 'float_date']:
                if c in df.columns: df[c] = pd.to_datetime(df[c])
            return df

    def save_stock_broker_reco(self, df: pd.DataFrame, code: str) -> int:
        """保存分析师评级"""
        if df is None or df.empty: return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    month = str(row.get('month', ''))
                    broker = str(row.get('broker', ''))
                    existing = session.execute(
                        select(StockBrokerReco).where(
                            and_(StockBrokerReco.code == code,
                                 StockBrokerReco.month == month,
                                 StockBrokerReco.broker == broker)
                        )
                    ).scalar_one_or_none()
                    if not existing:
                        session.add(StockBrokerReco(
                            code=code, month=month, broker=broker,
                            name=row.get('name'), data_source='Tushare',
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存分析师评级失败: {e}")
                raise
        return saved_count

    def get_stock_broker_reco(self, code: str, months: int = 3) -> pd.DataFrame:
        from datetime import timedelta
        cutoff = (date.today().replace(day=1) - timedelta(days=months * 31)).strftime('%Y%m')
        with self.get_session() as session:
            results = session.execute(
                select(StockBrokerReco).where(
                    and_(StockBrokerReco.code == code, StockBrokerReco.month >= cutoff)
                ).order_by(desc(StockBrokerReco.month))
            ).scalars().all()
            if not results: return pd.DataFrame()
            return pd.DataFrame([r.to_dict() for r in results])

    def save_stock_pledge(self, df: pd.DataFrame, code: str) -> int:
        """保存股权质押统计"""
        if df is None or df.empty: return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    end_date = parse_row_date(row.get('end_date'))
                    existing = session.execute(
                        select(StockPledge).where(
                            and_(StockPledge.code == code, StockPledge.end_date == end_date)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.pledge_count = row.get('pledge_count')
                        existing.unrest_pledge = row.get('unrest_pledge')
                        existing.rest_pledge = row.get('rest_pledge')
                        existing.total_share = row.get('total_share')
                        existing.pledge_ratio = row.get('pledge_ratio')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockPledge(
                            code=code, end_date=end_date,
                            pledge_count=row.get('pledge_count'),
                            unrest_pledge=row.get('unrest_pledge'),
                            rest_pledge=row.get('rest_pledge'),
                            total_share=row.get('total_share'),
                            pledge_ratio=row.get('pledge_ratio'),
                            data_source='Tushare',
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存股权质押数据失败: {e}")
                raise
        return saved_count

    def get_stock_pledge(self, code: str) -> pd.DataFrame:
        with self.get_session() as session:
            results = session.execute(
                select(StockPledge).where(StockPledge.code == code)
                .order_by(desc(StockPledge.end_date)).limit(20)
            ).scalars().all()
            if not results: return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'end_date' in df.columns: df['end_date'] = pd.to_datetime(df['end_date'])
            return df

    def save_stock_block_trade(self, df: pd.DataFrame, code: str) -> int:
        """保存大宗交易"""
        if df is None or df.empty: return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    price = float(row.get('price', 0))
                    vol = float(row.get('vol', 0))
                    existing = session.execute(
                        select(StockBlockTrade).where(
                            and_(StockBlockTrade.code == code,
                                 StockBlockTrade.trade_date == trade_date,
                                 StockBlockTrade.price == price,
                                 StockBlockTrade.vol == vol)
                        )
                    ).scalar_one_or_none()
                    if not existing:
                        session.add(StockBlockTrade(
                            code=code, trade_date=trade_date,
                            price=price, vol=vol,
                            amount=row.get('amount'),
                            buyer=row.get('buyer'), seller=row.get('seller'),
                            data_source='Tushare',
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存大宗交易数据失败: {e}")
                raise
        return saved_count

    def get_stock_block_trade(self, code: str, days: int = 90) -> pd.DataFrame:
        from datetime import timedelta
        cutoff = date.today() - timedelta(days=days)
        with self.get_session() as session:
            results = session.execute(
                select(StockBlockTrade).where(
                    and_(StockBlockTrade.code == code, StockBlockTrade.trade_date >= cutoff)
                ).order_by(desc(StockBlockTrade.trade_date))
            ).scalars().all()
            if not results: return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns: df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    def save_stock_top_list(self, df: pd.DataFrame, code: str) -> int:
        """保存龙虎榜"""
        if df is None or df.empty: return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = session.execute(
                        select(StockTopList).where(
                            and_(StockTopList.code == code, StockTopList.trade_date == trade_date)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.pct_change = row.get('pct_change')
                        existing.turnover_rate = row.get('turnover_rate')
                        existing.net_amount = row.get('net_amount')
                        existing.reason = row.get('reason')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockTopList(
                            code=code, trade_date=trade_date,
                            name=row.get('name'), close=row.get('close'),
                            pct_change=row.get('pct_change'),
                            turnover_rate=row.get('turnover_rate'),
                            amount=row.get('amount'),
                            l_sell=row.get('l_sell'), l_buy=row.get('l_buy'),
                            l_amount=row.get('l_amount'),
                            net_amount=row.get('net_amount'),
                            net_rate=row.get('net_rate'),
                            amount_rate=row.get('amount_rate'),
                            float_values=row.get('float_values'),
                            reason=row.get('reason'),
                            data_source='Tushare',
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存龙虎榜数据失败: {e}")
                raise
        return saved_count

    def get_stock_top_list(self, code: str, days: int = 90) -> pd.DataFrame:
        from datetime import timedelta
        cutoff = date.today() - timedelta(days=days)
        with self.get_session() as session:
            results = session.execute(
                select(StockTopList).where(
                    and_(StockTopList.code == code, StockTopList.trade_date >= cutoff)
                ).order_by(desc(StockTopList.trade_date))
            ).scalars().all()
            if not results: return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns: df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    def save_stock_top_inst(self, df: pd.DataFrame, code: str) -> int:
        """保存龙虎榜机构席位"""
        if df is None or df.empty: return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = session.execute(
                        select(StockTopInst).where(
                            and_(StockTopInst.code == code, StockTopInst.trade_date == trade_date)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.buy = row.get('buy')
                        existing.sell = row.get('sell')
                        existing.net_buy = row.get('net_buy')
                        existing.side = row.get('side')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockTopInst(
                            code=code, trade_date=trade_date,
                            exalter=row.get('exalter'),
                            buy=row.get('buy'), buy_rate=row.get('buy_rate'),
                            sell=row.get('sell'), sell_rate=row.get('sell_rate'),
                            net_buy=row.get('net_buy'),
                            side=row.get('side'), reason=row.get('reason'),
                            data_source='Tushare',
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存机构席位数据失败: {e}")
                raise
        return saved_count

    def get_stock_top_inst(self, code: str, days: int = 90) -> pd.DataFrame:
        from datetime import timedelta
        cutoff = date.today() - timedelta(days=days)
        with self.get_session() as session:
            results = session.execute(
                select(StockTopInst).where(
                    and_(StockTopInst.code == code, StockTopInst.trade_date >= cutoff)
                ).order_by(desc(StockTopInst.trade_date))
            ).scalars().all()
            if not results: return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns: df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    # ===== 监控清单 / 监控事件 ============================================

    def add_watch_target(self, name: str, target_type: str = 'company',
                         code: str = None, keywords: str = None,
                         source: str = 'manual') -> Dict[str, Any]:
        """
        添加监控标的（同名已存在则重新启用并更新代码/关键词）。
        source: manual=用户手动加（永不自动停用）/ auto=分析流程自动加（跟随触发条件生命周期）。
        用户手动加过的标的即使后来被分析流程再加一次，也保持 manual 不降级。
        """
        with self.get_session() as session:
            try:
                existing = session.execute(
                    select(WatchTarget).where(
                        and_(WatchTarget.target_type == target_type, WatchTarget.name == name)
                    )
                ).scalar_one_or_none()
                if existing:
                    existing.enabled = 1
                    if code:
                        existing.code = code
                    if keywords:
                        existing.keywords = keywords
                    if source == 'manual':
                        existing.source = 'manual'  # 手动加入优先级更高，只升不降
                    session.commit()
                    return existing.to_dict()
                record = WatchTarget(target_type=target_type, code=code, name=name,
                                     keywords=keywords, source=source)
                session.add(record)
                session.commit()
                return record.to_dict()
            except Exception:
                session.rollback()
                raise

    def disable_stale_auto_industry_targets(self) -> int:
        """
        自动停用"没有存活触发条件"的 auto 行业监控标的：
        分析流程加入的行业监控只为盯重估触发条件，条件全部命中/过期后继续每天
        扫新闻+LLM 评估纯属烧钱。手动（manual）加入的永不动。返回停用条数。
        """
        with self.get_session() as session:
            try:
                active_industries = set(session.execute(
                    select(IndustryReevalTrigger.industry).where(
                        IndustryReevalTrigger.status == 'active')
                ).scalars().all())
                targets = session.execute(
                    select(WatchTarget).where(and_(
                        WatchTarget.target_type == 'industry',
                        WatchTarget.source == 'auto',
                        WatchTarget.enabled == 1))
                ).scalars().all()
                count = 0
                for t in targets:
                    if t.name not in active_industries:
                        t.enabled = 0
                        count += 1
                session.commit()
                return count
            except Exception:
                session.rollback()
                raise

    def remove_watch_target(self, name_or_code: str) -> bool:
        """按名字或代码停用监控标的，返回是否找到"""
        with self.get_session() as session:
            try:
                targets = session.execute(
                    select(WatchTarget).where(
                        or_(WatchTarget.name == name_or_code, WatchTarget.code == name_or_code)
                    )
                ).scalars().all()
                if not targets:
                    return False
                for t in targets:
                    t.enabled = 0
                session.commit()
                return True
            except Exception:
                session.rollback()
                raise

    def get_watch_targets(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """获取监控清单"""
        with self.get_session() as session:
            stmt = select(WatchTarget)
            if enabled_only:
                stmt = stmt.where(WatchTarget.enabled == 1)
            results = session.execute(stmt.order_by(WatchTarget.created_at)).scalars().all()
            return [t.to_dict() for t in results]

    def monitor_event_exists(self, dedup_key: str) -> bool:
        """检查监控事件是否已存在（去重）"""
        with self.get_session() as session:
            found = session.execute(
                select(MonitorEvent.id).where(MonitorEvent.dedup_key == dedup_key)
            ).scalar_one_or_none()
            return found is not None

    def save_monitor_event(self, target: str, event_type: str, dedup_key: str,
                           title: str = None, content: str = None,
                           importance: str = None, pushed: bool = False) -> bool:
        """保存监控事件；dedup_key 冲突（已存在）返回 False"""
        with self.get_session() as session:
            try:
                record = MonitorEvent(
                    target=target, event_type=event_type, dedup_key=dedup_key,
                    title=(title or '')[:300], content=content, importance=importance,
                    pushed_at=datetime.now() if pushed else None,
                )
                session.add(record)
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False

    def count_events_pushed_today(self) -> int:
        """今日已推送事件数（用于日推送上限）"""
        with self.get_session() as session:
            today_start = datetime.combine(date.today(), datetime.min.time())
            results = session.execute(
                select(MonitorEvent.id).where(MonitorEvent.pushed_at >= today_start)
            ).scalars().all()
            return len(results)

    # ===== 公告正文缓存 =====================================================

    def get_announcement_text(self, code: str, title: str) -> Optional[str]:
        """按 (code, title) 取缓存的公告正文；无缓存返回 None"""
        with self.get_session() as session:
            found = session.execute(
                select(AnnouncementText.content).where(
                    and_(AnnouncementText.code == code,
                         AnnouncementText.title == title[:300]))
            ).scalar_one_or_none()
            return found

    def save_announcement_text(self, code: str, title: str,
                               ann_time: str = None, url: str = None,
                               content: str = None) -> bool:
        """缓存公告正文；(code, title) 已存在返回 False（不覆盖）"""
        with self.get_session() as session:
            try:
                session.add(AnnouncementText(
                    code=code, title=(title or '')[:300],
                    ann_time=ann_time, url=(url or '')[:500], content=content))
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False

    # ===== 行业重估触发条件 =================================================

    def save_industry_triggers(self, industry: str, triggers: List[Dict[str, Any]]) -> int:
        """
        保存行业重估触发条件：同一行业先把旧的 active 条目置为 expired（以最新一次分析为准），
        再写入新条目。返回写入条数。
        """
        import json as _json
        with self.get_session() as session:
            try:
                olds = session.execute(
                    select(IndustryReevalTrigger).where(
                        and_(IndustryReevalTrigger.industry == industry,
                             IndustryReevalTrigger.status == 'active')
                    )
                ).scalars().all()
                for o in olds:
                    o.status = 'expired'
                count = 0
                for t in triggers or []:
                    desc = str(t.get("description") or "").strip()
                    if not desc:
                        continue
                    pool = t.get("pool_codes")
                    session.add(IndustryReevalTrigger(
                        industry=industry,
                        trigger_type=str(t.get("trigger_type") or "news"),
                        description=desc[:300],
                        keywords=(str(t.get("keywords") or "")[:200] or None),
                        pool_codes=_json.dumps(pool, ensure_ascii=False) if pool else None,
                        pe_percentile_below=t.get("pe_percentile_below"),
                    ))
                    count += 1
                session.commit()
                return count
            except Exception:
                session.rollback()
                raise

    def get_active_industry_triggers(self, industry: str = None) -> List[Dict[str, Any]]:
        """获取生效中的行业重估触发条件（可按行业过滤）"""
        with self.get_session() as session:
            stmt = select(IndustryReevalTrigger).where(IndustryReevalTrigger.status == 'active')
            if industry:
                stmt = stmt.where(IndustryReevalTrigger.industry == industry)
            results = session.execute(stmt.order_by(IndustryReevalTrigger.created_at)).scalars().all()
            return [t.to_dict() for t in results]

    def mark_industry_trigger_hit(self, trigger_id: int, note: str = None) -> bool:
        """标记触发条件命中；已非 active 返回 False（防重复推送）"""
        with self.get_session() as session:
            try:
                record = session.execute(
                    select(IndustryReevalTrigger).where(IndustryReevalTrigger.id == trigger_id)
                ).scalar_one_or_none()
                if record is None or record.status != 'active':
                    return False
                record.status = 'hit'
                record.hit_note = (note or '')[:300]
                record.hit_at = datetime.now()
                session.commit()
                return True
            except Exception:
                session.rollback()
                raise

    def expire_stale_industry_triggers(self, days: int = 180) -> int:
        """
        触发条件自动过期：重估条件是有时效的（半年后行业格局早换了故事），
        永不过期会让监控清单越积越多、误报率上升。返回本次置为过期的条数。
        """
        with self.get_session() as session:
            try:
                cutoff = datetime.now() - timedelta(days=days)
                stale = session.execute(
                    select(IndustryReevalTrigger).where(
                        and_(IndustryReevalTrigger.status == 'active',
                             IndustryReevalTrigger.created_at <= cutoff)
                    )
                ).scalars().all()
                for t in stale:
                    t.status = 'expired'
                    t.hit_note = f'超过{days}天未命中，自动过期'
                session.commit()
                return len(stale)
            except Exception:
                session.rollback()
                raise

    # ===== 分析快照 / 复盘 ==================================================

    def save_analysis_snapshot(self, **kwargs) -> int:
        """保存分析快照，返回快照 id"""
        with self.get_session() as session:
            try:
                record = AnalysisSnapshot(**kwargs)
                session.add(record)
                session.commit()
                return record.id
            except Exception:
                session.rollback()
                raise

    def get_snapshots_pending_fundamental_check(self, max_age_days: int = 180) -> List[Dict[str, Any]]:
        """有基本面前瞻但尚未对账的快照（太老的不再追，前瞻只针对下一期财报）"""
        with self.get_session() as session:
            cutoff = datetime.now() - timedelta(days=max_age_days)
            results = session.execute(
                select(AnalysisSnapshot).where(
                    and_(AnalysisSnapshot.fundamental_outlook.isnot(None),
                         AnalysisSnapshot.fundamental_verdict.is_(None),
                         AnalysisSnapshot.created_at >= cutoff)
                ).order_by(AnalysisSnapshot.created_at)
            ).scalars().all()
            return [s.to_dict() for s in results]

    def set_snapshot_fundamental_verdict(self, snapshot_id: int, verdict: str, note: str = None) -> bool:
        """写入基本面前瞻对账结果"""
        with self.get_session() as session:
            try:
                record = session.execute(
                    select(AnalysisSnapshot).where(AnalysisSnapshot.id == snapshot_id)
                ).scalar_one_or_none()
                if record is None:
                    return False
                record.fundamental_verdict = (verdict or '')[:10]
                record.fundamental_note = (note or '')[:300]
                session.commit()
                return True
            except Exception:
                session.rollback()
                raise

    def get_fundamental_accuracy(self, recent_n: int = 50) -> Dict[str, Any]:
        """基本面前瞻成绩单：最近 N 条已对账前瞻的命中率"""
        with self.get_session() as session:
            results = session.execute(
                select(AnalysisSnapshot.fundamental_verdict).where(
                    AnalysisSnapshot.fundamental_verdict.isnot(None)
                ).order_by(AnalysisSnapshot.created_at.desc()).limit(recent_n)
            ).scalars().all()
            judged = [v for v in results if v in ('正确', '错误')]
            correct = sum(1 for v in judged if v == '正确')
            return {
                "total": len(results),
                "judged": len(judged),
                "correct": correct,
                "accuracy": round(correct / len(judged) * 100, 1) if judged else None,
            }

    def get_snapshots_due_review(self, after_days: int = 5) -> List[Dict[str, Any]]:
        """获取到期待复盘的快照（创建超过 after_days 个自然日且未复盘）"""
        with self.get_session() as session:
            cutoff = datetime.now() - timedelta(days=after_days)
            results = session.execute(
                select(AnalysisSnapshot).where(
                    and_(AnalysisSnapshot.review_done == 0,
                         AnalysisSnapshot.created_at <= cutoff)
                ).order_by(AnalysisSnapshot.created_at)
            ).scalars().all()
            return [r.to_dict() for r in results]

    def get_latest_snapshot(self, code: str, exclude_id: int = None) -> Optional[Dict[str, Any]]:
        """获取某股票最近一次分析快照"""
        with self.get_session() as session:
            stmt = select(AnalysisSnapshot).where(AnalysisSnapshot.code == code)
            if exclude_id is not None:
                stmt = stmt.where(AnalysisSnapshot.id != exclude_id)
            result = session.execute(
                stmt.order_by(desc(AnalysisSnapshot.created_at)).limit(1)
            ).scalar_one_or_none()
            return result.to_dict() if result else None

    def mark_snapshot_reviewed(self, snapshot_id: int) -> None:
        with self.get_session() as session:
            try:
                snap = session.execute(
                    select(AnalysisSnapshot).where(AnalysisSnapshot.id == snapshot_id)
                ).scalar_one_or_none()
                if snap:
                    snap.review_done = 1
                    session.commit()
            except Exception:
                session.rollback()
                raise

    def save_analysis_review(self, **kwargs) -> int:
        with self.get_session() as session:
            try:
                record = AnalysisReview(**kwargs)
                session.add(record)
                session.commit()
                return record.id
            except Exception:
                session.rollback()
                raise

    def save_improvement_rule(self, **kwargs) -> int:
        """保存一条改进规则"""
        with self.get_session() as session:
            try:
                record = ImprovementRule(**kwargs)
                session.add(record)
                session.commit()
                return record.id
            except Exception:
                session.rollback()
                raise

    def get_active_rules(self, code: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """获取有效的改进规则：如果传 code 则优先取该标的规则+通用规则，
           不传 code 只取通用规则"""
        with self.get_session() as session:
            query = select(ImprovementRule).where(ImprovementRule.is_active == 1)
            if code:
                query = query.where(
                    or_(ImprovementRule.code == code, ImprovementRule.code.is_(None))
                )
            else:
                query = query.where(ImprovementRule.code.is_(None))
            query = query.order_by(desc(ImprovementRule.created_at)).limit(limit)
            results = session.execute(query).scalars().all()
            return [r.to_dict() for r in results]

    def get_rule_by_snapshot(self, snapshot_id: int) -> Optional[Dict[str, Any]]:
        """根据复盘快照id查找是否已生成规则"""
        with self.get_session() as session:
            result = session.execute(
                select(ImprovementRule).where(
                    ImprovementRule.source_snapshot_id == snapshot_id
                ).limit(1)
            ).scalar_one_or_none()
            return result.to_dict() if result else None

    def increment_rule_hit(self, rule_id: int) -> None:
        """增加规则引用计数"""
        with self.get_session() as session:
            try:
                rule = session.execute(
                    select(ImprovementRule).where(ImprovementRule.id == rule_id)
                ).scalar_one_or_none()
                if rule:
                    rule.hit_count = (rule.hit_count or 0) + 1
                    session.commit()
            except Exception:
                session.rollback()

    def get_last_review_for_code(self, code: str) -> Optional[Dict[str, Any]]:
        """获取某股票最近一次复盘记录（供下次分析注入）"""
        with self.get_session() as session:
            result = session.execute(
                select(AnalysisReview).where(AnalysisReview.code == code)
                .order_by(desc(AnalysisReview.created_at)).limit(1)
            ).scalar_one_or_none()
            return result.to_dict() if result else None

    # ===== 产业链快照 / 复盘 ================================================

    def save_industry_snapshot(self, **kwargs) -> int:
        with self.get_session() as session:
            try:
                record = IndustrySnapshot(**kwargs)
                session.add(record)
                session.commit()
                return record.id
            except Exception:
                session.rollback()
                raise

    def get_industry_snapshots_due_review(self, after_days: int = 10) -> List[Dict[str, Any]]:
        """到期待复盘的产业链快照"""
        with self.get_session() as session:
            cutoff = datetime.now() - timedelta(days=after_days)
            results = session.execute(
                select(IndustrySnapshot).where(
                    and_(IndustrySnapshot.review_done == 0,
                         IndustrySnapshot.created_at <= cutoff)
                ).order_by(IndustrySnapshot.created_at)
            ).scalars().all()
            return [r.to_dict() for r in results]

    def get_latest_industry_snapshot(self, industry_name: str) -> Optional[Dict[str, Any]]:
        """按行业名模糊匹配最近一次产业链快照"""
        with self.get_session() as session:
            result = session.execute(
                select(IndustrySnapshot)
                .where(IndustrySnapshot.industry_name.like(f"%{industry_name}%"))
                .order_by(desc(IndustrySnapshot.created_at)).limit(1)
            ).scalar_one_or_none()
            return result.to_dict() if result else None

    def mark_industry_snapshot_reviewed(self, snapshot_id: int) -> None:
        with self.get_session() as session:
            try:
                snap = session.execute(
                    select(IndustrySnapshot).where(IndustrySnapshot.id == snapshot_id)
                ).scalar_one_or_none()
                if snap:
                    snap.review_done = 1
                    session.commit()
            except Exception:
                session.rollback()
                raise

    def save_industry_review(self, **kwargs) -> int:
        with self.get_session() as session:
            try:
                record = IndustryReview(**kwargs)
                session.add(record)
                session.commit()
                return record.id
            except Exception:
                session.rollback()
                raise

    # ===== 用户纠错记录 =====================================================

    def save_user_feedback(self, target_name: str, content: str,
                           code: str = None, snapshot_id: int = None) -> int:
        """保存一条用户纠错记录，返回 id"""
        with self.get_session() as session:
            try:
                record = UserFeedback(target_name=target_name, content=content,
                                      code=code, snapshot_id=snapshot_id)
                session.add(record)
                session.commit()
                return record.id
            except Exception:
                session.rollback()
                raise

    def get_feedback_for_target(self, code: str = None, name: str = None,
                                limit: int = 8) -> List[Dict[str, Any]]:
        """按代码或对象名取纠错记录（新→旧），供分析注入与复盘对账"""
        if not code and not name:
            return []
        with self.get_session() as session:
            conds = []
            if code:
                conds.append(UserFeedback.code == code)
            if name:
                conds.append(UserFeedback.target_name.like(f"%{name}%"))
            results = session.execute(
                select(UserFeedback).where(or_(*conds))
                .order_by(desc(UserFeedback.created_at)).limit(limit)
            ).scalars().all()
            return [r.to_dict() for r in results]

    def list_recent_feedback(self, limit: int = 20) -> List[Dict[str, Any]]:
        """最近的纠错记录（飞书「纠错列表」用）"""
        with self.get_session() as session:
            results = session.execute(
                select(UserFeedback).order_by(desc(UserFeedback.created_at)).limit(limit)
            ).scalars().all()
            return [r.to_dict() for r in results]

    def get_industry_track_record(self, recent_n: int = 20) -> Dict[str, Any]:
        """产业链选股成绩单：组合跑赢次数 / 排名有效次数"""
        with self.get_session() as session:
            results = session.execute(
                select(IndustryReview).order_by(desc(IndustryReview.created_at)).limit(recent_n)
            ).scalars().all()
            total = len(results)
            outperform = sum(1 for r in results if r.portfolio_verdict == "跑赢")
            rank_ok = sum(1 for r in results if r.rank_effective == "有效")
            return {"total": total, "outperform": outperform, "rank_effective": rank_ok}

    def get_direction_accuracy(self, recent_n: int = 30) -> Dict[str, Any]:
        """近 N 次复盘的方向判断命中率（系统成绩单）"""
        with self.get_session() as session:
            results = session.execute(
                select(AnalysisReview).order_by(desc(AnalysisReview.created_at)).limit(recent_n)
            ).scalars().all()
            verdicts = [r.direction_verdict for r in results if r.direction_verdict]
            judged = [v for v in verdicts if v in ("正确", "错误")]
            correct = sum(1 for v in judged if v == "正确")
            return {
                "total": len(verdicts),
                "judged": len(judged),
                "correct": correct,
                "accuracy": round(correct / len(judged) * 100, 1) if judged else None,
            }

    # ===== 社交媒体账号缓存 ===================================================

    def get_social_account(self, code: str) -> Optional[Dict[str, Any]]:
        """获取某股票缓存的社交媒体账号信息"""
        with self.get_session() as session:
            found = session.execute(
                select(CompanySocialAccount).where(CompanySocialAccount.code == code)
            ).scalar_one_or_none()
            return found.to_dict() if found else None

    def save_social_account(self, code: str, company_name: str,
                            weibo_uid: str = None, weibo_name: str = None,
                            wechat_name: str = None, wechat_id: str = None,
                            weibo_posts: str = None, wechat_articles: str = None) -> bool:
        """保存/更新公司社交媒体账号信息（code 唯一，upsert）"""
        with self.get_session() as session:
            try:
                existing = session.execute(
                    select(CompanySocialAccount).where(CompanySocialAccount.code == code)
                ).scalar_one_or_none()
                if existing:
                    if weibo_uid is not None: existing.weibo_uid = weibo_uid
                    if weibo_name is not None: existing.weibo_name = weibo_name
                    if wechat_name is not None: existing.wechat_name = wechat_name
                    if wechat_id is not None: existing.wechat_id = wechat_id
                    if weibo_posts is not None: existing.weibo_posts = weibo_posts
                    if wechat_articles is not None: existing.wechat_articles = wechat_articles
                    existing.updated_at = datetime.now()
                else:
                    session.add(CompanySocialAccount(
                        code=code, company_name=company_name,
                        weibo_uid=weibo_uid, weibo_name=weibo_name,
                        wechat_name=wechat_name, wechat_id=wechat_id,
                        weibo_posts=weibo_posts, wechat_articles=wechat_articles))
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False

    # ===== 分红送股数据 ===================================================

    def save_stock_dividend(self, df: pd.DataFrame, code: str) -> int:
        """
        保存分红送股数据到数据库（merge upsert，避免 UNIQUE 冲突）
        Args:
            df: 包含分红送股数据的DataFrame（NaN 已清理为 None）
            code: 股票代码
        Returns:
            int: 写入/更新的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存分红送股数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                # 防御性：确保 DataFrame 中无 NaN
                df = df.copy()
                for col in df.columns:
                    if df[col].dtype.kind == 'f':
                        df[col] = df[col].fillna(None)

                def _v(v):
                    """NaN → None 防御"""
                    if v is None:
                        return None
                    try:
                        return None if pd.isna(v) else v
                    except Exception:
                        return v

                # 去重：同一 end_date 只保留一行（Tushare 可能返回多个版本）
                df_dedup = df.drop_duplicates(subset=['end_date'], keep='first').reset_index(drop=True)

                for _, row in df_dedup.iterrows():
                    end_date = parse_row_date(row.get('end_date'))
                    if end_date is None:
                        continue
                    # 查找已有记录
                    existing = session.execute(
                        select(StockDividend).where(
                            and_(
                                StockDividend.code == code,
                                StockDividend.end_date == end_date
                            )
                        )
                    ).scalar_one_or_none()
                    if existing:
                        # 更新已有记录
                        existing.div_procf = _v(row.get('div_procf'))
                        existing.stk_bo_rate = _v(row.get('stk_bo_rate'))
                        existing.stk_co_rate = _v(row.get('stk_co_rate'))
                        existing.cash_div = _v(row.get('cash_div'))
                        existing.ex_date = parse_row_date(row.get('ex_date')) if row.get('ex_date') else None
                        existing.pay_date = parse_row_date(row.get('pay_date')) if row.get('pay_date') else None
                        existing.updated_at = datetime.now()
                    else:
                        # 新增记录
                        record = StockDividend(
                            code=code,
                            end_date=end_date,
                            div_procf=_v(row.get('div_procf')),
                            stk_bo_rate=_v(row.get('stk_bo_rate')),
                            stk_co_rate=_v(row.get('stk_co_rate')),
                            cash_div=_v(row.get('cash_div')),
                            ex_date=parse_row_date(row.get('ex_date')) if row.get('ex_date') else None,
                            pay_date=parse_row_date(row.get('pay_date')) if row.get('pay_date') else None,
                        )
                        session.add(record)
                    saved_count += 1

                session.commit()
                logger.info(f"保存 {code} 分红送股数据成功，写入/更新 {saved_count} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 分红送股数据失败: {e}")
                raise

        return saved_count

    def get_stock_dividend(self, code: str, limit: int = 10) -> pd.DataFrame:
        """
        获取分红送股数据
        Args:
            code: 股票代码
            limit: 返回记录数上限
        Returns:
            pd.DataFrame: 分红送股数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDividend)
                .where(StockDividend.code == code)
                .order_by(desc(StockDividend.end_date))
                .limit(limit)
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'end_date' in data_list.columns:
                data_list['end_date'] = data_list['end_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    # ===== 财务审计意见数据 ===================================================

    def save_stock_fina_audit(self, df: pd.DataFrame, code: str) -> int:
        """
        保存财务审计意见数据到数据库（支持UPSERT操作）
        Args:
            df: 包含财务审计意见数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存财务审计意见数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                end_dates = [parse_row_date(d) for d in df['end_date'].tolist()]
                existing_records = session.execute(
                    select(StockFinaAudit).where(
                        and_(
                            StockFinaAudit.code == code,
                            StockFinaAudit.end_date.in_(end_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.end_date: r for r in existing_records}

                for _, row in df.iterrows():
                    end_date = parse_row_date(row.get('end_date'))
                    existing = existing_map.get(end_date)

                    if existing:
                        existing.audit_opinion = row.get('audit_opinion')
                        existing.opinions = row.get('opinions')
                        existing.auditor = row.get('auditor')
                        existing.audit_fee = row.get('audit_fee')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockFinaAudit(
                            code=code,
                            end_date=end_date,
                            audit_opinion=row.get('audit_opinion'),
                            opinions=row.get('opinions'),
                            auditor=row.get('auditor'),
                            audit_fee=row.get('audit_fee'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 财务审计意见数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 财务审计意见数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 财务审计意见数据失败: {e}")
                raise

        return saved_count

    def get_stock_fina_audit(self, code: str, limit: int = 10) -> pd.DataFrame:
        """
        获取财务审计意见数据
        Args:
            code: 股票代码
            limit: 返回记录数上限
        Returns:
            pd.DataFrame: 财务审计意见数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockFinaAudit)
                .where(StockFinaAudit.code == code)
                .order_by(desc(StockFinaAudit.end_date))
                .limit(limit)
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'end_date' in data_list.columns:
                data_list['end_date'] = data_list['end_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    # ===== 财报披露计划数据 ===================================================

    def save_stock_disclosure_date(self, df: pd.DataFrame, code: str) -> int:
        """
        保存财报披露计划数据到数据库（支持UPSERT操作）
        Args:
            df: 包含财报披露计划数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存财报披露计划数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                end_dates = [parse_row_date(d) for d in df['end_date'].tolist()]
                existing_records = session.execute(
                    select(StockDisclosureDate).where(
                        and_(
                            StockDisclosureDate.code == code,
                            StockDisclosureDate.end_date.in_(end_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {r.end_date: r for r in existing_records}

                for _, row in df.iterrows():
                    end_date = parse_row_date(row.get('end_date'))
                    existing = existing_map.get(end_date)

                    if existing:
                        existing.stm_issue_date = parse_row_date(row.get('stm_issue_date')) if row.get('stm_issue_date') else None
                        existing.stm_comm_date = parse_row_date(row.get('stm_comm_date')) if row.get('stm_comm_date') else None
                        existing.actual_diss_date = parse_row_date(row.get('actual_diss_date')) if row.get('actual_diss_date') else None
                        existing.updated_at = datetime.now()
                    else:
                        record = StockDisclosureDate(
                            code=code,
                            end_date=end_date,
                            stm_issue_date=parse_row_date(row.get('stm_issue_date')) if row.get('stm_issue_date') else None,
                            stm_comm_date=parse_row_date(row.get('stm_comm_date')) if row.get('stm_comm_date') else None,
                            actual_diss_date=parse_row_date(row.get('actual_diss_date')) if row.get('actual_diss_date') else None,
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 财报披露计划数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 财报披露计划数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 财报披露计划数据失败: {e}")
                raise

        return saved_count

    def get_stock_disclosure_date(self, code: str, limit: int = 10) -> pd.DataFrame:
        """
        获取财报披露计划数据
        Args:
            code: 股票代码
            limit: 返回记录数上限
        Returns:
            pd.DataFrame: 财报披露计划数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDisclosureDate)
                .where(StockDisclosureDate.code == code)
                .order_by(desc(StockDisclosureDate.end_date))
                .limit(limit)
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'end_date' in data_list.columns:
                data_list['end_date'] = data_list['end_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    # ===== 股权质押明细数据 ===================================================

    def save_stock_pledge_detail(self, df: pd.DataFrame, code: str) -> int:
        """
        保存股权质押明细数据到数据库（支持UPSERT操作）
        Args:
            df: 包含股权质押明细数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存股权质押明细数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                end_dates = [parse_row_date(d) for d in df['end_date'].tolist()]
                existing_records = session.execute(
                    select(StockPledgeDetail).where(
                        and_(
                            StockPledgeDetail.code == code,
                            StockPledgeDetail.end_date.in_(end_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {(r.end_date, r.pledger): r for r in existing_records}

                for _, row in df.iterrows():
                    end_date = parse_row_date(row.get('end_date'))
                    pledger = row.get('pledger')
                    existing = existing_map.get((end_date, pledger))

                    if existing:
                        existing.pledge_amount = row.get('pledge_amount')
                        existing.pledge_ratio = row.get('pledge_ratio')
                        existing.pledge_total_ratio = row.get('pledge_total_ratio')
                        existing.pledge_start_date = parse_row_date(row.get('pledge_start_date')) if row.get('pledge_start_date') else None
                        existing.pledge_end_date = parse_row_date(row.get('pledge_end_date')) if row.get('pledge_end_date') else None
                        existing.pledge_status = row.get('pledge_status')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockPledgeDetail(
                            code=code,
                            end_date=end_date,
                            pledger=pledger,
                            pledge_amount=row.get('pledge_amount'),
                            pledge_ratio=row.get('pledge_ratio'),
                            pledge_total_ratio=row.get('pledge_total_ratio'),
                            pledge_start_date=parse_row_date(row.get('pledge_start_date')) if row.get('pledge_start_date') else None,
                            pledge_end_date=parse_row_date(row.get('pledge_end_date')) if row.get('pledge_end_date') else None,
                            pledge_status=row.get('pledge_status'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 股权质押明细数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 股权质押明细数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 股权质押明细数据失败: {e}")
                raise

        return saved_count

    def get_stock_pledge_detail(self, code: str, limit: int = 20) -> pd.DataFrame:
        """
        获取股权质押明细数据
        Args:
            code: 股票代码
            limit: 返回记录数上限
        Returns:
            pd.DataFrame: 股权质押明细数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockPledgeDetail)
                .where(StockPledgeDetail.code == code)
                .order_by(desc(StockPledgeDetail.end_date))
                .limit(limit)
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'end_date' in data_list.columns:
                data_list['end_date'] = data_list['end_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    # ===== 股东增减持数据 ===================================================

    def save_stock_holder_trade(self, df: pd.DataFrame, code: str) -> int:
        """
        保存股东增减持数据到数据库（支持UPSERT操作）
        Args:
            df: 包含股东增减持数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存股东增减持数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                ann_dates = [parse_row_date(d) for d in df['ann_date'].tolist()]
                existing_records = session.execute(
                    select(StockHolderTrade).where(
                        and_(
                            StockHolderTrade.code == code,
                            StockHolderTrade.ann_date.in_(ann_dates)
                        )
                    )
                ).scalars().all()
                existing_map = {(r.ann_date, r.holder_name): r for r in existing_records}

                for _, row in df.iterrows():
                    ann_date = parse_row_date(row.get('ann_date'))
                    holder_name = row.get('holder_name')
                    existing = existing_map.get((ann_date, holder_name))

                    if existing:
                        existing.trade_type = row.get('trade_type')
                        existing.trade_volume = row.get('trade_volume')
                        existing.trade_ratio = row.get('trade_ratio')
                        existing.after_ratio = row.get('after_ratio')
                        existing.avg_price = row.get('avg_price')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockHolderTrade(
                            code=code,
                            ann_date=ann_date,
                            holder_name=holder_name,
                            trade_type=row.get('trade_type'),
                            trade_volume=row.get('trade_volume'),
                            trade_ratio=row.get('trade_ratio'),
                            after_ratio=row.get('after_ratio'),
                            avg_price=row.get('avg_price'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 股东增减持数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 股东增减持数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 股东增减持数据失败: {e}")
                raise

        return saved_count

    def get_stock_holder_trade(self, code: str, limit: int = 20) -> pd.DataFrame:
        """
        获取股东增减持数据
        Args:
            code: 股票代码
            limit: 返回记录数上限
        Returns:
            pd.DataFrame: 股东增减持数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockHolderTrade)
                .where(StockHolderTrade.code == code)
                .order_by(desc(StockHolderTrade.ann_date))
                .limit(limit)
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'ann_date' in data_list.columns:
                data_list['ann_date'] = data_list['ann_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_report_rc(self, df: pd.DataFrame, code: str) -> int:
        """
        保存卖方盈利预测数据到数据库（支持UPSERT操作）
        Args:
            df: 包含卖方盈利预测数据的DataFrame
            code: 股票代码
        Returns:
            int: 新增的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存卖方盈利预测数据为空，跳过 {code}")
            return 0

        saved_count = 0
        with self.get_session() as session:
            try:
                report_dates = [parse_row_date(d) for d in df['report_date'].tolist()]
                forecast_orgs = df['forecast_org'].tolist() if 'forecast_org' in df.columns else df.get('org_name', ['']).tolist()
                forecast_types = df['forecast_type'].tolist() if 'forecast_type' in df.columns else df.get('type', ['']).tolist()
                existing_records = session.execute(
                    select(StockReportRc).where(
                        and_(
                            StockReportRc.code == code,
                            StockReportRc.report_date.in_(report_dates),
                            StockReportRc.forecast_org.in_(forecast_orgs),
                            StockReportRc.forecast_type.in_(forecast_types),
                        )
                    )
                ).scalars().all()
                existing_map = {(r.report_date, r.forecast_org, r.forecast_type): r for r in existing_records}

                for _, row in df.iterrows():
                    report_date = parse_row_date(row.get('report_date'))
                    forecast_type = row.get('forecast_type') or row.get('type')
                    forecast_value = row.get('forecast_value') or row.get('value')
                    forecast_org = row.get('forecast_org') or row.get('org_name')
                    target_price = row.get('target_price') or row.get('target')
                    key = (report_date, forecast_org, forecast_type)
                    existing = existing_map.get(key)

                    if existing:
                        existing.forecast_value = forecast_value
                        existing.forecast_org = forecast_org
                        existing.analyst = row.get('analyst')
                        existing.rating = row.get('rating')
                        existing.rating_change = row.get('rating_change')
                        existing.target_price = target_price
                        existing.period = row.get('period')
                        existing.updated_at = datetime.now()
                    else:
                        record = StockReportRc(
                            code=code,
                            report_date=report_date,
                            forecast_type=forecast_type,
                            forecast_value=forecast_value,
                            forecast_org=forecast_org,
                            analyst=row.get('analyst'),
                            rating=row.get('rating'),
                            rating_change=row.get('rating_change'),
                            target_price=target_price,
                            period=row.get('period'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                if saved_count > 0:
                    logger.info(f"保存 {code} 卖方盈利预测数据成功，新增 {saved_count} 条记录，更新 {len(df) - saved_count} 条记录")
                else:
                    logger.info(f"保存 {code} 卖方盈利预测数据成功，更新 {len(df)} 条记录")
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 卖方盈利预测数据失败: {e}")
                raise

        return saved_count

    def get_stock_report_rc(self, code: str, limit: int = 20) -> pd.DataFrame:
        """
        获取卖方盈利预测数据
        Args:
            code: 股票代码
            limit: 返回记录数上限
        Returns:
            pd.DataFrame: 卖方盈利预测数据
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockReportRc)
                .where(StockReportRc.code == code)
                .order_by(desc(StockReportRc.report_date))
                .limit(limit)
            ).scalars().all()

            if not results:
                return pd.DataFrame()

            data_list = pd.DataFrame([obj.to_dict() for obj in results])

            if 'report_date' in data_list.columns:
                data_list['report_date'] = data_list['report_date'].apply(lambda x: pd.Timestamp(x))
                data_list['code'] = data_list['code'].astype(str)

            return data_list

    def save_stock_margin(self, df: pd.DataFrame, code: str) -> int:
        """保存融资融券交易汇总"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = session.execute(
                        select(StockMargin).where(
                            and_(StockMargin.code == code, StockMargin.trade_date == trade_date)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.margin_balance = row.get('margin_balance')
                        existing.margin_buy = row.get('margin_buy')
                        existing.short_sell_balance = row.get('short_sell_balance')
                        existing.short_sell_volume = row.get('short_sell_volume')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockMargin(
                            code=code, trade_date=trade_date,
                            margin_balance=row.get('margin_balance'),
                            margin_buy=row.get('margin_buy'),
                            short_sell_balance=row.get('short_sell_balance'),
                            short_sell_volume=row.get('short_sell_volume'),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存融资融券数据失败: {e}")
                raise
        return saved_count

    def get_stock_margin(self, code: str, limit: int = 20) -> pd.DataFrame:
        """获取融资融券交易汇总"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMargin)
                .where(StockMargin.code == code)
                .order_by(desc(StockMargin.trade_date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    def save_stock_margin_detail(self, df: pd.DataFrame, code: str) -> int:
        """保存融资融券交易明细"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = session.execute(
                        select(StockMarginDetail).where(
                            and_(StockMarginDetail.code == code, StockMarginDetail.trade_date == trade_date)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.margin_buy = row.get('margin_buy')
                        existing.rzye = row.get('rzye')
                        existing.rqye = row.get('rqye')
                        existing.rzmre = row.get('rzmre')
                        existing.rqyl = row.get('rqyl')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockMarginDetail(
                            code=code, trade_date=trade_date,
                            margin_buy=row.get('margin_buy'),
                            rzye=row.get('rzye'),
                            rqye=row.get('rqye'),
                            rzmre=row.get('rzmre'),
                            rqyl=row.get('rqyl'),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存融资融券明细数据失败: {e}")
                raise
        return saved_count

    def get_stock_margin_detail(self, code: str, limit: int = 20) -> pd.DataFrame:
        """获取融资融券交易明细"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMarginDetail)
                .where(StockMarginDetail.code == code)
                .order_by(desc(StockMarginDetail.trade_date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    def save_stock_moneyflow(self, df: pd.DataFrame, code: str) -> int:
        """保存个股资金流"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = session.execute(
                        select(StockMoneyflow).where(
                            and_(StockMoneyflow.code == code, StockMoneyflow.trade_date == trade_date)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.buy_sm_vol = row.get('buy_sm_vol')
                        existing.buy_sm_amount = row.get('buy_sm_amount')
                        existing.sell_sm_vol = row.get('sell_sm_vol')
                        existing.sell_sm_amount = row.get('sell_sm_amount')
                        existing.buy_md_vol = row.get('buy_md_vol')
                        existing.buy_md_amount = row.get('buy_md_amount')
                        existing.sell_md_vol = row.get('sell_md_vol')
                        existing.sell_md_amount = row.get('sell_md_amount')
                        existing.buy_lg_vol = row.get('buy_lg_vol')
                        existing.buy_lg_amount = row.get('buy_lg_amount')
                        existing.sell_lg_vol = row.get('sell_lg_vol')
                        existing.sell_lg_amount = row.get('sell_lg_amount')
                        existing.buy_elg_vol = row.get('buy_elg_vol')
                        existing.buy_elg_amount = row.get('buy_elg_amount')
                        existing.sell_elg_vol = row.get('sell_elg_vol')
                        existing.sell_elg_amount = row.get('sell_elg_amount')
                        existing.net_mf_amount = row.get('net_mf_amount')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockMoneyflow(
                            code=code, trade_date=trade_date,
                            buy_sm_vol=row.get('buy_sm_vol'),
                            buy_sm_amount=row.get('buy_sm_amount'),
                            sell_sm_vol=row.get('sell_sm_vol'),
                            sell_sm_amount=row.get('sell_sm_amount'),
                            buy_md_vol=row.get('buy_md_vol'),
                            buy_md_amount=row.get('buy_md_amount'),
                            sell_md_vol=row.get('sell_md_vol'),
                            sell_md_amount=row.get('sell_md_amount'),
                            buy_lg_vol=row.get('buy_lg_vol'),
                            buy_lg_amount=row.get('buy_lg_amount'),
                            sell_lg_vol=row.get('sell_lg_vol'),
                            sell_lg_amount=row.get('sell_lg_amount'),
                            buy_elg_vol=row.get('buy_elg_vol'),
                            buy_elg_amount=row.get('buy_elg_amount'),
                            sell_elg_vol=row.get('sell_elg_vol'),
                            sell_elg_amount=row.get('sell_elg_amount'),
                            net_mf_amount=row.get('net_mf_amount'),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存个股资金流数据失败: {e}")
                raise
        return saved_count

    def get_stock_moneyflow(self, code: str, limit: int = 20) -> pd.DataFrame:
        """获取个股资金流"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMoneyflow)
                .where(StockMoneyflow.code == code)
                .order_by(desc(StockMoneyflow.trade_date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    def save_stock_hsgt_moneyflow(self, df: pd.DataFrame) -> int:
        """保存沪深港通资金流"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = session.execute(
                        select(StockHsgtMoneyflow).where(
                            StockHsgtMoneyflow.trade_date == trade_date
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.ggt_ss = row.get('ggt_ss')
                        existing.ggt_sz = row.get('ggt_sz')
                        existing.hgt = row.get('hgt')
                        existing.sgt = row.get('sgt')
                        existing.north_money = row.get('north_money')
                        existing.south_money = row.get('south_money')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockHsgtMoneyflow(
                            trade_date=trade_date,
                            ggt_ss=row.get('ggt_ss'),
                            ggt_sz=row.get('ggt_sz'),
                            hgt=row.get('hgt'),
                            sgt=row.get('sgt'),
                            north_money=row.get('north_money'),
                            south_money=row.get('south_money'),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存沪深港通资金流数据失败: {e}")
                raise
        return saved_count

    def get_stock_hsgt_moneyflow(self, limit: int = 20) -> pd.DataFrame:
        """获取沪深港通资金流"""
        with self.get_session() as session:
            results = session.execute(
                select(StockHsgtMoneyflow)
                .order_by(desc(StockHsgtMoneyflow.trade_date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    def save_stock_mkt_moneyflow_dc(self, df: pd.DataFrame) -> int:
        """保存大盘资金流（日频）"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    trade_date = parse_row_date(row.get('trade_date'))
                    existing = session.execute(
                        select(StockMktMoneyflowDC).where(
                            StockMktMoneyflowDC.trade_date == trade_date
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.close_sh = row.get('close_sh')
                        existing.change_pct = row.get('change_pct')
                        existing.main_net = row.get('main_net')
                        existing.retail_net = row.get('retail_net')
                        existing.total_net = row.get('total_net')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockMktMoneyflowDC(
                            trade_date=trade_date,
                            close_sh=row.get('close_sh'),
                            change_pct=row.get('change_pct'),
                            main_net=row.get('main_net'),
                            retail_net=row.get('retail_net'),
                            total_net=row.get('total_net'),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存大盘资金流数据失败: {e}")
                raise
        return saved_count

    def get_stock_mkt_moneyflow_dc(self, limit: int = 20) -> pd.DataFrame:
        """获取大盘资金流（日频）"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMktMoneyflowDC)
                .order_by(desc(StockMktMoneyflowDC.trade_date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'trade_date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
            return df

    def save_stock_macro_rate(self, df: pd.DataFrame, rate_type: str) -> int:
        """保存宏观利率数据"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    date_val = parse_row_date(row.get('date'))
                    existing = session.execute(
                        select(StockMacroRate).where(
                            and_(StockMacroRate.rate_type == rate_type, StockMacroRate.date == date_val)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.rate_value = row.get('rate_value') or row.get(row.get('rate_type'))
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockMacroRate(
                            rate_type=rate_type, date=date_val,
                            rate_value=row.get('rate_value') or row.get(rate_type),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存宏观利率数据失败: {e}")
                raise
        return saved_count

    def get_stock_macro_rate(self, rate_type: str, limit: int = 30) -> pd.DataFrame:
        """获取宏观利率数据"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMacroRate)
                .where(StockMacroRate.rate_type == rate_type)
                .order_by(desc(StockMacroRate.date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            return df

    def save_stock_wz_index(self, df: pd.DataFrame) -> int:
        """保存温州指数"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    date_val = parse_row_date(row.get('date'))
                    existing = session.execute(
                        select(StockWzIndex).where(
                            StockWzIndex.date == date_val
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.index_value = row.get('index_value') or row.get('close')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockWzIndex(
                            date=date_val,
                            index_value=row.get('index_value') or row.get('close'),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存温州指数数据失败: {e}")
                raise
        return saved_count

    def get_stock_wz_index(self, limit: int = 20) -> pd.DataFrame:
        """获取温州指数"""
        with self.get_session() as session:
            results = session.execute(
                select(StockWzIndex)
                .order_by(desc(StockWzIndex.date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            return df

    def save_stock_gz_index(self, df: pd.DataFrame) -> int:
        """保存贵阳指数"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    date_val = parse_row_date(row.get('date'))
                    existing = session.execute(
                        select(StockGzIndex).where(
                            StockGzIndex.date == date_val
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.index_value = row.get('index_value') or row.get('close')
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockGzIndex(
                            date=date_val,
                            index_value=row.get('index_value') or row.get('close'),
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存贵阳指数数据失败: {e}")
                raise
        return saved_count

    def get_stock_gz_index(self, limit: int = 20) -> pd.DataFrame:
        """获取贵阳指数"""
        with self.get_session() as session:
            results = session.execute(
                select(StockGzIndex)
                .order_by(desc(StockGzIndex.date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            df = pd.DataFrame([r.to_dict() for r in results])
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            return df

    def save_stock_macro_indicator(self, indicator_name: str, df: pd.DataFrame) -> int:
        """保存宏观指标数据（UPSERT）"""
        if df is None or df.empty:
            return 0
        saved_count = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    # 找到第一个看起来像日期/周期的列
                    period_col = None
                    for col in df.columns:
                        col_lower = col.lower()
                        if col_lower in ('month', 'year', 'quarter', 'date', 'period', 'trade_date', 'ann_date', 'end_date'):
                            period_col = col
                            break
                    if period_col is None:
                        period_col = df.columns[0]
                    period_val = str(row.get(period_col, ''))
                    if not period_val:
                        continue
                    # 将所有列值转为JSON
                    row_dict = {}
                    for col in df.columns:
                        val = row.get(col)
                        if val is not None and not (isinstance(val, float) and pd.isna(val)):
                            if isinstance(val, (date, datetime)):
                                row_dict[col] = str(val)
                            else:
                                row_dict[col] = val
                    value_json_str = json.dumps(row_dict, ensure_ascii=False, default=str)
                    existing = session.execute(
                        select(StockMacroIndicator).where(
                            and_(
                                StockMacroIndicator.indicator_name == indicator_name,
                                StockMacroIndicator.period == period_val,
                            )
                        )
                    ).scalar_one_or_none()
                    if existing:
                        existing.value_json = value_json_str
                        existing.updated_at = datetime.now()
                    else:
                        session.add(StockMacroIndicator(
                            indicator_name=indicator_name,
                            period=period_val,
                            value_json=value_json_str,
                        ))
                        saved_count += 1
                session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"保存宏观指标[{indicator_name}]数据失败: {e}")
                raise
        return saved_count

    def get_stock_macro_indicator(self, indicator_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        """获取宏观指标数据"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMacroIndicator)
                .where(StockMacroIndicator.indicator_name == indicator_name)
                .order_by(desc(StockMacroIndicator.period))
                .limit(limit)
            ).scalars().all()
            if not results:
                return []
            return [r.to_dict() for r in results]

    def get_latest_macro_period(self, indicator_name: str) -> Optional[str]:
        """获取宏观指标最新周期"""
        with self.get_session() as session:
            result = session.execute(
                select(StockMacroIndicator.period)
                .where(StockMacroIndicator.indicator_name == indicator_name)
                .order_by(desc(StockMacroIndicator.period))
                .limit(1)
            ).scalar_one_or_none()
            return result

    def has_stock_macro_indicator(self, indicator_name: str) -> bool:
        """检查宏观指标是否有缓存数据"""
        with self.get_session() as session:
            result = session.execute(
                select(StockMacroIndicator.id)
                .where(StockMacroIndicator.indicator_name == indicator_name)
                .limit(1)
            ).scalar_one_or_none()
            return result is not None

    def get_latest_stock_macro_end_date(self, indicator_name: str) -> Optional[date]:
        """获取宏观指标缓存中最新的 period 作为日期返回"""
        try:
            with self.get_session() as session:
                result = session.execute(
                    select(StockMacroIndicator.period)
                    .where(StockMacroIndicator.indicator_name == indicator_name)
                    .order_by(StockMacroIndicator.period.desc())
                    .limit(1)
                ).scalar_one_or_none()
                if result is None:
                    return None
                p = str(result).strip()
                # 尝试多种格式解析
                if len(p) == 8 and p.isdigit():  # YYYYMMDD
                    return datetime.strptime(p, "%Y%m%d").date()
                if len(p) == 6 and p.isdigit():  # YYYYMM
                    return datetime.strptime(p + "01", "%Y%m%d").date()
                return datetime.strptime(p[:10], "%Y-%m-%d").date()
        except Exception as e:
            logger.debug(f"解析宏观指标[{indicator_name}]最新日期失败: {e}")
            return None

    # ----- 同业对标板块成分股缓存 -----

    def get_cached_peer_cons(self, industry: str) -> Optional[pd.DataFrame]:
        """
        读取板块成分股缓存（JSON → DataFrame）。
        过期由调用方（_try_board_cons_cached）判断，本函数只做反序列化。
        """
        try:
            from sqlalchemy import select
            session = self.Session()
            stmt = select(PeerConsCache).where(PeerConsCache.industry == industry)
            row = session.execute(stmt).scalar_one_or_none()
            if row and row.data_json:
                import json
                data = json.loads(row.data_json)
                if data:
                    df = pd.DataFrame(data)
                    logger.debug(f"[DB缓存] 读取板块成分股成功: {industry}, {len(df)}只")
                    return df
            return None
        except Exception as e:
            logger.debug(f"[DB缓存] 读取板块成分股失败: {e}")
            return None
        finally:
            try:
                session.close()
            except Exception:
                pass

    def set_cached_peer_cons(self, industry: str, cons_df: pd.DataFrame) -> None:
        """保存板块成分股到缓存（upsert）。"""
        try:
            import json
            data_json = json.dumps(
                cons_df[["代码", "名称"]].to_dict(orient="records"),
                ensure_ascii=False,
            )
            session = self.Session()
            from sqlalchemy import select
            stmt = select(PeerConsCache).where(PeerConsCache.industry == industry)
            existing = session.execute(stmt).scalar_one_or_none()
            if existing:
                existing.data_json = data_json
                existing.cached_at = datetime.now()
            else:
                session.add(PeerConsCache(industry=industry, data_json=data_json))
            session.commit()
            logger.info(f"[DB缓存] 板块成分股缓存写入成功: {industry}, {len(cons_df)}只")
        except Exception as e:
            logger.debug(f"[DB缓存] 写入板块成分股缓存失败: {e}")
            session.rollback()
        finally:
            try:
                session.close()
            except Exception:
                pass

    def get_peer_cons_cache_time(self, industry: str) -> Optional[datetime]:
        """获取板块成分股缓存的时间戳。"""
        try:
            from sqlalchemy import select
            session = self.Session()
            stmt = select(PeerConsCache.cached_at).where(PeerConsCache.industry == industry)
            result = session.execute(stmt).scalar_one_or_none()
            return result
        except Exception as e:
            logger.debug(f"[DB缓存] 获取缓存时间失败: {e}")
            return None
        finally:
            try:
                session.close()
            except Exception:
                pass

# ----- fund_adj 复权因子 -----

    def save_stock_fund_adj(self, df: pd.DataFrame) -> int:
        """保存基金复权因子"""
        saved = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    ts_code = str(row.get("ts_code", "")).strip()
                    trade_date = parse_row_date(row)
                    if not ts_code or trade_date is None:
                        continue
                    stmt = select(StockFundAdj).where(
                        StockFundAdj.ts_code == ts_code,
                        StockFundAdj.trade_date == trade_date,
                    )
                    exist = session.execute(stmt).scalar_one_or_none()
                    if exist:
                        exist.adj_factor = _safe_float(row.get("adj_factor"))
                    else:
                        session.add(StockFundAdj(
                            ts_code=ts_code, trade_date=trade_date,
                            adj_factor=_safe_float(row.get("adj_factor")),
                        ))
                    saved += 1
                session.commit()
            except Exception:
                session.rollback()
                raise
        return saved

    def get_stock_fund_adj(self, ts_code: str, limit: int = 10) -> pd.DataFrame:
        """获取基金复权因子"""
        with self.get_session() as session:
            results = session.execute(
                select(StockFundAdj)
                .where(StockFundAdj.ts_code == ts_code)
                .order_by(desc(StockFundAdj.trade_date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            return pd.DataFrame([r.to_dict() for r in results])

    # ----- margin_secs 融资融券标的 -----

    def save_stock_margin_secs(self, df: pd.DataFrame) -> int:
        """保存融资融券标的列表"""
        saved = 0
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    ts_code = str(row.get("ts_code", "")).strip()
                    trade_date = parse_row_date(row)
                    if not ts_code or trade_date is None:
                        continue
                    stmt = select(StockMarginSecs).where(
                        StockMarginSecs.ts_code == ts_code,
                        StockMarginSecs.trade_date == trade_date,
                    )
                    exist = session.execute(stmt).scalar_one_or_none()
                    if exist:
                        exist.name = str(row.get("name", "")).strip()
                        exist.is_etf = str(row.get("is_etf", "")).strip()
                    else:
                        session.add(StockMarginSecs(
                            ts_code=ts_code, trade_date=trade_date,
                            name=str(row.get("name", "")).strip(),
                            is_etf=str(row.get("is_etf", "")).strip(),
                        ))
                    saved += 1
                session.commit()
            except Exception:
                session.rollback()
                raise
        return saved

    def get_stock_margin_secs(self, limit: int = 100) -> pd.DataFrame:
        """获取融资融券标的列表"""
        with self.get_session() as session:
            results = session.execute(
                select(StockMarginSecs)
                .order_by(desc(StockMarginSecs.trade_date))
                .limit(limit)
            ).scalars().all()
            if not results:
                return pd.DataFrame()
            return pd.DataFrame([r.to_dict() for r in results])


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

    dbM = get_db()
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

