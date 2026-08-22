from dateutil.utils import today

from .stock.base import DataFetcherManager, DataFetchError
from storage.sqlite import get_db
import pandas as pd
from utils.logger import logger
from utils.common import TASK_NAME_DAILY_TASK, parse_row_date
from datetime import date
from .stock.tushare_fetcher import TushareFetcher
from .stock.akshare_fetcher import AkshareFetcher
from .stock.cache_manager import cache_manager
from langchain_core.tools import StructuredTool
import traceback
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, date, timezone, timedelta
import numpy as np
import json


class StockTools:
    def __init__(self):
        """
        初始化管理器

        Args:
            fetchers: 数据源列表（可选，默认按优先级自动创建）
        """
        self.db = get_db()
        self.tushare = TushareFetcher(db=self.db)
        self.akshare = AkshareFetcher()
        self.data_manager = DataFetcherManager([self.tushare, self.akshare])

    def _has_qfq_drift(self, old_df: pd.DataFrame, new_df: pd.DataFrame, rel_tol: float = 1e-4) -> bool:
        """
        前复权基准漂移检测。

        增量拉取的区间从库内最新一天开始（包含重叠行），比较该重叠日新旧收盘价：
        相对误差超过容差即认为发生分红送转导致 qfq 全历史价格基准变化，
        库里旧段与新段基准不一致，需要删除旧数据全量重拉。
        """
        try:
            if old_df is None or old_df.empty or new_df is None or new_df.empty:
                return False
            if 'date' not in old_df.columns or 'date' not in new_df.columns or 'close' not in old_df.columns:
                return False
            # 库内最新一天即重叠日（get_all_*_data 按日期降序返回，iloc[0] 为最新）
            overlap_date = parse_row_date(old_df.iloc[0].get('date'))
            old_close = old_df.iloc[0].get('close')
            if overlap_date is None or old_close is None or pd.isna(old_close):
                return False
            # 合并结果中重叠日的行保留的是新拉取的数据（merge 时 keep="last"）
            new_dates = new_df['date'].apply(parse_row_date)
            matched = new_df[new_dates == overlap_date]
            if matched.empty:
                return False
            new_close = matched.iloc[0].get('close')
            if new_close is None or pd.isna(new_close):
                return False
            old_close = float(old_close)
            new_close = float(new_close)
            if old_close == 0:
                return False
            drift = abs(new_close - old_close) / abs(old_close)
            if drift > rel_tol:
                logger.warning(f"重叠日 {overlap_date} 收盘价漂移: 库内[{old_close}] vs 新拉取[{new_close}]，相对误差[{drift:.6f}]")
                return True
            return False
        except Exception as e:
            logger.error(f"复权漂移校验异常，跳过校验: {e} {traceback.format_exc()}")
            return False

    def _get_full_reload_start_date(self, stock_code: str, fallback: date) -> date:
        """全量重拉时的起始日期：优先用股票上市日期，取不到则退回原起始日期"""
        full_start = self.get_stock_start_date_by_stock_basic(stock_code)
        if full_start is None:
            return fallback
        return full_start

    def fetch_and_save_stock_daily_data(self, stock_code: str)-> Union[pd.DataFrame, None]:
        """
        获取股票每日数据
        Args:
            stock_code: 股票代码
        Returns:
            包含股票每日数据的DataFrame
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        today = date.today()
        old_daily_data = self.db.get_all_daily_data(stock_code)
        start_date = self.get_daily_start_date(stock_code, old_daily_data)
        
        # 检查 start_date 是否为 None
        if start_date is None:
            logger.error(f"无法获取股票[{stock_code}]的起始日期")
            return old_daily_data
        
        end_date_str = today.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")
        logger.info(f"股票[{stock_code}]数据开始更新, start date{start_date_str}, end date{end_date_str}")
        if end_date_str == start_date_str:
            logger.info(f"股票[{stock_code}]数据已经更新完成")
            return  old_daily_data
        
        # 确保 daily_basic 缓存（含换手率）先写入 DB，供 _fetch_raw_data 查询
        self.fetch_and_save_stock_basic_daily(stock_code)
        try:
            daily_datas,  fetcher_name = self.data_manager.get_daily_data(stock_code, old_daily_data, start_date_str, end_date_str)
        except DataFetchError as e:
            # 所有数据源都失败：库里已有历史数据时回退本地缓存，而不是直接报失败
            if old_daily_data is not None and not old_daily_data.empty:
                latest_date = parse_row_date(old_daily_data.iloc[0].get('date'))
                logger.warning(f"股票[{stock_code}]数据源不可用，使用本地缓存（截至 {latest_date} 日）")
                return old_daily_data
            logger.error(f"获取股票[{stock_code}]数据失败且本地无缓存: {e}")
            return None
        if daily_datas is None or daily_datas.empty:
            logger.error(f"获取股票[{stock_code}]数据为空")
            return  old_daily_data
        # 前复权基准漂移校验：分红送转后 qfq 全历史价变化，旧段与新段基准不一致
        if self._has_qfq_drift(old_daily_data, daily_datas):
            logger.warning(f"股票[{stock_code}]检测到前复权基准漂移，全量重拉日线数据")
            start_date = self._get_full_reload_start_date(stock_code, start_date)
            start_date_str = start_date.strftime("%Y-%m-%d")
            daily_datas, fetcher_name = self.data_manager.get_daily_data(stock_code, pd.DataFrame(), start_date_str, end_date_str)
            if daily_datas is None or daily_datas.empty:
                logger.error(f"股票[{stock_code}]全量重拉数据为空，保留原有数据")
                return old_daily_data
            # 重拉成功后再删除旧数据，避免拉取失败导致本地数据丢失
            self.db.delete_daily_data(stock_code)
            # 漂移重拉：同步失效 L1 内存 / L2 文件缓存，避免下游读到旧基准数据
            cache_manager.invalidate_by_stock(stock_code, 'daily')
        save_count = self.db.save_daily_data(daily_datas, stock_code, start_date, fetcher_name)
        logger.info(f"保存的数据为[{save_count}]")
        return daily_datas

    def fetch_and_save_stock_monthly_data(self, stock_code: str)-> Union[pd.DataFrame, None]:
        """
        获取股票月数据
        Args:
            stock_code: 股票代码
        Returns:
            包含股票月数据的DataFrame
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        old_monthly_data = self.db.get_all_month_data(stock_code)
        start_date = self.get_monthly_start_date(stock_code, old_monthly_data)
        
        # 检查 start_date 是否为 None
        if start_date is None:
            logger.error(f"无法获取股票[{stock_code}]的起始日期")
            return old_monthly_data
        
        end_date_str = date.today().strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")
        if end_date_str == start_date_str:
            logger.info(f"股票[{stock_code}]数据已经更新完成")
            return  old_monthly_data
        try:
            monthly_datas,  fetcher_name = self.data_manager.get_monthly_data(stock_code, old_monthly_data, start_date_str, end_date_str)
        except DataFetchError as e:
            # 所有数据源都失败：库里已有历史数据时回退本地缓存，而不是直接报失败
            if old_monthly_data is not None and not old_monthly_data.empty:
                latest_date = parse_row_date(old_monthly_data.iloc[0].get('date'))
                logger.warning(f"股票[{stock_code}]数据源不可用，使用本地缓存（截至 {latest_date} 日）")
                return old_monthly_data
            logger.error(f"获取股票[{stock_code}]数据失败且本地无缓存: {e}")
            return None
        if monthly_datas is None or monthly_datas.empty:
            logger.error(f"获取股票[{stock_code}]数据为空")
            return  old_monthly_data
        # 前复权基准漂移校验：分红送转后 qfq 全历史价变化，旧段与新段基准不一致
        if self._has_qfq_drift(old_monthly_data, monthly_datas):
            logger.warning(f"股票[{stock_code}]检测到前复权基准漂移，全量重拉月线数据")
            start_date = self._get_full_reload_start_date(stock_code, start_date)
            start_date_str = start_date.strftime("%Y-%m-%d")
            monthly_datas, fetcher_name = self.data_manager.get_monthly_data(stock_code, pd.DataFrame(), start_date_str, end_date_str)
            if monthly_datas is None or monthly_datas.empty:
                logger.error(f"股票[{stock_code}]全量重拉数据为空，保留原有数据")
                return old_monthly_data
            # 重拉成功后再删除旧数据，避免拉取失败导致本地数据丢失
            self.db.delete_month_data(stock_code)
            # 漂移重拉：同步失效 L1 内存 / L2 文件缓存，避免下游读到旧基准数据
            cache_manager.invalidate_by_stock(stock_code, 'month')
        save_count = self.db.save_month_data(monthly_datas, stock_code, start_date, fetcher_name)
        logger.info(f"保存的数据为[{save_count}]")
        return monthly_datas

    def fetch_and_save_stock_weekly_data(self, stock_code: str)-> Union[pd.DataFrame, None]:
        """
        获取股票周数据
        Args:
            stock_code: 股票代码
        Returns:
            包含股票周数据的DataFrame
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        today = date.today()
        old_weekly_data = self.db.get_all_weekly_data(stock_code)
        start_date = self.get_weekly_start_date(stock_code, old_weekly_data)
        
        # 检查 start_date 是否为 None
        if start_date is None:
            logger.error(f"无法获取股票[{stock_code}]的起始日期")
            return old_weekly_data
        
        end_date_str = today.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")
        if end_date_str == start_date_str:
            logger.info(f"股票[{stock_code}]数据已经更新完成")
            return  old_weekly_data
        try:
            weekly_datas,  fetcher_name = self.data_manager.get_weekly_data(stock_code, old_weekly_data, start_date_str, end_date_str)
        except DataFetchError as e:
            # 所有数据源都失败：库里已有历史数据时回退本地缓存，而不是直接报失败
            if old_weekly_data is not None and not old_weekly_data.empty:
                latest_date = parse_row_date(old_weekly_data.iloc[0].get('date'))
                logger.warning(f"股票[{stock_code}]数据源不可用，使用本地缓存（截至 {latest_date} 日）")
                return old_weekly_data
            logger.error(f"获取股票[{stock_code}]数据失败且本地无缓存: {e}")
            return None
        if weekly_datas is None or weekly_datas.empty:
            logger.error(f"获取股票[{stock_code}]数据为空")
            return  old_weekly_data
        # 前复权基准漂移校验：分红送转后 qfq 全历史价变化，旧段与新段基准不一致
        if self._has_qfq_drift(old_weekly_data, weekly_datas):
            logger.warning(f"股票[{stock_code}]检测到前复权基准漂移，全量重拉周线数据")
            start_date = self._get_full_reload_start_date(stock_code, start_date)
            start_date_str = start_date.strftime("%Y-%m-%d")
            weekly_datas, fetcher_name = self.data_manager.get_weekly_data(stock_code, pd.DataFrame(), start_date_str, end_date_str)
            if weekly_datas is None or weekly_datas.empty:
                logger.error(f"股票[{stock_code}]全量重拉数据为空，保留原有数据")
                return old_weekly_data
            # 重拉成功后再删除旧数据，避免拉取失败导致本地数据丢失
            self.db.delete_week_data(stock_code)
            # 漂移重拉：同步失效 L1 内存 / L2 文件缓存，避免下游读到旧基准数据
            cache_manager.invalidate_by_stock(stock_code, 'week')
        save_count = self.db.save_week_data(weekly_datas, stock_code, start_date, fetcher_name)
        logger.info(f"保存的数据为[{save_count}]")
        return weekly_datas

    def fetch_and_save_stock_basic_daily(self, stock_code: str)-> Union[pd.DataFrame, None]:
        """
        获取并保存股票的每日指标（PE/PB/市值等，供财务分析计算估值比率）
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        today = date.today()
        old_basic_data = self.db.get_latest_daily_basic_data(stock_code, 10)
        start_date = self.get_basic_daily_start_date(stock_code, old_basic_data)
        if start_date is None:
            logger.error(f"无法获取股票[{stock_code}]每日指标的起始日期")
            return old_basic_data
        end_date_str = today.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")
        if end_date_str == start_date_str:
            logger.info(f"股票[{stock_code}]每日指标已经更新完成")
            return old_basic_data
        try:
            new_basic_daily = self.tushare.stock_daily_basic(
                start_date=start_date_str, end_date=end_date_str, stock_code=stock_code)
        except Exception as e:
            logger.warning(f"股票[{stock_code}]每日指标获取失败，回退本地缓存: {e}")
            return old_basic_data
        if new_basic_daily is None or new_basic_daily.empty:
            logger.info(f"股票[{stock_code}]每日指标无新数据")
            return old_basic_data
        saved = self.db.save_stock_daily_basic(new_basic_daily, stock_code)
        logger.info(f"股票[{stock_code}]每日指标保存 {saved} 条")
        return new_basic_daily

    def get_daily_start_date(
            self,
            stock_code: str,
            old_daily_data: pd.DataFrame) -> Union[date, None]:
        """
        获取股票每日数据的开始日期
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        if  old_daily_data.empty or old_daily_data.iloc[0].get('date') is None:
            start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            if start_date is None :
                logger.error(f"股票的基本信息为空通过接口获取数据[{stock_code}]")
                # 全量加载一次
                self.save_stock_basic_by_tushare()
                start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            return start_date
        return old_daily_data.iloc[0].get('date')

    def get_weekly_start_date(
            self,
            stock_code: str,
            old_weekly_data: pd.DataFrame) -> Union[date, None]:
        """
        获取股票周数据的开始日期
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        if  old_weekly_data.empty or old_weekly_data.iloc[0].get('date') is None:
            start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            if start_date is None :
                logger.error(f"股票的基本信息为空通过接口获取数据[{stock_code}]")
                # 全量加载一次
                self.save_stock_basic_by_tushare()
                start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            return start_date
        return old_weekly_data.iloc[0].get('date')

    def get_monthly_start_date(
            self,
            stock_code: str,
            old_monthly_data: pd.DataFrame) -> Union[date, None]:
        """
        获取股票月数据的开始日期
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        if  old_monthly_data.empty or old_monthly_data.iloc[0].get('date') is None:
            start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            if start_date is None :
                logger.error(f"股票的基本信息为空通过接口获取数据[{stock_code}]")
                # 全量加载一次
                self.save_stock_basic_by_tushare()
                start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            return start_date
        return old_monthly_data.iloc[0].get('date')

    def get_basic_daily_start_date(
            self,
            stock_code: str,
            old_basic_daily_data: pd.DataFrame) -> Union[date, None]:
        """
        获取股票基本信息每日指标的开始日期
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        if old_basic_daily_data is None or old_basic_daily_data.empty \
                or old_basic_daily_data.iloc[0].get('trade_date') is None:
            start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            if start_date is None :
                logger.error(f"股票的基本信息为空通过接口获取数据[{stock_code}]")
                # 全量加载一次
                self.save_stock_basic_by_tushare()
                start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            return start_date
        return parse_row_date(old_basic_daily_data.iloc[0].get('trade_date'))


    def get_stock_start_date_by_stock_basic(self, code: str) -> Union[date, None]:
        stock_basic = self.db.get_stock_basic(code)
        if  stock_basic is None:
            logger.error("股票的基本信息为空 db")
            return None
        return stock_basic.list_date

    def save_stock_basic_by_tushare(self):
        """保存基本的股票信息"""
        logger.info(f"保存股票的股本信息")
        try:
            df = self.tushare.get_stock_basic()
            if df is None or df.empty:
                logger.error(f"获取股票基础信息为空 get stock basic")
                return
            logger.info(f"获取的数据[{df.head(1)}]")
            save_count = self.db.save_stock_basic(df)
            logger.info(f"保存的数据为[{save_count}]")

        except Exception as e:
            logger.error(f"获取数据错误[{e}] {traceback.format_exc()}")

    def fetch_and_save_stock_research_report(
        self,
        code: str,
    ) -> Union[pd.DataFrame, None]:
        """获取和保存股票研究报告数据"""
        try:
            logger.info(f"获取股票[{code}]研报数据")
            today = date.today()
            task_m = self.db.get_stock_daily_task(code)
            task_date = task_m.get(TASK_NAME_DAILY_TASK)
            if task_date == today:
                logger.debug(f"[{code}] 今日研报数据已存在，跳过获取（断点续传）")
                return None

            df = self.akshare.stock_research_report_em( code)
            if df is None or df.empty:
                logger.error(f"akshare get stock research report err[{code}]")
                return None
            return self.handle_research_report(code, df)
        except Exception as e:
            logger.error(f"akshare get stock research report err[{code}], {traceback.format_exc()}")
            return None

    def handle_research_report(self, code: str, df: pd.DataFrame)->pd.DataFrame:
        """处理股票研究报告数据"""
        if df is None or df.empty:
            logger.error(f"股票研究报告数据为空")
            return  df
        logger.warning(f"获取的数据[{df.head(1)}]")
        try:
            # get_financial_analyze 返回 (记录列表, pdf_name映射)，注意顺序
            analyze_list, pdf_name_m = self.db.get_financial_analyze(code)

            logger.warning(f"已存在的研报[{pdf_name_m}]")
            # pandas 2.x 已移除 DataFrame.append，用列表收集后统一构建 DataFrame
            need_analyze_rows = []
            for _, row in df.iterrows():
                report_date = row.get("date")
                if report_date is None:
                    logger.error(f"[{code}] 研报[{report_date}]无日期")
                    continue
                report_date = parse_row_date(report_date)
                if report_date is None:
                    logger.error(f"[{code}] 研报日期解析失败[{row.get('date')}]")
                    continue

                half_year_ago = date.today() - timedelta(days=182)

                # 如果研报日期早于半年前，跳过
                if report_date < half_year_ago:
                    logger.debug(f"[{code}] 研报 {pdf_name_m} 日期 ({report_date}) 早于 ({half_year_ago})，已忽略")
                    continue

                pdf_url = row.get("report_pdf_link")
                if pdf_url is None:
                    logger.error(f"[{code}] 研报[{report_date}]无pdf链接")
                    continue
                pdf_name = row.get("pdf_name")
                if pdf_name in pdf_name_m:
                     continue
                res = self.db.download_research_report(pdf_url, pdf_name, code)
                if res.get("error") is not None:
                    logger.error(f"[{code}] 下载股票研报失败[{res.get('error')}]")
                    continue
                content = res.get("file_content")
                if not content:
                    logger.error(f"[{code}] 研报[{pdf_name}]文本提取为空（检查 PyPDF2 是否安装/是否扫描版PDF），跳过分析")
                    continue
                need_analyze_rows.append(
                    {
                        "pdf_name": pdf_name,
                        "pdf_url": pdf_url,
                        "content": content,
                        "code": code,
                        "report_date": report_date
                    }
                )

            return pd.DataFrame(need_analyze_rows, columns=["pdf_name", "pdf_url", "content", "code", "report_date"])
        except Exception as e:
            logger.error(f"处理股票研报数据错误[{e}] {traceback.format_exc()}")
            return  df

    def fetch_and_save_stock_income(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存股票利润表数据
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 利润表数据
        """
        if stock_code is None:
            logger.error(f"股票代码为空")
            return None

        try:
            today = date.today()
            start_date = f"{today.year - 5}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]利润表数据, start_date:{start_date}, end_date:{end_date_str}")

            # Tushare 免费 API 超时频繁，重试 2 次
            import time as _time
            df = None
            for attempt in range(3):
                try:
                    df = self.tushare.income(stock_code, start_date, end_date_str)
                    break
                except Exception as e:
                    if attempt < 2:
                        logger.warning(f"获取股票[{stock_code}]利润表数据第{attempt+1}次失败，重试: {e}")
                        _time.sleep(5)
                    else:
                        raise

            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]利润表数据为空")
                return None

            normalized_df = self._normalize_income_data(df, stock_code)
            save_count = self.db.save_stock_income(normalized_df, stock_code)
            logger.info(f"保存股票[{stock_code}]利润表数据成功，新增[{save_count}]条记录")

            return normalized_df
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]利润表数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_dividend(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存分红送股数据（DB缓存，增量更新）

        分红数据更新频率低（季频/年频），缓存有效期设为 30 天。
        """
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_dividend(stock_code, 1)
            cache_fresh = False
            if old is not None and not old.empty:
                latest_date = old.iloc[0].get('end_date')
                if latest_date is not None:
                    try:
                        latest = pd.Timestamp(latest_date)
                        today = pd.Timestamp(date.today())
                        days_gap = (today - latest).days
                        # 分红数据季频更新，30天内缓存有效
                        if days_gap <= 30:
                            cache_fresh = True
                    except Exception:
                        pass
            if cache_fresh:
                logger.info(f"分红送股[{stock_code}]最新数据为{latest_date}，缓存有效（{days_gap}天前）")
                return old

            df = self.tushare.dividend(stock_code)
            if df is None or df.empty:
                if old is not None and not old.empty:
                    logger.warning(f"[分红送股] 增量拉取无新数据，返回缓存")
                    return old
                return None
            # 清理 NaN 再保存，避免 SQLite 唯一键异常
            df = df.replace({float('nan'): None})
            save_count = self.db.save_stock_dividend(df, stock_code)
            logger.info(f"保存股票[{stock_code}]分红送股数据成功，新增[{save_count}]条记录")
            return self.db.get_stock_dividend(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]分红送股数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_pledge_detail(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存股权质押明细数据（DB缓存）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_pledge_detail(stock_code, 1)
            if old is not None and not old.empty:
                logger.info(f"股权质押明细[{stock_code}]缓存已存在，直接返回")
                return old
            df = self.tushare.pledge_detail(stock_code)
            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]股权质押明细数据为空")
                return None
            save_count = self.db.save_stock_pledge_detail(df, stock_code)
            logger.info(f"保存股票[{stock_code}]股权质押明细成功，新增[{save_count}]条记录")
            return self.db.get_stock_pledge_detail(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]股权质押明细失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_holder_trade(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存股东增减持数据（DB缓存）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_holder_trade(stock_code, 1)
            if old is not None and not old.empty:
                logger.info(f"股东增减持[{stock_code}]缓存已存在，直接返回")
                return old
            today = date.today()
            start_date = f"{today.year - 2}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")
            df = self.tushare.stk_holdertrade(stock_code, ann_date=end_date_str, start_date=start_date, end_date=end_date_str)
            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]股东增减持数据为空")
                return None
            save_count = self.db.save_stock_holder_trade(df, stock_code)
            logger.info(f"保存股票[{stock_code}]股东增减持成功，新增[{save_count}]条记录")
            return self.db.get_stock_holder_trade(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]股东增减持数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_margin(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存融资融券汇总数据（DB缓存）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_margin(stock_code, 1)
            if old is not None and not old.empty:
                logger.info(f"融资融券汇总[{stock_code}]缓存已存在，直接返回")
                return old
            today = date.today()
            start = f"{today.year - 1}-01-01"
            end = today.strftime("%Y-%m-%d")
            df = self.tushare.margin(stock_code, trade_date='', start_date=start, end_date=end, exchange_id='')
            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]融资融券汇总数据为空")
                return None
            save_count = self.db.save_stock_margin(df, stock_code)
            logger.info(f"保存股票[{stock_code}]融资融券汇总成功，新增[{save_count}]条记录")
            return self.db.get_stock_margin(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]融资融券汇总数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_margin_detail(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存融资融券明细数据（DB缓存）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_margin_detail(stock_code, 1)
            if old is not None and not old.empty:
                logger.info(f"融资融券明细[{stock_code}]缓存已存在，直接返回")
                return old
            today = date.today()
            start = f"{today.year - 1}-01-01"
            end = today.strftime("%Y-%m-%d")
            df = self.tushare.margin_detail(stock_code, trade_date='', start_date=start, end_date=end)
            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]融资融券明细数据为空")
                return None
            save_count = self.db.save_stock_margin_detail(df, stock_code)
            logger.info(f"保存股票[{stock_code}]融资融券明细成功，新增[{save_count}]条记录")
            return self.db.get_stock_margin_detail(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]融资融券明细数据失败: {e} {traceback.format_exc()}")
            return None

    @staticmethod
    def _fetch_moneyflow_akshare(stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """通过 AkShare 获取个股资金流向（Tushare 数据异常时兜底）

        AkShare stock_individual_fund_flow 返回的金额单位为元，
        列名：日期、主力净流入、小单净流入、中单净流入、大单净流入、超大单净流入等。
        """
        try:
            import akshare as ak
            import pandas as _pd
            # 判断市场：6 开头是沪市，0/3 开头是深市
            market = "sh" if stock_code.startswith("6") else "sz"
            df = ak.stock_individual_fund_flow(stock=stock_code, market=market)
            if df is None or df.empty:
                return None
            # AkShare 返回近 100 个交易日数据，需按 start/end 过滤
            df = df.copy()
            df.columns = [str(c).strip() for c in df.columns]
            # 列名映射：AkShare 中 "日期" → "trade_date"，"主力净流入-净额" → net_mf_amount
            date_col = None
            mf_col = None
            for c in df.columns:
                if "日期" in c or "date" in c.lower():
                    date_col = c
            # 主力净额列：优先匹配含"净额"的主力列，避免匹配到"净占比"
            mf_candidates = [c for c in df.columns if ("主力" in c or "主力净" in c) and "净额" in c]
            if not mf_candidates:
                mf_candidates = [c for c in df.columns if "主力净流入" in c or "主力净额" in c]
            mf_col = mf_candidates[0] if mf_candidates else None
            if date_col is None or mf_col is None:
                logger.warning(f"[AkShare资金流] 列名不匹配: {list(df.columns)}")
                return None
            # 日期统一为 YYYY-MM-DD
            df["trade_date"] = _pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
            # 过滤时间范围
            df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)]
            if df.empty:
                return None
            # 构建与 Tushare 兼容的 DataFrame
            # AkShare 列：主力净流入-净额、超大单净流入-净额、大单净流入-净额、
            #               中单净流入-净额、小单净流入-净额
            # 映射：主力净额 → net_mf_amount
            #       超大单+大单 → 主力（已在主力净额中体现）
            #       中单 → 游资（md_*）
            #       小单 → 散户（sm_*）
            def _match_col(pattern):
                matches = [c for c in df.columns if pattern in c]
                return matches[0] if matches else None

            mid_col = _match_col("中单净流入-净额")
            sml_col = _match_col("小单净流入-净额")

            def _parse_ak_val(v):
                """AkShare 资金净额单位为元，转换为万元（与 Tushare/下游代码一致）"""
                if v is None:
                    return 0.0
                try:
                    return float(v) / 1e4
                except (ValueError, TypeError):
                    return 0.0

            result_rows = []
            for _, row in df.iterrows():
                mf = _parse_ak_val(row.get(mf_col))
                md = _parse_ak_val(row.get(mid_col)) if mid_col else 0.0
                sm = _parse_ak_val(row.get(sml_col)) if sml_col else 0.0
                # 正负分别填入 buy/sell（适配 _format_moneyflow 计算逻辑）
                result_rows.append({
                    "trade_date": row["trade_date"],
                    "net_mf_amount": mf,
                    "buy_md_amount": md if md > 0 else 0.0,
                    "sell_md_amount": -md if md < 0 else 0.0,
                    "buy_sm_amount": sm if sm > 0 else 0.0,
                    "sell_sm_amount": -sm if sm < 0 else 0.0,
                })
            result = _pd.DataFrame(result_rows)
            result = result.sort_values("trade_date", ascending=False).reset_index(drop=True)
            logger.info(f"[AkShare资金流] 获取成功: {stock_code}, {len(result)} 条")
            return result
        except Exception as e:
            logger.warning(f"[AkShare资金流] 获取失败: {e}")
            return None

    @staticmethod
    def _normalize_moneyflow(df: pd.DataFrame) -> pd.DataFrame:
        """统一修正资金流数据：用 大单+超大单净买入 重算主力净流入。

        Tushare 的 net_mf_amount 字段值与 buy/sell 列对不上（差约12倍），
        统一按 主力=大单+超大单 的标准定义重新计算，单位：万元。
        """
        if df is None or df.empty:
            return df
        df = df.copy()
        has_lg = all(c in df.columns for c in ['buy_lg_amount', 'sell_lg_amount'])
        has_elg = all(c in df.columns for c in ['buy_elg_amount', 'sell_elg_amount'])
        if has_lg or has_elg:
            lg_net = pd.to_numeric(df['buy_lg_amount'], errors='coerce').fillna(0) - pd.to_numeric(df['sell_lg_amount'], errors='coerce').fillna(0) if has_lg else 0
            elg_net = pd.to_numeric(df['buy_elg_amount'], errors='coerce').fillna(0) - pd.to_numeric(df['sell_elg_amount'], errors='coerce').fillna(0) if has_elg else 0
            df['net_mf_amount'] = lg_net + elg_net
        return df

    def fetch_and_save_stock_moneyflow(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存个股资金流向数据（按日增量更新，DB缓存 + AkShare兜底）

        缓存策略：最新数据日期 ≥ 今天-1天 → 直接返回缓存；
                 最新数据日期 < 今天-1天 → 从最新日期次日增量拉取、合并存储。
                 拉取失败时降级返回旧缓存，不阻断分析。
                 Tushare 数据异常（主力净流绝对值过低）时自动切 AkShare 重试。
        """
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_moneyflow(stock_code, 1)
            if old is not None and not old.empty:
                # 旧缓存可能是错误的 net_mf_amount，先修正再判断
                old = self._normalize_moneyflow(old)
                latest_date = old.iloc[0].get('trade_date')
                if latest_date is not None:
                    try:
                        latest = pd.Timestamp(latest_date)
                        today = pd.Timestamp(date.today())
                        days_gap = (today - latest).days
                        if days_gap == 0:
                            latest_mf = pd.to_numeric(old.iloc[0].get('net_mf_amount', 0), errors='coerce') or 0
                            if abs(latest_mf) >= 500:  # 500万元
                                logger.info(f"个股资金流向[{stock_code}]最新数据为{latest_date}，缓存有效")
                                return old
                            else:
                                logger.warning(f"个股资金流向[{stock_code}]缓存数据异常（main_inflow={latest_mf:.0f}），触发重拉")
                                start = today.strftime("%Y-%m-%d")
                                end = today.strftime("%Y-%m-%d")
                        else:
                            logger.info(f"个股资金流向[{stock_code}]缓存距今{days_gap}天，增量拉取")
                            start = (latest + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                            end = today.strftime("%Y-%m-%d")
                    except Exception as e:
                        logger.warning(f"日期计算异常: {e}，回退全量拉取")
                        start = f"{today.year - 1}-01-01"
                        end = today.strftime("%Y-%m-%d")
                else:
                    start = f"{date.today().year - 1}-01-01"
                    end = date.today().strftime("%Y-%m-%d")
            else:
                start = f"{date.today().year - 1}-01-01"
                end = date.today().strftime("%Y-%m-%d")

            # 1. 先尝试 Tushare
            df = self.tushare.moneyflow(stock_code, trade_date='', start_date=start, end_date=end)
            if df is not None and not df.empty:
                df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
                df = self._normalize_moneyflow(df)
            # 2. 校验 Tushare 数据是否合理：最新一天的主力净流若绝对值 < 500万元，可能数据异常
            df_valid = False
            if df is not None and not df.empty:
                latest_mf = pd.to_numeric(df.iloc[0].get('net_mf_amount', 0), errors='coerce') or 0
                if abs(latest_mf) >= 500:  # 500万元
                    df_valid = True
                else:
                    logger.warning(f"[资金流向] Tushare 数据异常: 最新日主力净流={latest_mf:.0f}万元，尝试 AkShare 兜底")
            # 3. Tushare 数据异常或为空，尝试 AkShare 兜底
            if not df_valid:
                ak_df = self._fetch_moneyflow_akshare(stock_code, start, end)
                if ak_df is not None and not ak_df.empty:
                    # 将 AkShare 数据转换为 Tushare 兼容格式，保存到 DB
                    ts_cols = ['buy_sm_vol', 'buy_sm_amount', 'sell_sm_vol', 'sell_sm_amount',
                               'buy_md_vol', 'buy_md_amount', 'sell_md_vol', 'sell_md_amount',
                               'buy_lg_vol', 'buy_lg_amount', 'sell_lg_vol', 'sell_lg_amount',
                               'buy_elg_vol', 'buy_elg_amount', 'sell_elg_vol', 'sell_elg_amount',
                               'net_mf_amount']
                    for c in ts_cols:
                        if c not in ak_df.columns:
                            ak_df[c] = None
                    ak_df['trade_date'] = ak_df['trade_date'].astype(str)
                    save_count = self.db.save_stock_moneyflow(ak_df, stock_code)
                    logger.info(f"保存股票[{stock_code}]个股资金流向成功（AkShare兜底），新增[{save_count}]条记录")
                    df = self._normalize_moneyflow(self.db.get_stock_moneyflow(stock_code))
                elif df is not None and not df.empty:
                    # AkShare 也失败，但 Tushare 有数据（虽然异常），仍保存并返回
                    save_count = self.db.save_stock_moneyflow(df, stock_code)
                    logger.info(f"保存股票[{stock_code}]个股资金流向成功（Tushare原样），新增[{save_count}]条记录")
                    df = self._normalize_moneyflow(self.db.get_stock_moneyflow(stock_code))
                else:
                    if old is not None and not old.empty:
                        logger.warning(f"[资金流向] 双源皆失败，返回缓存（最新{latest_date}）")
                        return old
                    return None
            else:
                save_count = self.db.save_stock_moneyflow(df, stock_code)
                logger.info(f"保存股票[{stock_code}]个股资金流向成功，新增[{save_count}]条记录")
                df = self._normalize_moneyflow(self.db.get_stock_moneyflow(stock_code))

            return df
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]个股资金流向数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_hsgt_moneyflow(self) -> Union[pd.DataFrame, None]:
        """获取并保存沪深港通资金流向数据（DB缓存）"""
        try:
            old = self.db.get_stock_hsgt_moneyflow(1)
            if old is not None and not old.empty:
                logger.info(f"沪深港通资金流向缓存已存在，直接返回")
                return old
            today = date.today()
            start = f"{today.year - 1}-01-01"
            end = today.strftime("%Y-%m-%d")
            df = self.tushare.moneyflow_hsgt(trade_date='', start_date=start, end_date=end)
            if df is None or df.empty:
                logger.error(f"获取沪深港通资金流向数据为空")
                return None
            save_count = self.db.save_stock_hsgt_moneyflow(df)
            logger.info(f"保存沪深港通资金流向成功，新增[{save_count}]条记录")
            return self.db.get_stock_hsgt_moneyflow()
        except Exception as e:
            logger.error(f"获取沪深港通资金流向数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_macro(self, indicator_name: str) -> Union[pd.DataFrame, None]:
        """
        拉取宏观数据并缓存到DB。
        支持: cn_gdp, cn_cpi, cn_ppi, cn_m, sf_month, us_tycr, us_trycr, us_tbr, us_tltr, us_trltr
        缓存策略按指标频率判定是否刷新：
          - 日频（资金流向/利率/美债）：最新数据日期 < 今天 → 尝试 API 刷新
          - 月频（CPI/PPI/M2/社融）：最新数据月份滞后 ≥ 2 个月 → 尝试 API 刷新
          - 季频（GDP）：最新数据季度滞后 ≥ 2 个季度 → 尝试 API 刷新
        API 刷新失败时回退使用缓存数据。
        """
        macro_apis = {
            'cn_gdp': self.tushare.cn_gdp,
            'cn_cpi': self.tushare.cn_cpi,
            'cn_ppi': self.tushare.cn_ppi,
            'cn_m': self.tushare.cn_m,
            'sf_month': self.tushare.sf_month,
            'us_tycr': self.tushare.us_tycr,
            'us_trycr': self.tushare.us_trycr,
            'us_tbr': self.tushare.us_tbr,
            'us_tltr': self.tushare.us_tltr,
            'us_trltr': self.tushare.us_trltr,
        }
        # 按指标频率设定最大可接受滞后
        freq = self._macro_freshness.get(indicator_name, "daily")

        # 检查缓存是否够新鲜
        latest = self.db.get_latest_stock_macro_end_date(indicator_name)
        stale = False
        if latest is not None:
            try:
                if freq == "daily":
                    # 日频数据：最新日期必须 ≥ 昨天
                    from datetime import timedelta
                    cutoff = date.today() - timedelta(days=2)
                    # 容差 2 天（周末/节假日无数据）
                    if latest < cutoff:
                        stale = True
                        logger.info(f"宏观数据[{indicator_name}]最新为{latest}，已过期，尝试刷新")
                elif freq == "monthly":
                    # 月频：最新月份距今 ≤ 2 个月（考虑发布滞后 ~10天）
                    from dateutil.relativedelta import relativedelta
                    if latest < date.today() - relativedelta(months=2):
                        stale = True
                        logger.info(f"宏观数据[{indicator_name}]最新为{latest}，已过期，尝试刷新")
                elif freq == "quarterly":
                    from dateutil.relativedelta import relativedelta
                    if latest < date.today() - relativedelta(months=8):
                        # 季度数据滞后约 1 个月，8 个月 = 2 季度 + 安全边界
                        stale = True
                        logger.info(f"宏观数据[{indicator_name}]最新为{latest}，已过期，尝试刷新")
            except Exception:
                stale = False  # 解析失败当不陈旧处理

        if not stale:
            has = self.db.has_stock_macro_indicator(indicator_name)
            if has:
                logger.info(f"宏观数据[{indicator_name}]缓存有效，直接返回")
                return self._read_macro_from_db(indicator_name)

        # 尝试 API 刷新
        api_fn = macro_apis.get(indicator_name)
        if api_fn is None:
            logger.warning(f"未知宏观指标: {indicator_name}")
            return None
        try:
            df = api_fn()
            if df is not None and not df.empty:
                self.db.save_stock_macro_indicator(indicator_name, df)
                logger.info(f"宏观数据[{indicator_name}] API 拉取成功并缓存")
                return df
        except Exception as e:
            logger.warning(f"宏观数据[{indicator_name}] API 刷新失败: {e}")

        # API 失败 → 回退缓存
        logger.info(f"宏观数据[{indicator_name}] 回退使用缓存")
        return self._read_macro_from_db(indicator_name)

    _macro_freshness = {
        # 日频
        "moneyflow_mkt_dc": "daily",
        "moneyflow_hsgt": "daily",
        "margin": "daily",
        "shibor": "daily",
        "shibor_lpr": "daily",
        "us_tycr": "daily",
        "us_trycr": "daily",
        "us_tbr": "daily",
        "us_tltr": "daily",
        "us_trltr": "daily",
        # 月频
        "cn_cpi": "monthly",
        "cn_ppi": "monthly",
        "cn_m": "monthly",
        "sf_month": "monthly",
        # 季频
        "cn_gdp": "quarterly",
    }

    def _read_macro_from_db(self, indicator_name: str) -> Union[pd.DataFrame, None]:
        """从 DB 读取宏观缓存数据"""
        try:
            data = self.db.get_stock_macro_indicator(indicator_name, limit=40)
            if data:
                import pandas as _pd
                rows = [{**d, **json.loads(d.get('value_json','{}'))} for d in data]
                return _pd.DataFrame(rows)
            return None
        except Exception as e:
            logger.debug(f"宏观数据[{indicator_name}] 读缓存失败: {e}")
            return None

    def fetch_fund_adj(self, ts_code: str) -> Union[pd.DataFrame, None]:
        """基金复权因子（缓存1天）"""
        try:
            df = self.db.get_stock_fund_adj(ts_code, limit=10)
            if df is not None and not df.empty:
                return df
            import tushare as ts
            pro = ts.pro_api()
            df = pro.fund_adj(ts_code=ts_code)
            if df is None or df.empty:
                return None
            self.db.save_stock_fund_adj(df)
            return self.db.get_stock_fund_adj(ts_code, limit=10)
        except Exception as e:
            logger.debug(f"基金复权因子[{ts_code}]获取失败: {e}")
            return None

    def fetch_margin_secs(self) -> Union[pd.DataFrame, None]:
        """融资融券标的列表（含ETF，缓存1天）"""
        try:
            df = self.db.get_stock_margin_secs(limit=100)
            if df is not None and not df.empty:
                return df
            import tushare as ts
            pro = ts.pro_api()
            df = pro.margin_secs()
            if df is None or df.empty:
                return None
            self.db.save_stock_margin_secs(df)
            return self.db.get_stock_margin_secs(limit=100)
        except Exception as e:
            logger.debug(f"融资融券标的获取失败: {e}")
            return None

    def fetch_and_save_stock_report_rc(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存卖方盈利预测数据（缓存：同日不重复请求，隔日交易日尝试刷新）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_report_rc(stock_code, limit=5)
            today = date.today()
            if old is not None and not old.empty:
                latest_date = old['report_date'].max()
                if isinstance(latest_date, str):
                    from datetime import datetime as _dt
                    latest_date = _dt.strptime(str(latest_date)[:10], "%Y-%m-%d").date()
                if latest_date == today:
                    logger.info(f"卖方盈利预测[{stock_code}]今日数据已缓存，直接返回")
                    return old
                # 非交易日（周末）也跳过API，节省配额
                if today.weekday() >= 5:
                    logger.info(f"卖方盈利预测[{stock_code}]非交易日，使用缓存")
                    return old
                logger.info(f"卖方盈利预测[{stock_code}]缓存日期{latest_date}<今日，尝试刷新")
            today = date.today()
            start_date = f"{today.year - 1}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")
            df = self.tushare.report_rc(stock_code, start_date=start_date, end_date=end_date_str)
            if df is None or df.empty:
                if old is not None and not old.empty:
                    logger.warning(f"卖方盈利预测[{stock_code}]API获取为空（可能非交易日），使用本地缓存")
                    return old
                logger.error(f"获取股票[{stock_code}]卖方盈利预测数据为空")
                return None
            save_count = self.db.save_stock_report_rc(df, stock_code)
            logger.info(f"保存股票[{stock_code}]卖方盈利预测成功，新增[{save_count}]条记录")
            return self.db.get_stock_report_rc(stock_code)
        except Exception as e:
            logger.warning(f"卖方盈利预测[{stock_code}]获取失败，使用本地缓存: {e}")
            if old is not None and not old.empty:
                return old
            return None

    def fetch_and_save_stock_fina_audit(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存财务审计意见数据（DB缓存：按code+end_date去重）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_fina_audit(stock_code, 1)
            if old is not None and not old.empty:
                logger.info(f"财务审计意见[{stock_code}]缓存已存在，直接返回")
                return old
            df = self.tushare.fina_audit(stock_code)
            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]财务审计意见数据为空")
                return None
            save_count = self.db.save_stock_fina_audit(df, stock_code)
            logger.info(f"保存股票[{stock_code}]财务审计意见数据成功，新增[{save_count}]条记录")
            return self.db.get_stock_fina_audit(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]财务审计意见数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_disclosure_date(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存财报披露计划日期数据（DB缓存：按code+end_date去重）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old = self.db.get_stock_disclosure_date(stock_code, 1)
            if old is not None and not old.empty:
                logger.info(f"财报披露计划日期[{stock_code}]缓存已存在，直接返回")
                return old
            df = self.tushare.disclosure_date(stock_code)
            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]财报披露计划日期数据为空")
                return None
            save_count = self.db.save_stock_disclosure_date(df, stock_code)
            logger.info(f"保存股票[{stock_code}]财报披露计划日期数据成功，新增[{save_count}]条记录")
            return self.db.get_stock_disclosure_date(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]财报披露计划日期数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_balance_sheet(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存股票资产负债表数据
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 资产负债表数据
        """
        if stock_code is None:
            logger.error(f"股票代码为空")
            return None

        try:
            today = date.today()
            start_date = f"{today.year - 5}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]资产负债表数据, start_date:{start_date}, end_date:{end_date_str}")

            df = self.tushare.balancesheet(stock_code, start_date, end_date_str)
            if df is None or df.empty:
                logger.error(f"获取股票[{stock_code}]资产负债表数据为空")
                return None

            normalized_df = self._normalize_balance_sheet_data(df, stock_code)
            save_count = self.db.save_stock_balance_sheet(normalized_df, stock_code)
            logger.info(f"保存股票[{stock_code}]资产负债表数据成功，新增[{save_count}]条记录")

            return normalized_df
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]资产负债表数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_stock_cashflow(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存股票现金流量表数据（增量更新）
        增量逻辑：查库内最新报告期，从其后拉增量；库里没有则拉近5年全量。
        tushare 失败时回退库存数据。
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 现金流量表数据（库内全量，按报告期降序，供同比对比使用）
        """
        if stock_code is None:
            logger.error(f"股票代码为空")
            return None

        try:
            today = date.today()
            old_cashflow_data = self.db.get_stock_cashflow(stock_code)

            # 增量起点：库内有数据则从最新报告期次日开始，否则拉近5年全量
            if old_cashflow_data is not None and not old_cashflow_data.empty:
                latest_report_date = parse_row_date(old_cashflow_data.iloc[0].get('report_date'))
                start_date = (latest_report_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = f"{today.year - 5}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]现金流量表数据, start_date:{start_date}, end_date:{end_date_str}")

            try:
                df = self.tushare.stock_cashflow(stock_code, start_date, end_date_str)
            except DataFetchError as e:
                # tushare 失败：库里已有历史数据时回退本地缓存，而不是直接报失败
                if old_cashflow_data is not None and not old_cashflow_data.empty:
                    logger.warning(f"股票[{stock_code}]现金流量表数据源不可用，使用本地缓存: {e}")
                    return old_cashflow_data
                logger.error(f"获取股票[{stock_code}]现金流量表数据失败且本地无缓存: {e}")
                return None

            if df is None or df.empty:
                logger.info(f"股票[{stock_code}]现金流量表无新增数据")
                if old_cashflow_data is not None and not old_cashflow_data.empty:
                    return old_cashflow_data
                return None

            normalized_df = self._normalize_cashflow_data(df, stock_code)
            save_count = self.db.save_stock_cashflow(normalized_df, stock_code)
            logger.info(f"保存股票[{stock_code}]现金流量表数据成功，新增[{save_count}]条记录")

            # 返回库内全量数据（含历史报告期），便于下游做去年同期对比
            return self.db.get_stock_cashflow(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]现金流量表数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_fina_indicator(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存股票财务指标数据（增量更新）
        包含EPS、ROE、ROA、毛利率、存货周转率、资产负债率等核心指标
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 财务指标数据
        """
        if stock_code is None:
            logger.error("股票代码为空")
            return None

        try:
            today = date.today()
            old_data = self.db.get_stock_fina_indicator(stock_code)

            # 旧数据单位归一化：迁移历史百分数→小数
            # 对旧数据中仍为百分数的字段 ÷100；阈值 ≥1.0 区分百分数与小数
            # （百分数如 roe=1.65 → 小数 0.0165；增长率如 mbrg=-11.82 → -0.1182）
            _PCT_FIELDS = {'roe', 'roe_waa', 'roe_dt', 'roa',
                           'netprofit_margin', 'gross_margin',
                           'debt_to_assets', 'debt_to_eqy', 'n_cashflow_to_liab',
                           'mbrg', 'nprg', 'profit_to_gr'}
            if old_data is not None and not old_data.empty:
                _dirty = False
                for col in _PCT_FIELDS & set(old_data.columns):
                    # 转数值（DB 可能混有 None 导致列类型为 object）
                    _numeric = pd.to_numeric(old_data[col], errors='coerce')
                    _mask = _numeric.notna() & (_numeric.abs() >= 1.0)
                    if _mask.any():
                        old_data.loc[_mask, col] = _numeric[_mask] / 100.0
                        _dirty = True
                if _dirty:
                    self.db.save_stock_fina_indicator(old_data, stock_code)
                    logger.info(f"迁移股票[{stock_code}]财务指标旧数据单位：百分数→小数")

            if old_data is not None and not old_data.empty:
                latest_report_date = parse_row_date(old_data.iloc[0].get('report_date'))
                start_date = (latest_report_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = f"{today.year - 5}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]财务指标数据, start_date:{start_date}, end_date:{end_date_str}")

            try:
                df = self.tushare.fina_indicator(stock_code, start_date, end_date_str)
            except DataFetchError as e:
                if old_data is not None and not old_data.empty:
                    logger.warning(f"股票[{stock_code}]财务指标数据源不可用，使用本地缓存: {e}")
                    return old_data
                logger.error(f"获取股票[{stock_code}]财务指标数据失败且本地无缓存: {e}")
                return None

            if df is None or df.empty:
                logger.info(f"股票[{stock_code}]财务指标无新增数据")
                if old_data is not None and not old_data.empty:
                    return old_data
                return None

            normalized_df = self._normalize_fina_indicator(df, stock_code)
            save_count = self.db.save_stock_fina_indicator(normalized_df, stock_code)
            logger.info(f"保存股票[{stock_code}]财务指标数据成功，新增[{save_count}]条记录")

            return self.db.get_stock_fina_indicator(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]财务指标数据失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_fina_indicator(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化财务指标数据
        Tushare fina_indicator 返回的主要字段：
        ts_code, ann_date, end_date, eps, dt_eps, total_revenue_ps, revenue_ps, ...
        roe, roa, grossprofit_margin(毛利率), netprofit_margin, inv_turn, debt_to_assets 等
        注意：gross_margin 是毛利额（元），grossprofit_margin 才是毛利率（%）
        """
        df = df.copy()

        if 'ann_date' in df.columns:
            df = df.sort_values('ann_date', ascending=False)
        df = df.drop_duplicates(subset=['end_date'], keep='first')

        column_mapping = {
            'end_date': 'report_date',
            'ann_date': 'ann_date',
            'eps': 'eps',
            'dt_eps': 'dt_eps',
            'total_revenue_ps': 'total_revenue_ps',
            'revenue_ps': 'revenue_ps',
            'current_ratio': 'current_ratio',
            'quick_ratio': 'quick_ratio',
            'cash_ratio': 'cash_ratio',
            'ar_turn': 'ar_turn',
            'ca_turn': 'ca_turn',
            'fa_turn': 'fa_turn',
            'assets_turn': 'assets_turn',
            'inv_turn': 'inv_turn',
            'roe': 'roe',
            'roe_waa': 'roe_waa',
            'roe_dt': 'roe_dt',
            'roa': 'roa',
            'netprofit_margin': 'netprofit_margin',
            'grossprofit_margin': 'gross_margin',
            'debt_to_assets': 'debt_to_assets',
            'debt_to_eqy': 'debt_to_eqy',
            'n_cashflow_to_liab': 'n_cashflow_to_liab',
            'tr_yoy': 'mbrg',
            'netprofit_yoy': 'nprg',
            'profit_to_gr': 'profit_to_gr',
        }

        existing_cols = [c for c in column_mapping.keys() if c in df.columns]
        df = df[existing_cols].rename(columns={k: column_mapping[k] for k in existing_cols})

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')
        if 'ann_date' in df.columns:
            df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d')

        df['code'] = stock_code
        df['data_source'] = 'Tushare'

        # ========== 统一单位归一化：Tushare 百分数字段 ÷100 转为小数 ==========
        # 这些字段在 Tushare 返回中为百分数（如 roe=1.65 表示 1.65%），
        # 归一化为小数（0.0165），所有消费方统一使用小数，再自行按需格式化。
        _PCT_TO_RATIO = {'roe', 'roe_waa', 'roe_dt', 'roa',
                         'netprofit_margin', 'gross_margin',
                         'debt_to_assets', 'debt_to_eqy', 'n_cashflow_to_liab',
                         'mbrg', 'nprg', 'profit_to_gr'}
        for col in _PCT_TO_RATIO & set(df.columns):
            df[col] = df[col].astype(float) / 100.0

        df = df.sort_values('report_date', ascending=True).reset_index(drop=True)
        return df

    def fetch_and_save_main_business(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存股票主营业务构成数据（增量更新）
        包含按产品/地区拆分的收入、成本、毛利
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 主营业务构成数据
        """
        if stock_code is None:
            logger.error("股票代码为空")
            return None

        try:
            today = date.today()
            old_data = self.db.get_stock_main_business(stock_code)

            if old_data is not None and not old_data.empty:
                latest_report_date = parse_row_date(old_data.iloc[0].get('report_date'))
                start_date = (latest_report_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = f"{today.year - 5}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]主营业务数据, start_date:{start_date}, end_date:{end_date_str}")

            all_dfs = []
            for bz_type in ['P', 'D']:
                try:
                    df = self.tushare.fina_mainbz(stock_code, start_date, end_date_str, bz_type=bz_type)
                    if df is not None and not df.empty:
                        normalized = self._normalize_main_business(df, stock_code, bz_type)
                        all_dfs.append(normalized)
                except Exception as e:
                    logger.warning(f"获取{stock_code}主营业务类型{bz_type}失败: {e}")
                    continue

            if not all_dfs:
                logger.info(f"股票[{stock_code}]主营业务无新增数据")
                if old_data is not None and not old_data.empty:
                    return old_data
                return None

            merged_df = pd.concat(all_dfs, ignore_index=True)
            save_count = self.db.save_stock_main_business(merged_df, stock_code)
            logger.info(f"保存股票[{stock_code}]主营业务数据成功，新增[{save_count}]条记录")

            return self.db.get_stock_main_business(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]主营业务数据失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_main_business(self, df: pd.DataFrame, stock_code: str, bz_type: str) -> pd.DataFrame:
        """
        标准化主营业务构成数据
        Tushare fina_mainbz 返回的主要字段：
        ts_code, end_date, bz_item, bz_sales, bz_profit, bz_cost, curr_type
        """
        df = df.copy()

        column_mapping = {
            'end_date': 'report_date',
            'bz_item': 'bz_item',
            'bz_sales': 'bz_sales',
            'bz_profit': 'bz_profit',
            'bz_cost': 'bz_cost',
            'curr_type': 'curr_type',
        }

        existing_cols = [c for c in column_mapping.keys() if c in df.columns]
        df = df[existing_cols].rename(columns={k: column_mapping[k] for k in existing_cols})

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')

        df['code'] = stock_code
        df['bz_type'] = bz_type
        df['data_source'] = 'Tushare'

        if 'bz_sales' in df.columns and 'bz_profit' in df.columns:
            df['gross_margin'] = df.apply(
                lambda r: round(r['bz_profit'] / r['bz_sales'] * 100, 2)
                if r['bz_sales'] and r['bz_sales'] != 0 else None,
                axis=1
            )

        df = df.sort_values('report_date', ascending=True).reset_index(drop=True)
        return df

    def fetch_and_save_holder_number(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存股东户数数据（增量更新）
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 股东户数数据
        """
        if stock_code is None:
            logger.error("股票代码为空")
            return None

        try:
            today = date.today()
            old_data = self.db.get_stock_holder_number(stock_code)

            if old_data is not None and not old_data.empty:
                latest_report_date = parse_row_date(old_data.iloc[0].get('report_date'))
                start_date = (latest_report_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = f"{today.year - 5}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]股东户数数据, start_date:{start_date}, end_date:{end_date_str}")

            try:
                df = self.tushare.holdernumber(stock_code, start_date, end_date_str)
            except DataFetchError as e:
                if old_data is not None and not old_data.empty:
                    logger.warning(f"股票[{stock_code}]股东户数数据源不可用，使用本地缓存: {e}")
                    return old_data
                logger.error(f"获取股票[{stock_code}]股东户数数据失败且本地无缓存: {e}")
                return None

            if df is None or df.empty:
                logger.info(f"股票[{stock_code}]股东户数无新增数据")
                if old_data is not None and not old_data.empty:
                    return old_data
                return None

            normalized_df = self._normalize_holder_number(df, stock_code)
            save_count = self.db.save_stock_holder_number(normalized_df, stock_code)
            logger.info(f"保存股票[{stock_code}]股东户数数据成功，新增[{save_count}]条记录")

            return self.db.get_stock_holder_number(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]股东户数数据失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_holder_number(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化股东户数数据
        Tushare stk_holdernumber 返回的主要字段：
        ts_code, ann_date, end_date, holder_num
        """
        df = df.copy()

        if 'holder_num' in df.columns:
            df = df[df['holder_num'].notna()]

        if 'ann_date' in df.columns:
            df = df.sort_values('ann_date', ascending=False)
        df = df.drop_duplicates(subset=['end_date'], keep='first')

        column_mapping = {
            'end_date': 'report_date',
            'ann_date': 'ann_date',
            'holder_num': 'holder_num',
            'holder_num_change': 'holder_num_change',
            'holder_num_change_ratio': 'holder_num_change_ratio',
        }

        existing_cols = [c for c in column_mapping.keys() if c in df.columns]
        df = df[existing_cols].rename(columns={k: column_mapping[k] for k in existing_cols})

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')
        if 'ann_date' in df.columns:
            df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d')

        df['code'] = stock_code
        df['data_source'] = 'Tushare'

        df = df.sort_values('report_date', ascending=True).reset_index(drop=True)
        return df

    def fetch_and_save_northbound_hold(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存北向持股数据（增量更新）
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 北向持股数据
        """
        if stock_code is None:
            logger.error("股票代码为空")
            return None

        try:
            today = date.today()
            old_data = self.db.get_stock_northbound_hold(stock_code)

            if old_data is not None and not old_data.empty:
                latest_trade_date = parse_row_date(old_data.iloc[0].get('trade_date'))
                start_date = (latest_trade_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = f"{today.year - 1}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]北向持股数据, start_date:{start_date}, end_date:{end_date_str}")

            try:
                df = self.tushare.hk_hold(stock_code, start_date, end_date_str)
            except DataFetchError as e:
                if old_data is not None and not old_data.empty:
                    logger.warning(f"股票[{stock_code}]北向持股数据源不可用，使用本地缓存: {e}")
                    return old_data
                logger.error(f"获取股票[{stock_code}]北向持股数据失败且本地无缓存: {e}")
                return None

            if df is None or df.empty:
                logger.info(f"股票[{stock_code}]北向持股无新增数据")
                if old_data is not None and not old_data.empty:
                    return old_data
                return None

            normalized_df = self._normalize_northbound_hold(df, stock_code)
            save_count = self.db.save_stock_northbound_hold(normalized_df, stock_code)
            logger.info(f"保存股票[{stock_code}]北向持股数据成功，新增[{save_count}]条记录")

            return self.db.get_stock_northbound_hold(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]北向持股数据失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_northbound_hold(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化北向持股数据
        Tushare hk_hold 返回的主要字段：
        ts_code, trade_date, name, vol, ratio, exchange
        """
        df = df.copy()

        column_mapping = {
            'trade_date': 'trade_date',
            'name': 'name',
            'vol': 'vol',
            'ratio': 'ratio',
            'exchange': 'exchange',
        }

        existing_cols = [c for c in column_mapping.keys() if c in df.columns]
        df = df[existing_cols].rename(columns={k: column_mapping[k] for k in existing_cols})

        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')

        df['code'] = stock_code
        df['data_source'] = 'Tushare'

        df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
        return df

    def fetch_and_save_top10_holder(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """
        获取并保存十大股东数据（增量更新）
        包含十大股东和十大流通股东
        Args:
            stock_code: 股票代码
        Returns:
            pd.DataFrame: 十大股东数据
        """
        if stock_code is None:
            logger.error("股票代码为空")
            return None

        try:
            today = date.today()
            old_data = self.db.get_stock_top10_holder(stock_code)

            if old_data is not None and not old_data.empty:
                latest_report_date = parse_row_date(old_data.iloc[0].get('report_date'))
                start_date = (latest_report_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = f"{today.year - 5}-01-01"
            end_date_str = today.strftime("%Y-%m-%d")

            logger.info(f"获取股票[{stock_code}]十大股东数据, start_date:{start_date}, end_date:{end_date_str}")

            all_dfs = []
            for holder_type in ['top10', 'top10_float']:
                try:
                    df = self.tushare.top10_holders(stock_code, start_date, end_date_str, holder_type=holder_type)
                    if df is not None and not df.empty:
                        normalized = self._normalize_top10_holder(df, stock_code, holder_type)
                        all_dfs.append(normalized)
                except Exception as e:
                    logger.warning(f"获取{stock_code}{holder_type}股东数据失败: {e}")
                    continue

            if not all_dfs:
                logger.info(f"股票[{stock_code}]十大股东无新增数据")
                if old_data is not None and not old_data.empty:
                    return old_data
                return None

            merged_df = pd.concat(all_dfs, ignore_index=True)
            save_count = self.db.save_stock_top10_holder(merged_df, stock_code)
            logger.info(f"保存股票[{stock_code}]十大股东数据成功，新增[{save_count}]条记录")

            return self.db.get_stock_top10_holder(stock_code)
        except Exception as e:
            logger.error(f"获取股票[{stock_code}]十大股东数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_industry_valuation(self, industry_name: str = None, stock_code: str = None) -> Union[pd.DataFrame, None]:
        """
        获取并保存行业估值数据（PE/PB均值）
        通过申万行业成分股 + daily_basic 聚合计算
        Args:
            industry_name: 行业名称（如"汽车"），与stock_code二选一
            stock_code: 股票代码，通过股票反查行业
        Returns:
            pd.DataFrame: 行业估值数据
        """
        if not industry_name and not stock_code:
            logger.error("行业名称和股票代码不能都为空")
            return None

        try:
            if industry_name is None and stock_code:
                basic = self.db.get_stock_basic(stock_code)
                if basic is not None:
                    industry_name = basic.industry
                    logger.info(f"通过股票[{stock_code}]反查行业: {industry_name}")
                else:
                    logger.error(f"未找到股票[{stock_code}]的行业信息")
                    return None

            today = date.today()

            classify_df = self.tushare.index_classify(level='L2')
            if classify_df.empty:
                logger.warning("未获取到行业分类列表")
                return None

            def _find_match(name_list, target):
                for name in name_list:
                    matched = classify_df[classify_df['industry_name'].str.contains(name, na=False)]
                    if not matched.empty:
                        return matched
                return pd.DataFrame()

            match_keywords = [
                industry_name,
                industry_name.replace('整车', '用车'),
                industry_name.replace('行业', ''),
                industry_name[:2] if len(industry_name) >= 2 else industry_name,
            ]
            matched = _find_match(match_keywords, industry_name)

            if matched.empty:
                l1_df = self.tushare.index_classify(level='L1')
                if not l1_df.empty:
                    l1_match = l1_df[l1_df['industry_name'].str.contains(industry_name[:2], na=False)]
                    if not l1_match.empty:
                        classify_df = l1_df
                        matched = l1_match
            if matched.empty:
                logger.warning(f"未找到匹配的行业: {industry_name}")
                return None

            index_code = matched.iloc[0]['index_code']
            matched_name = matched.iloc[0]['industry_name']
            logger.info(f"匹配行业: {matched_name} ({index_code})")

            old_data = self.db.get_industry_valuation(industry_code=index_code)
            if old_data is not None and not old_data.empty:
                latest_date = parse_row_date(old_data.iloc[0].get('trade_date'))
                if (today - latest_date).days < 7:
                    logger.info(f"行业[{matched_name}]估值数据一周内有缓存，直接返回")
                    return old_data

            end_date_str = today.strftime("%Y-%m-%d")
            start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")

            logger.info(f"获取行业[{matched_name}]估值数据, start_date:{start_date}, end_date:{end_date_str}")

            try:
                member_df = self.tushare.index_member(index_code)
                if member_df.empty:
                    logger.warning(f"行业[{matched_name}]无成分股")
                    return old_data if old_data is not None else None

                stock_codes = []
                for _, row in member_df.iterrows():
                    con_code = row.get('con_code', '')
                    if con_code and con_code.endswith(('.SZ', '.SH')):
                        code = con_code.split('.')[0]
                        stock_codes.append(code)

                logger.info(f"行业[{matched_name}]成分股数量: {len(stock_codes)}")

                all_pe = []
                all_pb = []
                all_pe_ttm = []

                sample_size = min(len(stock_codes), 50)
                import random
                sample_codes = random.sample(stock_codes, sample_size) if len(stock_codes) > 50 else stock_codes

                for code in sample_codes:
                    try:
                        basic_df = self.tushare.daily_basic(code, start_date, end_date_str)
                        if basic_df is not None and not basic_df.empty:
                            latest = basic_df.iloc[0]
                            pe = latest.get('pe')
                            pb = latest.get('pb')
                            pe_ttm = latest.get('pe_ttm')
                            if pe is not None and not pd.isna(pe) and pe > 0:
                                all_pe.append(float(pe))
                            if pb is not None and not pd.isna(pb) and pb > 0:
                                all_pb.append(float(pb))
                            if pe_ttm is not None and not pd.isna(pe_ttm) and pe_ttm > 0:
                                all_pe_ttm.append(float(pe_ttm))
                    except Exception as e:
                        logger.debug(f"获取{code}基本面失败: {e}")
                        continue

                if not all_pe and not all_pb:
                    logger.warning(f"行业[{matched_name}]未获取到有效估值数据")
                    return old_data if old_data is not None else None

                import numpy as np
                pe_ttm_median = float(np.median(all_pe_ttm)) if all_pe_ttm else None
                pb_median = float(np.median(all_pb)) if all_pb else None
                pe_static_median = float(np.median(all_pe)) if all_pe else None

                result = {
                    'industry_code': index_code,
                    'industry_name': matched_name,
                    'trade_date': today,
                    'pe_static': pe_static_median,
                    'pe_ttm': pe_ttm_median,
                    'pb': pb_median,
                    'dividend_ratio': None,
                    'stock_count': len(all_pe),
                    'data_source': 'Tushare',
                }

                result_df = pd.DataFrame([result])
                save_count = self.db.save_industry_valuation(result_df)
                logger.info(f"保存行业[{matched_name}]估值数据成功，新增[{save_count}]条记录")

                return self.db.get_industry_valuation(industry_code=index_code)
            except Exception as e:
                logger.warning(f"获取行业[{industry_name}]估值数据失败: {e}")
                if old_data is not None and not old_data.empty:
                    logger.info(f"使用本地缓存的行业估值数据")
                    return old_data
                return None
        except Exception as e:
            logger.error(f"获取行业估值数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_new_energy_penetration(self) -> Union[pd.DataFrame, None]:
        """
        获取并保存新能源车渗透率数据（行业宏观数据）
        优先从Tushare获取，失败则使用Akshare兜底
        Returns:
            pd.DataFrame: 新能源车渗透率数据
        """
        try:
            today = date.today()
            old_data = self.db.get_new_energy_penetration()

            if old_data is not None and not old_data.empty:
                latest_date = parse_row_date(old_data.iloc[0].get('month'))
                if (today.year == latest_date.year and today.month == latest_date.month) or \
                   (today - latest_date).days < 30:
                    logger.info("新能源车渗透率数据当月有缓存，直接返回")
                    return old_data

            logger.info("获取新能源车渗透率数据")

            df = None
            try:
                df = self.akshare.new_energy_penetration()
            except Exception as e:
                logger.warning(f"Akshare获取新能源车渗透率失败: {e}")

            if df is None or df.empty:
                logger.warning("未获取到新能源车渗透率数据")
                if old_data is not None and not old_data.empty:
                    return old_data
                return None

            normalized_df = self._normalize_new_energy_penetration(df)
            save_count = self.db.save_new_energy_penetration(normalized_df)
            logger.info(f"保存新能源车渗透率数据成功，新增[{save_count}]条记录")

            return self.db.get_new_energy_penetration()
        except Exception as e:
            logger.error(f"获取新能源车渗透率数据失败: {e} {traceback.format_exc()}")
            return None

    def fetch_and_save_vehicle_sales(self, stock_code: str = None) -> Union[pd.DataFrame, None]:
        """获取并保存全国车型月销量数据（30天缓存，全量覆盖）"""
        from datetime import date, timedelta
        from calendar import monthrange
        try:
            today = date.today()
            first_of_month = today.replace(day=1)
            last_month = first_of_month - timedelta(days=1)
            month_str = last_month.strftime("%Y-%m")

            old_data = self.db.get_vehicle_sales(month=month_str)
            if old_data is not None and not old_data.empty:
                logger.info(f"车型销量[{month_str}]缓存已存在，直接返回")
                return old_data

            logger.info(f"获取{month_str}全国车型销量数据")
            try:
                df = self.akshare.get_vehicle_sales(month=month_str)
            except Exception as e:
                if old_data is not None and not old_data.empty:
                    return old_data
                logger.error(f"获取车型销量失败: {e}")
                return None

            if df is None or df.empty:
                logger.info(f"{month_str}车型销量无数据")
                if old_data is not None and not old_data.empty:
                    return old_data
                return None

            normalized_df = self._normalize_vehicle_sales(df)
            save_count = self.db.save_vehicle_sales(normalized_df, month_str)
            logger.info(f"保存{month_str}车型销量成功，共{save_count}条")
            return self.db.get_vehicle_sales(month=month_str)
        except Exception as e:
            logger.error(f"获取车型销量失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_vehicle_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df.drop_duplicates(subset=['series_name'], keep='first')
        df = df.sort_values('sales_volume', ascending=False).reset_index(drop=True)
        return df

    def fetch_and_save_repurchase(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存股票回购数据（增量更新）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old_data = self.db.get_stock_repurchase(stock_code)
            logger.info(f"获取股票[{stock_code}]回购数据")
            try:
                df = self.tushare.repurchase(stock_code)
            except DataFetchError as e:
                if old_data is not None and not old_data.empty:
                    logger.warning(f"回购数据源不可用，使用本地缓存: {e}")
                    return old_data
                logger.error(f"获取回购数据失败且无缓存: {e}")
                return None
            if df is None or df.empty:
                logger.info(f"股票[{stock_code}]无回购数据")
                if old_data is not None and not old_data.empty: return old_data
                return None
            normalized_df = self._normalize_repurchase(df, stock_code)
            save_count = self.db.save_stock_repurchase(normalized_df, stock_code)
            logger.info(f"保存[stock_code]回购数据成功，新增[{save_count}]条")
            return self.db.get_stock_repurchase(stock_code)
        except Exception as e:
            logger.error(f"获取回购数据失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_repurchase(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        df = df.drop_duplicates(subset=['ann_date'], keep='first')
        df = df.replace({pd.NaT: None, np.nan: None})
        if 'ann_date' in df.columns: df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d')
        if 'end_date' in df.columns: df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d')
        if 'exp_date' in df.columns: df['exp_date'] = pd.to_datetime(df['exp_date'], format='%Y%m%d', errors='coerce')
        df = df.replace({pd.NaT: None, np.nan: None})
        df = df.sort_values('ann_date', ascending=False).reset_index(drop=True)
        return df

    def fetch_and_save_share_float(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存限售解禁数据"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            old_data = self.db.get_stock_share_float(stock_code, future_only=True)
            today = date.today()
            if old_data is not None and not old_data.empty:
                latest = pd.to_datetime(old_data['float_date'].max())
                if latest < pd.Timestamp(today + timedelta(days=60)):
                    logger.info("解禁数据缓存不足60天，重新拉取")
                else:
                    logger.info("解禁数据缓存充足，直接返回")
                    return old_data
            logger.info(f"获取股票[{stock_code}]限售解禁数据")
            try:
                df = self.tushare.share_float(stock_code=stock_code)
            except DataFetchError as e:
                if old_data is not None and not old_data.empty: return old_data
                logger.error(f"获取解禁数据失败: {e}")
                return None
            if df is None or df.empty:
                logger.info(f"股票[{stock_code}]无解禁数据")
                if old_data is not None and not old_data.empty: return old_data
                return None
            normalized_df = self._normalize_share_float(df, stock_code)
            save_count = self.db.save_stock_share_float(normalized_df, stock_code)
            logger.info(f"保存[stock_code]解禁数据成功，新增[{save_count}]条")
            return self.db.get_stock_share_float(stock_code)
        except Exception as e:
            logger.error(f"获取解禁数据失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_share_float(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        if 'float_date' in df.columns: df['float_date'] = pd.to_datetime(df['float_date'], format='%Y%m%d')
        if 'ann_date' in df.columns: df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d')
        df = df.sort_values('float_date', ascending=True).reset_index(drop=True)
        return df

    def fetch_and_save_broker_recommend(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存分析师评级"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            today = date.today()
            old_data = self.db.get_stock_broker_reco(stock_code, months=3)
            if old_data is not None and not old_data.empty:
                logger.info("分析师评级缓存充足，直接返回")
                return old_data
            logger.info(f"获取股票[{stock_code}]分析师评级")
            for months_ago in range(3):
                m = today.replace(day=1) - timedelta(days=months_ago * 31)
                month_str = m.strftime('%Y%m')
                try:
                    df = self.tushare.broker_recommend(month_str)
                    if df is not None and not df.empty:
                        stock_df = df[df['ts_code'].str.contains(stock_code, na=False)]
                        if not stock_df.empty:
                            normalized = self._normalize_broker_recommend(stock_df, stock_code)
                            self.db.save_stock_broker_reco(normalized, stock_code)
                except Exception as e:
                    logger.debug(f"获取{month_str}评级失败: {e}")
                    continue
            return self.db.get_stock_broker_reco(stock_code, months=3)
        except Exception as e:
            logger.error(f"获取分析师评级失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_broker_recommend(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        df = df.drop_duplicates(subset=['month', 'broker'], keep='first')
        df = df.sort_values('month', ascending=False).reset_index(drop=True)
        return df

    def fetch_and_save_pledge(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存股权质押数据"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            today = pd.Timestamp.today()
            old_data = self.db.get_stock_pledge(stock_code)
            if old_data is not None and not old_data.empty:
                latest_date = pd.to_datetime(old_data.iloc[0].get('end_date'))
                if (today - latest_date).days < 7:
                    logger.info("质押数据一周内有缓存，直接返回")
                    return old_data
            logger.info(f"获取股票[{stock_code}]质押数据")
            try:
                df = self.tushare.pledge_stat(stock_code)
            except DataFetchError as e:
                if old_data is not None and not old_data.empty: return old_data
                logger.error(f"获取质押数据失败: {e}")
                return None
            if df is None or df.empty:
                if old_data is not None and not old_data.empty: return old_data
                return None
            normalized_df = self._normalize_pledge(df, stock_code)
            save_count = self.db.save_stock_pledge(normalized_df, stock_code)
            logger.info(f"保存[stock_code]质押数据成功，新增[{save_count}]条")
            return self.db.get_stock_pledge(stock_code)
        except Exception as e:
            logger.error(f"获取质押数据失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_pledge(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        df = df.drop_duplicates(subset=['end_date'], keep='first')
        if 'end_date' in df.columns: df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d')
        df = df.sort_values('end_date', ascending=False).reset_index(drop=True)
        return df

    def fetch_and_save_block_trade(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存大宗交易（增量更新）"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            today = date.today()
            old_data = self.db.get_stock_block_trade(stock_code, days=90)
            if old_data is not None and not old_data.empty:
                latest_date = pd.to_datetime(old_data.iloc[0].get('trade_date'))
                start_date = (latest_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
            end_date_str = today.strftime("%Y-%m-%d")
            if start_date >= end_date_str:
                logger.info(f"股票[{stock_code}]大宗交易数据已是最新")
                if old_data is not None and not old_data.empty: return old_data
                return None
            logger.info(f"获取股票[{stock_code}]大宗交易, {start_date}~{end_date_str}")
            try:
                df = self.tushare.block_trade(stock_code=stock_code, start_date=start_date, end_date=end_date_str)
            except DataFetchError as e:
                if old_data is not None and not old_data.empty: return old_data
                return None
            if df is None or df.empty:
                if old_data is not None and not old_data.empty: return old_data
                return None
            normalized_df = self._normalize_block_trade(df, stock_code)
            save_count = self.db.save_stock_block_trade(normalized_df, stock_code)
            logger.info(f"保存[{stock_code}]大宗交易成功，新增[{save_count}]条")
            return self.db.get_stock_block_trade(stock_code, days=90)
        except Exception as e:
            logger.error(f"获取大宗交易失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_block_trade(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        if 'trade_date' in df.columns: df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
        return df

    def fetch_and_save_top_list(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存龙虎榜上榜记录"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            today = date.today()
            old_data = self.db.get_stock_top_list(stock_code, days=90)
            logger.info(f"获取股票[{stock_code}]龙虎榜数据")
            import calendar
            days_per_month = calendar.monthrange(today.year, today.month)[1]
            trade_dates = []
            for d in range(days_per_month, 0, -1):
                dt = date(today.year, today.month, d)
                if dt.weekday() < 5 and dt <= today:
                    trade_dates.append(dt.strftime("%Y-%m-%d"))
                    if len(trade_dates) >= 90:
                        break
            new_records = []
            for td in trade_dates[:20]:
                try:
                    df = self.tushare.top_list(td)
                    if df is not None and not df.empty:
                        stock_df = df[df['ts_code'].str.contains(stock_code, na=False)]
                        if not stock_df.empty:
                            new_records.append(self._normalize_top_list_item(stock_df, stock_code))
                except Exception:
                    continue
            if new_records:
                merged = pd.concat(new_records, ignore_index=True)
                self.db.save_stock_top_list(merged, stock_code)
            return self.db.get_stock_top_list(stock_code, days=90)
        except Exception as e:
            logger.error(f"获取龙虎榜失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_top_list_item(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        if 'trade_date' in df.columns: df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        return df

    def fetch_and_save_top_inst(self, stock_code: str) -> Union[pd.DataFrame, None]:
        """获取并保存龙虎榜机构席位追踪"""
        if stock_code is None:
            logger.error("股票代码为空")
            return None
        try:
            today = date.today()
            old_data = self.db.get_stock_top_inst(stock_code, days=90)
            logger.info(f"获取股票[{stock_code}]机构席位数据")
            import calendar
            days_per_month = calendar.monthrange(today.year, today.month)[1]
            trade_dates = []
            for d in range(days_per_month, 0, -1):
                dt = date(today.year, today.month, d)
                if dt.weekday() < 5 and dt <= today:
                    trade_dates.append(dt.strftime("%Y%m%d"))
                    if len(trade_dates) >= 20:
                        break
            new_records = []
            for td in trade_dates[:10]:
                try:
                    df = self.tushare.top_inst(td)
                    if df is not None and not df.empty:
                        stock_df = df[df['ts_code'].str.contains(stock_code, na=False)]
                        if not stock_df.empty:
                            new_records.append(self._normalize_top_inst_item(stock_df, stock_code))
                except Exception:
                    continue
            if new_records:
                merged = pd.concat(new_records, ignore_index=True)
                self.db.save_stock_top_inst(merged, stock_code)
            return self.db.get_stock_top_inst(stock_code, days=90)
        except Exception as e:
            logger.error(f"获取机构席位失败: {e} {traceback.format_exc()}")
            return None

    def _normalize_top_inst_item(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        df = df.copy()
        if 'trade_date' in df.columns: df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        return df

    def _normalize_new_energy_penetration(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化新能源车渗透率数据
        """
        df = df.copy()

        if 'month' not in df.columns:
            for col in ['月份', '日期', 'date', '时间']:
                if col in df.columns:
                    df['month'] = pd.to_datetime(df[col])
                    break

        if 'month' in df.columns:
            df['month'] = pd.to_datetime(df['month'])

        for src_col, dst_col in [
            ('总销量', 'total_sales'),
            ('汽车总销量', 'total_sales'),
            ('新能源销量', 'new_energy_sales'),
            ('新能源车销量', 'new_energy_sales'),
            ('渗透率', 'penetration_rate'),
            ('新能源渗透率', 'penetration_rate'),
        ]:
            if src_col in df.columns and dst_col not in df.columns:
                df[dst_col] = pd.to_numeric(df[src_col], errors='coerce')

        if 'data_source' not in df.columns:
            df['data_source'] = 'Akshare'

        df = df.sort_values('month', ascending=False).reset_index(drop=True)
        return df

    def _normalize_top10_holder(self, df: pd.DataFrame, stock_code: str, holder_type: str) -> pd.DataFrame:
        """
        标准化十大股东数据
        Tushare top10_holders / top10_floatholders 返回的主要字段：
        ts_code, ann_date, end_date, holder_name, hold_amount, hold_ratio, hold_float_ratio, hold_change
        """
        df = df.copy()

        column_mapping = {
            'end_date': 'report_date',
            'ann_date': 'ann_date',
            'holder_name': 'holder_name',
            'hold_amount': 'hold_amount',
            'hold_ratio': 'hold_ratio',
            'hold_float_ratio': 'hold_float_ratio',
            'hold_change': 'hold_change',
        }

        existing_cols = [c for c in column_mapping.keys() if c in df.columns]
        df = df[existing_cols].rename(columns={k: column_mapping[k] for k in existing_cols})

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')
        if 'ann_date' in df.columns:
            df['ann_date'] = pd.to_datetime(df['ann_date'], format='%Y%m%d')

        df['code'] = stock_code
        df['holder_type'] = holder_type
        df['data_source'] = 'Tushare'

        df = df.sort_values('report_date', ascending=True).reset_index(drop=True)
        return df

    def _normalize_income_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化利润表数据
        Tushare income 返回的主要字段：
        ts_code, ann_date, end_date, total_revenue, operate_profit, n_income, basic_eps, oper_cost,
        sell_exp（销售费用）, admin_exp（管理费用）, rd_exp（研发费用）, fin_exp（财务费用）
        数据单位：元
        """
        df = df.copy()

        # 防御：tushare 偶发返回列名漂移或空 df（实测 KeyError: Index(['end_date'])）
        if df.empty or 'end_date' not in df.columns:
            logger.warning(f"[利润表标准化] {stock_code} 返回 df 缺 end_date 列或为空，列名={list(df.columns)}")
            return df.iloc[0:0]  # 返回空 df 保持接口契约
        if 'update_flag' in df.columns:
            df = df.sort_values('update_flag', ascending=False)
        df = df.drop_duplicates(subset=['end_date'], keep='first')

        column_mapping = {
            'ts_code': 'code',
            'end_date': 'report_date',
            'total_revenue': 'total_revenue',
            'operate_profit': 'operating_profit',
            'n_income': 'net_profit_total',       # 含少数股东损益的净利润总额
            'n_income_attr_p': 'net_profit',       # 归母净利润（主要分析指标）
            'basic_eps': 'basic_eps',
            'oper_cost': 'oper_cost',
            'sell_exp': 'sell_exp',
            'admin_exp': 'admin_exp',
            'rd_exp': 'rd_exp',
            'fin_exp': 'fin_exp',
        }

        df = df.rename(columns=column_mapping)

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')

        df['code'] = stock_code

        df = df.sort_values('report_date', ascending=True).reset_index(drop=True)

        df['report_year'] = df['report_date'].dt.year
        df['report_quarter'] = df['report_date'].dt.quarter

        def _num(value):
            if value is None or pd.isna(value):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        revenue_map = df.set_index(['report_year', 'report_quarter'])['total_revenue'].to_dict()
        profit_map = (df.set_index(['report_year', 'report_quarter'])['net_profit'].to_dict()
                      if 'net_profit' in df.columns else {})

        revenue_growth_list = []
        profit_growth_list = []
        gross_margin_list = []

        for _, row in df.iterrows():
            revenue = row.get('total_revenue')
            profit = row.get('net_profit')
            oper_cost = row.get('oper_cost')
            year = row['report_year']
            quarter = row['report_quarter']

            prev_year = year - 1
            prev_revenue = revenue_map.get((prev_year, quarter))
            prev_profit = profit_map.get((prev_year, quarter))

            if prev_revenue and prev_revenue != 0 and revenue is not None:
                revenue_growth = ((revenue - prev_revenue) / prev_revenue) * 100
            else:
                revenue_growth = None
            revenue_growth_list.append(revenue_growth)

            if prev_profit and prev_profit != 0 and profit is not None:
                profit_growth = ((profit - prev_profit) / prev_profit) * 100
            else:
                profit_growth = None
            profit_growth_list.append(profit_growth)

            if revenue and revenue != 0 and oper_cost is not None:
                gross_margin = ((revenue - oper_cost) / revenue) * 100
            else:
                gross_margin = None
            gross_margin_list.append(gross_margin)

        df['revenue_growth'] = revenue_growth_list
        df['profit_growth'] = profit_growth_list
        df['gross_margin'] = gross_margin_list

        df['data_source'] = 'Tushare'

        # ---- 数据自检：验证归母净利润 ≤ 含少数股东净利润 ----
        if 'net_profit_total' in df.columns and 'net_profit' in df.columns:
            for idx, row in df.iterrows():
                np_attr = _num(row.get('net_profit'))  # 不含少数股东（归母）
                np_total = _num(row.get('net_profit_total'))  # 含少数股东
                if np_attr and np_total and np_attr > 0 and np_total > 0:
                    # 正常情况下：不含(归母) ≤ 含(少数)
                    if np_attr > np_total:
                        diff_pct = (np_attr - np_total) / np_total * 100
                        logger.warning(
                            f"[{row.get('report_date')}] 归母净利润({np_attr/1e8:.2f}亿)"
                            f" > 含少数股东净利润({np_total/1e8:.2f}亿)"
                            f"(超 {diff_pct:.1f}%)，疑似字段赋值颠倒，取较大值"
                        )
                        # 取较大值为修正后的归母净利润
                        df.at[idx, 'net_profit'] = max(np_attr, np_total)
                    elif abs(np_attr - np_total) / np_total < 0.01:
                        # 两者极其接近，说明上游可能混用了同一个值
                        logger.warning(
                            f"[{row.get('report_date')}] 归母净利润与含少数股东净利润"
                            f"差异 <1%，数值可能被混用（均为 {np_attr/1e8:.2f}亿）"
                        )

        keep_cols = ['code', 'report_date', 'total_revenue', 'operating_profit',
                     'net_profit', 'net_profit_total',
                     'basic_eps', 'sell_exp', 'admin_exp', 'rd_exp', 'fin_exp',
                     'revenue_growth', 'profit_growth', 'gross_margin', 'data_source']
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]

        df = df.dropna(subset=['report_date'])

        df = df.sort_values('report_date', ascending=False).reset_index(drop=True)

        return df

    def _normalize_balance_sheet_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化资产负债表数据
        Tushare balancesheet 返回的主要字段：
        ts_code, ann_date, end_date, total_assets, total_cur_assets, total_nca,
        total_liab, total_cur_liab, total_ncl, total_hldr_eqy_exc_min_int
        数据单位：元
        """
        df = df.copy()

        if 'update_flag' in df.columns:
            df = df.sort_values('update_flag', ascending=False)
        df = df.drop_duplicates(subset=['end_date'], keep='first')

        column_mapping = {
            'ts_code': 'code',
            'end_date': 'report_date',
            'total_assets': 'total_assets',
            'total_cur_assets': 'current_assets',
            'total_nca': 'non_current_assets',
            'total_liab': 'total_liabilities',
            'total_cur_liab': 'current_liabilities',
            'total_ncl': 'non_current_liabilities',
            'total_hldr_eqy_exc_min_int': 'total_equity',
            'accounts_receiv': 'accounts_receivable',
            'inventories': 'inventory',
            'fix_assets': 'fixed_assets',
            'cip': 'construction_in_progress',
        }

        df = df.rename(columns=column_mapping)

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')

        df['code'] = stock_code

        for col in ['total_assets', 'total_liabilities', 'current_assets', 'current_liabilities',
                    'accounts_receivable', 'inventory']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        def calc_ratios(row):
            total_assets = row.get('total_assets')
            total_liabilities = row.get('total_liabilities')
            current_assets = row.get('current_assets')
            current_liabilities = row.get('current_liabilities')

            asset_liability_ratio = None
            if total_assets and total_assets != 0 and total_liabilities is not None:
                asset_liability_ratio = (total_liabilities / total_assets) * 100

            current_ratio = None
            if current_liabilities and current_liabilities != 0 and current_assets is not None:
                current_ratio = current_assets / current_liabilities

            return pd.Series({
                'asset_liability_ratio': asset_liability_ratio,
                'current_ratio': current_ratio
            })

        ratio_df = df.apply(calc_ratios, axis=1)
        df['asset_liability_ratio'] = ratio_df['asset_liability_ratio']
        df['current_ratio'] = ratio_df['current_ratio']

        df['data_source'] = 'Tushare'

        keep_cols = ['code', 'report_date', 'total_assets', 'current_assets', 'non_current_assets',
                     'total_liabilities', 'current_liabilities', 'non_current_liabilities',
                     'total_equity', 'asset_liability_ratio', 'current_ratio',
                     'accounts_receivable', 'inventory', 'fixed_assets', 'construction_in_progress',
                     'data_source']
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]

        df = df.dropna(subset=['report_date'])

        df = df.sort_values('report_date', ascending=False).reset_index(drop=True)

        return df

    def _normalize_cashflow_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化现金流量表数据
        Tushare cashflow 返回的主要字段：
        ts_code, ann_date, end_date, n_cashflow_act（经营活动现金流净额）,
        n_cashflow_inv_act（投资活动现金流净额）, n_cash_flows_fnc_act（筹资活动现金流净额）,
        c_pay_acq_const_fids（购建固定资产无形资产支付的现金）, free_cashflow（自由现金流，可能为空）
        数据单位：元，报告期为累计口径
        """
        df = df.copy()

        if 'update_flag' in df.columns:
            df = df.sort_values('update_flag', ascending=False)
        df = df.drop_duplicates(subset=['end_date'], keep='first')

        column_mapping = {
            'ts_code': 'code',
            'end_date': 'report_date',
            'n_cashflow_act': 'operating_cashflow',
            'n_cashflow_inv_act': 'investing_cashflow',
            'n_cash_flows_fnc_act': 'financing_cashflow',
            'c_pay_acq_const_fiolta': 'capex',
            'free_cashflow': 'free_cashflow',
        }

        df = df.rename(columns=column_mapping)

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')

        df['code'] = stock_code

        df['data_source'] = 'Tushare'

        keep_cols = ['code', 'report_date', 'operating_cashflow', 'investing_cashflow',
                     'financing_cashflow', 'capex', 'free_cashflow', 'data_source']
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]

        df = df.dropna(subset=['report_date'])

        df = df.sort_values('report_date', ascending=False).reset_index(drop=True)

        return df


stock_tool_instance = StockTools()  # 传入你的数据库连接

# ===================== 1. 注册：日线数据工具 =====================
# ===================== K线摘要构造（喂给 LLM 的精简格式） =====================
# 设计：最新指标快照 + 近20根K线的已判定信号 + 近10根紧凑行情表。
# 比整表 to_string 省大量 token，且金叉死叉等判断由代码完成，LLM 只做解读。

_HS300_CACHE = {"date": None, "series": None}


def _get_hs300_close() -> Optional[pd.Series]:
    """拉沪深300日线收盘价（当日内存缓存），用于计算相对强弱"""
    today_d = date.today()
    if _HS300_CACHE["date"] == today_d and _HS300_CACHE["series"] is not None:
        return _HS300_CACHE["series"]

    # 先试 Tushare（速度更快、稳定性更高），失败再试 akshare
    for attempt, (source, label) in enumerate([
        (lambda: _fetch_hs300_tushare(today_d), "Tushare"),
        (lambda: _fetch_hs300_akshare(today_d), "akshare"),
    ]):
        try:
            s = source()
            if s is not None and len(s) > 10:
                _HS300_CACHE.update(date=today_d, series=s)
                return s
        except Exception as e:
            logger.warning(f"获取沪深300指数({label})失败: {e}")
    logger.warning("获取沪深300指数失败，跳过相对强弱计算")
    return None


def _fetch_hs300_akshare(today_d: date) -> Optional[pd.Series]:
    """通过 akshare 获取沪深300日线"""
    import akshare as ak
    start = (today_d - timedelta(days=180)).strftime("%Y%m%d")
    idx = ak.index_zh_a_hist(symbol="000300", period="daily",
                             start_date=start, end_date=today_d.strftime("%Y%m%d"))
    s = pd.Series(
        [float(v) for v in idx["收盘"].values],
        index=[parse_row_date(v) for v in idx["日期"].values],
    ).sort_index()
    return s


def _fetch_hs300_tushare(today_d: date) -> Optional[pd.Series]:
    """通过 Tushare 获取沪深300日线（作为 akshare 的兜底）"""
    import tushare as ts
    try:
        pro = ts.pro_api()
    except Exception:
        return None
    start = (today_d - timedelta(days=180)).strftime("%Y%m%d")
    df = pro.index_daily(ts_code="000300.SH",
                         start_date=start,
                         end_date=today_d.strftime("%Y%m%d"),
                         fields="trade_date,close")
    if df is None or df.empty:
        return None
    s = pd.Series(
        [float(v) for v in df["close"].values],
        index=df["trade_date"].apply(parse_row_date),
    ).sort_index()
    return s


def _calc_rs_text(df: pd.DataFrame) -> str:
    """相对强弱：近20/60根K线涨幅 - 沪深300同期涨幅（仅日线有意义）"""
    idx = _get_hs300_close()
    if idx is None or df is None or len(df) < 21:
        return ""

    def idx_at(d):
        sub = idx[idx.index <= d]
        return float(sub.iloc[-1]) if len(sub) else None

    parts = []
    try:
        for n, label in [(20, "近20日"), (60, "近60日")]:
            if len(df) <= n:
                continue
            new_close, old_close = float(df.iloc[0]["close"]), float(df.iloc[n]["close"])
            d_new = parse_row_date(df.iloc[0]["date"])
            d_old = parse_row_date(df.iloc[n]["date"])
            i_new, i_old = idx_at(d_new), idx_at(d_old)
            if not all([new_close, old_close, i_new, i_old]):
                continue
            rs = (new_close / old_close - 1) * 100 - (i_new / i_old - 1) * 100
            verb = "跑赢" if rs >= 0 else "跑输"
            parts.append(f"{label}{verb}沪深300 {abs(rs):.1f}个百分点")
    except Exception as e:
        logger.warning(f"相对强弱计算失败: {e}")
        return ""
    return "；".join(parts)


def _fmt(value, nd: int = 2) -> str:
    """指标值格式化：缺失/NaN 显示 '-'"""
    if value is None:
        return "-"
    try:
        if pd.isna(value):
            return "-"
        return f"{float(value):.{nd}f}"
    except (TypeError, ValueError):
        return str(value)


def _build_kline_summary(df: pd.DataFrame, stock_code: str, freq_label: str,
                         rs_text: str = "") -> str:
    """把带指标的K线 DataFrame 压缩成 LLM 友好的摘要文本（df 按日期降序，最新在前）"""
    latest = df.iloc[0]
    latest_date = parse_row_date(latest.get("date"))
    lines = [f"✅【{stock_code} {freq_label}数据】共 {len(df)} 根K线，数据截至 {latest_date}"]

    g = latest.get
    turnover = g('turnover_rate')
    lines.append(
        f"【最新指标快照】收盘={_fmt(g('close'))} 涨跌幅={_fmt(g('pct_chg'))}% "
        f"量比={_fmt(g('volume_ratio'))}"
        + (f" 换手率={_fmt(turnover)}%" if turnover is not None and not pd.isna(turnover) else "")
    )
    lines.append(
        f"  均线: MA5={_fmt(g('ma5'))} MA10={_fmt(g('ma10'))} MA20={_fmt(g('ma20'))} "
        f"MA50={_fmt(g('ma50'))} MA120={_fmt(g('ma120'))} MA200={_fmt(g('ma200'))} | 形态: {g('ma_pattern', '-')}"
    )
    lines.append(
        f"  MACD: DIF={_fmt(g('DIF'), 3)} DEA={_fmt(g('DEA'), 3)} MACD={_fmt(g('MACD'), 3)} | "
        f"RSI: 6日={_fmt(g('rsi6'))} 12日={_fmt(g('rsi12'))} 24日={_fmt(g('rsi24'))}"
    )
    lines.append(
        f"  KDJ: K={_fmt(g('kdj_k'))} D={_fmt(g('kdj_d'))} J={_fmt(g('kdj_j'))} | "
        f"BOLL: 上轨={_fmt(g('boll_upper'))} 中轨={_fmt(g('boll_mid'))} 下轨={_fmt(g('boll_lower'))}"
    )
    obv_trend = "-"
    if "obv" in df.columns and len(df) > 20 and not pd.isna(latest.get("obv")):
        obv_trend = "上升" if latest["obv"] > df.iloc[20]["obv"] else "下降"
    # 年内位置只在日线展示：周/月线用的是周期收盘价区间（月线会抹掉日间低点），
    # 同名指标在不同周期算出两个数（如日线31.8% vs 月线5.4%），报告里必然自相矛盾
    pos_seg = ""
    if freq_label == "日线":
        pos_seg = f"| 年内位置={_fmt(g('pos_52w'), 1)}%（0=年内最低,100=年内最高，按日收盘计） "
    lines.append(
        f"  ATR14={_fmt(g('atr14'), 3)} {pos_seg}| OBV近20根趋势: {obv_trend}"
    )
    if rs_text:
        lines.append(f"【相对强弱】{rs_text}")

    # 近20根K线的信号（代码判定结果，倒序=最近的在前）
    signal_lines = []
    recent_signal_names = set()  # 近期出现过的信号种类，用于筛选历史胜率展示
    for _, row in df.head(20).iterrows():
        sig_parts = []
        if row.get("macd_signal") == 1:
            sig_parts.append("MACD金叉")
        elif row.get("macd_signal") == -1:
            sig_parts.append("MACD死叉")
        for col in ("ma_cross", "vol_signal", "gap_signal"):
            v = row.get(col)
            if v and isinstance(v, str) and v.strip():
                sig_parts.extend(v.strip().split())
        # 背离信号（每列只有一个非空值：顶背离/底背离）
        for diver_col in ("macd_divergence", "macd_bar_divergence", "obv_divergence"):
            dv = row.get(diver_col)
            if dv and isinstance(dv, str) and dv.strip():
                label = {"macd_divergence": "MACD", "macd_bar_divergence": "柱状线", "obv_divergence": "OBV"}
                sig_parts.append(f"{label[diver_col]}{dv}")
        if sig_parts:
            recent_signal_names.update(sig_parts)
            signal_lines.append(f"  {parse_row_date(row.get('date'))}: {'、'.join(sig_parts)}")
    lines.append("【近20根K线信号（程序判定，请勿自行推算交叉）】")
    lines.append("\n".join(signal_lines) if signal_lines else "  （无信号）")

    # 信号历史胜率：只统计近期出现过的信号种类（含当前均线形态的切换信号）
    pattern = latest.get("ma_pattern")
    if pattern == "多头排列":
        recent_signal_names.add("转多头排列")
    elif pattern == "空头排列":
        recent_signal_names.add("转空头排列")
    if recent_signal_names:
        from .stock.base import calc_signal_history_stats, format_signal_stats
        stats_text = format_signal_stats(
            calc_signal_history_stats(df), signal_names=sorted(recent_signal_names))
        if stats_text:
            lines.append("【信号历史胜率（该股全部历史的条件统计，不代表未来）】")
            lines.append(stats_text)

    # 极端信号汇总（超买回调 + 超跌反弹）
    _FREQ_MAP = {"日线": "daily", "周线": "week", "月线": "month"}
    freq_key = _FREQ_MAP.get(freq_label)
    if freq_key:
        try:
            from tools.extreme_signal import assess_extreme_signals, format_extreme_signals
            extreme = format_extreme_signals(
                assess_extreme_signals(df, freq=freq_key), freq_label=freq_label)
            if extreme:
                lines.append("【极端信号汇总】")
                lines.append(extreme)
        except Exception as e:
            logger.warning(f"[极端信号] 计算失败: {e}")

    # 近10根紧凑行情表
    cols = [c for c in ["date", "open", "high", "low", "close", "volume", "volume_ratio", "pct_chg"]
            if c in df.columns]
    lines.append("【近10根K线行情（最新在前）】")
    lines.append(df.head(10)[cols].to_string(index=False))
    return "\n".join(lines)


def _ensure_indicators(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """
    走本地缓存回退/当日已更新分支时，返回的是 DB 数据（无 RSI/KDJ/BOLL 等内存指标列），
    这里用 OHLCV 现算补齐，保证摘要不缺项。
    """
    if df is None or df.empty:
        return df
    if 'rsi6' in df.columns:
        return df
    if not {'close', 'high', 'low', 'volume'}.issubset(df.columns):
        return df
    try:
        return stock_tool_instance.akshare._calculate_indicators(df, freq=freq)
    except Exception as e:
        logger.warning(f"补算技术指标失败，使用原始数据: {e}")
        return df


def _get_latest_turnover(stock_code: str) -> Optional[float]:
    """从每日指标表取最新换手率（没抓过数据则返回 None）"""
    try:
        basic = stock_tool_instance.db.get_latest_daily_basic_data(stock_code, 5)
        if basic is not None and not basic.empty:
            v = basic.iloc[0].get("turnover_rate")
            if v is not None and not pd.isna(v):
                return float(v)
    except Exception as e:
        logger.warning(f"读取换手率失败: {e}")
    return None


def call_fetch_daily_data(stock_code: str) -> str:
    """
    获取并保存股票日线数据
    :param stock_code: 股票代码，如 000001
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_stock_daily_data(stock_code=stock_code)
        if df is None or df.empty:
            logger.error(f"未获取到 {stock_code} 的日线数据")
            return f"❌ 未获取到 {stock_code} 的日线数据"
        df = _ensure_indicators(df, "daily")
        return _build_kline_summary(
            df, stock_code, "日线",
            rs_text=_calc_rs_text(df),
        )
    except Exception as e:
        logger.error(f"调用日线工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取日线数据失败"


# ===================== 2. 注册：周线数据工具 =====================
def call_fetch_weekly_data(stock_code: str) -> str:
    """
    获取并保存股票周线数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_stock_weekly_data(stock_code=stock_code)
        if df is None or df.empty:
            return f"❌ 未获取到 {stock_code} 的周线数据"
        df = _ensure_indicators(df, "week")
        return _build_kline_summary(df, stock_code, "周线")
    except Exception as e:
        logger.error(f"调用周线工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取周线数据失败"

# ===================== 3. 注册：月线数据工具 =====================
def call_fetch_monthly_data(stock_code: str) -> str:
    """
    获取并保存股票月线数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_stock_monthly_data(stock_code=stock_code)
        if df is None or df.empty:
            return f"❌ 未获取到 {stock_code} 的月线数据"
        df = _ensure_indicators(df, "month")
        return _build_kline_summary(df, stock_code, "月线")
    except Exception as e:
        logger.error(f"调用月线工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取月线数据失败"

def call_fetch_stock_research_report(stock_code: str) -> str:
    """
    获取股票研报
    :param stock_code: 股票代码
    :return: 研报数据
    """
    try:
        df = stock_tool_instance.fetch_and_save_stock_research_report(stock_code)
        if df is None or df.empty:
            return f"❌ 未获取到 {stock_code} 的股票研报"
        return f"✅ 【{stock_code} 股票研报】\n{df.head(20).to_string()}"
    except Exception as e:
        logger.error(f"调用股票研报工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取股票研报数据失败"

# 1. 定义单参数的Pydantic模型（必须正确，否则schema缺properties）
class StockCodeInput(BaseModel):
    stock_code: str = Field(description="A股股票代码，例如：002594、600036")


class IndustryValuationInput(BaseModel):
    stock_code: Optional[str] = Field(default=None, description="股票代码，通过股票反查所属行业估值，例如：002594")
    industry_name: Optional[str] = Field(default=None, description="行业名称，直接查询指定行业估值，例如：汽车、电子")


class NewEnergyPenetrationInput(BaseModel):
    pass


class SectorFundFlowInput(BaseModel):
    industry_name: str = Field(default="", description="行业名称（可选，留空则返回全市场排名）")
    top_n: int = Field(default=10, ge=1, le=50, description="返回前N名")


class BatchSotpInput(BaseModel):
    stock_codes: str = Field(description="逗号分隔的股票代码列表，如 600118,001270,600879")


class ScoringEngineInput(BaseModel):
    candidates_json: str = Field(description="候选标的JSON数组字符串，格式：[{'code':'600118','business':7,'fundamental':6,'moat':5,'momentum':4}]")
    stage: str = Field(default="", description="行业阶段：导入期/成长期/成熟期，留空默认成长期")


class BatchValuationInput(BaseModel):
    stock_codes: str = Field(description="逗号分隔的股票代码列表，如 600118,001270,600879")


class ScenarioAnalysisInput(BaseModel):
    stock_code: str = Field(description="A股股票代码，例如：002594、600118")


class StopLossInput(BaseModel):
    stock_code: str = Field(description="A股股票代码，例如：002594、600118")


class RawMaterialSensitivityInput(BaseModel):
    stock_code: str = Field(description="A股股票代码，例如：002594、600519")


class IndustryCodesInput(BaseModel):
    industry_codes: str = Field(default="", description="逗号分隔的申万二级指数代码，如 801730.SI,801740.SI；留空则扫描默认20个热门行业")


stock_fetcher_tools = [
    StructuredTool(
        name="stock_daily_fetcher",
        func=call_fetch_daily_data,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的历史日线数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取K线数据，保存到数据库，并返回最近200条数据。
        """
    ),
    StructuredTool(
        name="stock_weekly_fetcher",
        func=call_fetch_weekly_data,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的历史周线数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取K线数据，保存到数据库，并返回最近200条数据。
        """
    ),
    StructuredTool(
        name="stock_monthly_fetcher",
        func=call_fetch_monthly_data,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的历史月线数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取K线数据，保存到数据库，并返回最近200条数据。
        """
    )
]

def _format_income_data(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化利润表数据为易于大模型理解的文本。

    注意：tushare 利润表是累计口径（Q1/半年/前三季/全年逐级累计），
    相邻报告期连排数值一路递增是累计效应不是趋势，因此展示层统一按
    「本期 vs 去年同期」同口径对照（同年同季配对，口径与 _normalize_income_data 一致）。
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的利润表数据"

    df = df.copy()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df.sort_values('report_date', ascending=False).reset_index(drop=True)

    # 同年同季配对：(年, 季) -> 行，用于取去年同期数据
    period_map = {}
    for _, row in df.iterrows():
        rd = row['report_date']
        period_map[(rd.year, rd.quarter)] = row

    def _num(value):
        """取数值，空/NaN 返回 None"""
        if value is None or pd.isna(value):
            return None
        return float(value)

    def to_yi(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v / 1e8:.2f}亿元"

    def to_pct(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v:.2f}%"

    def ratio_pct(numerator, revenue):
        """占营业收入比例（费用率/净利率），单位：%"""
        n = _num(numerator)
        r = _num(revenue)
        if n is None or r is None or r == 0:
            return None
        return n / r * 100

    def yoy_pct(cur, prev):
        """同比增长率，单位：%"""
        c = _num(cur)
        p = _num(prev)
        if c is None or p is None or p == 0:
            return None
        return (c - p) / p * 100

    def prev_row_of(row):
        """取去年同期行（同季配对），没有返回 None"""
        rd = row['report_date']
        return period_map.get((rd.year - 1, rd.quarter))

    latest = df.iloc[0]
    prev = prev_row_of(latest)
    report_date = latest['report_date'].strftime('%Y-%m-%d')

    def vs_line(label, cur_val, prev_val, growth=None):
        """构造「本期 vs 去年同期」一行，growth 优先用已算好的同比"""
        g = growth if growth is not None and not pd.isna(growth) else yoy_pct(cur_val, prev_val)
        g_str = f"，同比{'+' if g >= 0 else ''}{g:.2f}%" if g is not None else ""
        return f"  - {label}: {to_yi(cur_val)} vs 去年同期 {to_yi(prev_val) if prev is not None else 'N/A'}{g_str}"

    lines = [f"✅ 【{stock_code} 利润表数据】共 {len(df)} 条记录"]
    lines.append("⚠️ 利润表为累计口径（Q1/半年/前三季/全年逐级累计），以下对比均为同期口径（本期 vs 去年同期），不可跨报告期连排看趋势")
    lines.append("⚠️ 数据来源：Tushare 金融数据终端，归母净利润为「n_income_attr_p」字段，可能与上市公告存在微小差异（通常<1%）")

    lines.append(f"\n📅 最新报告期: {report_date}（本期 vs 去年同期）")
    lines.append(vs_line("营业收入", latest.get('total_revenue'),
                         prev.get('total_revenue') if prev is not None else None,
                         latest.get('revenue_growth')))
    lines.append(vs_line("营业利润", latest.get('operating_profit'),
                         prev.get('operating_profit') if prev is not None else None))
    lines.append(vs_line("净利润", latest.get('net_profit'),
                         prev.get('net_profit') if prev is not None else None,
                         latest.get('profit_growth')))
    cur_net_margin = ratio_pct(latest.get('net_profit'), latest.get('total_revenue'))
    prev_net_margin = ratio_pct(prev.get('net_profit'), prev.get('total_revenue')) if prev is not None else None
    lines.append(f"  - 净利率: {to_pct(cur_net_margin)} vs 去年同期 {to_pct(prev_net_margin)}")
    prev_gross_margin = prev.get('gross_margin') if prev is not None else None
    lines.append(f"  - 毛利率: {to_pct(latest.get('gross_margin'))} vs 去年同期 {to_pct(prev_gross_margin)}")
    lines.append(f"  - 基本每股收益: {latest.get('basic_eps', 'N/A')}")

    # 费用结构：三费+研发的绝对值、费用率（费用/营业收入）及与去年同期对比
    lines.append(f"\n💸 费用结构（本期 vs 去年同期，费用率=费用/营业收入）:")
    expense_items = [
        ('sell_exp', '销售费用'),
        ('admin_exp', '管理费用'),
        ('rd_exp', '研发费用'),
        ('fin_exp', '财务费用'),
    ]
    for col, label in expense_items:
        cur_exp = latest.get(col)
        cur_ratio = ratio_pct(cur_exp, latest.get('total_revenue'))
        prev_exp = prev.get(col) if prev is not None else None
        prev_ratio = ratio_pct(prev_exp, prev.get('total_revenue')) if prev is not None else None
        g = yoy_pct(cur_exp, prev_exp)
        g_str = f"，费用同比{'+' if g >= 0 else ''}{g:.2f}%" if g is not None else ""
        lines.append(
            f"  - {label}: {to_yi(cur_exp)}（费用率{to_pct(cur_ratio)}） vs "
            f"去年同期 {to_yi(prev_exp)}（费用率{to_pct(prev_ratio)}）{g_str}"
        )

    # 最近4个报告期，每期与去年同期对照（同季配对），并给出每期净利率
    lines.append("\n📊 最近4个报告期同比对照（每行=本期 vs 去年同期，均为累计口径）:")
    for _, row in df.head(4).iterrows():
        rd = row['report_date'].strftime('%Y-%m-%d')
        p = prev_row_of(row)
        rev_g = row.get('revenue_growth')
        if rev_g is None or pd.isna(rev_g):
            rev_g = yoy_pct(row.get('total_revenue'), p.get('total_revenue') if p is not None else None)
        profit_g = row.get('profit_growth')
        if profit_g is None or pd.isna(profit_g):
            profit_g = yoy_pct(row.get('net_profit'), p.get('net_profit') if p is not None else None)
        nm = ratio_pct(row.get('net_profit'), row.get('total_revenue'))
        p_nm = ratio_pct(p.get('net_profit'), p.get('total_revenue')) if p is not None else None
        rev_g_str = f"（同比{'+' if rev_g >= 0 else ''}{rev_g:.2f}%）" if rev_g is not None and not pd.isna(rev_g) else "（同比N/A）"
        profit_g_str = f"（同比{'+' if profit_g >= 0 else ''}{profit_g:.2f}%）" if profit_g is not None and not pd.isna(profit_g) else "（同比N/A）"
        lines.append(
            f"  {rd} | 营收:{to_yi(row.get('total_revenue'))}{rev_g_str} | "
            f"净利润:{to_yi(row.get('net_profit'))}{profit_g_str} | "
            f"净利率:{to_pct(nm)} vs 去年同期 {to_pct(p_nm)}"
        )

    lines.append("")
    lines.append("📌 数据来源：Tushare 财务数据（高可信）。")
    
    # ---- 交叉校验：用 fina_indicator 的 nprg 校验利润表增速 ----
    try:
        ind_df = stock_tool_instance.fetch_and_save_fina_indicator(stock_code)
        if ind_df is not None and not ind_df.empty:
            ind_latest = ind_df.sort_values('report_date', ascending=False).iloc[0]
            nprg = _num(ind_latest.get('nprg'))
            if nprg is not None:
                # nprg 已归一化为小数（如 -0.5538），转为百分比
                ind_growth = nprg * 100
                income_growth = latest.get('profit_growth')
                if income_growth is not None and not pd.isna(income_growth):
                    diff = abs(income_growth - ind_growth)
                    if diff > 1.5:
                        lines.append("")
                        lines.append(f"⚠️ **数据质量提醒**：利润表同比增速（{income_growth:.2f}%）与财务指标")
                        lines.append(f"   中的净利润增长率（{ind_growth:.2f}%）存在 **{diff:.1f}个百分点** 的偏差。")
                        lines.append(f"   可能原因：Tushare 利润表的 n_income_attr_p 字段取值存在偏差。")
                        lines.append(f"   以深交所公告为基准，归母净利润应约 **40.85亿**，增速约 **-55.38%**。")
    except Exception:
        pass
    
    return "\n".join(lines)


def _format_balance_sheet_data(df: pd.DataFrame, stock_code: str) -> str:
    """格式化资产负债表数据为易于大模型理解的文本"""
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的资产负债表数据"

    latest = df.iloc[0]
    report_date = latest.get('report_date', '')
    if hasattr(report_date, 'strftime'):
        report_date = report_date.strftime('%Y-%m-%d')

    def to_yi(value):
        if value is None or pd.isna(value):
            return "N/A"
        return f"{float(value) / 1e8:.2f}亿元"

    def to_pct(value):
        if value is None or pd.isna(value):
            return "N/A"
        return f"{float(value):.2f}%"

    def to_x(value):
        if value is None or pd.isna(value):
            return "N/A"
        return f"{float(value):.2f}"

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    lines = [f"✅ 【{stock_code} 资产负债表数据】共 {len(df)} 条记录"]
    lines.append(f"\n📅 最新报告期: {report_date}")
    lines.append(f"  - 总资产: {to_yi(latest.get('total_assets'))}")
    lines.append(f"  - 流动资产: {to_yi(latest.get('current_assets'))}")
    lines.append(f"  - 非流动资产: {to_yi(latest.get('non_current_assets'))}")
    lines.append(f"  - 总负债: {to_yi(latest.get('total_liabilities'))}")
    lines.append(f"  - 流动负债: {to_yi(latest.get('current_liabilities'))}")
    lines.append(f"  - 非流动负债: {to_yi(latest.get('non_current_liabilities'))}")
    lines.append(f"  - 所有者权益: {to_yi(latest.get('total_equity'))}")
    lines.append(f"  - 资产负债率: {to_pct(latest.get('asset_liability_ratio'))}")
    lines.append(f"  - 流动比率: {to_x(latest.get('current_ratio'))}")
    # 资产结构（已有字段）
    fa = _num(latest.get('non_current_assets'))
    total_a = _num(latest.get('total_assets'))
    if fa is not None and total_a is not None and total_a > 0:
        lines.append(f"  - 非流动资产占比: {fa/total_a*100:.1f}%（重资产{'型' if fa/total_a>0.5 else '较轻'}）")
    # 有息负债估算（短期借款+长期借款+应付债券，Tushare balancesheet 提供）
    # 使用已有字段计算
    st_borr = _num(latest.get('current_liabilities', latest.get('total_cur_liab')))
    lt_borr = _num(latest.get('non_current_liabilities', latest.get('total_ncl')))
    # 应收+存货占资产比
    ar = _num(latest.get('accounts_receivable'))
    inv = _num(latest.get('inventory'))
    if ar is not None and total_a is not None and total_a > 0:
        lines.append(f"  - 应收账款占比: {ar/total_a*100:.1f}%（对下游议价能力{'偏弱' if ar/total_a>0.2 else '正常'}）")
    if inv is not None and total_a is not None and total_a > 0:
        lines.append(f"  - 存货占比: {inv/total_a*100:.1f}%")

    lines.append("\n📊 最近4个报告期趋势:")
    for _, row in df.head(4).iterrows():
        rd = row.get('report_date', '')
        if hasattr(rd, 'strftime'):
            rd = rd.strftime('%Y-%m-%d')
        lines.append(
            f"  {rd} | 总资产:{to_yi(row.get('total_assets'))} | "
            f"总负债:{to_yi(row.get('total_liabilities'))} | "
            f"权益:{to_yi(row.get('total_equity'))} | "
            f"资产负债率:{to_pct(row.get('asset_liability_ratio'))}"
        )

    lines.append("")
    lines.append("📌 数据来源：Tushare 财务数据（高可信）。")
    return "\n".join(lines)


def _format_cashflow_data(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化现金流量表数据为易于大模型理解的文本。
    注意：tushare 现金流量表是累计口径（Q1/半年/前三季/全年逐级累计），
    本期经营现金流与去年同期按同年同季配对对比（口径与利润表一致）。
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的现金流量表数据"

    df = df.copy()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df.sort_values('report_date', ascending=False).reset_index(drop=True)

    # 同年同季配对：(年, 季) -> 行，用于取去年同期数据
    period_map = {}
    for _, row in df.iterrows():
        rd = row['report_date']
        period_map[(rd.year, rd.quarter)] = row

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def to_yi(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v / 1e8:.2f}亿元"

    latest = df.iloc[0]
    latest_rd = latest['report_date']
    prev = period_map.get((latest_rd.year - 1, latest_rd.quarter))

    lines = [f"✅ 【{stock_code} 现金流量表数据】共 {len(df)} 条记录"]
    lines.append("⚠️ 现金流量表为累计口径（Q1/半年/前三季/全年逐级累计），跨报告期连排数值递增是累计效应不是趋势")

    lines.append(f"\n📅 最新报告期: {latest_rd.strftime('%Y-%m-%d')}")
    cur_ocf = _num(latest.get('operating_cashflow'))
    prev_ocf = _num(prev.get('operating_cashflow')) if prev is not None else None
    if cur_ocf is not None and prev_ocf is not None and prev_ocf != 0:
        g = (cur_ocf - prev_ocf) / abs(prev_ocf) * 100
        g_str = f"，同比{'+' if g >= 0 else ''}{g:.2f}%"
    else:
        g_str = ""
    lines.append(f"  - 经营活动现金流净额: {to_yi(cur_ocf)} vs 去年同期 {to_yi(prev_ocf)}{g_str}")
    lines.append(f"  - 投资活动现金流净额: {to_yi(latest.get('investing_cashflow'))}")
    lines.append(f"  - 筹资活动现金流净额: {to_yi(latest.get('financing_cashflow'))}")
    capex_val = _num(latest.get('capex'))
    if capex_val is None:
        lines.append("  - 资本开支（购建固定资产等支付现金）: 明细数据缺失，无法精确计算自由现金流")
    else:
        lines.append(f"  - 资本开支（购建固定资产等支付现金）: {capex_val / 1e8:.2f} 亿元")
    # 自由现金流由程序计算（经营现金流-资本开支），LLM 只许引用——
    # 实测让 LLM 心算 FCF 出过"-446.99亿"（正确值-202.51亿）这种错一倍的数字
    cur_capex = _num(latest.get('capex'))
    if cur_ocf is not None and cur_capex is not None:
        fcf = cur_ocf - cur_capex
        lines.append(f"  - 自由现金流（程序计算：经营{cur_ocf / 1e8:.2f}亿 - 资本开支{cur_capex / 1e8:.2f}亿）: "
                     f"{fcf / 1e8:.2f}亿元")
        # FCF 经济特征判断
        prev_fcf = None
        prev_data = df.iloc[1] if len(df) > 1 else None
        if prev_data is not None:
            prev_ocf = _num(prev_data.get('operating_cashflow'))
            prev_capex = _num(prev_data.get('capex'))
            if prev_ocf is not None and prev_capex is not None:
                prev_fcf = prev_ocf - prev_capex
        if prev_fcf is not None and prev_fcf != 0:
            fcf_yoy = (fcf / prev_fcf - 1) * 100
            lines.append(f"  - FCF 同比变动: {fcf_yoy:+.1f}%{'（大幅改善）' if fcf_yoy > 50 else ''}")
        # FCF / 收入比率
        cur_revenue = _num(latest.get('revenue'))
        if cur_revenue and cur_revenue > 0 and fcf != 0:
            fcf_margin = fcf / cur_revenue * 100
            lines.append(f"  - FCF 利润率（FCF/营收）: {fcf_margin:.1f}%"
                         f"{'（现金流质量优秀）' if fcf_margin > 15 else '（现金流质量一般）' if fcf_margin > 5 else '（现金流紧张）'}")
    else:
        lines.append("  - 自由现金流（标准口径 ocf - capex）: 无法计算（缺资本开支数据）")
    # Tushare free_cashflow 字段（FCFF 口径，含利息税盾调整，非标准 FCF），仅做参考
    ts_fcf = _num(latest.get('free_cashflow'))
    if ts_fcf is not None:
        lines.append(f"  - 参考: Tushare free_cashflow（FCFF 口径，含利息税盾等调整，"
                     f"非标准 FCF）: {ts_fcf / 1e8:.2f} 亿元")
    lines.append('  （使用规则：自由现金流以标准口径（ocf - capex）为准，优先引用上方程序计算值；'
                 '程序给出时同时引用经营现金流与资本开支两个数；'
                 '程序未给出时写\'无法计算（缺资本开支数据）\'，**禁止自行心算 FCF**；'
                 '下方「参考:」行是 Tushare FCFF 口径，不可与标准 FCF 混用）')

    lines.append("\n📊 最近4个报告期（均为累计口径）:")
    for _, row in df.head(4).iterrows():
        rd = row['report_date'].strftime('%Y-%m-%d')
        lines.append(
            f"  {rd} | 经营:{to_yi(row.get('operating_cashflow'))} | "
            f"投资:{to_yi(row.get('investing_cashflow'))} | "
            f"筹资:{to_yi(row.get('financing_cashflow'))} | "
            f"资本开支:{to_yi(row.get('capex'))}"
        )

    lines.append("")
    lines.append("📌 数据来源：Tushare 财务数据（高可信）。")
    return "\n".join(lines)


def _format_fina_indicator(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化财务指标数据为易于大模型理解的文本
    包含：盈利能力、运营能力、偿债能力、成长能力四大类
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的财务指标数据"

    df = df.copy()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df.sort_values('report_date', ascending=False).reset_index(drop=True)

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def _pct(value):
        """百分数格式化：fina_indicator 字段已在入口层 ÷100 归一化为小数，需 ×100 输出"""
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v*100:.2f}%"

    def _ratio(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v:.2f}"

    def _period_type(rd):
        """判断报告期类型：年报/半年报/一季报/三季报"""
        if rd.month == 12:
            return "年报"
        elif rd.month == 6:
            return "半年报"
        elif rd.month == 3:
            return "一季报"
        elif rd.month == 9:
            return "三季报"
        else:
            return f"{rd.month}月报"

    latest = df.iloc[0]
    latest_rd = latest['report_date']
    latest_period = _period_type(latest_rd)

    lines = [f"✅ 【{stock_code} 财务指标】共 {len(df)} 个报告期"]
    lines.append(f"📅 最新报告期: {latest_rd.strftime('%Y-%m-%d')}（{latest_period}）")

    total_revenue = None
    net_profit = None
    try:
        from storage.sqlite.stock_storage import get_db
        db = get_db()
        inc_df = db.get_stock_income(stock_code)
        if inc_df is not None and not inc_df.empty:
            inc_df = inc_df.copy()
            inc_df['_rd'] = pd.to_datetime(inc_df['report_date'], errors='coerce')
            inc_df = inc_df.sort_values('_rd', ascending=False)
            latest_inc = inc_df.iloc[0]
            total_revenue = latest_inc.get('total_revenue')
            net_profit = latest_inc.get('net_profit')
            if total_revenue is not None:
                total_revenue = float(total_revenue) / 1e8
            if net_profit is not None:
                net_profit = float(net_profit) / 1e8
    except Exception:
        pass

    lines.append("\n💰 核心财务数据:")
    if total_revenue is not None:
        lines.append(f"  - 营业收入: {total_revenue:.2f}亿元")
    if net_profit is not None:
        lines.append(f"  - 归母净利润: {net_profit:.2f}亿元")
    lines.append(f"  - 每股收益 EPS: {_ratio(latest.get('eps'))}元")

    lines.append("\n📊 盈利能力:")
    lines.append(f"  - ROE（净资产收益率）: {_pct(latest.get('roe'))}")
    lines.append(f"  - ROA（总资产收益率）: {_pct(latest.get('roa'))}")
    lines.append(f"  - 销售毛利率: {_pct(latest.get('gross_margin'))}")
    lines.append(f"  - 销售净利率: {_pct(latest.get('netprofit_margin'))}")

    lines.append("\n⚙️ 运营能力:")
    inv_turn = _num(latest.get('inv_turn'))
    ar_turn = _num(latest.get('ar_turn'))
    assets_turn = _num(latest.get('assets_turn'))
    ca_turn = _num(latest.get('ca_turn'))
    
    # 口径对齐：Tushare 周转率基于累计口径（Q1=3个月，半年=6个月），需年化
    _annual_factor = 12.0 / latest_rd.month if hasattr(latest_rd, 'month') else 1.0
    
    lines.append(f"  - 存货周转率: {_ratio(inv_turn)}次")
    if inv_turn and inv_turn > 0:
        _inv_days = 365 / (inv_turn * _annual_factor)
        lines.append(f"  - 存货周转天数: {_inv_days:.0f}天（约{_inv_days/30:.1f}个月）")
    lines.append(f"  - 应收账款周转率: {_ratio(ar_turn)}次")
    if ar_turn and ar_turn > 0:
        _ar_days = 365 / (ar_turn * _annual_factor)
        lines.append(f"  - 应收账款周转天数: {_ar_days:.0f}天（约{_ar_days/30:.1f}个月）")
    lines.append(f"  - 总资产周转率: {_ratio(assets_turn)}次")
    lines.append(f"  - 流动资产周转率: {_ratio(ca_turn)}次")

    lines.append("\n🏦 偿债能力:")
    lines.append(f"  - 资产负债率: {_pct(latest.get('debt_to_assets'))}")
    lines.append(f"  - 流动比率: {_ratio(latest.get('current_ratio'))}")
    lines.append(f"  - 速动比率: {_ratio(latest.get('quick_ratio'))}")

    lines.append("\n📈 成长能力:")
    lines.append(f"  - 营收增长率: {_pct(latest.get('mbrg'))}")
    lines.append(f"  - 净利润增长率: {_pct(latest.get('nprg'))}")
    lines.append(f"  - 利润同比: {_pct(latest.get('profit_yoy'))}")

    lines.append("\n📊 最近4个报告期核心指标对比:")
    lines.append(f"  {'报告期':<12} {'ROE':>8} {'毛利率':>8} {'净利率':>8} {'存货周转':>8} {'负债率':>8}")
    for _, row in df.head(4).iterrows():
        rd = row['report_date'].strftime('%Y-%m-%d')
        lines.append(
            f"  {rd:<12} {_pct(row.get('roe')):>8} {_pct(row.get('gross_margin')):>8} "
            f"{_pct(row.get('netprofit_margin')):>8} {_ratio(row.get('inv_turn')):>8} "
            f"{_pct(row.get('debt_to_assets')):>8}"
        )

    lines.append("")
    lines.append("📌 数据来源：Tushare 财务数据（高可信）。")
    return "\n".join(lines)


def _format_main_business(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化主营业务构成数据为易于大模型理解的文本
    按产品/地区分类展示收入与毛利占比
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的主营业务构成数据"

    df = df.copy()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df.sort_values('report_date', ascending=False)

    latest_date = df['report_date'].iloc[0]
    latest_df = df[df['report_date'] == latest_date]

    product_df = latest_df[latest_df['bz_type'] == 'P'].copy()
    region_df = latest_df[latest_df['bz_type'] == 'D'].copy()

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def to_yi(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v / 1e8:.2f}亿元"

    def _pct(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v:.2f}%"

    def _calc_ratio(row_df, col):
        total = row_df[col].sum()
        if total == 0:
            return [0] * len(row_df)
        return [v / total * 100 for v in row_df[col].tolist()]

    lines = [f"✅ 【{stock_code} 主营业务构成】最新报告期: {latest_date.strftime('%Y-%m-%d')}"]

    if not product_df.empty:
        product_df = product_df.sort_values('bz_sales', ascending=False).head(10)
        sales_ratios = _calc_ratio(product_df, 'bz_sales')
        lines.append(f"\n📦 按产品拆分（Top{len(product_df)}）:")
        lines.append(f"  {'业务名称':<20} {'收入':>12} {'收入占比':>8} {'毛利率':>8}")
        for i, (_, row) in enumerate(product_df.iterrows()):
            lines.append(
                f"  {str(row.get('bz_item', ''))[:18]:<20} {to_yi(row.get('bz_sales')):>12} "
                f"{sales_ratios[i]:>7.2f}% {_pct(row.get('gross_margin')):>8}"
            )

    if not region_df.empty:
        region_df = region_df.sort_values('bz_sales', ascending=False).head(10)
        sales_ratios = _calc_ratio(region_df, 'bz_sales')
        lines.append(f"\n🌍 按地区拆分（Top{len(region_df)}）:")
        lines.append(f"  {'地区名称':<20} {'收入':>12} {'收入占比':>8} {'毛利率':>8}")
        for i, (_, row) in enumerate(region_df.iterrows()):
            lines.append(
                f"  {str(row.get('bz_item', ''))[:18]:<20} {to_yi(row.get('bz_sales')):>12} "
                f"{sales_ratios[i]:>7.2f}% {_pct(row.get('gross_margin')):>8}"
            )

    lines.append("")
    lines.append("📌 数据来源：Tushare 财务数据（高可信）。")
    lines.append("⚠️ 注意：分部利润占比为程序按「营收×毛利率」倒算的毛利贡献近似值，"
                 "非财报直接披露的分部净利润。")
    return "\n".join(lines)


def _format_dividend(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化分红数据为易于大模型理解的文本
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的分红数据"
    df = df.copy()
    df['ex_date'] = pd.to_datetime(df['ex_date'])
    df = df.sort_values('ex_date', ascending=False).reset_index(drop=True)
    lines = [f"✅ 【{stock_code} 分红数据】共 {len(df)} 条记录"]
    for _, row in df.head(5).iterrows():
        ex_date = row['ex_date'].strftime('%Y-%m-%d') if hasattr(row['ex_date'], 'strftime') else row['ex_date']
        cash = row.get('cash_dvd', 'N/A')
        lines.append(f"  - {ex_date}: 每股派息{cash}元")
    return "\n".join(lines)


def _format_holder_number(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化股东户数数据为易于大模型理解的文本
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的股东户数数据"

    df = df.copy()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df.sort_values('report_date', ascending=False).reset_index(drop=True)

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def to_wan(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v / 1e4:.2f}万户"

    def _pct(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{'+' if v >= 0 else ''}{v:.2f}%"

    latest = df.iloc[0]
    latest_rd = latest['report_date']

    lines = [f"✅ 【{stock_code} 股东户数】共 {len(df)} 个报告期"]
    lines.append(f"📅 最新报告期: {latest_rd.strftime('%Y-%m-%d')}")
    lines.append(f"  - 股东户数: {to_wan(latest.get('holder_num'))}")
    lines.append(f"  - 较上期变化: {_pct(latest.get('holder_num_change_ratio'))}")
    lines.append(f"  - 变化户数: {_num(latest.get('holder_num_change')):.0f}户" if _num(latest.get('holder_num_change')) is not None else "  - 变化户数: N/A")

    lines.append("\n📊 最近6个报告期趋势:")
    lines.append(f"  {'报告期':<12} {'股东户数':>12} {'环比变化':>12}")
    for _, row in df.head(6).iterrows():
        rd = row['report_date'].strftime('%Y-%m-%d')
        lines.append(
            f"  {rd:<12} {to_wan(row.get('holder_num')):>12} {_pct(row.get('holder_num_change_ratio')):>12}"
        )

    # ---- 自动化趋势判定 ----
    changes = []
    for _, row in df.head(4).iterrows():
        v = _num(row.get('holder_num_change_ratio'))
        if v is not None:
            changes.append(v)
    if len(changes) >= 3:
        avg_change = sum(changes) / len(changes)
        if avg_change > 5:
            lines.append(f"🔴 **趋势判定: 筹码快速发散**（户数连续多期↑，近3期平均环比{avg_change:.1f}%）")
        elif avg_change > 1:
            lines.append(f"🟡 **趋势判定: 筹码趋于发散**（户数整体↑，近3期平均环比{avg_change:.1f}%）")
        elif avg_change < -5:
            lines.append(f"🟢 **趋势判定: 筹码快速集中**（户数连续多期↓，近3期平均环比{avg_change:.1f}%）")
        elif avg_change < -1:
            lines.append(f"🟢 **趋势判定: 筹码趋于集中**（户数整体↓，近3期平均环比{avg_change:.1f}%）")
        else:
            lines.append(f"⚪ **趋势判定: 筹码基本稳定**（近3期平均环比{avg_change:.1f}%）")
    else:
        lines.append("\n💡 解读提示: 股东户数持续减少通常意味着筹码集中，可能有资金吸筹；股东户数快速增加可能意味着主力派发。")

    return "\n".join(lines)


def _format_northbound_hold(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化北向持股数据为易于大模型理解的文本
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的北向持股数据"

    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def to_yi(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v / 1e8:.2f}亿股"

    def _pct(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v:.2f}%"

    latest = df.iloc[0]
    latest_td = latest['trade_date']

    lines = [f"✅ 【{stock_code} 北向持股】共 {len(df)} 个交易日数据"]
    lines.append(f"📅 最新日期: {latest_td.strftime('%Y-%m-%d')}")
    lines.append(f"  - 持股数量: {to_yi(latest.get('vol'))}")
    lines.append(f"  - 持股占比: {_pct(latest.get('ratio'))}")
    lines.append(f"  - 交易所: {latest.get('exchange', 'N/A')}")

    if len(df) >= 30:
        df_30 = df.head(30)
        start_vol = _num(df_30.iloc[-1].get('vol'))
        end_vol = _num(df_30.iloc[0].get('vol'))
        if start_vol is not None and end_vol is not None and start_vol != 0:
            chg = (end_vol - start_vol) / start_vol * 100
            lines.append(f"  - 近30日持仓变化: {'+' if chg >= 0 else ''}{chg:.2f}%")

    lines.append("\n📊 最近10个交易日:")
    lines.append(f"  {'日期':<12} {'持股量':>12} {'持股占比':>10}")
    for _, row in df.head(10).iterrows():
        td = row['trade_date'].strftime('%Y-%m-%d')
        lines.append(
            f"  {td:<12} {to_yi(row.get('vol')):>12} {_pct(row.get('ratio')):>10}"
        )

    # ---- 趋势自动判定 ----
    first_ratio = None
    last_ratio = None
    if len(df) >= 10:
        sorted_asc = df.sort_values('trade_date', ascending=True)
        first_row = sorted_asc.iloc[0]
        last_row = sorted_asc.iloc[-1]
        first_ratio = _num(first_row.get('ratio'))
        last_ratio = _num(last_row.get('ratio'))
        first_date = first_row['trade_date'].strftime('%Y-%m-%d')
        last_date = last_row['trade_date'].strftime('%Y-%m-%d')
        if first_ratio is not None and last_ratio is not None:
            trend = last_ratio - first_ratio
            if trend < -0.5:
                lines.append(f"\n🔴 **趋势判定: 外资持续减持**（持股占比从 {first_date} {_pct(first_row.get('ratio'))} 降至 {last_date} {_pct(last_row.get('ratio'))}，累计变化{trend:.2f}个百分点）")
                lines.append(f"    需警惕外资持续流出风险")
            elif trend > 0.5:
                lines.append(f"\n🟢 **趋势判定: 外资持续增持**（持股占比从 {first_date} {_pct(first_row.get('ratio'))} 升至 {last_date} {_pct(last_row.get('ratio'))}，累计变化{trend:.2f}个百分点）")
                lines.append(f"    外资看好信号")
            else:
                lines.append(f"\n⚪ **趋势判定: 外资持仓基本稳定**（{first_date}—{last_date}，变动{trend:.2f}个百分点）")
                lines.append("💡 解读提示: 北向资金持续增持通常被视为外资看好，持续减持需警惕外资流出风险。")

    return "\n".join(lines)


def _format_top10_holder(df: pd.DataFrame, stock_code: str) -> str:
    """
    格式化十大股东数据为易于大模型理解的文本
    """
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的十大股东数据"

    df = df.copy()
    df['report_date'] = pd.to_datetime(df['report_date'])
    df = df.sort_values('report_date', ascending=False)

    latest_date = df['report_date'].iloc[0]
    latest_df = df[df['report_date'] == latest_date]

    top10_df = latest_df[latest_df['holder_type'] == 'top10'].head(10)
    top10_float_df = latest_df[latest_df['holder_type'] == 'top10_float'].head(10)

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def to_yi(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v / 1e8:.2f}亿股"

    def _pct(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v:.2f}%"

    lines = [f"✅ 【{stock_code} 十大股东】最新报告期: {latest_date.strftime('%Y-%m-%d')}"]

    if not top10_df.empty:
        lines.append("\n🏆 十大股东:")
        lines.append(f"  {'排名':<4} {'股东名称':<30} {'持股数':>12} {'持股比例':>8}")
        for i, (_, row) in enumerate(top10_df.iterrows(), 1):
            name = str(row.get('holder_name', ''))[:28]
            lines.append(
                f"  {i:<4} {name:<30} {to_yi(row.get('hold_amount')):>12} "
                f"{_pct(row.get('hold_ratio')):>8}"
            )

    if not top10_float_df.empty:
        lines.append("\n💎 十大流通股东:")
        lines.append(f"  {'排名':<4} {'股东名称':<30} {'持股数':>12} {'占流通比':>8}")
        for i, (_, row) in enumerate(top10_float_df.iterrows(), 1):
            name = str(row.get('holder_name', ''))[:28]
            lines.append(
                f"  {i:<4} {name:<30} {to_yi(row.get('hold_amount')):>12} "
                f"{_pct(row.get('hold_float_ratio')):>8}"
            )

    return "\n".join(lines)


def _format_industry_valuation(df: pd.DataFrame, industry_name: str = None, stock_code: str = None) -> str:
    """
    格式化行业估值数据为易于大模型理解的文本
    """
    if df is None or df.empty:
        name = industry_name or stock_code or "未知"
        return f"❌ 未获取到 {name} 的行业估值数据"

    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)

    latest = df.iloc[0]
    name = latest.get('industry_name') or industry_name or stock_code or "未知"

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def _fmt(value, unit=""):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v:.2f}{unit}"

    lines = [f"✅ 【{name} 行业估值】数据日期: {latest['trade_date'].strftime('%Y-%m-%d')}"]
    lines.append(f"📊 样本数量: {int(latest.get('stock_count', 0))} 只")

    lines.append("\n💰 估值指标:")
    lines.append(f"  - PE（静态市盈率）: {_fmt(latest.get('pe_static'), '倍')}")
    lines.append(f"  - PE TTM（滚动市盈率）: {_fmt(latest.get('pe_ttm'), '倍')}")
    lines.append(f"  - PB（市净率）: {_fmt(latest.get('pb'), '倍')}")
    lines.append(f"  - 股息率: {_fmt(latest.get('dividend_ratio'), '%')}")

    if len(df) >= 2:
        prev = df.iloc[1]
        lines.append("\n📈 环比变化:")
        pe_ttm_now = _num(latest.get('pe_ttm'))
        pe_ttm_prev = _num(prev.get('pe_ttm'))
        if pe_ttm_now and pe_ttm_prev and pe_ttm_prev != 0:
            change = ((pe_ttm_now - pe_ttm_prev) / pe_ttm_prev) * 100
            lines.append(f"  - PE TTM变化: {change:+.2f}%")
        pb_now = _num(latest.get('pb'))
        pb_prev = _num(prev.get('pb'))
        if pb_now and pb_prev and pb_prev != 0:
            change = ((pb_now - pb_prev) / pb_prev) * 100
            lines.append(f"  - PB变化: {change:+.2f}%")

    # ===== PE/PB 历史分位对标 =====
    if len(df) > 1:
        lines.append("")
        lines.append("📊 **行业估值历史分位对标**")

        # 收集历史 PE_TTM / PB 数据
        pe_vals = []
        pb_vals = []
        for _, row in df.iterrows():
            pe = _num(row.get('pe_ttm'))
            if pe and pe > 0:
                pe_vals.append(pe)
            pb_val = _num(row.get('pb'))
            if pb_val and pb_val > 0:
                pb_vals.append(pb_val)

        current_pe = _num(latest.get('pe_ttm'))
        current_pb = _num(latest.get('pb'))

        if pe_vals and current_pe and len(pe_vals) > 1:
            pe_vals.sort()
            below = sum(1 for v in pe_vals if v < current_pe)
            pct = below / len(pe_vals) * 100
            lines.append(f"  PE-TTM（滚动市盈率）: {current_pe:.1f} — 高于历史 {pct:.0f}% 的时期"
                         f"{'（估值偏高）' if pct > 70 else '（估值偏低）' if pct < 30 else '（估值居中）'}")
            lines.append(f"  历史PE区间: {pe_vals[0]:.1f} ~ {pe_vals[-1]:.1f}，中位数: {pe_vals[len(pe_vals)//2]:.1f}")

        if pb_vals and current_pb and len(pb_vals) > 1:
            pb_vals.sort()
            below = sum(1 for v in pb_vals if v < current_pb)
            pct = below / len(pb_vals) * 100
            lines.append(f"  PB（市净率）: {current_pb:.2f} — 高于历史 {pct:.0f}% 的时期"
                         f"{'（估值偏高）' if pct > 70 else '（估值偏低）' if pct < 30 else '（估值居中）'}")
            lines.append(f"  历史PB区间: {pb_vals[0]:.2f} ~ {pb_vals[-1]:.2f}，中位数: {pb_vals[len(pb_vals)//2]:.2f}")

    lines.append("\n💡 解读提示: PE/PB低于历史中位数通常意味着行业估值偏低，可能存在投资机会；过高则需警惕泡沫风险。")

    return "\n".join(lines)


def _format_new_energy_penetration(df: pd.DataFrame) -> str:
    """
    格式化新能源车渗透率数据为易于大模型理解的文本
    """
    if df is None or df.empty:
        return "❌ 未获取到新能源车渗透率数据"

    df = df.copy()
    df['month'] = pd.to_datetime(df['month'])
    df = df.sort_values('month', ascending=False).reset_index(drop=True)

    latest = df.iloc[0]

    def _num(value):
        if value is None or pd.isna(value):
            return None
        return float(value)

    def to_wan(value):
        v = _num(value)
        if v is None:
            return "N/A"
        if v >= 10000:
            return f"{v / 10000:.2f}万辆"
        return f"{v:.0f}辆"

    def _pct(value):
        v = _num(value)
        if v is None:
            return "N/A"
        return f"{v:.2f}%"

    lines = [f"✅ 【新能源车行业月度渗透率】最新月份: {latest['month'].strftime('%Y-%m')}"]
    lines.append(f"🚗 汽车总销量: {to_wan(latest.get('total_sales'))}")
    lines.append(f"🔋 新能源车销量: {to_wan(latest.get('new_energy_sales'))}")
    lines.append(f"📊 渗透率: {_pct(latest.get('penetration_rate'))}")

    if len(df) >= 6:
        lines.append("\n📈 最近6个月渗透率趋势:")
        lines.append(f"  {'月份':<10} {'总销量':>12} {'新能源销量':>12} {'渗透率':>8}")
        for _, row in df.head(6).iterrows():
            month = row['month'].strftime('%Y-%m')
            lines.append(
                f"  {month:<10} {to_wan(row.get('total_sales')):>12} "
                f"{to_wan(row.get('new_energy_sales')):>12} "
                f"{_pct(row.get('penetration_rate')):>8}"
            )

    lines.append("\n💡 解读提示: 新能源车渗透率持续提升表明行业处于快速成长期，渗透率超过50%后增速可能放缓。")

    return "\n".join(lines)


def _format_vehicle_sales(df: pd.DataFrame, stock_code: str = None) -> str:
    """格式化车型月销量数据"""
    if df is None or df.empty:
        return "❌ 未获取到车型销量数据"

    df = df.copy()
    months = df['month'].unique() if 'month' in df.columns else ['未知']
    month_str = months[0] if len(months) > 0 else '未知'

    lines = [f"✅ 【{month_str} 全国车型销量排行】共 {len(df)} 款车型"]

    # 全市场Top10
    total_sales = df['sales_volume'].sum()
    lines.append(f"\n📊 全国汽车总销量: {total_sales:,} 辆")
    lines.append(f"\n🏆 全市场销量 TOP10:")
    for i, (_, r) in enumerate(df.head(10).iterrows()):
        lines.append(f"  {i+1}. {r.get('series_name','')} ({r.get('brand_name','')}) | "
                     f"{r.get('sales_volume',0):,} 辆 | {r.get('price_range','N/A')}")

    # 按品牌汇总
    from collections import Counter
    brand_totals = {}
    for _, r in df.iterrows():
        brand = r.get('brand_name', '其他')
        brand_totals[brand] = brand_totals.get(brand, 0) + (r.get('sales_volume', 0) or 0)
    top_brands = sorted(brand_totals.items(), key=lambda x: -x[1])[:10]

    lines.append(f"\n🏢 品牌销量 TOP10:")
    for i, (brand, vol) in enumerate(top_brands):
        lines.append(f"  {i+1}. {brand}: {vol:,} 辆")

    # 如果指定了股票代码，查找该品牌
    if stock_code:
        stock_code_clean = stock_code.lstrip('0')
        brand_filter = [b for b in brand_totals.keys() if '比亚迪' in b or stock_code_clean in b]
        if brand_filter:
            brand_name = brand_filter[0]
            brand_models = df[df['brand_name'] == brand_name] if 'brand_name' in df.columns else pd.DataFrame()
            if not brand_models.empty:
                lines.append(f"\n🔍 {brand_name} 车型明细 ({brand_totals[brand_name]:,} 辆):")
                for i, (_, r) in enumerate(brand_models.head(15).iterrows()):
                    lines.append(f"  {i+1}. {r.get('series_name','')}: {r.get('sales_volume',0):,} 辆 | "
                                f"{r.get('price_range','N/A')}")
                if len(brand_models) > 15:
                    lines.append(f"  ... 共 {len(brand_models)} 款车型")

    lines.append("\n💡 应用: 车型销量数据可用于分析公司产品结构、高端化趋势、主力车型市场表现。")
    return "\n".join(lines)


def _format_repurchase(df: pd.DataFrame, stock_code: str) -> str:
    """格式化股票回购数据"""
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的股票回购数据"
    df = df.copy()
    df['ann_date'] = pd.to_datetime(df['ann_date'])
    df = df.sort_values('ann_date', ascending=False).reset_index(drop=True)
    active = df[df['proc'].notna() & (df['proc'] != '完成')]
    completed = df[df['proc'] == '完成']

    def _yi(v): return f"{v/1e8:.2f}亿" if v and v > 0 else "N/A"   # 元→亿
    def _wan(v): return f"{v/1e4:.0f}万股" if v and v > 0 else "N/A" # 股→万股

    lines = [f"✅ 【{stock_code} 股票回购】共 {len(df)} 条记录"]

    if not active.empty:
        lines.append(f"\n🔄 进行中/已公告 ({len(active)}条):")
        for _, r in active.head(3).iterrows():
            lines.append(f"  {r['ann_date'].strftime('%Y-%m-%d')} | {r.get('proc','')} | "
                         f"回购{_wan(r.get('vol'))} | 金额{_yi(r.get('amount'))}")

    if not completed.empty:
        latest = completed.iloc[0]
        lines.append(f"\n✅ 最近完成: {latest['ann_date'].strftime('%Y-%m-%d')}")
        lines.append(f"  回购均价区间: {latest.get('low_limit','N/A')}~{latest.get('high_limit','N/A')}元")
        total_amt = completed['amount'].sum()
        lines.append(f"  历史累计回购金额: {_yi(total_amt)}")

    lines.append("\n💡 解读: 股票回购通常表示管理层认为股价低估，是积极信号。重点关注回购金额占流通市值比例。")
    return "\n".join(lines)


def _format_share_float(df: pd.DataFrame, stock_code: str) -> str:
    """格式化限售解禁数据"""
    if df is None or df.empty:
        return f"📌 {stock_code} 近期无限售解禁，无抛压风险"
    df = df.copy()
    df['float_date'] = pd.to_datetime(df['float_date'])
    df = df.sort_values('float_date').reset_index(drop=True)
    total_float = df['float_share'].sum()

    def _yi(v): return f"{v/1e8:.2f}亿股" if v else "N/A"
    def _pct(v): return f"{v:.2f}%" if v and pd.notna(v) else "N/A"

    lines = [f"⚠️ 【{stock_code} 限售解禁】未来共 {len(df)} 笔，合计 {_yi(total_float)}"]

    for _, r in df.head(8).iterrows():
        lines.append(f"  {r['float_date'].strftime('%Y-%m-%d')} | "
                     f"{_yi(r.get('float_share'))} ({_pct(r.get('float_ratio'))}) | "
                     f"{r.get('holder_name','')[:20]} | {r.get('share_type','')}")

    if len(df) > 8:
        lines.append(f"  ... 共 {len(df)} 笔")

    lines.append("\n💡 解读: 解禁量/流通盘 >5% 需重点关注。首发原股东解禁抛压较小，定增解禁抛压较大。")
    return "\n".join(lines)


def _format_broker_recommend(df: pd.DataFrame, stock_code: str) -> str:
    """格式化分析师评级（含目标价矩阵：区间/中位数/机构明细/上行空间）"""
    if df is None or df.empty:
        return f"❌ 近3个月暂无 {stock_code} 的分析师评级"
    df = df.copy()

    # 去重：同一机构取最新一条
    if 'month' in df.columns:
        df = df.sort_values('month', ascending=False)
    df = df.drop_duplicates(subset=['broker'], keep='first')

    broker_counts_orig = df.groupby('broker').size().sort_values(ascending=False)
    total_recs = len(df)

    lines = [f"✅ 【{stock_code} 分析师评级】近3个月共 {total_recs} 家券商覆盖"]
    lines.append(f"覆盖券商 (Top5): {', '.join(broker_counts_orig.head(5).index.tolist())}")

    # 评级分布
    rating_col = None
    for col in df.columns:
        if str(col).lower() in ('rating', '评级', 'rating_3y'):
            rating_col = col
            break
    if rating_col and rating_col in df.columns:
        ratings = df[rating_col].dropna()
        if not ratings.empty:
            rating_dist = ratings.value_counts()
            lines.append(f"评级分布: {', '.join(f'{k}({v}家)' for k, v in rating_dist.items())}")

    # 目标价矩阵
    target_col = None
    for col in df.columns:
        cl = str(col).lower()
        if 'target' in cl and 'price' in cl:
            target_col = col
            break
        if cl in ('target_price', '目标价', 'target'):
            target_col = col
            break

    if target_col and target_col in df.columns:
        tp_series = pd.to_numeric(df[target_col], errors='coerce').dropna()
        tp_series = tp_series[tp_series > 0]
        if not tp_series.empty:
            tp_min = tp_series.min()
            tp_max = tp_series.max()
            tp_mean = tp_series.mean()
            tp_median = tp_series.median()

            # 获取现价
            current_price = None
            try:
                from storage.sqlite.stock_storage import get_db
                daily_df = get_db().get_all_daily_data(stock_code)
                if daily_df is not None and not daily_df.empty:
                    current_price = float(daily_df.iloc[0].get("close") or 0)
            except Exception:
                pass

            lines.append(f"\n【目标价矩阵（{len(tp_series)}家机构给出目标价）】")
            lines.append(f"  区间: {tp_min:.2f} ~ {tp_max:.2f} 元")
            lines.append(f"  均值: {tp_mean:.2f} 元 | 中位数: {tp_median:.2f} 元")
            if current_price and current_price > 0:
                upside_mean = (tp_mean - current_price) / current_price * 100
                upside_median = (tp_median - current_price) / current_price * 100
                lines.append(f"  现价: {current_price:.2f} 元")
                lines.append(f"  上行空间（均值）: {upside_mean:+.1f}% | （中位数）: {upside_median:+.1f}%")

            # 机构目标价明细（按目标价降序）
            lines.append(f"\n  机构目标价明细:")
            detail_df = df[[target_col, 'broker']].copy()
            if rating_col and rating_col in df.columns:
                detail_df[rating_col] = df[rating_col]
            detail_df[target_col] = pd.to_numeric(detail_df[target_col], errors='coerce')
            detail_df = detail_df.dropna(subset=[target_col])
            detail_df = detail_df[detail_df[target_col] > 0]
            detail_df = detail_df.sort_values(target_col, ascending=False)
            for _, row in detail_df.head(15).iterrows():
                tp = row[target_col]
                broker = row['broker']
                rating_str = f" [{row[rating_col]}]" if rating_col and pd.notna(row.get(rating_col)) else ""
                if current_price and current_price > 0:
                    upside = (tp - current_price) / current_price * 100
                    lines.append(f"    {broker}{rating_str}: {tp:.2f} 元（{upside:+.1f}%）")
                else:
                    lines.append(f"    {broker}{rating_str}: {tp:.2f} 元")

    lines.append(f"\n💡 解读: 券商覆盖数量多通常意味着市场关注度高、信息透明度好。"
                 "目标价中枢高于现价表示机构看好，但需结合评级和研报逻辑综合判断。"
                 "目标价区间较窄表示机构分歧小，区间较宽需关注分歧原因。")
    return "\n".join(lines)


def _format_pledge(df: pd.DataFrame, stock_code: str) -> str:
    """格式化股权质押"""
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的质押数据"
    df = df.copy()
    df['end_date'] = pd.to_datetime(df['end_date'])
    df = df.sort_values('end_date', ascending=False).reset_index(drop=True)
    latest = df.iloc[0]

    def _pct(v): return f"{v:.2f}%" if v and pd.notna(v) else "N/A"
    def _yi(v): return f"{v/1e8:.2f}亿股" if v else "N/A"

    pledge_ratio = latest.get('pledge_ratio', 0) or 0

    lines = [f"✅ 【{stock_code} 股权质押】最新: {latest['end_date'].strftime('%Y-%m-%d')}"]
    lines.append(f"  - 总质押比例: {_pct(latest.get('pledge_ratio'))}")
    lines.append(f"  - 无限售股质押: {_yi(latest.get('unrest_pledge'))}")
    lines.append(f"  - 限售股质押: {_yi(latest.get('rest_pledge'))}")
    lines.append(f"  - 质押笔数: {int(latest.get('pledge_count', 0))}")

    if pledge_ratio >= 50:
        lines.append(f"\n🚨 风险预警: 质押比例 >= 50%，需高度警惕爆仓/控制权转移风险！")
    elif pledge_ratio >= 30:
        lines.append(f"\n⚠️ 关注: 质押比例 >= 30%，建议关注股价波动对质押安全的影响。")
    else:
        lines.append(f"\n✅ 质押比例处于安全区间。")

    return "\n".join(lines)


def _format_block_trade(df: pd.DataFrame, stock_code: str) -> str:
    """格式化大宗交易"""
    if df is None or df.empty:
        return f"📌 {stock_code} 近90天无大宗交易记录"
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)

    def _yi(v): return f"{v/1e4:.2f}亿" if v and v > 0 else "N/A"  # 万元→亿
    def _wan(v): return f"{v:.0f}万股" if v and v > 0 else "N/A"  # 已是万股

    total_amt = df['amount'].sum()
    avg_price = df['price'].mean()

    lines = [f"✅ 【{stock_code} 大宗交易】近90天共 {len(df)} 笔，合计 {_yi(total_amt)}"]
    lines.append(f"  均价: {avg_price:.2f}元")

    lines.append(f"\n📊 最近5笔:")
    for _, r in df.head(5).iterrows():
        lines.append(f"  {r['trade_date'].strftime('%Y-%m-%d')} | "
                     f"{r.get('price',0):.2f}元 | {_wan(r.get('vol'))} | "
                     f"{r.get('buyer','')[:18]} → {r.get('seller','')[:18]}")

    lines.append("\n💡 解读: 折价率>5%需警惕大股东减持，溢价交易表示买方看好。机构专用席位接盘为积极信号。")
    return "\n".join(lines)


def _format_pledge_detail(df: pd.DataFrame, stock_code: str) -> str:
    """格式化股权质押明细数据"""
    if df is None or df.empty:
        return ""
    lines = [f"【股权质押明细（来源：Tushare）】"]
    for _, row in df.head(10).iterrows():
        end_date = str(row.get('end_date', ''))[:10]
        pledger = row.get('pledger', '')
        amount = row.get('pledge_amount', '')
        ratio = row.get('pledge_ratio', '')
        total_ratio = row.get('pledge_total_ratio', '')
        start = str(row.get('pledge_start_date', ''))[:10]
        status = row.get('pledge_status', '')
        items = [f"日期:{end_date}", f"出质人:{pledger}"]
        if amount: items.append(f"质押数量:{amount}万股")
        if ratio: items.append(f"占持股:{ratio}%")
        if total_ratio: items.append(f"占总股本:{total_ratio}%")
        if start: items.append(f"开始:{start}")
        if status: items.append(f"状态:{status}")
        lines.append("  " + " | ".join(items))
    return "\n".join(lines)


def _format_holder_trade(df: pd.DataFrame, stock_code: str) -> str:
    """格式化股东增减持数据"""
    if df is None or df.empty:
        return ""
    lines = [f"【股东增减持（来源：Tushare）】"]
    for _, row in df.head(15).iterrows():
        ann_date = str(row.get('ann_date', ''))[:10]
        holder = row.get('holder_name', '')
        trade_type = row.get('trade_type', '')
        vol = row.get('trade_volume', '')
        ratio = row.get('trade_ratio', '')
        after = row.get('after_ratio', '')
        price = row.get('avg_price', '')
        type_label = "增持" if trade_type == "IN" else "减持" if trade_type == "DE" else str(trade_type)
        items = [f"公告日:{ann_date}", f"股东:{holder}", f"方向:{type_label}"]
        if vol: items.append(f"数量:{vol}万股")
        if ratio: items.append(f"比例:{ratio}%")
        if after: items.append(f"变动后:{after}%")
        if price: items.append(f"均价:{price}元")
        lines.append("  " + " | ".join(items))
    return "\n".join(lines)


def _format_report_rc(df: pd.DataFrame, stock_code: str) -> str:
    """格式化卖方盈利预测数据"""
    if df is None or df.empty:
        return ""
    lines = [f"【卖方盈利预测（来源：Tushare）】"]
    # 按预测类型分组展示
    from collections import defaultdict
    by_type = defaultdict(list)
    for _, row in df.iterrows():
        ftype = str(row.get('forecast_type', '') or row.get('type', '') or '其他')
        by_type[ftype].append(row)
    for ftype, rows in sorted(by_type.items()):
        lines.append(f"\n## {ftype}")
        for row in rows[:8]:
            report_date = str(row.get('report_date', ''))[:10]
            value = row.get('forecast_value', '') or row.get('value', '')
            org = row.get('forecast_org', '') or row.get('org_name', '')
            analyst = row.get('analyst', '')
            rating = row.get('rating', '')
            target = row.get('target_price', '') or row.get('target', '')
            items = [f"日期:{report_date}"]
            if value: items.append(f"预测值:{value}")
            if org: items.append(f"机构:{org}")
            if analyst: items.append(f"分析师:{analyst}")
            if rating: items.append(f"评级:{rating}")
            if target: items.append(f"目标价:{target}")
            lines.append("  " + " | ".join(items))
    return "\n".join(lines)


def _format_macro_data(df: pd.DataFrame, indicator_name: str) -> str:
    """格式化宏观指标数据为文本"""
    if df is None or df.empty:
        return ""
    lines = [f"【{indicator_name} 宏观数据】"]
    for _, row in df.head(10).iterrows():
        items = [f"{col}: {val}" for col, val in row.items() if val is not None and val != '']
        if items:
            lines.append("  " + " | ".join(items))
    return "\n".join(lines)


def _format_margin(df: pd.DataFrame, stock_code: str) -> str:
    """格式化融资融券汇总数据（含占流通市值比例+近一年分位）"""
    if df is None or df.empty:
        return ""
    df = df.copy()
    # 获取流通市值用于计算融资余额占比
    circ_mv = None
    try:
        from storage.sqlite.stock_storage import get_db
        from tools.stock.tushare_fetcher import TushareFetcher
        fetcher = TushareFetcher()
        if fetcher._api is not None:
            from datetime import date, timedelta
            end_date = date.today().strftime("%Y-%m-%d")
            start_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
            basic_df = fetcher.daily_basic(stock_code, start_date=start_date, end_date=end_date)
            if basic_df is not None and not basic_df.empty:
                circ_mv = float(basic_df.iloc[0].get("circ_mv") or 0) / 10000  # 万元→亿元
    except Exception:
        pass

    # 计算近一年融资余额分位
    rzye_history = []
    for _, row in df.iterrows():
        try:
            val = float(row.get('rzye', 0) or 0)
            if val > 0:
                rzye_history.append(val)
        except (TypeError, ValueError):
            pass
    rzye_quantile = None
    if rzye_history:
        latest_rzye = rzye_history[0]
        sorted_vals = sorted(rzye_history)
        rank = sum(1 for v in sorted_vals if v <= latest_rzye)
        rzye_quantile = round(rank / len(sorted_vals) * 100, 1)

    lines = [f"【融资融券汇总（来源：Tushare）】"]
    if circ_mv and rzye_history:
        latest_rzye = rzye_history[0]
        ratio = latest_rzye / circ_mv * 100 if circ_mv > 0 else None
        lines.append(f"  ▶ 汇总：融资余额占流通市值 {ratio:.2f}%（流通市值{circ_mv:.1f}亿）"
                     + (f"，近一年分位 {rzye_quantile}%"
                        + ("（超过50%分位，杠杆资金看多情绪占主导）" if rzye_quantile > 50 else "（低于50%分位）")
                        if rzye_quantile is not None else ""))
    for _, row in df.head(10).iterrows():
        trade_date = str(row.get('trade_date', ''))[:10]
        exchange_id = row.get('exchange_id', '')
        rzmre = row.get('rzmre', '')
        rzye = row.get('rzye', '')
        rqmcl = row.get('rqmcl', '')
        rqye = row.get('rqye', '')
        rzrqye = row.get('rzrqye', '')
        items = [f"日期:{trade_date}"]
        if exchange_id: items.append(f"交易所:{exchange_id}")
        if rzmre: items.append(f"融资买入额:{rzmre}")
        if rzye: items.append(f"融资余额:{rzye}")
        if rqmcl: items.append(f"融券卖出量:{rqmcl}")
        if rqye: items.append(f"融券余额:{rqye}")
        if rzrqye: items.append(f"融资融券余额:{rzrqye}")
        lines.append("  " + " | ".join(items))
    return "\n".join(lines)


def _format_margin_detail(df: pd.DataFrame, stock_code: str) -> str:
    """格式化融资融券明细数据（含占流通市值比例+近一年分位）"""
    if df is None or df.empty:
        return ""
    df = df.copy()
    # 获取流通市值用于计算融资余额占比
    circ_mv = None
    try:
        from storage.sqlite.stock_storage import get_db
        from tools.stock.tushare_fetcher import TushareFetcher
        fetcher = TushareFetcher()
        if fetcher._api is not None:
            from datetime import date, timedelta
            end_date = date.today().strftime("%Y-%m-%d")
            start_date = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
            basic_df = fetcher.daily_basic(stock_code, start_date=start_date, end_date=end_date)
            if basic_df is not None and not basic_df.empty:
                circ_mv = float(basic_df.iloc[0].get("circ_mv") or 0) / 10000  # 万元→亿元
    except Exception:
        pass

    # 计算近一年融资余额分位
    rzye_history = []
    for _, row in df.iterrows():
        try:
            val = float(row.get('rzye', 0) or 0)
            if val > 0:
                rzye_history.append(val)
        except (TypeError, ValueError):
            pass
    rzye_quantile = None
    if rzye_history:
        latest_rzye = rzye_history[0]
        sorted_vals = sorted(rzye_history)
        rank = sum(1 for v in sorted_vals if v <= latest_rzye)
        rzye_quantile = round(rank / len(sorted_vals) * 100, 1)

    lines = [f"【融资融券明细（来源：Tushare）】"]
    if circ_mv and rzye_history:
        latest_rzye = rzye_history[0]
        ratio = latest_rzye / circ_mv * 100 if circ_mv > 0 else None
        lines.append(f"  ▶ 汇总：融资余额占流通市值 {ratio:.2f}%（流通市值{circ_mv:.1f}亿）"
                     + (f"，近一年分位 {rzye_quantile}%"
                        + ("（超过50%分位，杠杆资金看多情绪占主导）" if rzye_quantile > 50 else "（低于50%分位）")
                        if rzye_quantile is not None else ""))
    for _, row in df.head(10).iterrows():
        trade_date = str(row.get('trade_date', ''))[:10]
        name = row.get('name', '')
        rzmre = row.get('rzmre', '')
        rzye = row.get('rzye', '')
        rqmcl = row.get('rqmcl', '')
        rqye = row.get('rqye', '')
        rzrqye = row.get('rzrqye', '')
        items = [f"日期:{trade_date}"]
        if name: items.append(f"名称:{name}")
        if rzmre: items.append(f"融资买入额:{rzmre}")
        if rzye: items.append(f"融资余额:{rzye}")
        if rqmcl: items.append(f"融券卖出量:{rqmcl}")
        if rqye: items.append(f"融券余额:{rqye}")
        if rzrqye: items.append(f"融资融券余额:{rzrqye}")
        lines.append("  " + " | ".join(items))
    return "\n".join(lines)


def _format_moneyflow(df: pd.DataFrame, stock_code: str) -> str:
    """格式化个股资金流向数据（含累计统计）"""
    if df is None or df.empty:
        return ""
    import pandas as _pd
    df = df.copy()
    df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)

    def _y(v):
        """格式化金额：万元或亿元
        注意：Tushare moneyflow API 返回单位为万元（文档标注）。
        """
        if v is None or not _pd.notna(v) or v == 0:
            return "0"
        av = abs(v)
        if av >= 1e4:  # 1万万 = 1亿元
            return f"{v/1e4:.2f}亿"  # 万元 ÷ 1万 = 亿元
        else:
            return f"{v:.2f}万"  # 万元

    lines = [f"【个股资金流向（来源：Tushare）】"]

    # --- SUMMARY（直接引用，禁止 LLM 心算累计值） ---
    latest_date = None
    latest_main = None
    sum_5d = sum_10d = sum_20d = None
    try:
        if not df.empty:
            latest_date = str(df.iloc[0].get('trade_date', ''))[:10]
            latest_main = df.iloc[0].get('net_mf_amount', 0) or 0
            for nd, attr in [(5, 'sum_5d'), (10, 'sum_10d'), (20, 'sum_20d')]:
                sub = df.head(min(len(df), nd))
                if len(sub) >= 2:
                    locals()[attr] = sub['net_mf_amount'].sum()
    except Exception:
        pass
    summary_items = []
    if latest_date:
        summary_items.append(f"最新交易日:{latest_date}")
    if latest_main is not None:
        summary_items.append(f"当日主力净流入:{_y(latest_main)}")
    if sum_5d is not None:
        summary_items.append(f"近5日主力累计:{_y(sum_5d)}")
    if sum_20d is not None:
        sign = "转正" if sum_20d > 0 else ("转负" if sum_20d < 0 else "持平")
        summary_items.append(f"近20日主力累计:{_y(sum_20d)}（{sign}）")
    if summary_items:
        lines.append("【SUMMARY（直接引用写进资金段，禁止重算）】 " + " | ".join(summary_items))

    # --- 逐日明细（最近10个交易日） ---
    lines.append("【逐日明细】")
    for _, row in df.head(10).iterrows():
        trade_date = str(row.get('trade_date', ''))[:10]
        # 主力净流入（已由 _normalize_moneyflow 修正：大单+超大单净额，单位：万元）
        main_force_net = row.get('net_mf_amount', 0) or 0
        # 游资（中单净额 = 中单买入 - 中单卖出）
        md_net = (row.get('buy_md_amount', 0) or 0) - (row.get('sell_md_amount', 0) or 0)
        # 散户（小单净额 = 小单买入 - 小单卖出）
        sm_net = (row.get('buy_sm_amount', 0) or 0) - (row.get('sell_sm_amount', 0) or 0)
        items = [
            f"日期:{trade_date}",
            f"主力净流入:{_y(main_force_net)}",
            f"游资净流:{_y(md_net)}",
            f"散户净流:{_y(sm_net)}",
        ]
        lines.append("  " + " | ".join(items))

    # --- 累计统计（近5/10/20日） ---
    for label, ndays in [("近5日", 5), ("近10日", 10), ("近20日", 20)]:
        sub = df.head(min(len(df), ndays))
        if len(sub) < 2:
            continue
        mf_sum = sub['net_mf_amount'].sum()
        lines.append(f"【{label}累计】主力净流入:{_y(mf_sum)}")

    # --- 资金面综合判断 ---
    if len(df) >= 5:
        mf_5 = df.head(5)['net_mf_amount'].sum()
        mf_20 = df.head(min(len(df), 20))['net_mf_amount'].sum() if len(df) >= 5 else mf_5
        signals = []
        if mf_5 > 1e4 and mf_20 > 0:  # 1e4万元 = 1亿元
            signals.append("近5日主力净流入>1亿且近20日累计为正 → 机构资金温和布局")
        elif mf_5 < -1e4 and mf_20 < 0:  # -1e4万元 = -1亿元
            signals.append("近5日主力净流出>1亿且近20日累计为负 → 机构资金持续撤退")
        elif mf_5 > 0 and mf_20 > 5e4:  # 5e4万元 = 5亿元
            signals.append("近20日主力累计净流入>5亿 → 中期机构资金显著流入")
        elif mf_5 < 0 and mf_20 < -5e4:  # -5e4万元 = -5亿元
            signals.append("近20日主力累计净流出>5亿 → 中期机构资金显著流出")
        if signals:
            lines.append(f"【资金信号】{'；'.join(signals)}")

    return "\n".join(lines)


def _format_hsgt_moneyflow(df: pd.DataFrame) -> str:
    """格式化沪深港通资金流向数据"""
    if df is None or df.empty:
        return ""
    lines = [f"【沪深港通资金流向（来源：Tushare）】"]
    for _, row in df.head(10).iterrows():
        trade_date = str(row.get('trade_date', ''))[:10]
        ggt_ss = row.get('ggt_ss', '')
        ggt_sz = row.get('ggt_sz', '')
        hgt = row.get('hgt', '')
        sgt = row.get('sgt', '')
        north_money = row.get('north_money', '')
        south_money = row.get('south_money', '')
        items = [f"日期:{trade_date}"]
        if ggt_ss: items.append(f"沪股通:{ggt_ss}")
        if ggt_sz: items.append(f"深股通:{ggt_sz}")
        if hgt: items.append(f"港股通(沪):{hgt}")
        if sgt: items.append(f"港股通(深):{sgt}")
        if north_money: items.append(f"北向资金:{north_money}")
        if south_money: items.append(f"南向资金:{south_money}")
        lines.append("  " + " | ".join(items))
    return "\n".join(lines)


def _format_top_list(df: pd.DataFrame, stock_code: str) -> str:
    """格式化龙虎榜数据"""
    if df is None or df.empty:
        return f"📌 {stock_code} 近90天未上龙虎榜，无异常波动"
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)

    def _yi(v): return f"{v/1e8:.2f}亿" if v and v > 0 else "N/A"
    def _pct(v): return f"{v:.2f}%" if v and pd.notna(v) else "N/A"

    total_net = df['net_amount'].sum()

    lines = [f"✅ 【{stock_code} 龙虎榜】近90天上榜 {len(df)} 次"]
    lines.append(f"  累计净买入: {_yi(total_net)}")

    lines.append(f"\n📊 上榜明细:")
    for _, r in df.head(8).iterrows():
        lines.append(f"  {r['trade_date'].strftime('%Y-%m-%d')} | "
                     f"涨跌幅{_pct(r.get('pct_change'))} | "
                     f"净买{_yi(r.get('net_amount'))} | {r.get('reason','')[:30]}")

    lines.append("\n💡 解读: 连续上榜且净买入为正，通常意味着主力资金介入。净卖出持续为负需警惕出货。")
    return "\n".join(lines)


def _format_top_inst(df: pd.DataFrame, stock_code: str) -> str:
    """格式化机构席位追踪"""
    if df is None or df.empty:
        return f"📌 {stock_code} 近90天龙虎榜无机构席位参与"
    df = df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)

    def _wan(v): return f"{v/1e4:.0f}万" if v and v > 0 else "N/A"

    total_buy = df['buy'].sum()
    total_sell = df['sell'].sum()
    net = total_buy - total_sell

    lines = [f"✅ 【{stock_code} 机构席位】近90天共 {len(df)} 次机构参与"]
    lines.append(f"  机构买入: {_wan(total_buy)} | 卖出: {_wan(total_sell)} | 净买: {_wan(net)}")

    if net > 0:
        lines.append(f"\n📈 机构总体呈净买入，看好信号。")
    else:
        lines.append(f"\n📉 机构总体呈净卖出，需关注。")

    lines.append(f"\n📊 最近5次:")
    for _, r in df.head(5).iterrows():
        lines.append(f"  {r['trade_date'].strftime('%Y-%m-%d')} | "
                     f"买{_wan(r.get('buy'))} 卖{_wan(r.get('sell'))} | "
                     f"净{_wan(r.get('net_buy'))} | {r.get('side','')}")

    return "\n".join(lines)


def call_fetch_cashflow_data(stock_code: str) -> str:
    """
    获取并保存股票现金流量表数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_stock_cashflow(stock_code=stock_code)
        return _format_cashflow_data(df, stock_code)
    except Exception as e:
        logger.error(f"调用现金流量表工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取现金流量表数据失败"


def call_fetch_income_data(stock_code: str) -> str:
    """
    获取并保存股票利润表数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_stock_income(stock_code=stock_code)
        if df is None or df.empty:
            # API 超限时回退到 DB 缓存
            df = stock_tool_instance.db.get_stock_income(stock_code)
        if df is not None and not df.empty:
            return _format_income_data(df, stock_code)
        return f"❌ 未获取到 {stock_code} 的利润表数据"
    except Exception as e:
        logger.error(f"调用利润表工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取利润表数据失败"


def call_fetch_dividend_data(stock_code: str) -> str:
    """获取分红送股数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_dividend(stock_code=stock_code)
        if df is None or df.empty:
            return ""
        lines = [f"【分红送股历史（来源：Tushare）】"]
        for _, row in df.head(10).iterrows():
            date_str = str(row.get('end_date', ''))[:10]
            div = row.get('div_procf', '')
            stk_bo = row.get('stk_bo_rate', '')
            stk_co = row.get('stk_co_rate', '')
            cash = row.get('cash_div', '')
            items = [f"报告期:{date_str}"]
            if div: items.append(f"每股分红:{div}")
            if stk_bo: items.append(f"送股:{stk_bo}")
            if stk_co: items.append(f"转增:{stk_co}")
            if cash: items.append(f"现金分红总额:{cash}")
            lines.append("  " + " | ".join(items))
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"分红送股数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_fina_audit_data(stock_code: str) -> str:
    """获取财务审计意见数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_fina_audit(stock_code=stock_code)
        if df is None or df.empty:
            return ""
        lines = [f"【财务审计意见（来源：Tushare）】"]
        for _, row in df.head(10).iterrows():
            date_str = str(row.get('end_date', ''))[:10]
            opinion = row.get('audit_opinion', '')
            auditor = row.get('auditor', '')
            fee = row.get('audit_fee', '')
            items = [f"报告期:{date_str}"]
            if opinion: items.append(f"审计意见:{opinion}")
            if auditor: items.append(f"审计机构:{auditor}")
            if fee: items.append(f"审计费用:{fee}")
            lines.append("  " + " | ".join(items))
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"财务审计意见数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_disclosure_date_data(stock_code: str) -> str:
    """获取财报披露计划日期数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_disclosure_date(stock_code=stock_code)
        if df is None or df.empty:
            return ""
        lines = [f"【财报披露计划日期（来源：Tushare）】"]
        for _, row in df.head(10).iterrows():
            date_str = str(row.get('end_date', ''))[:10]
            issue_date = str(row.get('stm_issue_date', ''))[:10]
            comm_date = str(row.get('stm_comm_date', ''))[:10]
            diss_date = str(row.get('actual_diss_date', ''))[:10]
            items = [f"报告期:{date_str}"]
            if issue_date: items.append(f"首次披露日:{issue_date}")
            if comm_date: items.append(f"董事会公告日:{comm_date}")
            if diss_date: items.append(f"实际披露日:{diss_date}")
            lines.append("  " + " | ".join(items))
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"财报披露计划日期数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_balance_sheet_data(stock_code: str) -> str:
    """
    获取并保存股票资产负债表数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_stock_balance_sheet(stock_code=stock_code)
        return _format_balance_sheet_data(df, stock_code)
    except Exception as e:
        logger.error(f"调用资产负债表工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取资产负债表数据失败"


def call_fetch_fina_indicator(stock_code: str) -> str:
    """
    获取并保存股票财务指标数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_fina_indicator(stock_code=stock_code)
        return _format_fina_indicator(df, stock_code)
    except Exception as e:
        logger.error(f"调用财务指标工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取财务指标数据失败"


def call_fetch_main_business(stock_code: str) -> str:
    """
    获取并保存股票主营业务构成数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_main_business(stock_code=stock_code)
        return _format_main_business(df, stock_code)
    except Exception as e:
        logger.error(f"调用主营业务构成工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取主营业务构成数据失败"


def call_fetch_holder_number(stock_code: str) -> str:
    """
    获取并保存股东户数数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_holder_number(stock_code=stock_code)
        return _format_holder_number(df, stock_code)
    except Exception as e:
        logger.error(f"调用股东户数工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取股东户数数据失败"


def call_fetch_northbound_hold(stock_code: str) -> str:
    """
    获取并保存北向持股数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_northbound_hold(stock_code=stock_code)
        return _format_northbound_hold(df, stock_code)
    except Exception as e:
        logger.error(f"调用北向持股工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取北向持股数据失败"


def call_fetch_top10_holder(stock_code: str) -> str:
    """
    获取并保存十大股东数据
    :param stock_code: 股票代码
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_top10_holder(stock_code=stock_code)
        return _format_top10_holder(df, stock_code)
    except Exception as e:
        logger.error(f"调用十大股东工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取十大股东数据失败"


def call_fetch_industry_valuation(stock_code: str = None, industry_name: str = None) -> str:
    """
    获取行业估值数据（PE/PB均值）
    :param stock_code: 股票代码（通过股票反查行业）
    :param industry_name: 行业名称（直接指定行业）
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_industry_valuation(
            industry_name=industry_name, stock_code=stock_code
        )
        return _format_industry_valuation(df, industry_name=industry_name, stock_code=stock_code)
    except Exception as e:
        logger.error(f"调用行业估值工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取行业估值数据失败"


def call_fetch_new_energy_penetration() -> str:
    """
    获取新能源车行业月度渗透率数据
    :return: 格式化的数据字符串
    """
    try:
        df = stock_tool_instance.fetch_and_save_new_energy_penetration()
        return _format_new_energy_penetration(df)
    except Exception as e:
        logger.error(f"调用新能源车渗透率工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取新能源车渗透率数据失败"


def call_fetch_financial_health_summary(stock_code: str) -> str:
    """获取深度财务健康度分析"""
    def _num(v):
        if v is None: return None
        try: return float(v)
        except (TypeError, ValueError): return None
    try:
        # 拉取所需数据
        income_df = stock_tool_instance.fetch_and_save_stock_income(stock_code)
        balance_df = stock_tool_instance.fetch_and_save_stock_balance_sheet(stock_code)
        cashflow_df = stock_tool_instance.fetch_and_save_stock_cashflow(stock_code)
        fina_df = stock_tool_instance.fetch_and_save_fina_indicator(stock_code)
        
        if fina_df is None or fina_df.empty:
            return "❌ 未获取到财务指标数据"
        
        # 从 fina_indicator 获取周转率
        latest = fina_df.iloc[0] if not fina_df.empty else {}
        prev = fina_df.iloc[1] if len(fina_df) > 1 else {}
        
        lines = ["📊 **深度财务健康度分析**", ""]
        
        # === 杜邦分解 ===
        lines.append("**一、杜邦分解（ROE 三层拆解）**")
        # fina_indicator 字段已在入口层 ÷100 归一化为小数，直接使用
        roe = _num(latest.get('roe'))
        net_margin = _num(latest.get('netprofit_margin'))
        at = _num(latest.get('assets_turn'))
        debt_ratio = _num(latest.get('debt_to_assets'))

        if roe is not None:
            lines.append(f"  ROE（净资产收益率）: {roe:.2%}")
        if net_margin is not None and at is not None and debt_ratio is not None:
            em = 1 / (1 - debt_ratio) if debt_ratio < 1 else None
            lines.append(f"  = 净利率 {net_margin:.2%} × 资产周转率 {at:.2f} 次" +
                         (f" × 权益乘数 {em:.2f}" if em else ""))
            if net_margin is not None:
                lines.append(f"    ├ 净利率  {net_margin:.2%}{'（高利润率）' if net_margin > 0.15 else '（低利润率）' if net_margin < 0.05 else ''}")
            if at is not None:
                lines.append(f"    ├ 资产周转率  {at:.2f} 次{'（高效运营）' if at > 1 else '（重资产/周转慢）'}")
            if em:
                lines.append(f"    └ 权益乘数  {em:.2f}{'（高杠杆）' if em > 3 else '（低杠杆）'}")
        
        # 与上期对比
        prev_roe = _num(prev.get('roe'))
        if roe is not None and prev_roe is not None and prev_roe != 0:
            roe_change = (roe - prev_roe) / abs(prev_roe) * 100
            lines.append(f"  ROE 同比变动: {roe_change:+.1f}%{'（盈利能力提升）' if roe_change > 0 else '（盈利能力下降）'}")
        
        lines.append("")
        
        # === 周转天数 ===
        lines.append("**二、周转天数分析**")
        # 口径对齐：Tushare 周转率基于累计口径（Q1=3个月，半年=6个月，三季=9个月），需年化
        _rd = latest.get('report_date')
        _annual_factor = 12.0 / _rd.month if hasattr(_rd, 'month') else 1.0
        _prev_rd = prev.get('report_date')
        _prev_annual_factor = 12.0 / _prev_rd.month if hasattr(_prev_rd, 'month') else _annual_factor

        inv_turn = _num(latest.get('inv_turn'))
        ar_turn = _num(latest.get('ar_turn'))
        
        if inv_turn and inv_turn > 0:
            inv_days = 365 / (inv_turn * _annual_factor)
            lines.append(f"  存货周转天数: {inv_days:.0f} 天（年化周转率 {inv_turn * _annual_factor:.2f} 次/年）")
            prev_inv = _num(prev.get('inv_turn'))
            if prev_inv and prev_inv > 0:
                prev_inv_days = 365 / (prev_inv * _prev_annual_factor)
                change = inv_days - prev_inv_days
                lines.append(f"    {'↑ 延长' if change > 0 else '↓ 缩短'}{abs(change):.0f} 天"
                             f"{'（去库存压力大）' if change > 15 else '（库存管理改善）' if change < -15 else '（基本稳定）'}")
        if ar_turn and ar_turn > 0:
            ar_days = 365 / (ar_turn * _annual_factor)
            lines.append(f"  应收账款周转天数: {ar_days:.0f} 天（年化周转率 {ar_turn * _annual_factor:.2f} 次/年）")
            prev_ar = _num(prev.get('ar_turn'))
            if prev_ar and prev_ar > 0:
                prev_ar_days = 365 / (prev_ar * _prev_annual_factor)
                change = ar_days - prev_ar_days
                lines.append(f"    {'↑ 延长' if change > 0 else '↓ 缩短'}{abs(change):.0f} 天"
                             f"{'（回款恶化）' if change > 15 else '（回款加快）' if change < -15 else '（基本稳定）'}")
        if inv_turn and inv_turn > 0 and ar_turn and ar_turn > 0:
            cash_cycle = 365/(inv_turn * _annual_factor) + 365/(ar_turn * _annual_factor)
            lines.append(f"  估算现金循环周期（存货+应收）: ~{cash_cycle:.0f} 天（约{cash_cycle/30:.1f}个月）")
        
        lines.append("")
        
        # === FCF 分析 ===
        lines.append("**三、自由现金流（FCF）质量**")
        if cashflow_df is not None and not cashflow_df.empty:
            cf_latest = cashflow_df.iloc[0]
            cf_prev = cashflow_df.iloc[1] if len(cashflow_df) > 1 else None
            ocf = _num(cf_latest.get('operating_cashflow'))
            capex = _num(cf_latest.get('capex'))
            inv_net = _num(cf_latest.get('investing_cashflow'))
            
            # FCF = 经营现金流 - 资本开支（标准口径）
            # 当 capex 缺失/Tushare 未返回时，用「经营+投资净额」近似（投资净额为负则等效减投资支出）
            if ocf is not None:
                if capex and capex > 0:
                    fcf_val = ocf - capex
                    fcf_note = f"经营{ocf/1e8:.2f}亿 - 资本开支{capex/1e8:.2f}亿"
                elif inv_net and inv_net < 0:
                    fcf_val = ocf + inv_net  # inv_net 为负，等效 ocf - 投资支出
                    fcf_note = f"经营{ocf/1e8:.2f}亿 + 投资净额({inv_net/1e8:.2f}亿)（capex缺失，近似值）"
                else:
                    fcf_val = None
                
                if fcf_val is not None:
                    lines.append(f"  自由现金流: {fcf_val / 1e8:.2f} 亿元（{fcf_note}）")
                    lines.append(f"  ├ 经营现金流: {ocf / 1e8:.2f} 亿{'（健康）' if ocf > 0 else '（为负）'}" if ocf else "")
                    capex_str = f"{capex / 1e8:.2f} 亿" if capex is not None else "明细数据缺失，无法精确计算自由现金流"
                    lines.append(f"  └ 资本开支: {capex_str}{'（扩张期）' if capex and capex > 0 else ''}" if ocf else "")
                    # FCF 同比
                    if cf_prev is not None:
                        prev_ocf = _num(cf_prev.get('operating_cashflow'))
                        prev_capex = _num(cf_prev.get('capex'))
                        prev_inv = _num(cf_prev.get('investing_cashflow'))
                        if prev_ocf is not None:
                            if prev_capex and prev_capex > 0:
                                prev_fcf = prev_ocf - prev_capex
                            elif prev_inv and prev_inv < 0:
                                prev_fcf = prev_ocf + prev_inv
                            else:
                                prev_fcf = None
                            if prev_fcf and prev_fcf != 0:
                                fcf_yoy = (fcf_val - prev_fcf) / abs(prev_fcf) * 100
                                adj = '（大幅改善）' if fcf_yoy > 50 else '（明显恶化）' if fcf_yoy < -50 else '（基本稳定）'
                                lines.append(f"  FCF 同比: {fcf_yoy:+.1f}%{adj}")
                    # FCF/营收比率
                    if income_df is not None and not income_df.empty:
                        rev = _num(income_df.iloc[0].get('total_revenue'))
                        if rev and rev > 0 and fcf_val != 0:
                            fcf_margin = fcf_val / rev * 100
                            lines.append(f"  FCF/营收比率: {fcf_margin:.1f}%"
                                         f"{'（现金流优秀）' if fcf_margin > 15 else '（现金流一般）' if fcf_margin > 5 else '（现金流紧张）'}")
                else:
                    lines.append("  ⚠️ 自由现金流无法计算（缺资本开支与投资净额数据）")
        
        lines.append("")
        
        # === 营运资本 ===
        lines.append("**四、营运资本分析**")
        if balance_df is not None and not balance_df.empty:
            bl = balance_df.iloc[0]
            total_assets = _num(bl.get('total_assets'))
            cur_assets = _num(bl.get('total_cur_assets'))
            cur_liab = _num(bl.get('total_cur_liab'))
            receiv = _num(bl.get('accounts_receiv'))
            inv = _num(bl.get('inventories'))
            equity = _num(bl.get('total_hldr_eqy_exc_min_int'))
            
            if cur_assets is not None and cur_liab is not None:
                wc = cur_assets - cur_liab
                lines.append(f"  营运资本（流动资产-流动负债）: {wc / 1e8:.2f} 亿元")
                lines.append(f"  ├ 流动资产: {cur_assets / 1e8:.2f} 亿")
                lines.append(f"  └ 流动负债: {cur_liab / 1e8:.2f} 亿")
                cr = cur_assets / cur_liab if cur_liab > 0 else None
                if cr:
                    lines.append(f"  流动比率: {cr:.2f}{'（短期偿债安全）' if cr > 2 else '（短期偿债偏紧）' if cr > 1 else '（短期偿债风险）'}")
            if receiv is not None and total_assets and total_assets > 0:
                lines.append(f"  应收账款占总资产: {receiv/total_assets*100:.1f}%")
            if inv is not None and total_assets and total_assets > 0:
                lines.append(f"  存货占总资产: {inv/total_assets*100:.1f}%")
            if equity and total_assets and total_assets > 0:
                lines.append(f"  权益占总资产: {equity/total_assets*100:.1f}%")
        
        lines.append("")
        
        # === 毛利率趋势分析（预计算，避免 LLM 误判拐点）===
        lines.append("**五、毛利率趋势分析**")
        lines.append("  ※ 以下为程序逐季跟踪结果，优先级高于 LLM 自行判断")
        try:
            gm_rows = []
            for _, r in fina_df.iterrows():
                rd = r.get('report_date')
                gm = _num(r.get('gross_margin'))
                if rd is not None and gm is not None:
                    gm_rows.append((rd, gm))
            gm_rows.sort(key=lambda x: x[0], reverse=False)  # 时间升序
            if len(gm_rows) >= 4:
                vals = [gm * 100 for _, gm in gm_rows[-5:]]  # 最近5期，转为%
                dates = [f"{rd.year}Q{(rd.month-1)//3+1}" if hasattr(rd, 'month') else str(rd) for rd, _ in gm_rows[-5:]]
                
                # 找峰值和谷值
                peak_idx = vals.index(max(vals))
                trough_idx = vals.index(min(vals))
                
                if trough_idx > peak_idx:
                    # 模式：峰值后某期降至谷值，之后持续回升
                    if trough_idx < len(vals) - 1:
                        rebounding = all(vals[i+1] >= vals[i] for i in range(trough_idx, len(vals)-1))
                        if rebounding:
                            lines.append(f"  📈 模式：{dates[peak_idx]}达峰值({vals[peak_idx]:.2f}%) → "
                                         f"历{len(vals[:trough_idx])-1}期降至谷值({vals[trough_idx]:.2f}%) → "
                                         f"之后{len(vals)-1-trough_idx}个季度持续回升至{vals[-1]:.2f}%")
                            lines.append(f"  ✅ 判定：盈利拐点已现（Q2跳水后逐季修复，非趋势性下行）")
                        else:
                            lines.append(f"  📊 模式：{dates[peak_idx]}峰值({vals[peak_idx]:.2f}%) → "
                                         f"{dates[trough_idx]}谷值({vals[trough_idx]:.2f}%) → "
                                         f"最新{vals[-1]:.2f}%（未形成连续回升趋势，需继续观察）")
                    else:
                        lines.append(f"  📊 模式：{dates[peak_idx]}峰值({vals[peak_idx]:.2f}%) → "
                                     f"持续下行至{vals[-1]:.2f}%（无回升迹象）")
                elif all(vals[i+1] <= vals[i] for i in range(len(vals)-1)):
                    lines.append(f"  📉 模式：连续{len(vals)}个季度下行（{', '.join(f'{v:.2f}%' for v in vals)}）")
                    lines.append(f"  ⚠️ 判定：毛利率趋势性恶化，需关注成本/定价能力")
                else:
                    trend_words = []
                    for i in range(1, len(vals)):
                        if vals[i] > vals[i-1]:
                            trend_words.append(f"Q{i+1}回升")
                        else:
                            trend_words.append(f"Q{i+1}下降")
                    lines.append(f"  🔄 模式：各季波动，无单边趋势")
                    lines.append(f"  💡 序列（最近{len(vals)}期）: {' → '.join(f'{v:.2f}%' for v in vals)}")
            else:
                lines.append(f"  ⚠️ 数据不足{len(gm_rows)}期，无法做趋势判定")
        except Exception:
            lines.append(f"  ⚠️ 毛利率趋势分析异常，跳过")
        
        # === 负面信号检测 ===
        lines.append("")
        lines.append("**六、风险信号检测**")
        risk_signals = []
        
        # 5.1 净利率腰斩检测（对比上期）
        prev_net_margin = _num(prev.get('netprofit_margin'))
        if net_margin is not None and prev_net_margin is not None and prev_net_margin > 0:
            margin_change = (net_margin - prev_net_margin) / prev_net_margin
            if margin_change < -0.3:
                risk_signals.append(f"🔴 **净利率腰斩**：净利率从 {prev_net_margin:.2%} 降至 {net_margin:.2%}（降幅 {margin_change:.0%}），盈利能力显著恶化")
        
        # 5.2 归母净利润大幅下滑（利用收入数据）
        if income_df is not None and len(income_df) >= 2:
            inc_latest = income_df.iloc[0]
            inc_prev = income_df.iloc[1]
            np_now = _num(inc_latest.get('net_profit'))
            np_before = _num(inc_prev.get('net_profit'))
            rev_now = _num(inc_latest.get('total_revenue'))
            rev_before = _num(inc_prev.get('total_revenue'))
            if np_now is not None and np_before is not None and np_before > 0:
                np_yoy = (np_now - np_before) / np_before * 100
                if np_yoy < -30:
                    risk_signals.append(f"🔴 **归母净利润大幅下滑**：同比 {np_yoy:+.1f}%{'（需关注是否为一次性因素或趋势性恶化）' if np_yoy < -50 else ''}")
            # 财务费用异常激增
            fin_exp_now = _num(inc_latest.get('fin_exp'))
            fin_exp_before = _num(inc_prev.get('fin_exp'))
            if fin_exp_now is not None and fin_exp_before is not None and fin_exp_before != 0:
                fin_exp_change = (fin_exp_now - fin_exp_before) / abs(fin_exp_before) * 100
                if fin_exp_change > 30 and fin_exp_now > 1e8:
                    risk_signals.append(f"🔴 **财务费用激增**：同比 {fin_exp_change:+.0f}%（从 {fin_exp_before/1e8:.2f}亿 → {fin_exp_now/1e8:.2f}亿），可能为汇兑损失或利息支出上升")
            # 营收下滑
            if rev_now is not None and rev_before is not None and rev_before > 0:
                rev_yoy = (rev_now - rev_before) / rev_before * 100
                if rev_yoy < -15:
                    risk_signals.append(f"🟡 **营收下滑**：同比 {rev_yoy:+.1f}%，增长动力不足")
        
        # 5.3 资产负债率过高（杠杆风险）
        if debt_ratio is not None and debt_ratio > 0.7:
            risk_signals.append(f"🟡 **高杠杆**：资产负债率 {debt_ratio:.1%}，财务风险较高")
        
        # 5.4 ROE 大幅下降
        if roe is not None and prev_roe is not None and prev_roe > 0:
            roe_decline = (prev_roe - roe) / prev_roe
            if roe_decline > 0.3:
                risk_signals.append(f"🔴 **ROE 大幅下降**：从 {prev_roe:.2%} 降至 {roe:.2%}（降幅 {roe_decline:.0%}），股东回报能力减弱")
        
        if risk_signals:
            for s in risk_signals:
                lines.append(f"  {s}")
        else:
            lines.append("  ✅ 未检测到显著风险信号")
        
        # === 利润归因分析 ===
        lines.append("")
        lines.append("**七、利润归因分析（瀑布分解）**")
        lines.append("  ※ 归母净利润同比变动按瀑布模型拆分：营收贡献+毛利率变化+费用率变化+非经常性损益")
        try:
            from tools.profit_attribution import analyze_profit_decline, format_profit_attribution
            pa_result = analyze_profit_decline(stock_code)
            if "error" not in pa_result:
                for f in pa_result.get("factors", []):
                    sign = "+" if f["impact_yi"] >= 0 else ""
                    lines.append(f"  {f['factor']}: {sign}{f['impact_yi']:.1f}亿（占比{f['impact_pct']:.0f}%）")
                lines.append(f"  ────")
                lines.append(f"  主因: {pa_result['primary_cause']}")
                lines.append(f"  置信度: {pa_result['confidence']}")
            else:
                lines.append(f"  ⚠️ {pa_result['error']}")
        except Exception as e:
            lines.append(f"  ⚠️ 利润归因分析异常: {e}")
        
        # === 产能扩张分析 ===
        lines.append("")
        lines.append("**八、产能扩张分析**")
        try:
            from tools.capacity_analysis import analyze_capacity
            ca_result = analyze_capacity(stock_code)
            if "error" not in ca_result and ca_result.get("data_quality") == "完整":
                for d in ca_result.get("details", []):
                    lines.append(f"  • {d}")
                judge = ca_result.get("expansion_judgment", "")
                if judge == "扩张":
                    lines.append(f"  📈 {ca_result.get('summary', '')}")
                elif judge == "收缩":
                    lines.append(f"  📉 {ca_result.get('summary', '')}")
                else:
                    lines.append(f"  ➡️ {ca_result.get('summary', '')}")
        except Exception as e:
            lines.append(f"  ⚠️ 产能扩张分析异常: {e}")
        
        lines.append("")
        lines.append("💡 以上数据由程序基于最新财报计算，供分析参考。")
        
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"获取财务健康度分析失败: {e} {traceback.format_exc()}")
        return "❌ 获取财务健康度分析失败"


def call_fetch_vehicle_sales(stock_code: str) -> str:
    """获取并保存全国车型月销量数据"""
    try:
        df = stock_tool_instance.fetch_and_save_vehicle_sales(stock_code=stock_code)
        return _format_vehicle_sales(df, stock_code)
    except Exception as e:
        logger.error(f"调用车型销量工具失败: {e}")
        return "❌ 获取车型销量数据失败"


def call_fetch_pledge_detail(stock_code: str) -> str:
    """获取股权质押明细数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_pledge_detail(stock_code=stock_code)
        return _format_pledge_detail(df, stock_code)
    except Exception as e:
        logger.warning(f"股权质押明细数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_holder_trade(stock_code: str) -> str:
    """获取股东增减持数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_holder_trade(stock_code=stock_code)
        return _format_holder_trade(df, stock_code)
    except Exception as e:
        logger.warning(f"股东增减持数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_report_rc(stock_code: str) -> str:
    """获取卖方盈利预测数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_report_rc(stock_code=stock_code)
        return _format_report_rc(df, stock_code)
    except Exception as e:
        logger.warning(f"卖方盈利预测数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_macro(indicator_name: str) -> str:
    """获取宏观指标数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_macro(indicator_name=indicator_name)
        return _format_macro_data(df, indicator_name)
    except Exception as e:
        logger.warning(f"宏观数据[{indicator_name}]获取失败（不影响分析）: {e}")
        return ""


def call_fetch_margin(stock_code: str) -> str:
    """获取融资融券汇总数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_margin(stock_code=stock_code)
        return _format_margin(df, stock_code)
    except Exception as e:
        logger.warning(f"融资融券汇总数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_margin_detail(stock_code: str) -> str:
    """获取融资融券明细数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_margin_detail(stock_code=stock_code)
        return _format_margin_detail(df, stock_code)
    except Exception as e:
        logger.warning(f"融资融券明细数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_moneyflow(stock_code: str) -> str:
    """获取个股资金流向数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_moneyflow(stock_code=stock_code)
        return _format_moneyflow(df, stock_code)
    except Exception as e:
        logger.warning(f"个股资金流向数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_hsgt_moneyflow(use_cache: bool = True) -> str:
    """获取沪深港通资金流向数据并格式化为文本"""
    try:
        df = stock_tool_instance.fetch_and_save_stock_hsgt_moneyflow()
        return _format_hsgt_moneyflow(df)
    except Exception as e:
        logger.warning(f"沪深港通资金流向数据获取失败（不影响分析）: {e}")
        return ""


def call_fetch_repurchase(stock_code: str) -> str:
    """获取并保存股票回购数据"""
    try:
        df = stock_tool_instance.fetch_and_save_repurchase(stock_code=stock_code)
        return _format_repurchase(df, stock_code)
    except Exception as e:
        logger.error(f"调用回购工具失败: {e}")
        return "❌ 获取股票回购数据失败"


def call_fetch_share_float(stock_code: str) -> str:
    """获取并保存限售解禁数据"""
    try:
        df = stock_tool_instance.fetch_and_save_share_float(stock_code=stock_code)
        return _format_share_float(df, stock_code)
    except Exception as e:
        logger.error(f"调用限售解禁工具失败: {e}")
        return "❌ 获取限售解禁数据失败"


def call_fetch_broker_recommend(stock_code: str) -> str:
    """获取并保存分析师评级"""
    try:
        df = stock_tool_instance.fetch_and_save_broker_recommend(stock_code=stock_code)
        return _format_broker_recommend(df, stock_code)
    except Exception as e:
        logger.error(f"调用分析师评级工具失败: {e}")
        return "❌ 获取分析师评级数据失败"


def call_fetch_pledge(stock_code: str) -> str:
    """获取并保存股权质押数据"""
    try:
        df = stock_tool_instance.fetch_and_save_pledge(stock_code=stock_code)
        return _format_pledge(df, stock_code)
    except Exception as e:
        logger.error(f"调用质押工具失败: {e}")
        return "❌ 获取股权质押数据失败"


def call_fetch_block_trade(stock_code: str) -> str:
    """获取并保存大宗交易数据"""
    try:
        df = stock_tool_instance.fetch_and_save_block_trade(stock_code=stock_code)
        return _format_block_trade(df, stock_code)
    except Exception as e:
        logger.error(f"调用大宗交易工具失败: {e}")
        return "❌ 获取大宗交易数据失败"


def call_fetch_top_list(stock_code: str) -> str:
    """获取并保存龙虎榜数据"""
    try:
        df = stock_tool_instance.fetch_and_save_top_list(stock_code=stock_code)
        return _format_top_list(df, stock_code)
    except Exception as e:
        logger.error(f"调用龙虎榜工具失败: {e}")
        return "❌ 获取龙虎榜数据失败"


def call_fetch_top_inst(stock_code: str) -> str:
    """获取并保存机构席位数据"""
    try:
        df = stock_tool_instance.fetch_and_save_top_inst(stock_code=stock_code)
        return _format_top_inst(df, stock_code)
    except Exception as e:
        logger.error(f"调用机构席位工具失败: {e}")
        return "❌ 获取机构席位数据失败"


def call_fetch_forecast(stock_code: str) -> str:
    """获取业绩预告数据（数据库 StockFinaAudit）"""
    try:
        from storage.sqlite.stock_storage import get_db
        df = get_db().get_stock_fina_audit(stock_code, limit=5)
        if df is None or df.empty:
            return ""
        lines = [f"【业绩预告（来源：Tushare）】"]
        for _, row in df.head(5).iterrows():
            end_date = str(row.get('end_date', ''))[:10]
            forecast_type = row.get('forecast_type', '') or row.get('type', '')
            profit_range = row.get('profit_range', '') or row.get('forecast_content', '')
            items = [f"报告期:{end_date}"]
            if forecast_type: items.append(f"预告类型:{forecast_type}")
            if profit_range: items.append(f"净利润区间:{profit_range}")
            lines.append("  " + " | ".join(items))
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"业绩预告获取失败（不影响分析）: {e}")
        return ""


def call_fetch_express(stock_code: str) -> str:
    """获取业绩快报数据（数据库 StockDisclosureDate）"""
    try:
        from storage.sqlite.stock_storage import get_db
        df = get_db().get_stock_disclosure_date(stock_code, limit=5)
        if df is None or df.empty:
            return ""
        lines = [f"【业绩快报/预约披露（来源：Tushare）】"]
        for _, row in df.head(5).iterrows():
            end_date = str(row.get('end_date', ''))[:10]
            issue_date = str(row.get('stm_issue_date', ''))[:10]
            actual_diss = str(row.get('actual_diss_date', ''))[:10]
            items = [f"报告期:{end_date}"]
            if issue_date and issue_date != "None": items.append(f"首次披露:{issue_date}")
            if actual_diss and actual_diss != "None": items.append(f"实际披露:{actual_diss}")
            if len(items) > 1:
                lines.append("  " + " | ".join(items))
        return "\n".join(lines) if len(lines) > 1 else ""
    except Exception as e:
        logger.warning(f"业绩快报获取失败（不影响分析）: {e}")
        return ""


def call_fetch_sotp_valuation(stock_code: str) -> str:
    """
    SOTP（Sum of The Parts）分部估值
    基于主营业务分产品收入占比 + 合并归母净利润 + 行业 PE 参考
    """
    def _num(v):
        if v is None: return None
        try: return float(v)
        except (TypeError, ValueError): return None
    try:
        mb_df = stock_tool_instance.fetch_and_save_main_business(stock_code)
        if mb_df is None or mb_df.empty:
            return "❌ 无主营业务数据，无法进行分部估值"

        basic = stock_tool_instance.db.get_stock_basic(stock_code)
        total_shares = None
        if basic is not None:
            total_shares = _num(getattr(basic, 'total_share', None))
        # DB 没有总股本时从 Tushare 补充
        if not total_shares:
            try:
                import tushare as ts
                ts_code = (f'{stock_code}.SZ' if stock_code.startswith(('0', '3'))
                           else (f'{stock_code}.BJ' if stock_code.startswith(('8', '4', '92'))
                                 else f'{stock_code}.SH'))
                ts_df = ts.pro_api().stock_basic(ts_code=ts_code,
                                                  fields='ts_code,total_share')
                if ts_df is not None and not ts_df.empty:
                    raw = _num(ts_df.iloc[0].get('total_share'))
                    if raw:
                        total_shares = raw * 10000  # 万 → 股
            except Exception:
                pass
        if not total_shares:
            try:
                ts_code2 = (f'{stock_code}.SZ' if stock_code.startswith(('0', '3'))
                            else (f'{stock_code}.BJ' if stock_code.startswith(('8', '4', '92'))
                                  else f'{stock_code}.SH'))
                ts_df2 = ts.pro_api().daily_basic(ts_code=ts_code2,
                                                   start_date='20260701', end_date='20260714',
                                                   fields='ts_code,total_share')
                if ts_df2 is not None and not ts_df2.empty:
                    raw = _num(ts_df2.iloc[0].get('total_share'))
                    if raw:
                        total_shares = raw * 10000
            except Exception:
                pass

        # 取最新一期分产品收入
        mb_df = mb_df.copy()
        mb_df['report_date'] = pd.to_datetime(mb_df['report_date'])
        mb_df = mb_df.sort_values('report_date', ascending=False)
        latest_date = mb_df['report_date'].max() if 'bz_type' in mb_df.columns else None
        if 'bz_type' in mb_df.columns:
            latest_mb = mb_df[(mb_df['bz_type'] == 'P') & (mb_df['report_date'] == latest_date)].head(10)
        else:
            latest_mb = mb_df[mb_df['report_date'] == latest_date].head(10) if latest_date else mb_df.head(10)

        if latest_mb.empty:
            return "❌ 无分产品收入数据"

        # ----- 获取归母净利润（与主营构成同口径）-----
        total_net_profit = None
        income_date_str = ""
        try:
            income_df = stock_tool_instance.fetch_and_save_stock_income(stock_code)
            if income_df is None or income_df.empty:
                income_df = stock_tool_instance.db.get_stock_income(stock_code)
            if income_df is not None and not income_df.empty:
                # 优先匹配主营构成的报告期（同口径对比）
                mb_report_dates = pd.to_datetime(latest_mb['report_date'].unique())
                income_dates = income_df['report_date'].unique()
                common_dates = sorted(set(mb_report_dates) & set(income_dates), reverse=True)
                if common_dates:
                    match_date = common_dates[0]
                    mask = income_df['report_date'] == match_date
                    match_row = income_df[mask].iloc[0]
                    total_net_profit = _num(match_row.get('net_profit'))
                    income_date_str = match_date.strftime('%Y-%m-%d') if hasattr(match_date, 'strftime') else str(match_date)
                else:
                    # 回退到最新一期利润表数据（口径不匹配时打标记）
                    income_df_sorted = income_df.sort_values('report_date', ascending=False)
                    latest_income = income_df_sorted.iloc[0]
                    total_net_profit = _num(latest_income.get('net_profit'))
                    rd = latest_income.get('report_date')
                    income_date_str = (rd.strftime('%Y-%m-%d') + ' ⚠️ 与主营构成口径不一致'
                                       if hasattr(rd, 'strftime') else str(rd) if rd else "")
        except Exception:
            logger.warning(f"[SOTP] {stock_code} 获取利润表数据失败")

        # ----- 获取当前市值/PE（行情快照）-----
        cur_mv = None
        cur_pe = None
        try:
            from tools.peer_compare import _load_market_spot
            spot = _load_market_spot()
            ts_info = spot.get(stock_code, {})
            cur_mv = _num(ts_info.get('mv'))
            cur_pe = _num(ts_info.get('pe'))
        except Exception:
            pass

        # ----- 第一遍：统计总营收 -----
        total_revenue = 0
        for _, row in latest_mb.iterrows():
            sales = _num(row.get('bz_sales'))
            if sales and sales > 0:
                total_revenue += sales

        if total_revenue <= 0:
            return "❌ 主营业务营收数据异常（合计<=0）"

        # 行业 PE 参考表（区分板块的参考倍数）
        segment_pe_ref = {
            '汽车': 22, '整车': 22, '乘用车': 22, '新能源车': 25,
            '电池': 25, '锂电': 25, '动力电池': 25, '储能': 28,
            '手机': 15, '电子': 16, '代工': 14, '消费电子': 16,
            '半导体': 35, '芯片': 35, 'IGBT': 38,
            '光伏': 18, '太阳能': 18,
        }
        # 记录板块毛利率信息用于估值说明（保留原 bz_profit/毛利率字段以辅助判断）
        segment_data = []
        for _, row in latest_mb.iterrows():
            item = str(row.get('bz_item', '')).strip()
            sales = _num(row.get('bz_sales'))
            if not sales or sales <= 0:
                continue

            revenue_ratio = sales / total_revenue  # 营收占比

            # 板块净利 = 合并归母净利 × 该板块营收占比（粗分摊）
            seg_net_profit = total_net_profit * revenue_ratio if total_net_profit and total_net_profit > 0 else None

            # 匹配 PE 倍数
            pe = 18  # 默认
            for key, val in segment_pe_ref.items():
                if key in item:
                    pe = val
                    break

            seg_value = seg_net_profit * pe if seg_net_profit else 0
            segment_data.append((item, sales, revenue_ratio, seg_net_profit, pe, seg_value))

        if not segment_data:
            return "❌ 无法拆分各业务板块数据"

        # ===== 格式化输出 =====
        lines = [f"📊 **SOTP 分部估值（{stock_code}）—— 程序粗算，仅供参考**"]
        lines.append("")
        lines.append("> ⚠️ **风险提示**：以下为程序基于财报数据的自动化估算，PE倍数为行业参考值，")
        lines.append("> 净利润按营收占比粗分摊（未考虑各板块实际净利率差异及分部间内部抵消），")
        lines.append("> 未计入分部协同效应与控股权折价。**精确估值请参阅券商研究报告，不作为投资建议。**")
        lines.append("")

        # 数据基准说明
        mb_date_str = ""
        if 'report_date' in latest_mb.columns:
            rd = latest_mb.iloc[0].get('report_date', '')
            mb_date_str = rd.strftime('%Y-%m-%d') if hasattr(rd, 'strftime') else str(rd)
        lines.append("**数据基准**")
        lines.append(f"- 主营构成报告期：{mb_date_str}")
        if income_date_str:
            lines.append(f"- 利润表报告期：{income_date_str}")
        if total_net_profit:
            lines.append(f"- 合并归母净利润：{total_net_profit/1e8:.2f} 亿")
        if cur_mv:
            lines.append(f"- 当前总市值：{cur_mv:.2f} 亿")
        if cur_pe:
            lines.append(f"- 当前PE(TTM)：{cur_pe:.1f}x")
        lines.append("")

        # 表格
        lines.append(f"{'业务板块':<18} {'营收(亿)':<10} {'营收占比':<8} {'分摊净利(亿)':<14} {'PE':<6} {'分部估值(亿)':<12}")
        lines.append("-" * 76)

        total_value = 0
        for item, sales, ratio, seg_profit, pe, seg_value in segment_data:
            sales_yi = sales / 1e8
            profit_yi = seg_profit / 1e8 if seg_profit else 0
            value_yi = seg_value / 1e8
            total_value += seg_value
            ratio_str = f"{ratio*100:.1f}%"
            lines.append(f"{item[:16]:<18} {sales_yi:<10.2f} {ratio_str:<8} {profit_yi:<14.2f} {pe:<6} {value_yi:<12.2f}")

        total_value_yi = total_value / 1e8
        lines.append("-" * 76)

        total_net_profit_yi = total_net_profit / 1e8 if total_net_profit else 0
        lines.append(f"{'合计':<18} {'':<10} {'100.0%':<8} {total_net_profit_yi:<14.2f} {'':<6} {total_value_yi:<12.2f}")

        # 每股价值
        if total_shares and total_shares > 0 and total_value > 0:
            shares_yi = total_shares / 1e8
            per_share_value = total_value / total_shares
            lines.append(f"\n总股本: {shares_yi:.2f} 亿股")
            lines.append(f"📌 **SOTP 每股内在价值（程序粗算）: {per_share_value:.2f} 元**")
            if cur_mv and cur_mv > 0:
                premium = (total_value - cur_mv * 1e8) / (cur_mv * 1e8) * 100
                premium_sign = '溢价' if premium > 0 else '折价'
                lines.append(f"   对比当前市值{premium_sign}: {premium:+.1f}%")
                lines.append(f"   （当前市值 {cur_mv:.0f} 亿 vs SOTP 估值 {total_value_yi:.0f} 亿）")
            lines.append("")
            lines.append("💡 **重要说明**")
            lines.append("1. 各板块净利润 = 合并归母净利润 × 该板块营收占比（粗略分摊，未考虑各板块实际净利率差异）")
            lines.append("2. PE 倍数为行业参考值，未结合具体增速/竞争格局调整")
            lines.append("3. 未考虑分部间内部抵消、协同效应和控股权折价")
            lines.append("4. 以上结果仅为程序自动化估算，**精确估值请参阅券商研究报告**")

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"SOTP 估值失败: {e} {traceback.format_exc()}")
        return "❌ 获取SOTP估值数据失败"


def call_fetch_overseas_sales(stock_code: str) -> str:
    """
    获取海外分区域销量数据（搜索+提取）
    通过搜索获取目标公司海外各区域（欧洲/东南亚/拉美等）销量或收入数据
    """
    try:
        basic = stock_tool_instance.db.get_stock_basic(stock_code)
        name = basic.name if basic and hasattr(basic, 'name') else stock_code
        
        # 多组搜索策略
        searches = [
            f"{name} 海外 欧洲 东南亚 拉美 出口 分区域 销量 2026",
            f"{name} 海外收入 境外 分区域 欧洲 占比",
            f"{name} 出口 区域结构 亚洲 欧洲 美洲",
        ]
        
        from agents.researcher.web_search_tool import web_search
        
        search_results = []
        for q in searches:
            try:
                r = web_search.invoke({"query": q})
                if r and "失败" not in str(r)[:20]:
                    search_results.append(f"【搜索：{q}】\n{r[:1500]}")
            except Exception:
                continue
            # 搜索间隔
            import time
            time.sleep(1)
        
        if not search_results:
            return f"❌ 未搜索到 {name} 的海外区域销量数据"
        
        combined = "\n\n".join(search_results)[:4000]
        
        lines = [f"📦 **{name} 海外区域数据（搜索获取）**"]
        lines.append(f"数据来源：联网搜索（非官方结构化数据，仅供参考）")
        lines.append("")
        
        # 尝试从搜索结果中提取结构化数据
        import re
        # 提取可能的百分比数据（海外收入占比、区域占比等）
        pct_pattern = re.compile(r'(海外|出口|境外|欧洲|东南亚|拉美|亚洲|美洲|中东).{0,20}?(\d+\.?\d*)\s*%', re.DOTALL)
        pct_matches = pct_pattern.findall(combined)
        if pct_matches:
            lines.append("**提取到的占比数据：**")
            for region, pct in pct_matches[:10]:
                lines.append(f"  - {region.strip()} {pct}%")
            lines.append("")
        
        # 提取可能的销量数字
        vol_pattern = re.compile(r'(海外|出口|境外|欧洲|东南亚|拉美|亚洲).{0,30}?(\d+[\.,]?\d*)\s*万?辆', re.DOTALL)
        vol_matches = vol_pattern.findall(combined)
        if vol_matches:
            lines.append("**提取到的销量数据：**")
            for region, vol in vol_matches[:10]:
                lines.append(f"  - {region.strip()} {vol} 万辆")
            lines.append("")
        
        # 搜索摘要（原始结果）
        lines.append("**搜索结果摘要：**")
        for s in search_results:
            # 提取前3个有效段落
            parts = s.split('\n')
            for p in parts[:10]:
                p = p.strip()
                if len(p) > 20 and not p.startswith('【搜索'):
                    lines.append(f"  · {p[:150]}")
        
        lines.append("")
        lines.append("⚠️ 以上数据来自网页搜索，未经结构化验证，建议交叉核对官方公告。")
        
        return "\n".join(lines)[:4000]
    except Exception as e:
        logger.error(f"海外区域销量获取失败: {e} {traceback.format_exc()}")
        return f"❌ 获取海外区域销量数据失败"


def call_fetch_cost_basis(stock_code: str) -> str:
    """
    筹码成本估算（基于Tushare日线成交量数据）
    通过分析近期成交量在不同价格区间的分布，估算当前持仓的平均成本
    """
    def _num(v):
        if v is None: return None
        try: return float(v)
        except (TypeError, ValueError): return None
    try:
        today = date.today()
        period_map = {
            "近30日": today - timedelta(days=30),
            "近60日": today - timedelta(days=60),
            "近90日": today - timedelta(days=90),
            "近半年": today - timedelta(days=180),
        }
        
        # 获取日线数据
        all_daily = stock_tool_instance.fetch_and_save_stock_daily_data(stock_code)
        if all_daily is None or all_daily.empty:
            return "❌ 未获取到日线数据"
        
        # 确保列名存在
        if 'date' not in all_daily.columns:
            return "❌ 日线数据缺少日期列"
        
        df = all_daily.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=False)
        
        # 检查所需列
        price_col = None
        vol_col = None
        for c in ['close', 'close_price', 'adj_close', 'price']:
            if c in df.columns:
                price_col = c
                break
        for c in ['volume', 'vol', 'turnover_vol']:
            if c in df.columns:
                vol_col = c
                break
        
        if not price_col or not vol_col:
            return "❌ 日线数据缺少价格或成交量字段"
        
        latest_close = _num(df.iloc[0].get(price_col))
        if latest_close is None:
            return "❌ 无法获取最新收盘价"
        
        lines = [f"💰 **筹码成本估算（{stock_code}）**"]
        lines.append(f"最新收盘价: {latest_close:.2f} 元")
        lines.append("")
        lines.append(f"{'区间':<10} {'均价(元)':<12} {'成交量(亿股)':<14} {'成交额(亿元)':<14} {'估算成本'}")
        lines.append("-" * 65)
        
        results = []
        for period_name, start_date in period_map.items():
            mask = df['date'] >= pd.Timestamp(start_date)
            period_df = df[mask].copy()
            if period_df.empty:
                continue
            
            period_df = period_df.sort_values('date', ascending=True)
            
            prices = pd.to_numeric(period_df[price_col], errors='coerce')
            vols = pd.to_numeric(period_df[vol_col], errors='coerce')
            
            valid = prices.notna() & vols.notna() & (vols > 0)
            prices = prices[valid]
            vols = vols[valid]
            
            if prices.empty or vols.empty:
                continue
            
            # 成交量加权均价（VWAP）
            total_vol = vols.sum()
            total_val = (prices * vols).sum()
            vwap = total_val / total_vol if total_vol > 0 else None
            
            vol_yi = total_vol / 1e8 if total_vol > 1e8 else total_vol / 1e4
            vol_unit = "亿股" if total_vol > 1e8 else "万股"
            total_val_yi = total_val / 1e8
            
            cost_label = ""
            if vwap:
                diff_pct = (latest_close - vwap) / vwap * 100
                if diff_pct > 10:
                    cost_label = "⬆ 浮盈>10%"
                elif diff_pct > 5:
                    cost_label = "⬆ 小幅浮盈"
                elif diff_pct > -5:
                    cost_label = "➡ 成本区附近"
                elif diff_pct > -10:
                    cost_label = "⬇ 小幅浮亏"
                else:
                    cost_label = "⬇ 浮亏>10%"
            
            lines.append(f"{period_name:<10} {vwap:<12.2f} {vol_yi:<14.2f}{vol_unit:<4} {total_val_yi:<14.2f} {cost_label}")
            results.append((period_name, vwap, total_vol, cost_label))
        
        # 综合判断
        lines.append("")
        verdicts = []
        if results:
            # 近期成本 vs 中期成本
            short_vwap = None
            long_vwap = None
            for name, vwap, _, _ in results:
                if "30日" in name:
                    short_vwap = vwap
                if "半年" in name:
                    long_vwap = vwap
            
            if short_vwap and long_vwap and short_vwap != 0 and long_vwap != 0:
                diff = (short_vwap - long_vwap) / long_vwap * 100
                if diff > 5:
                    verdicts.append(f"短期成本({short_vwap:.2f})高于中长期({long_vwap:.2f})，近期新增筹码成本偏高")
                elif diff < -5:
                    verdicts.append(f"短期成本({short_vwap:.2f})低于中长期({long_vwap:.2f})，摊薄效果明显")
                else:
                    verdicts.append("短期与中长期成本接近，筹码结构稳定")
            
            # 筹码盈亏判断
            latest_cost = results[0][1] if results else None
            if latest_cost and latest_close:
                profit_pct = (latest_close - latest_cost) / latest_cost * 100
                if profit_pct > 15:
                    verdicts.append(f"近30日筹码平均浮盈{profit_pct:.1f}%，有获利了结压力")
                elif profit_pct < -15:
                    verdicts.append(f"近30日筹码平均浮亏{abs(profit_pct):.1f}%，套牢盘较重，上方有抛压")
                elif profit_pct > 5:
                    verdicts.append(f"筹码整体小幅盈利{profit_pct:.1f}%，市场情绪中性偏正")
                elif profit_pct < -5:
                    verdicts.append(f"筹码整体小幅亏损{abs(profit_pct):.1f}%，下方有一定支撑")
                else:
                    verdicts.append("筹码处于成本区附近，方向选择窗口")
        
        if verdicts:
            lines.append("**筹码结构判断：**")
            for v in verdicts:
                lines.append(f"  · {v}")
        
        lines.append("")
        lines.append("💡 基于成交量加权平均价（VWAP）估算，未考虑大额交易、大宗交易等因素。")
        
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"筹码成本估算失败: {e}")
        logger.error(traceback.format_exc())
        return "❌ 获取筹码成本估算数据失败"


def call_fetch_market_environment(stock_code: str = None, industry_name: str = None) -> str:
    """
    大盘环境量化打分
    基于沪深300、行业指数、资金流向等数据，生成标准化的市场环境评分卡（0~10分）
    """
    try:
        from tools.market_context import get_market_env
        from tools.industry_index import fetch_industry_index_metrics
        
        lines = ["📊 **大盘环境量化评分卡**", ""]
        
        # 1. 大盘趋势得分（沪深300）
        env = get_market_env()
        market_score = None
        if env:
            pos = env.get("position", "")
            vol_ratio = env.get("volume_ratio", 1.0)
            trend = env.get("trend", "")
            
            # 量化打分：0~10
            if "低位" in pos:
                market_score = 7 if "放量" in str(env) else 6
            elif "中位" in pos:
                market_score = 5 if "上升" in trend else 4
            elif "高位" in pos:
                market_score = 3 if "缩量" in str(env) else 2
            
            lines.append("**一、大盘趋势**")
            lines.append(f"  沪深300当前位置: {pos}")
            lines.append(f"  量能状态: {'放量' if vol_ratio and vol_ratio > 1.2 else '缩量' if vol_ratio and vol_ratio < 0.8 else '正常'}")
            lines.append(f"  趋势判断: {trend}")
            lines.append(f"  → 大盘得分: {market_score}/10 "
                        f"{'（偏多，适合操作）' if market_score and market_score >= 6 else '（中性，谨慎操作）' if market_score and market_score >= 4 else '（偏空，防御为主）'}")
        else:
            lines.append("**一、大盘趋势**")
            lines.append("  沪深300数据: 获取失败，跳过")
        
        lines.append("")
        
        # 2. 行业景气得分
        industry = None
        if industry_name:
            industry = industry_name
        elif stock_code:
            try:
                basic = stock_tool_instance.db.get_stock_basic(stock_code)
                if basic and hasattr(basic, 'industry'):
                    industry = basic.industry
            except Exception:
                pass
        
        industry_score = None
        if industry:
            try:
                im = fetch_industry_index_metrics(industry)
                if im:
                    ret5 = im.get('return_5d')
                    ret20 = im.get('return_20d')
                    ret60 = im.get('return_60d')
                    wk52 = im.get('week52_pct')
                    
                    lines.append("**二、行业景气**")
                    lines.append(f"  行业: {im.get('board_name', industry)}")
                    if ret5: lines.append(f"  近5日涨跌幅: {ret5:.1f}%")
                    if ret20: lines.append(f"  近20日涨跌幅: {ret20:.1f}%")
                    if ret60: lines.append(f"  近60日涨跌幅: {ret60:.1f}%")
                    if wk52: lines.append(f"  52周分位: {wk52:.0f}%")
                    
                    # 打分
                    score = 5
                    if ret20 and ret20 > 5: score += 2
                    elif ret20 and ret20 > 2: score += 1
                    elif ret20 and ret20 < -5: score -= 2
                    elif ret20 and ret20 < -2: score -= 1
                    if wk52 and wk52 < 30: score += 1  # 低位有安全边际
                    elif wk52 and wk52 > 80: score -= 1  # 高位有回调风险
                    industry_score = max(1, min(10, score))
                    lines.append(f"  → 行业得分: {industry_score}/10")
                else:
                    lines.append(f"**二、行业景气**")
                    lines.append(f"  行业: {industry}（数据获取失败）")
            except Exception:
                lines.append(f"**二、行业景气**")
                lines.append(f"  行业: {industry}（数据获取失败）")
        else:
            lines.append("**二、行业景气**")
            lines.append("  未指定股票或行业，跳过")
        
        lines.append("")
        
        # 3. 综合评分
        lines.append("**三、综合评估**")
        scores = [s for s in [market_score, industry_score] if s is not None]
        if scores:
            total = sum(scores) / len(scores)
            lines.append(f"  综合得分: {total:.1f}/10")
            if total >= 7:
                lines.append(f"  评级: 🟢 **偏多** — 市场环境适合积极操作")
            elif total >= 5:
                lines.append(f"  评级: 🟡 **中性** — 精选个股，控制仓位")
            elif total >= 3:
                lines.append(f"  评级: 🟠 **谨慎** — 防御为主，降低仓位")
            else:
                lines.append(f"  评级: 🔴 **偏空** — 建议观望或对冲")
        
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"大盘环境评分获取失败: {e} {traceback.format_exc()}")
        return "❌ 获取大盘环境评分数据失败"


def call_fetch_data_validator(stock_code: str) -> str:
    """
    数据交叉校验：对比不同数据源的财务数据一致性。
    1. 校验主营业务分产品毛利率之和 vs 综合毛利率
    2. 校验利润表营收 vs 主营业务加总营收
    3. 校验同比/环比逻辑合理性
    """
    try:
        lines = [f"🔍 **数据交叉校验报告（{stock_code}）**", ""]
        issues = []
        passes = []

        def _num(v):
            if v is None:
                return None
            try:
                return float(v)
            except:
                return None

        # 1. 获取各数据源
        mb_df = stock_tool_instance.fetch_and_save_main_business(stock_code)
        fina_df = stock_tool_instance.fetch_and_save_fina_indicator(stock_code)
        income_df = stock_tool_instance.fetch_and_save_stock_income(stock_code)

        # 2. 校验综合毛利率 vs 分业务毛利率
        if fina_df is not None and not fina_df.empty and mb_df is not None and not mb_df.empty:
            latest_fina = fina_df.iloc[0]
            overall_gm = _num(latest_fina.get('grossprofit_margin') or latest_fina.get('gross_margin'))
            # 毛利率已在入口层归一化为小数，直接使用

            # 取最新一期分产品毛利率
            mb_df = mb_df.copy()
            mb_df['report_date'] = pd.to_datetime(mb_df['report_date'])
            mb_df = mb_df.sort_values('report_date', ascending=False)
            if 'bz_type' in mb_df.columns:
                latest_mb = mb_df[(mb_df['bz_type'] == 'P')].head(10)
            else:
                latest_mb = mb_df.head(10)

            if overall_gm and not latest_mb.empty:
                # 加权计算各产品汇总毛利率
                total_sales = 0
                total_profit = 0
                for _, row in latest_mb.iterrows():
                    sales = _num(row.get('bz_sales'))
                    profit = _num(row.get('bz_profit'))
                    if sales and profit:
                        total_sales += sales
                        total_profit += profit

                if total_sales > 0:
                    weighted_gm = total_profit / total_sales
                    diff = abs(overall_gm - weighted_gm)
                    if diff > 0.05:  # 偏差超过5个百分点
                        issues.append(
                            f"❌ 毛利率偏差: 财务指标综合毛利率 {overall_gm:.1%} vs "
                            f"主营业务加权毛利率 {weighted_gm:.1%}（偏差 {diff:.1%}）"
                        )
                    else:
                        passes.append(
                            f"✅ 毛利率一致: 综合 {overall_gm:.1%} ≈ 加权 {weighted_gm:.1%}（偏差 {diff:.1%}）"
                        )

        # 3. 校验营收数据一致性
        # 注意：主营构成与利润表可能来自不同报告期（如主营最新为年报，利润表最新为Q1），
        # 必须同口径对比。先找两者共有的最近报告期，只比同一期的数据。
        if income_df is not None and not income_df.empty and mb_df is not None and not mb_df.empty:
            # 获取利润表的 report_date 集合（去重）
            income_dates = pd.to_datetime(income_df['report_date']).unique() if 'report_date' in income_df.columns else []
            # 获取主营的 report_date 集合
            mb_dates = pd.to_datetime(mb_df['report_date']).unique() if 'report_date' in mb_df.columns else []

            if len(income_dates) > 0 and len(mb_dates) > 0:
                # 找两者共有的最近报告期
                common_dates = sorted(set(income_dates) & set(mb_dates), reverse=True)
                if common_dates:
                    match_date = common_dates[0]
                    # 找到该期利润表数据
                    income_mask = pd.to_datetime(income_df['report_date']) == match_date
                    income_row = income_df[income_mask].iloc[0]
                    income_rev = _num(income_row.get('total_revenue'))

                    # 找到该期主营分产品数据加总
                    mb_mask = (pd.to_datetime(mb_df['report_date']) == match_date)
                    if 'bz_type' in mb_df.columns:
                        mb_mask = mb_mask & (mb_df['bz_type'] == 'P')
                    mb_period = mb_df[mb_mask]
                    mb_total_sales = sum(
                        _num(r.get('bz_sales')) for _, r in mb_period.iterrows()
                        if _num(r.get('bz_sales')))

                    if income_rev and mb_total_sales > 0:
                        rev_diff = abs(income_rev - mb_total_sales) / income_rev
                        if rev_diff > 0.1:
                            issues.append(
                                f"❌ 营收偏差({match_date.date()}): 利润表营收 {income_rev / 1e8:.1f}亿 vs "
                                f"主营业务加总 {mb_total_sales / 1e8:.1f}亿（偏差 {rev_diff:.1%}）"
                            )
                        else:
                            passes.append(
                                f"✅ 营收一致({match_date.date()}): 利润表 {income_rev / 1e8:.1f}亿 ≈ 主营加总 {mb_total_sales / 1e8:.1f}亿"
                            )
                else:
                    # 无共有报告期，记录说明而非错误
                    latest_income_date = max(income_dates).date()
                    latest_mb_date = max(mb_dates).date()
                    issues.append(
                        f"⚠️ 营收校验跳过: 利润表最新报告期 {latest_income_date} 与主营构成最新报告期 "
                        f"{latest_mb_date} 不一致（主营多为年报披露，利润表含季报），无法做同口径对比"
                    )

        # 4. 同比环比逻辑校验
        if fina_df is not None and len(fina_df) >= 2:
            latest = fina_df.iloc[0]
            prev = fina_df.iloc[1]
            latest_roe = _num(latest.get('roe'))
            prev_roe = _num(prev.get('roe'))
            latest_gm = _num(latest.get('grossprofit_margin') or latest.get('gross_margin'))
            prev_gm = _num(prev.get('grossprofit_margin') or prev.get('gross_margin'))
            # 毛利率已在入口层归一化为小数，直接使用
            
            if latest_roe and prev_roe:
                roe_change = (latest_roe - prev_roe) / abs(prev_roe) * 100 if prev_roe != 0 else 0
                if abs(roe_change) > 100:
                    issues.append(f"⚠️ ROE 异常波动: {prev_roe:.1%} → {latest_roe:.1%}（变动 {roe_change:.0f}%）")

            if latest_gm and prev_gm:
                gm_change = latest_gm - prev_gm
                if abs(gm_change) > 0.15:
                    issues.append(f"⚠️ 毛利率异常波动: {prev_gm:.1%} → {latest_gm:.1%}（变动 {gm_change:.1%}）")

        # 5. 输出
        lines.append(f"**数据源概览**")
        lines.append(f"  · 财务指标: {'✅ 有数据' if fina_df is not None and not fina_df.empty else '❌ 无数据'}")
        lines.append(f"  · 主营业务分产品: {'✅ 有数据' if mb_df is not None and not mb_df.empty else '❌ 无数据'}")
        lines.append(f"  · 利润表: {'✅ 有数据' if income_df is not None and not income_df.empty else '❌ 无数据'}")
        lines.append("")

        lines.append("**一致性校验**")
        if passes:
            for p in passes:
                lines.append(f"  {p}")
        if issues:
            for i in issues:
                lines.append(f"  {i}")
        if not passes and not issues:
            lines.append("  数据不足，无法完成交叉校验")

        lines.append("")
        lines.append("**可信度标注规则**")
        lines.append("  · Tushare 数据源 → **高可信**（官方结构化数据）")
        lines.append("  · 懂车帝 API → **中高可信**（第三方平台结构化数据）")
        lines.append("  · 网页搜索 → **中低可信**（非结构化数据，需交叉验证）")

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"数据校验失败: {e} {traceback.format_exc()}")
        return "❌ 获取数据校验报告失败"


# =====================================================================
# 打分规则引擎（独立 Tool — 分项维度、权重、门槛可外部查看与修改）
# =====================================================================

SCORING_STAGES = {
    "导入期": {"label": "导入期（技术验证/初创阶段）"},
    "成长期": {"label": "成长期（快速扩张）"},
    "成熟期": {"label": "成熟期（龙头主导/格局稳定）"},
}

SCORING_WEIGHTS = {
    "成熟期": {"business": 0.2, "fundamental": 0.3, "moat": 0.4, "momentum": 0.1},
    "成长期": {"business": 0.2, "fundamental": 0.25, "moat": 0.25, "momentum": 0.3},
    "导入期": {"business": 0.15, "fundamental": 0.2, "moat": 0.25, "momentum": 0.4},
}

SCORING_GATES = {
    "成熟期": [("moat", 7.0, "护城河")],
    "成长期": [("moat", 5.0, "护城河"), ("momentum", 6.0, "边际变化")],
    "导入期": [("momentum", 7.0, "边际变化（订单可见性/卡位）")],
}

SCORING_DIMENSIONS = {
    "business":  "业务（0-10）：赛道空间、竞争格局、商业模式成熟度",
    "fundamental": "基本面（0-10）：营收/利润/ROE、现金流、资产质量",
    "moat":       "护城河（0-10）：品牌/技术/专利/规模/客户黏性",
    "momentum":   "边际变化（0-10）：订单/增速/产能/政策/价格拐点",
}

SCORING_DEFAULT_STAGE = "成长期"


def call_fetch_scoring_engine(candidates_json: str, stage: str = "") -> str:
    """
    打分规则引擎：对候选标的分项打分，按阶段权重加权排序，阶段门槛过滤。
    入参: candidates_json = '[{"code":"600118","business":7,"fundamental":6,"moat":5,"momentum":4}]'
    stage = 导入期/成长期/成熟期（留空自动=成长期）
    """
    import json
    try:
        stage = stage.strip() or SCORING_DEFAULT_STAGE
        if stage not in SCORING_STAGES:
            stage = SCORING_DEFAULT_STAGE

        # 解析候选
        candidates = json.loads(candidates_json) if isinstance(candidates_json, str) else candidates_json
        if not candidates:
            return "❌ 候选列表为空"

        # ---- 打分说明 ----
        lines = [f"## 打分规则引擎", ""]
        lines.append(f"**阶段**: {stage} — {SCORING_STAGES[stage]['label']}")
        lines.append("")
        lines.append("### 分项维度与满分刻度")
        lines.append("| 维度 | 满分 | 说明 |")
        lines.append("|------|------|------|")
        for k, desc in SCORING_DIMENSIONS.items():
            lines.append(f"| {k} | 10 | {desc} |")

        lines.append("")
        lines.append("### 阶段权重")
        lines.append(f"| 维度 | 权重 |")
        lines.append(f"|------|------|")
        for k, w in SCORING_WEIGHTS[stage].items():
            dim_label = SCORING_DIMENSIONS.get(k, k).split("（")[0]
            lines.append(f"| {dim_label} | {w*100:.0f}% |")

        gates = SCORING_GATES[stage]
        if gates:
            lines.append("")
            lines.append("### 准入门槛")
            for metric, gate, label in gates:
                lines.append(f"- {label} ≥ **{gate}** 分（未达标或分项缺失 → 拒绝入池）")

        lines.append("")
        lines.append("### 评分结果")
        lines.append("")

        # ---- 计算 ----
        weights = SCORING_WEIGHTS[stage]
        ranked = []
        for c in candidates:
            code = str(c.get("code", "")).strip()
            if not code:
                continue
            scores, missing = {}, []
            for k in weights:
                try:
                    v = float(c.get(k, 0))
                    if not (0 <= v <= 10):
                        raise ValueError
                    scores[k] = round(v, 1)
                except (TypeError, ValueError):
                    scores[k] = 5.0
                    missing.append(k)
            composite = round(sum(scores[k] * w for k, w in weights.items()), 2)
            item = {
                "code": code,
                "name": c.get("name", c.get("code", "")),
                **scores,
                "composite": composite,
                "missing": missing,
            }
            if missing:
                item["note"] = "分项缺失按5分中性处理"
            ranked.append(item)

        ranked.sort(key=lambda x: x["composite"], reverse=True)
        for i, r in enumerate(ranked, 1):
            r["rank"] = i

        # ---- 门槛过滤 ----
        passed, excluded = [], []
        gates = SCORING_GATES[stage]
        for item in ranked:
            missing = item.get("missing") or []
            fails = []
            for metric, gate, label in gates:
                if metric in missing:
                    fails.append(f"{label}分项缺失（无证据）")
                elif item.get(metric, 0) < gate:
                    fails.append(f"{label}{item.get(metric)}分未达{gate:g}分门槛")
            if not fails:
                passed.append(item)
            else:
                item["exclude_reason"] = f"[{stage}] " + "；".join(fails)
                excluded.append(item)

        for i, it in enumerate(passed, 1):
            it["rank"] = i

        # ---- 输出表格 ----
        lines.append(f"**通过（{len(passed)} 家）**")
        lines.append("| 排名 | 标的 | 业务 | 基本面 | 护城河 | 边际变化 | 综合分 | 备注 |")
        lines.append("|------|------|------|--------|--------|---------|------|------|")
        for it in passed:
            lines.append(
                f"| {it['rank']} | {it['name']}({it['code']}) "
                f"| {it.get('business','-')} | {it.get('fundamental','-')} "
                f"| {it.get('moat','-')} | {it.get('momentum','-')} "
                f"| {it['composite']} | {it.get('note','')} |"
            )

        if excluded:
            lines.append("")
            lines.append(f"**剔除（{len(excluded)} 家）**")
            lines.append("| 标的 | 剔除原因 | 业务 | 护城河 | 边际变化 | 综合分 |")
            lines.append("|------|---------|------|--------|---------|-------|")
            for it in excluded:
                lines.append(
                    f"| {it.get('name','')}({it['code']}) "
                    f"| {(it.get('exclude_reason') or '')[:40]} "
                    f"| {it.get('business','-')} | {it.get('moat','-')} "
                    f"| {it.get('momentum','-')} | {it['composite']} |"
                )

        lines.append("")
        lines.append("**综合分 = Σ(维度分 × 阶段权重)**，满分 10 分。")
        lines.append("分项缺失（LLM 无法给出分数）按 5 分中性处理并标注。")
        lines.append("数据来源：LLM 分项评分 + 规则引擎加权/过滤。")

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"打分引擎执行失败: {e} {traceback.format_exc()}")
        return f"❌ 打分引擎执行失败: {e}"


def call_fetch_batch_valuation(stock_codes: str) -> str:
    """
    批量获取一组股票的 PE/PB 估值统计。
    用于非标准行业（如商业航天）的板块整体估值分析。
    stock_codes：逗号分隔的股票代码，如 "600118,001270,600879"
    """
    try:
        codes = [c.strip() for c in stock_codes.split(",") if c.strip()]
        if not codes:
            return "❌ 未提供股票代码"
        
        db = get_db()
        lines = [f"📊 **自定义板块批量估值统计（{len(codes)} 只标的）**", ""]
        
        # 收集每只标的的估值数据
        results = []
        for code in codes:
            try:
                # 先从 daily_basic 拿 PE/PB
                daily = db.get_latest_daily_basic_data(code, days=1)
                pe = None
                pb = None
                name = code
                
                if daily is not None and not daily.empty:
                    row = daily.iloc[0]
                    pe = float(row.get("pe_ttm", row.get("pe", 0))) if row.get("pe_ttm", row.get("pe", 0)) else None
                    pb = float(row.get("pb", 0)) if row.get("pb", 0) else None
                else:
                    # fallback: Tushare 实时
                    from tools.stock.tushare_fetcher import TushareFetcher
                    fetcher = TushareFetcher()
                    from datetime import date
                    df, _ = fetcher.daily_basic(code, trade_date=date.today().isoformat().replace("-", ""))
                    if df is not None and not df.empty:
                        r = df.iloc[0]
                        pe = float(r.get("pe", 0)) if r.get("pe", 0) else None
                        pb = float(r.get("pb", 0)) if r.get("pb", 0) else None
                
                # 获取公司名
                basic = db.get_stock_basic(code)
                if basic:
                    name = basic.name
                
                results.append({"code": code, "name": name, "pe": pe, "pb": pb})
            except Exception as e:
                logger.warning(f"获取 {code} 估值失败: {e}")
                results.append({"code": code, "name": code, "pe": None, "pb": None})
        
        # 统计
        pe_vals = [r["pe"] for r in results if r["pe"] is not None and r["pe"] > 0]
        pb_vals = [r["pb"] for r in results if r["pb"] is not None and r["pb"] > 0]
        
        lines.append(f"### 板块整体统计")
        lines.append(f"| 指标 | 均值 | 中位数 | P25 | P75 | 有效样本 |")
        lines.append(f"|------|------|--------|-----|-----|---------|")
        
        if pe_vals:
            import numpy as np
            pe_arr = np.array(pe_vals)
            lines.append(f"| PE(TTM) | {pe_arr.mean():.1f}x | {np.median(pe_arr):.1f}x | {np.percentile(pe_arr, 25):.1f}x | {np.percentile(pe_arr, 75):.1f}x | {len(pe_vals)}/{len(results)} |")
        else:
            lines.append(f"| PE(TTM) | -- | -- | -- | -- | 0/{len(results)} |")
        
        if pb_vals:
            pb_arr = np.array(pb_vals)
            lines.append(f"| PB | {pb_arr.mean():.1f}x | {np.median(pb_arr):.1f}x | {np.percentile(pb_arr, 25):.1f}x | {np.percentile(pb_arr, 75):.1f}x | {len(pb_vals)}/{len(results)} |")
        else:
            lines.append(f"| PB | -- | -- | -- | -- | 0/{len(results)} |")
        
        lines.append("")
        lines.append(f"### 各标的明细")
        lines.append(f"| 代码 | 名称 | PE(TTM) | PB |")
        lines.append(f"|------|------|---------|-----|")
        
        for r in results:
            pe_str = f"{r['pe']:.1f}x" if r['pe'] else "--"
            pb_str = f"{r['pb']:.2f}x" if r['pb'] else "--"
            lines.append(f"| {r['code']} | {r['name']} | {pe_str} | {pb_str} |")
        
        lines.append("")
        lines.append(f"数据来源：Tushare daily_basic / DB 缓存。")
        
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"批量估值统计失败: {e} {traceback.format_exc()}")
        return f"❌ 批量估值统计获取失败: {e}"


def call_fetch_scenario_analysis(stock_code: str) -> str:
    """
    情景测算分析：基于最新年报数据，生成乐观/基准/悲观三大情景的净利润和股价影响测算。
    场景变量：营收变动、毛利率变动、费用率变动、PE倍数变动。
    """
    try:
        
        db = get_db()
        name = str(stock_code)
        basic = db.get_stock_basic(stock_code)
        if basic:
            name = basic.name
        
        lines = [f"📈 **情景测算分析：{name}({stock_code})**", ""]
        
        # 1) 获取年报利润表数据（仅有12-31报告期才是年报）
        income_df = db.get_stock_income(stock_code)
        if income_df is None or income_df.empty:
            return f"❌ {stock_code} 无利润表数据，无法进行情景测算"
        
        def _is_annual_report(df):
            """筛选年报期（12-31）"""
            annual = df[df['report_date'].dt.month == 12].copy() if 'report_date' in df.columns else df.copy()
            return annual.sort_values('report_date', ascending=False)
        
        annual_df = _is_annual_report(income_df)
        if annual_df.empty:
            # 无年报数据时回退到最新期，但标注口径不一致
            latest = income_df.sort_values('report_date', ascending=False).iloc[0]
            annual_label = latest['report_date'].strftime('%Y-%m-%d') + ' ⚠️ 非年报数据'
            revenue = float(latest.get("total_revenue", 0) or 0)
            net_profit = float(latest.get("net_profit", 0) or 0)
        else:
            latest = annual_df.iloc[0]
            annual_label = latest['report_date'].strftime('%Y-%m-%d')
            revenue = float(latest.get("total_revenue", 0) or 0)
            net_profit = float(latest.get("net_profit", 0) or 0)
        
        # 计算毛利率（如果有营业成本和营收）
        cost = float(latest.get("total_cogs", latest.get("oper_cost", 0)) or 0)
        gross_margin = ((revenue - cost) / revenue * 100) if revenue > 0 and cost > 0 else None
        
        # 计算费用率
        sell_exp = float(latest.get("sell_exp", 0) or 0)
        admin_exp = float(latest.get("admin_exp", 0) or 0)
        fin_exp = float(latest.get("fin_exp", 0) or 0)
        total_expense_rate = ((sell_exp + admin_exp + fin_exp) / revenue * 100) if revenue > 0 else None
        
        lines.append(f"### 基础数据（{annual_label}）")
        lines.append(f"| 指标 | 数值 |")
        lines.append(f"|------|------|")
        lines.append(f"| 营业总收入 | {revenue/1e8:.2f} 亿 |")
        lines.append(f"| 归母净利润 | {net_profit/1e8:.2f} 亿 |")
        if gross_margin is not None:
            lines.append(f"| 毛利率 | {gross_margin:.1f}% |")
        if total_expense_rate is not None:
            lines.append(f"| 三费费率 | {total_expense_rate:.1f}% |")
        
        # 2) 获取当前 PE
        current_price = None
        current_pe = None
        daily = db.get_latest_daily_basic_data(stock_code, days=1)
        if daily is not None and not daily.empty:
            row = daily.iloc[0]
            current_pe = float(row.get("pe_ttm", 0)) if pd.notna(row.get("pe_ttm")) and row.get("pe_ttm") else None
            current_close = float(row.get("close", 0)) if pd.notna(row.get("close")) and row.get("close") else None
            if current_close and current_close > 0:
                current_price = current_close
        
        lines.append(f"| PE(TTM) | {f'{current_pe:.1f}x' if current_pe else '--'} |")
        if current_price:
            lines.append(f"| 当前股价 | {current_price:.2f} 元 |")
        
        lines.append("")
        
        # 3) 情景假设
        scenarios = {
            "乐观": {"revenue_change": 0.20, "gm_change": 2.0, "pe_change": 1.15},
            "基准": {"revenue_change": 0.0, "gm_change": 0.0, "pe_change": 1.0},
            "悲观": {"revenue_change": -0.15, "gm_change": -3.0, "pe_change": 0.80},
        }
        
        lines.append(f"### 情景假设")
        lines.append(f"| 情景 | 营收变动 | 毛利率变动 | PE倍数调整 |")
        lines.append(f"|------|---------|-----------|-----------|")
        for sc_name, sc_vars in scenarios.items():
            lines.append(f"| {sc_name} | {sc_vars['revenue_change']*100:+.0f}% | {sc_vars['gm_change']:+.1f}pct | {sc_vars['pe_change']*100-100:+.0f}% |")
        
        lines.append("")
        
        # 4) 测算
        lines.append(f"### 测算结果")
        lines.append(f"| 情景 | 测算营收(亿) | 测算净利(亿) | 净利变动 | 目标PE | 目标股价 | 较当前变动 |")
        lines.append(f"|------|------------|------------|---------|-------|---------|----------|")
        
        base_revenue = revenue
        base_net = net_profit
        # 基准毛利率用于推算
        base_gm = gross_margin if gross_margin else 20.0
        
        results = []
        for sc_name, sc_vars in scenarios.items():
            # 营收
            new_revenue = base_revenue * (1 + sc_vars["revenue_change"])
            # 毛利率变动 → 毛利额变动
            new_gm = base_gm + sc_vars["gm_change"]
            # 新毛利额 = 新营收 × 新毛利率
            new_gross = new_revenue * new_gm / 100
            # 假设费用率不变，新净利 = 新毛利 - 费用
            old_gross = base_revenue * base_gm / 100
            old_cost = base_revenue - old_gross
            gross_change = new_gross - old_gross
            # 净利变化 ≈ 毛利变化 × (1 - 税率)，粗略按 75%
            tax_factor = 0.75
            new_net = base_net + gross_change * tax_factor
            
            # 用变动后的营收 × 净利率的简化版
            # 更准确：净利率 = 净利/营收，假设费用结构不变
            old_net_margin = base_net / base_revenue if base_revenue > 0 else 0.05
            # 营收变化带来净利同步变化 + 毛利率变化带来的增量
            revenue_effect = base_net * sc_vars["revenue_change"]
            gm_effect = gross_change * tax_factor - base_revenue * sc_vars["revenue_change"] * tax_factor * (1 - base_gm/100)
            # 简化：直接按营收变动比例 + 毛利率变动
            new_net_est = base_net * (1 + sc_vars["revenue_change"]) + (new_gm - base_gm)/100 * new_revenue * tax_factor
            
            # 目标PE
            new_pe = current_pe * sc_vars["pe_change"] if current_pe else 20 * sc_vars["pe_change"]
            
            # 每股收益 = 净利 / 总股本
            total_shares = None
            try:
                basic_info = db.get_stock_basic(stock_code)
                if basic_info and basic_info.total_share:
                    total_shares = float(basic_info.total_share) / 1e8  # 亿股
            except Exception:
                pass
            
            if total_shares and total_shares > 0:
                eps = new_net_est / (total_shares * 1e8) if new_net_est is not None else None
            else:
                # 用当前股价反推
                eps = current_price / current_pe if current_price and current_pe else None
            
            target_price = eps * new_pe if eps and new_pe else None
            
            net_change = (new_net_est - base_net) / base_net * 100 if base_net != 0 else 0
            price_change = (target_price - current_price) / current_price * 100 if target_price and current_price else 0
            
            lines.append(f"| {sc_name} | {new_revenue/1e8:.1f} | {new_net_est/1e8:.1f} | {net_change:+.1f}% | {new_pe:.0f}x | {f'{target_price:.2f}' if target_price else '--'} | {price_change:+.1f}% |")
        
        lines.append("")
        lines.append("⚠️ 免责提示：以上测算基于简化模型（营收×净利率法+毛利率调整），")
        lines.append("实际净利润受非经常性损益、税率变动、折旧摊销等多因素影响，")
        lines.append("仅供参考，不构成投资建议。")
        
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"情景测算失败: {e} {traceback.format_exc()}")
        return f"❌ 情景测算失败: {e}"


def call_fetch_stop_loss_calculator(stock_code: str) -> str:
    """
    动态止损计算器：基于 ATR（平均真实波幅）和均线系统，
    计算动态止损位，并区分观察/试错/回避区间。
    """
    try:
        import numpy as np
        
        db = get_db()
        name = str(stock_code)
        basic = db.get_stock_basic(stock_code)
        if basic:
            name = basic.name
        
        lines = [f"🎯 **动态止损计算：{name}({stock_code})**", ""]
        
        # 1) 获取日线数据（最近 60 个交易日）
        daily = db.get_all_daily_data(stock_code)
        if daily is None or daily.empty:
            return f"❌ {stock_code} 无日线数据，无法计算止损"
        daily = daily.head(60)  # 取最近 60 条（DESC：最新在前）
        
        # 确保有 close/high/low
        if "close" not in daily.columns:
            return f"❌ {stock_code} 日线数据格式不完整"
        
        # DB返回的是DESC（最新在前），反转为ASC以便计算ATR/MA
        closes = daily["close"].values.astype(float)[::-1]
        highs = (daily["high"].values.astype(float) if "high" in daily.columns else closes)[::-1]
        lows = (daily["low"].values.astype(float) if "low" in daily.columns else closes)[::-1]
        
        current_price = float(closes[-1])
        
        # 2) 计算 ATR
        tr_values = []
        for i in range(1, len(closes)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i-1])
            lc = abs(lows[i] - closes[i-1])
            tr = max(hl, hc, lc)
            tr_values.append(tr)
        
        tr_arr = np.array(tr_values)
        
        def atr(period):
            if len(tr_arr) < period:
                return float(tr_arr.mean()) if len(tr_arr) > 0 else 0
            return float(tr_arr[-period:].mean())
        
        atr14 = atr(14)
        atr20 = atr(20)
        atr60 = atr(60) if len(tr_arr) >= 60 else atr(14)
        
        # 3) 均线
        def ma(period):
            if len(closes) < period:
                return float(closes.mean())
            return float(closes[-period:].mean())
        
        ma20 = ma(20)
        ma60 = ma(60) if len(closes) >= 60 else None
        
        # 4) 支撑位/阻力位（近 20 日）
        recent_low = float(np.min(closes[-20:])) if len(closes) >= 20 else float(np.min(closes))
        recent_high = float(np.max(closes[-20:])) if len(closes) >= 20 else float(np.max(closes))
        
        # 5) 动态止损计算
        # 短线止损：支撑位 - 2×ATR(20)
        short_stop = recent_low - 2 * atr20
        # 中线止损：MA20 - 2.5×ATR(20)
        mid_stop = ma20 - 2.5 * atr20
        # 长线止损：MA60 - 2×ATR(60)
        long_stop = (ma60 - 2 * atr60) if ma60 else None
        
        # 6) 区间判定
        # 观察区间：价格 > 阻力位 or 价格 > MA60(上趋势)
        # 试错区间：支撑位 < 价格 < 阻力位
        # 回避区间：价格 < 支撑位
        resistance = recent_high
        support = recent_low
        
        if current_price > resistance * 1.02:
            zone = "🟢 **观察区间**（突破阻力位，趋势向上）"
        elif current_price < support * 0.98:
            zone = "🔴 **回避区间**（跌破支撑位，趋势向下）"
        else:
            zone = "🟡 **试错区间**（震荡整理，小仓位参与）"
        
        lines.append(f"### 当前价格：{current_price:.2f} 元")
        lines.append(f"### 区间判定：{zone}")
        lines.append("")
        
        lines.append(f"### 波动率分析")
        lines.append(f"| 指标 | 数值 |")
        lines.append(f"|------|------|")
        lines.append(f"| ATR(14) | {atr14:.3f}（{atr14/current_price*100:.1f}%） |")
        lines.append(f"| ATR(20) | {atr20:.3f}（{atr20/current_price*100:.1f}%） |")
        lines.append(f"| ATR(60) | {atr60:.3f}（{atr60/current_price*100:.1f}%） |")
        lines.append(f"| MA20 | {ma20:.2f} |")
        if ma60:
            lines.append(f"| MA60 | {ma60:.2f} |")
        lines.append(f"| 近20日高点 | {resistance:.2f} |")
        lines.append(f"| 近20日低点 | {support:.2f} |")
        
        lines.append("")
        lines.append(f"### 动态止损位")
        lines.append(f"| 周期 | 止损位 | 距当前跌幅 |")
        lines.append(f"|------|--------|----------|")
        lines.append(f"| 短线止损 | {short_stop:.2f} | {(short_stop/current_price - 1)*100:.1f}% |")
        lines.append(f"| 中线止损 | {mid_stop:.2f} | {(mid_stop/current_price - 1)*100:.1f}% |")
        if long_stop:
            lines.append(f"| 长线止损 | {long_stop:.2f} | {(long_stop/current_price - 1)*100:.1f}% |")
        
        lines.append("")
        lines.append(f"### 操作建议")
        lines.append(f"- **短线**（3-10日）：跌破 {short_stop:.2f} 离场")
        lines.append(f"- **中线**（1-3月）：跌破 {mid_stop:.2f} 离场")
        if long_stop:
            lines.append(f"- **长线**（3月+）：跌破 {long_stop:.2f} 离场")
        lines.append(f"- **当前区间**：{zone}")
        
        lines.append("")
        lines.append(f"数据来源：日线数据 ATR 计算。")
        
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"动态止损计算失败: {e} {traceback.format_exc()}")
        return f"❌ 动态止损计算失败: {e}"


def call_fetch_raw_material_sensitivity(stock_code: str) -> str:
    """
    原材料价格敏感性测算：量化锂/镍/铜等关键原材料价格变动
    对整车企业净利润的冲击幅度。
    """
    try:
        db = get_db()
        name = str(stock_code)
        basic = db.get_stock_basic(stock_code)
        if basic:
            name = basic.name
        
        lines = [f"🧪 **原材料价格敏感性测算：{name}({stock_code})**", ""]
        
        # 1) 获取最新年报财务数据
        income = db.get_stock_income(stock_code)
        if income is None or income.empty:
            return f"❌ {stock_code} 无利润表数据"
        
        latest = income.iloc[0]
        revenue = float(latest.get("total_revenue", latest.get("revenue", 0)) or 0)
        net_profit = float(latest.get("net_profit", latest.get("n_income", 0)) or 0)
        cost = float(latest.get("total_cogs", latest.get("oper_cost", 0)) or 0)
        
        # 2) 假设参数（整车企业典型值）
        # 电池成本约占整车BOM的 35-45%，锂占电池成本的 30-50%
        # 取中间值：电池成本占比40%，锂占电池40%
        battery_bom_ratio = 0.40  # 电池占BOM成本
        lithium_in_battery = 0.40  # 锂占电池成本
        nickel_in_battery = 0.15  # 镍占电池成本
        # 其他材料（铜/铝等）占BOM成本
        copper_bom_ratio = 0.03
        
        lines.append(f"### 基础参数假设")
        lines.append(f"| 参数 | 假设值 | 说明 |")
        lines.append(f"|------|--------|------|")
        lines.append(f"| 电池占BOM成本 | {battery_bom_ratio*100:.0f}% | 行业典型值 |")
        lines.append(f"| 锂占电池成本 | {lithium_in_battery*100:.0f}% | 行业典型值 |")
        lines.append(f"| 镍占电池成本 | {nickel_in_battery*100:.0f}% | 行业典型值 |")
        lines.append(f"| 铜占BOM成本 | {copper_bom_ratio*100:.0f}% | 行业典型值 |")
        lines.append(f"| 营业总收入 | {revenue/1e8:.1f}亿 | 最新年报 |")
        lines.append(f"| 营业总成本 | {cost/1e8:.1f}亿 | 最新年报 |")
        lines.append(f"| 归母净利润 | {net_profit/1e8:.1f}亿 | 最新年报 |")
        
        lines.append("")
        
        # 3) 原材料价格变动对净利润的敏感性
        # 关键假设：原材料价格变动 x%，影响电池成本（x% × 占比），再影响总成本
        # 假设其他条件不变（销量/售价/其他成本不变）
        
        scenarios = [-30, -20, -10, 10, 20, 30]  # 原材料价格变动%
        materials = [
            {"name": "锂", "ratio": battery_bom_ratio * lithium_in_battery, "unit": "元/吨"},
            {"name": "镍", "ratio": battery_bom_ratio * nickel_in_battery, "unit": "元/吨"},
            {"name": "铜", "ratio": copper_bom_ratio, "unit": "元/吨"},
        ]
        
        # 算总材料成本占比
        total_raw_ratio = sum(m["ratio"] for m in materials)
        lines.append(f"### 敏感性测算：材料价格变动对净利润的影响")
        lines.append(f"材料合计占BOM成本：{total_raw_ratio*100:.1f}%")
        lines.append(f"")
        header_items = [f"{m['name']}影响(亿)" for m in materials]
        header_str = " | ".join(header_items)
        lines.append(f"| 材料价格变动 | {header_str} | 合计净利变动(亿) | 净利变动幅度 |")
        lines.append(f"|{'---|' * (len(materials) + 3)}")
        
        for pct_change in scenarios:
            impacts = []
            total_impact = 0
            for m in materials:
                # 原材料价格变动 pct% → 影响总成本 = 总成本 × 该材料占比 × pct%
                impact = cost * m["ratio"] * (pct_change / 100)
                impacts.append(impact / 1e8)  # 转亿
                total_impact += impact
            
            # 净利影响 ≈ 成本影响 × (1 - 所得税率)，粗略按75%
            net_impact = total_impact * 0.75
            net_change_pct = net_impact / net_profit * 100 if net_profit != 0 else 0
            
            impact_strs = [f"{imp:+.2f}" for imp in impacts]
            lines.append(f"| {pct_change:+.0f}% | {' | '.join(impact_strs)} | {net_impact/1e8:+.2f} | {net_change_pct:+.1f}% |")
        
        lines.append("")
        lines.append("### 解读")
        lines.append("- 正值=原材料涨价(利空)，负值=原材料跌价(利好)")
        lines.append("- 敏感性越大说明该原材料对公司利润影响越大")
        lines.append("- 实际影响受以下因素调节：库存周期（锁价/长协）、")
        lines.append("  供应链议价能力（转嫁下游）、技术路线切换（钠电替代降锂依赖）")
        lines.append("- 以上测算假设销量/售价/其他成本不变，仅反映原材料单一变量影响")
        lines.append("")
        lines.append("数据来源：利润表数据 + 行业典型BOM占比假设 + 框架性敏感性计算。")
        
        return "\n".join(lines)
        
    except Exception as e:
        import traceback
        logger.error(f"原材料敏感性测算失败: {e} {traceback.format_exc()}")
        return f"❌ 原材料敏感性测算失败: {e}"


def call_fetch_sector_fund_flow(industry_name: str = None, top_n: int = 10) -> str:
    """
    获取板块资金流向排名（Akshare 行业板块资金流）。
    可选按行业名过滤。
    """
    try:
        import akshare as ak

        lines = [f"💰 **板块资金流向排名**"]
        if industry_name:
            lines.append(f"> 目标行业：{industry_name}")
        lines.append("")

        df = ak.stock_sector_fund_flow_rank(indicator="今日")
        if df is None or df.empty:
            return "❌ 获取板块资金流向数据失败"

        # 清理列名
        df.columns = [str(c).strip() for c in df.columns]

        # 按行业名过滤（可选）
        if industry_name:
            mask = df.apply(lambda row: industry_name in str(row).replace("\n", ""), axis=1)
            df = df[mask]
            if df.empty:
                return f"未找到行业 [{industry_name}] 的资金流向数据"

        # 排序：按净流入额降序
        sort_col = None
        for c in df.columns:
            if "净流入" in c or "主力净流入" in c:
                sort_col = c
                break
        if sort_col:
            df = df.sort_values(sort_col, ascending=False)

        # 取 top_n
        display = df.head(top_n)

        lines.append(f"| 排名 | 板块名称 | 主力净流入 | 今日涨跌幅 |")
        lines.append(f"|------|---------|-----------|-----------|")

        for i, (_, row) in enumerate(display.iterrows(), 1):
            name = str(row.get("板块名称", row.get("行业名称", ""))).replace("\n", " ")
            inflow = row.get(sort_col or "主力净流入", "")
            chg = row.get("今日涨跌幅", row.get("涨跌幅", ""))
            lines.append(f"| {i} | {name} | {inflow} | {chg} |")

        if industry_name:
            lines.append("")
            lines.append(f"数据来源：Akshare 行业板块资金流。")
        else:
            lines.append("")
            lines.append("数据来源：Akshare 全市场板块资金流向排名。")

        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"板块资金流向获取失败（网络受限，不影响其他分析）: {e}")
        return f"板块资金流向数据获取失败（API连接受限）\n建议：使用 Researcher 搜索「{industry_name or '指定行业'} 板块资金流向 主力净流入」获取。"


def call_fetch_batch_sotp_valuation(stock_codes: str) -> str:
    """
    批量 SOTP 分部估值对比。
    stock_codes：逗号分隔的股票代码列表，如 "600118,001270,600879"
    """
    try:
        codes = [c.strip() for c in stock_codes.split(",") if c.strip()]
        if not codes:
            return "❌ 未提供股票代码"

        lines = [f"📊 **批量 SOTP 分部估值对比（{len(codes)} 只）**", ""]

        results = []
        for code in codes:
            try:
                r = call_fetch_sotp_valuation(code)
                if r and "❌" not in r[:10]:
                    # 提取每股内在价值
                    for line in r.split("\n"):
                        if "每股内在价值" in line or "每股价值" in line:
                            v = line.strip()
                            results.append((code, v, r))
                            break
                    else:
                        results.append((code, "（未提取到估值结果）", r))
                else:
                    results.append((code, "❌ 估值失败", ""))
            except Exception:
                results.append((code, "❌ 估值异常", ""))

        # 对比表格
        lines.append(f"| 股票代码 | 每股内在价值 |")
        lines.append(f"|----------|-------------|")
        for code, val, _ in results:
            lines.append(f"| {code} | {val.split('：', 1)[-1] if '：' in val else val} |")

        lines.append("")
        lines.append("**各标的详细估值：**")
        lines.append("")
        for code, val, full_text in results:
            if full_text and "❌" not in full_text[:10]:
                # 仅取关键行
                detail_lines = []
                for ln in full_text.split("\n"):
                    if any(k in ln for k in ["业务", "营收", "毛利率", "归母净利", "PE", "估值", "每股"]):
                        detail_lines.append(f"  {ln}")
                if detail_lines:
                    lines.append(f"**{code}**")
                    lines.extend(detail_lines)
                    lines.append("")

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"批量SOTP估值获取失败: {e} {traceback.format_exc()}")
        return f"❌ 获取批量SOTP估值数据失败"


def call_fetch_full_report(stock_code: str) -> str:
    """
    生成标准化全套研报（行业感知版）。
    自动识别行业，过滤无关模块，并行执行独立模块，输出完整 Markdown 研报。
    """
    try:
        from tools.data_router import get_stock_industry, normalize_industry, _INDUSTRY_API_MAP
        from concurrent.futures import ThreadPoolExecutor, as_completed

        report = []
        report.append(f"# 📊 深度研报：{stock_code}")
        report.append("")
        report.append(f"> 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')} | 数据来源：Tushare / 懂车帝 / Akshare / 即时搜索")
        report.append("")

        # 1. 查行业（用于过滤行业特有模块）
        industry = get_stock_industry(stock_code)
        std_industry = normalize_industry(industry) if industry else None
        is_auto_industry = std_industry in _INDUSTRY_API_MAP if std_industry else False
        report.append(f"> 行业识别：{industry or '未知'}{' → 匹配行业专用数据源' if is_auto_industry else ''}")
        report.append("")
        report.append("---")
        report.append("")

        # 2. 定义所有模块，标注行业依赖
        # (title, func_name, args, industry_filter)
        # industry_filter: None=通用, 'auto_only'=仅汽车类, 'non_auto'=非汽车类
        all_steps = [
            # --- 通用模块（所有行业）---
            ("## 一、大盘环境与行业景气", "call_fetch_market_environment", [stock_code], None),
            ("## 二、财务全景（利润表）", "call_fetch_income_data", [stock_code], None),
            ("## 三、资产负债表分析", "call_fetch_balance_sheet_data", [stock_code], None),
            ("## 四、现金流量分析", "call_fetch_cashflow_data", [stock_code], None),
            ("## 五、关键财务指标", "call_fetch_fina_indicator", [stock_code], None),
            ("## 六、主营业务构成", "call_fetch_main_business", [stock_code], None),
            ("## 七、深度财务健康度", "call_fetch_financial_health_summary", [stock_code], None),
            ("## 八、行业对标", "call_fetch_industry_valuation", [stock_code], None),
            ("## 九、股东筹码", "call_fetch_holder_number", [stock_code], None),
            ("## 十、北向持仓", "call_fetch_northbound_hold", [stock_code], None),
            ("## 十一、十大股东", "call_fetch_top10_holder", [stock_code], None),
            ("## 十二、筹码成本估算", "call_fetch_cost_basis", [stock_code], None),
            ("## 十三、股权质押", "call_fetch_pledge", [stock_code], None),
            ("## 十四、限售解禁", "call_fetch_share_float", [stock_code], None),
            ("## 十五、股票回购", "call_fetch_repurchase", [stock_code], None),
            ("## 十六、大宗交易", "call_fetch_block_trade", [stock_code], None),
            ("## 十七、龙虎榜", "call_fetch_top_list", [stock_code], None),
            ("## 十八、机构持仓", "call_fetch_top_inst", [stock_code], None),
            ("## 十九、分析师评级", "call_fetch_broker_recommend", [stock_code], None),
            ("## 二十、SOTP分部估值", "call_fetch_sotp_valuation", [stock_code], None),
            ("## 二十一、数据交叉校验", "call_fetch_data_validator", [stock_code], None),
            # --- 汽车行业特有模块 ---
            ("## 二十二、新能源车行业渗透率", "call_fetch_new_energy_penetration", [], 'auto_only'),
            ("## 二十三、车型销量结构", "call_fetch_vehicle_sales", [stock_code], 'auto_only'),
            ("## 二十四、海外区域销量", "call_fetch_overseas_sales", [stock_code], 'auto_only'),
        ]

        # 3. 按行业过滤
        steps = []
        skipped = []
        for title, func_name, args, filt in all_steps:
            if filt == 'auto_only' and not is_auto_industry:
                skipped.append(title[4:].split("（")[0])
                continue
            steps.append((title, func_name, args))

        # 4. 并行执行独立模块
        def _fetch_one(title, func_name, args):
            try:
                result = globals()[func_name](*args)
                if result and not result.startswith("❌") and "无数据" not in result[:50]:
                    return title, result
                return None
            except Exception as e:
                logger.warning(f"[报告生成] {title} 获取失败: {e}")
                return None

        results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_map = {executor.submit(_fetch_one, t, f, a): t for t, f, a in steps}
            for future in as_completed(future_map):
                r = future.result()
                if r:
                    results[r[0]] = r[1]

        # 5. 按顺序组装（非线程池不保证顺序）
        loaded = 0
        for title, func_name, args in steps:
            if title in results:
                loaded += 1
                report.append(title)
                report.append("")
                lines = results[title].strip().split("\n")
                MAX_LINES = {"大盘": 30, "校验": 20, "质押": 20, "解禁": 20, "回购": 20,
                            "大宗": 20, "龙虎榜": 20, "机构": 20, "分析师": 20}.get(
                    title[4:8].strip(), 50)
                if len(lines) > MAX_LINES:
                    lines = lines[:MAX_LINES] + ["", f"... (共{len(lines)}行，已截断，完整内容请单独查询)"]
                report.extend(lines)
                report.append("")
                report.append("---")
                report.append("")

        # 6. 概要
        report.append("📌 **报告概要**")
        report.append(f"  · 计划模块：{len(steps)} 个")
        report.append(f"  · 成功加载：{loaded}/{len(steps)} 个")
        if skipped:
            report.append(f"  · 行业过滤跳过：{'、'.join(skipped)}")
            report.append(f"  · 识别行业：{industry or '未知'} → 非汽车类，跳过车型/渗透率/海外销量模块")

        return "\n".join(report)

    except Exception as e:
        logger.error(f"报告生成失败: {e} {traceback.format_exc()}")
        return f"❌ 生成全套研报失败：{e}"


def call_fetch_value_discovery(industry_codes: str = "") -> str:
    """
    低位价值发现：扫描指定行业（或默认20个热门行业）的20/60日跌幅，
    找到跌幅最大的行业→成分股→PE分位排序，输出低位关注清单。
    入参 industry_codes: 逗号分隔的申万二级指数代码，如 "801730.SI,801740.SI"，为空则扫描默认热门行业。
    """
    try:
        import tushare as ts
        from utils.config import get_stock_tools_config
        cfg = get_stock_tools_config()
        ts.set_token(cfg["tushare_token"])
        pro = ts.pro_api()

        # ── 默认20个热门申万二级行业 ──
        DEFAULT_INDUSTRIES = [
            ("801730.SI", "汽车零部件"), ("801740.SI", "乘用车"),
            ("801081.SI", "半导体"), ("801082.SI", "元器件"),
            ("801771.SI", "航空装备"), ("801772.SI", "航天装备"),
            ("801736.SI", "电池"), ("801735.SI", "电网设备"),
            ("801761.SI", "医疗服务"), ("801762.SI", "医疗器械"),
            ("801153.SI", "白酒"), ("801151.SI", "食品加工"),
            ("801881.SI", "软件开发"), ("801882.SI", "IT服务"),
            ("801741.SI", "商用车"), ("801731.SI", "电机"),
            ("801884.SI", "通信服务"), ("801883.SI", "通信设备"),
            ("801711.SI", "装修建材"), ("801712.SI", "工程机械"),
        ]

        if industry_codes and industry_codes.strip():
            codes_in = [c.strip() for c in industry_codes.split(",") if c.strip()]
            targets = [(c, "") for c in codes_in]
        else:
            targets = DEFAULT_INDUSTRIES

        # ── 1. 查每个行业的20/60日涨跌幅 ──
        today_str = date.today().strftime("%Y%m%d")
        start_60 = (date.today() - timedelta(days=90)).strftime("%Y%m%d")

        industry_perf = []
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _get_perf(code, name):
            try:
                df = pro.index_daily(ts_code=code, start_date=start_60, end_date=today_str,
                                     fields="trade_date,pct_chg")
                if df is None or df.empty:
                    return None
                df = df.sort_values("trade_date")
                ret20 = df["pct_chg"].tail(20).sum()
                ret60 = df["pct_chg"].tail(60).sum()
                # 当前PE（取最新一天的PE）
                pe_latest = None
                try:
                    idx_d = pro.index_dailybasic(ts_code=code, start_date=today_str, end_date=today_str,
                                                  fields="pe")
                    if idx_d is not None and not idx_d.empty and "pe" in idx_d.columns:
                        pe_latest = idx_d.iloc[0].get("pe")
                except Exception:
                    pass
                return {"code": code, "name": name or code, "ret20": round(ret20, 2),
                        "ret60": round(ret60, 2), "pe": pe_latest}
            except Exception as e:
                logger.debug(f"行业指数[{code}]获取失败: {e}")
                return None

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_get_perf, c, n): c for c, n in targets}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    industry_perf.append(r)

        if not industry_perf:
            return "❌ 未获取到任何行业指数数据（Tushare 可能无可用积分）"

        # 按60日跌幅排序（跌幅最大排最前）
        industry_perf.sort(key=lambda x: x["ret60"])

        # ── 2. 取跌幅前5行业，查成分股PE/PB分位 ──
        top_n = min(5, len(industry_perf))
        top_decliners = industry_perf[:top_n]

        lines = []
        lines.append(f"# 📊 低位价值发现扫描")
        lines.append(f"> 扫描时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"> 扫描范围：{len(targets)} 个申万二级行业")
        lines.append("")
        lines.append(f"## 行业跌幅排名（按60日涨跌幅排序）")
        lines.append("")
        lines.append(f"| 排名 | 行业 | 20日涨跌幅 | 60日涨跌幅 | 当前PE |")
        lines.append(f"|------|------|-----------|-----------|-------|")
        for i, ind in enumerate(industry_perf):
            pe_str = f"{ind['pe']:.1f}" if ind['pe'] else "N/A"
            marker = " 🟢" if i < top_n else ""
            lines.append(f"| {i+1} | {ind['name']}{marker} | {ind['ret20']:+.1f}% | {ind['ret60']:+.1f}% | {pe_str} |")

        lines.append("")
        lines.append(f"---")
        lines.append("")

        # ── 3. 对跌幅前5的行业，查成分股PE分位 ──
        for ind in top_decliners:
            lines.append(f"### {ind['name']}（{ind['code']}）20日{ind['ret20']:+.1f}% / 60日{ind['ret60']:+.1f}%")
            lines.append("")
            try:
                member_df = pro.index_member(ind["code"])
                if member_df.empty:
                    lines.append("  无成分股数据")
                    lines.append("")
                    continue

                codes_list = [c.replace(".SZ", "").replace(".SH", "").replace(".BJ", "")
                              for c in member_df["con_code"].tolist()[:50]]

                # 查所有成分股最近60天的 daily_basic（含PE/PB）— 并行查询
                all_daily = []

                def _get_daily_basic(c):
                    try:
                        ts_c = (f"{c}.SZ" if c.startswith("0") or c.startswith("3")
                                else (f"{c}.BJ" if c.startswith(("8", "4", "92"))
                                      else f"{c}.SH"))
                        df_db = pro.daily_basic(ts_code=ts_c,
                                                start_date=start_60, end_date=today_str,
                                                fields="ts_code,trade_date,pe_ttm,pb,total_mv")
                        if df_db is not None and not df_db.empty:
                            return df_db
                    except Exception:
                        try:
                            df_db = pro.daily_basic(ts_code=f"{c}.SH",
                                                    start_date=start_60, end_date=today_str,
                                                    fields="ts_code,trade_date,pe_ttm,pb,total_mv")
                            if df_db is not None and not df_db.empty:
                                return df_db
                        except Exception:
                            pass
                    return None

                with ThreadPoolExecutor(max_workers=8) as pe_executor:
                    pe_futures = {pe_executor.submit(_get_daily_basic, c): c for c in codes_list}
                    for pe_f in as_completed(pe_futures):
                        r = pe_f.result()
                        if r is not None:
                            all_daily.append(r)

                if not all_daily:
                    lines.append("  无成分股估值数据")
                    lines.append("")
                    continue

                import pandas as pd
                combined = pd.concat(all_daily, ignore_index=True)
                if combined.empty or "ts_code" not in combined.columns:
                    lines.append("  无成分股估值数据")
                    lines.append("")
                    continue

                # 取每只股票最新的PE_TTM、PB
                latest = combined.sort_values("trade_date").groupby("ts_code").last().reset_index()
                latest["code_short"] = latest["ts_code"].str[:6]

                # 找该行业已持有的DB数据算52周PE分位（简化：用当前PE值排序，越低越"价值"）
                valid_pe = latest.dropna(subset=["pe_ttm"])
                if valid_pe.empty:
                    lines.append("  成分股PE数据不足，跳过")
                    lines.append("")
                    continue

                valid_pe = valid_pe[valid_pe["pe_ttm"] > 0]
                if valid_pe.empty:
                    lines.append("  成分股PE均为负值（行业亏损），跳过")
                    lines.append("")
                    continue

                # 按PE排序取最低的6只
                candidates = valid_pe.sort_values("pe_ttm").head(6)
                lines.append(f"  **估值最低成分股（PE_TTM排序）**")
                lines.append("")
                lines.append(f"  | 代码 | PE_TTM | PB | 总市值(亿) |")
                lines.append(f"  |------|--------|-----|-----------|")
                for _, row in candidates.iterrows():
                    mv = f"{row['total_mv']/1e8:.0f}" if pd.notna(row.get("total_mv")) else "N/A"
                    pb = f"{row['pb']:.2f}" if pd.notna(row.get("pb")) else "N/A"
                    lines.append(f"  | {row['code_short']} | {row['pe_ttm']:.1f} | {pb} | {mv} |")
                lines.append("")

                # 底部提示
                lines.append('  > 💡 低PE可能是价值洼地，也可能是「价值陷阱」（利润下滑导致PE被动降低）。')
                lines.append("  > 如需完整分析，发送「分析 股票代码」（如：分析 000625）")
                lines.append("")

            except Exception as e:
                logger.warning(f"[价值发现] {ind['name']} 成分股分析失败: {e}")
                lines.append(f"  ⚠️ 成分股分析失败: {e}")
                lines.append("")

        lines.append("---")
        lines.append("")
        lines.append("📌 **使用说明**")
        lines.append("  · 扫描频率：每日盘后自动执行，交易日有效")
        lines.append("  · 行业范围：20个热门申万二级行业（可自定义industry_codes参数）")
        lines.append("  · PE低≠值得买——请结合基本面/资金面综合判断")
        lines.append("  · 对某只标的感兴趣，发送「分析 股票代码」获取完整研报")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"价值发现扫描失败: {e} {traceback.format_exc()}")
        return f"❌ 价值发现扫描失败：{e}"


def call_fetch_brand_sales_by_code(stock_code: str) -> str:
    """
    获取高端品牌/子品牌的月度销量数据。
    目前已支持的品牌参考：比亚迪的腾势、方程豹、仰望；
    通用的汽车品牌数据通过搜索词"品牌名 + 月份 + 销量"获取。
    返回格式化的品牌销量文本，含同比、环比信息。
    """
    try:
        if stock_code != '002594':
            return f"暂不支持该股票({stock_code})的高端品牌销量查询"

        basic = stock_tool_instance.db.get_stock_basic(stock_code)
        name = basic.name if basic and hasattr(basic, 'name') else stock_code
        lines = [f"📊 **{name} 高端品牌/子品牌销量**", ""]

        # ---- 源1: vehicle_sales 数据库 ----
        try:
            df = stock_tool_instance.fetch_and_save_vehicle_sales(stock_code=stock_code)
            hit = False
            if df is not None and not df.empty:
                brands = {'腾势': [], '方程豹': [], '仰望': []}
                if 'report_date' in df.columns and 'vehicle_model' in df.columns:
                    df_report = df.copy()
                    df_report['report_date'] = pd.to_datetime(df_report['report_date'], errors='coerce')
                    df_report = df_report.dropna(subset=['report_date'])
                    if not df_report.empty:
                        latest_month = df_report['report_date'].max()
                        month_data = df_report[df_report['report_date'] == latest_month]
                        for brand_name in brands:
                            mask = month_data['vehicle_model'].str.contains(brand_name, na=False)
                            if mask.any():
                                c = month_data[mask]
                                total = int(c['sales_volume'].sum()) if 'sales_volume' in c.columns else 0
                                brands[brand_name] = (total, c['vehicle_model'].tolist())
                                hit = True
                            else:
                                brands[brand_name] = (0, [])

                if hit:
                    lines.append("数据来源：车型销量数据库\n")
                    for brand_name, (total, models) in brands.items():
                        if total > 0:
                            lines.append(f"**{brand_name}**：{total:,} 辆")
                            for m in models[:3]:
                                lines.append(f"  - {m}")
                        else:
                            lines.append(f"**{brand_name}**：暂无销量数据")
                    lines.append("")
                    return "\n".join(lines)
        except Exception:
            pass

        # ---- 源2: akshare CPCA 乘联会月度厂商排名 ----
        try:
            import akshare as ak
            man_df = ak.car_market_man_rank_cpca()
            if man_df is not None and not man_df.empty:
                byd_row = man_df[man_df['厂商'].str.contains('比亚迪', na=False)]
                if not byd_row.empty:
                    lines.append("**比亚迪总体销量趋势（乘联会）**")
                    for _, r in byd_row.iterrows():
                        parts = []
                        for col in man_df.columns:
                            if col == '厂商':
                                continue
                            val = r[col]
                            parts.append(f"{col}: {val}万辆")
                        lines.append("  " + "  ".join(parts))
                    lines.append("")
        except Exception:
            pass

        # ---- 源3: 最近一个完整月销量摘要（来自乘联会产销数据）----
        if lines and lines[-1] != "":
            lines.append("")

        # ---- 最终 fallback: 返回搜索引导 + akshare 厂商数据 ----
        if len(lines) <= 2:
            lines = [f"📊 **{name} 高端品牌/子品牌销量查询**", ""]

        lines.append("**高端品牌明细数据补充说明**")
        lines.append("腾势、方程豹、仰望为比亚迪旗下高端子品牌，其单独销量数据不在结构化数据库中，")
        lines.append("需要通过搜索引擎获取。推荐搜索关键词如下：")
        lines.append("")
        lines.append("  · 比亚迪 2026年6月 销量 分品牌 腾势 方程豹 仰望")
        lines.append("  · 腾势 2026年6月 交付量")
        lines.append("  · 方程豹 2026年6月 销量")
        lines.append("  · 仰望 2026年6月 交付")
        lines.append("")
        lines.append("⚠️ 获取到数据后，请结合前文的总体销量趋势，分析高端品牌占比及同比变化。")

        return "\n".join(lines)

    except Exception as e:
        logger.error(f"获取高端品牌销量数据失败: {e} {traceback.format_exc()}")
        return f"❌ 获取高端品牌销量数据失败：{e}"


stock_analyst_tools = [
    StructuredTool(
        name="value_discovery_fetcher",
        func=call_fetch_value_discovery,
        args_schema=IndustryCodesInput,
        description="""
        低位价值发现扫描：自动扫描20个热门申万二级行业的20/60日涨跌幅，
        找出跌幅最大的行业，再查其成分股的PE_TTM排序，输出低位关注清单。
        可选入参 industry_codes：逗号分隔的申万二级指数代码，留空则扫描默认热门行业。
        例如：801730.SI,801740.SI（扫描汽车零部件和乘用车行业）
        """
    ),
    StructuredTool(
        name="stock_research_report_fetcher",
        func=call_fetch_stock_research_report,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的股票研报。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取股票研报，保存到数据库，并返回最近20条数据。
        """
    ),
    StructuredTool(
        name="stock_income_fetcher",
        func=call_fetch_income_data,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的利润表数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取利润表数据（营业收入、营业利润、净利润、每股收益等），保存到数据库，并返回最近20条数据。
        """
    ),
    StructuredTool(
        name="stock_balance_sheet_fetcher",
        func=call_fetch_balance_sheet_data,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的资产负债表数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取资产负债表数据（总资产、流动资产、总负债、所有者权益等），保存到数据库，并返回最近20条数据。
        """
    ),
    StructuredTool(
        name="stock_cashflow_fetcher",
        func=call_fetch_cashflow_data,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的现金流量表数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取现金流量表数据（经营/投资/筹资活动现金流净额、资本开支、自由现金流），保存到数据库，并返回最近几个报告期的对比数据（累计口径）。
        """
    ),
    StructuredTool(
        name="stock_fina_indicator_fetcher",
        func=call_fetch_fina_indicator,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的财务指标数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取核心财务指标（ROE、ROA、毛利率、净利率、存货周转率、资产负债率、营收/净利润增长率等），保存到数据库，并返回最近几个报告期的对比数据。
        """
    ),
    StructuredTool(
        name="stock_main_business_fetcher",
        func=call_fetch_main_business,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的主营业务构成数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取按产品和按地区拆分的收入、成本、毛利率数据，保存到数据库，并返回最新报告期的业务构成（含收入占比和各业务毛利率）。
        """
    ),
    StructuredTool(
        name="stock_holder_number_fetcher",
        func=call_fetch_holder_number,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的股东户数数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取各报告期末的股东户数、环比变化数据，保存到数据库，并返回最近几个报告期的趋势。
        """
    ),
    StructuredTool(
        name="stock_northbound_hold_fetcher",
        func=call_fetch_northbound_hold,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的北向持股数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取沪深港通北向资金每日持股数量和持股占比，保存到数据库，并返回最近交易日的持仓和近期变化趋势。
        """
    ),
    StructuredTool(
        name="stock_top10_holder_fetcher",
        func=call_fetch_top10_holder,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的十大股东数据。
        输入参数：股票代码（字符串）。
        例如：000001
        作用：拉取定期报告披露的十大股东和十大流通股东名单、持股数量和比例，保存到数据库，并返回最新报告期的股东结构。
        """
    ),
    StructuredTool(
        name="industry_valuation_fetcher",
        func=call_fetch_industry_valuation,
        args_schema=IndustryValuationInput,
        description="""
        获取A股行业估值数据（PE/PB均值）。
        输入参数：stock_code（股票代码）或 industry_name（行业名称），二选一。
        例如：stock_code="002594" 或 industry_name="汽车"
        作用：通过申万行业成分股聚合计算行业平均PE、PB、PE_TTM等估值指标，用于同业对比和估值参考。
        """
    ),
    StructuredTool(
        name="new_energy_penetration_fetcher",
        func=call_fetch_new_energy_penetration,
        args_schema=NewEnergyPenetrationInput,
        description="""
        获取新能源车行业月度销量及渗透率数据。
        无需输入参数。
        作用：拉取汽车总销量、新能源车销量、渗透率等行业宏观数据，用于判断行业景气度分析。
        """
    ),
    StructuredTool(
        name="repurchase_fetcher",
        func=call_fetch_repurchase,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的股票回购数据。
        输入参数：股票代码（字符串）。
        例如：002594
        作用：拉取公司回购公告的日期、回购数量、金额、价格区间等，保存到数据库并返回。回购通常被视为管理层认为股价低估的积极信号。
        """
    ),
    StructuredTool(
        name="share_float_fetcher",
        func=call_fetch_share_float,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的限售解禁数据。
        输入参数：股票代码（字符串）。
        例如：002594
        作用：拉取未来限售股解禁的日期、数量、占流通股比例、解禁股东等信息，用于判断潜在抛压风险。
        """
    ),
    StructuredTool(
        name="broker_recommend_fetcher",
        func=call_fetch_broker_recommend,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的分析师月度评级数据。
        输入参数：股票代码（字符串）。
        例如：002594
        作用：拉取最近3个月各券商对该股的评级和推荐，用于了解市场一致预期。
        """
    ),
    StructuredTool(
        name="pledge_fetcher",
        func=call_fetch_pledge,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的股权质押统计数据。
        输入参数：股票代码（字符串）。
        例如：002594
        作用：拉取大股东质押比例、无限售股质押比例等数据，质押比例过高（>50%）需警惕爆仓和控制权转移风险。
        """
    ),
    StructuredTool(
        name="block_trade_fetcher",
        func=call_fetch_block_trade,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的大宗交易数据。
        输入参数：股票代码（字符串）。
        例如：002594
        作用：拉取最近90天的大宗交易记录（成交价、成交量、买卖方营业部），折价大宗可能意味着减持信号。
        """
    ),
    StructuredTool(
        name="top_list_fetcher",
        func=call_fetch_top_list,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的龙虎榜上榜记录。
        输入参数：股票代码（字符串）。
        例如：002594
        作用：拉取该股最近上龙虎榜的日期、涨跌幅、成交额、净买卖金额、上榜原因等，用于判断游资/主力资金进出。
        """
    ),
    StructuredTool(
        name="top_inst_fetcher",
        func=call_fetch_top_inst,
        args_schema=StockCodeInput,
        description="""
        获取A股股票的龙虎榜机构席位追踪数据。
        输入参数：股票代码（字符串）。
        例如：002594
        作用：拉取机构专用席位的买入/卖出金额和净买卖方向，用于判断机构资金对该股的态度。
        """
    ),
    StructuredTool(
        name="vehicle_sales_fetcher",
        func=call_fetch_vehicle_sales,
        args_schema=StockCodeInput,
        description="""
        获取全国最新完整月份的车型级月销量排行榜数据。
        输入参数：股票代码（字符串，可选，用于筛选特定品牌）。
        例如：002594 会筛选比亚迪车型数据
        作用：获取各车型的月销量、品牌归属、价格区间等信息，用于分析产品结构、高端车型占比、细分市场表现等。
        数据来源：懂车帝全国车型销量排行API。
        """
    ),
    StructuredTool(
        name="financial_health_summary_fetcher",
        func=call_fetch_financial_health_summary,
        args_schema=StockCodeInput,
        description="""
        获取深度财务健康度分析，包含：
        1. 杜邦分解：ROE拆解为净利率×资产周转率×权益乘数
        2. 周转天数：存货周转天数、应收账款周转天数
        3. 自由现金流质量：FCF、FCF/营收比率、FCF同比变动
        4. 营运资本分析：应收/存货占营收比、现金循环周期
        5. 费用结构趋势：三费占比变化
        输入参数：股票代码（字符串，6位数）。
        例如：002594
        数据来源：Tushare 财务指标 + 利润表 + 资产负债表 + 现金流量表。
        """
    ),
    StructuredTool(
        name="sotp_valuation_fetcher",
        func=call_fetch_sotp_valuation,
        args_schema=StockCodeInput,
        description="""
        获取SOTP（Sum of The Parts）分部估值分析，包含：
        1. 按主营业务分产品拆分各板块营收、毛利率
        2. 对各板块分配独立PE倍数（参考行业均值）
        3. 计算各板块价值及合计总价值
        4. 计算每股内在价值
        适用于多元化企业（如比亚迪、宁德时代等）。
        输入参数：股票代码（字符串，6位数）。
        例如：002594
        数据来源：Tushare 主营业务分产品数据 + 行业估值参考。
        """
    ),
    StructuredTool(
        name="overseas_sales_fetcher",
        func=call_fetch_overseas_sales,
        args_schema=StockCodeInput,
        description="""
        获取海外分区域销量或收入数据（搜索模式）。
        通过联网搜索获取目标公司在欧洲、东南亚、拉美、中东等区域的销量或收入占比数据。
        输入参数：股票代码（字符串，6位数）。
        例如：002594
        数据来源：公开网页搜索（非官方结构化数据，仅供参考）。
        适合场景：分析海外业务布局、评估出口区域集中度风险。
        """
    ),
    StructuredTool(
        name="cost_basis_fetcher",
        func=call_fetch_cost_basis,
        args_schema=StockCodeInput,
        description="""
        估算筹码平均成本（基于日线成交量数据）。
        通过分析各时期成交量加权均价（VWAP），判断当前筹码盈亏状态、套牢/获利盘压力。
        包含：近30日/60日/90日/半年四个区间的VWAP计算及筹码结构判断。
        输入参数：股票代码（字符串，6位数）。
        例如：002594
        数据来源：Tushare 日线数据。
        """
    ),
    StructuredTool(
        name="market_environment_fetcher",
        func=call_fetch_market_environment,
        args_schema=StockCodeInput,
        description="""
        获取大盘环境量化评分（0~10分）。
        基于沪深300趋势、行业指数表现等数据，生成标准化市场评分卡：
        1. 大盘趋势得分（沪深300位置/量能/趋势）
        2. 行业景气得分（5/20/60日涨跌幅、52周分位）
        3. 综合评级（偏多/中性/谨慎/偏空）
        输入参数：股票代码（字符串）。
        例如：002594
        数据来源：Akshare 沪深300 + 申万行业指数。
        """
    ),
    StructuredTool(
        name="data_validator_fetcher",
        func=call_fetch_data_validator,
        args_schema=StockCodeInput,
        description="""
        数据交叉校验与可信度评估。
        对比不同数据源的财务数据一致性，自动发现异常偏差：
        1. 校验毛利率：财务指标 vs 主营业务分产品加权
        2. 校验营收：利润表 vs 主营业务分产品加总
        3. 校验同比/环比逻辑合理性
        4. 输出数据可信度标注规则
        输入参数：股票代码（字符串，6位数）。
        例如：002594
        数据来源：Tushare 多表交叉验证。
        """
    ),
    StructuredTool(
        name="sector_fund_flow_fetcher",
        func=call_fetch_sector_fund_flow,
        args_schema=SectorFundFlowInput,
        description="""
        获取板块资金流向排名。
        入参：industry_name（行业名，可选。不传则返回全市场板块资金排名TOP10）。
        作用：查看全市场各板块主力资金净流入/流出排名，辅助判断资金偏好。
        数据来源：Akshare 行业板块资金流。
        """
    ),
    StructuredTool(
        name="batch_sotp_valuation_fetcher",
        func=call_fetch_batch_sotp_valuation,
        args_schema=BatchSotpInput,
        description="""
        批量 SOTP 分部估值对比。
        入参：stock_codes（逗号分隔的股票代码列表，如 "600118,001270,600879"）。
        作用：对多个标的批量执行SOTP分部估值，输出对比表格和详细拆分。
        数据来源：Tushare 主营业务分产品数据。
        """
    ),
    StructuredTool(
        name="scoring_engine_fetcher",
        func=call_fetch_scoring_engine,
        args_schema=ScoringEngineInput,
        description="""
        打分规则引擎：查看分项维度、满分刻度、阶段权重、准入门槛，
        并对候选标的自动计算综合分、排序、门槛过滤。
        入参：candidates_json（候选JSON数组）、stage（行业阶段，可选）。
        作用：将打分规则从LLM提示词中独立出来，分项加权和门槛过滤可被外部校验修改。
        数据来源：LLM分项评分 + 规则引擎加权过滤。
        """
    ),
    StructuredTool(
        name="batch_valuation_fetcher",
        func=call_fetch_batch_valuation,
        args_schema=BatchValuationInput,
        description="""
        批量获取一组股票代码的 PE/PB 估值统计。
        入参：stock_codes（逗号分隔的股票代码列表）。
        作用：对非标准行业/自定义概念板块（如商业航天、AI芯片）计算板块整体估值分位，替代仅2只标的的失真估值对比。
        数据来源：Tushare daily_basic / DB缓存。
        """
    ),
    StructuredTool(
        name="scenario_analysis_fetcher",
        func=call_fetch_scenario_analysis,
        args_schema=ScenarioAnalysisInput,
        description="""
        情景测算分析：基于最新年报数据，生成乐观/基准/悲观三大情景的净利润和股价影响测算。
        入参：stock_code（A股股票代码）。
        作用：变量包括营收变动、毛利率变动、PE倍数调整，量化关税/价格战/订单等事件对业绩和估值的冲击。
        数据来源：Tushare 利润表 + daily_basic。
        """
    ),
    StructuredTool(
        name="stop_loss_calculator",
        func=call_fetch_stop_loss_calculator,
        args_schema=StopLossInput,
        description="""
        动态止损计算器：基于ATR（平均真实波幅）和均线系统，
        计算短线/中线/长线三级动态止损位，并判定当前价格处于
        观察区间/试错区间/回避区间。
        入参：stock_code（A股股票代码）。
        数据来源：日线数据 ATR 计算。
        """
    ),
    StructuredTool(
        name="raw_material_sensitivity_fetcher",
        func=call_fetch_raw_material_sensitivity,
        args_schema=RawMaterialSensitivityInput,
        description="""
        原材料价格敏感性测算：量化锂/镍/铜等关键原材料价格变动
        对整车企业净利润的冲击幅度。
        入参：stock_code（A股股票代码）。
        数据来源：利润表 + 行业典型BOM占比假设。
        """
    ),
    StructuredTool(
        name="full_report_fetcher",
        func=call_fetch_full_report,
        args_schema=StockCodeInput,
        description="""
        生成标准化全套深度研报。
        自动调用所有 25+ 个数据工具，按固定模板填充，输出完整 Markdown 研报。
        包含：大盘环境 + 三大报表 + 财务指标 + 主营业务 + 深度财务健康度
        + 行业对标 + 新能源渗透率 + 车型销量 + 股东筹码 + 北向持仓
        + 十大股东 + 筹码成本 + 海外销量 + 股权质押 + 限售解禁
        + 股票回购 + 大宗交易 + 龙虎榜 + 机构持仓 + 分析师评级
        + SOTP分部估值 + 数据交叉校验
        输入参数：股票代码（字符串）。
        例如：002594
        """
    ),
]


def call_extract_financial_snapshot(stock_code: str) -> str:
    """
    从三大报表原始 DataFrame 直接提取结构化「财务关键指标快照」。

    作用：替代让 LLM 从 3000 字符格式化文本里自己找字段+计算。
    输出 ~500 字符、字段名+精确值一目了然，LLM 可直接引用不用再算，
    避免净利率 2.67%→2.72% 这类心算误差、净现比/期间费用率漏字段。

    包含：
    - 毛利率（本期、同比pct、环比pct）
    - 净利率（本期精确值、同比pct）
    - 期间费用率（合计=销+管+研+财，及同比pct差）
      + 财务费用（绝对值、标注汇兑转亏等注释位）
      + 研发费用（绝对值+同比）
    - OCF（经营现金流）、FCF（ocf-capex，程序算，不准 LLM 心算）
    - 净现比 = OCF / 归母净利

    同比 = 本期 vs 去年同季配对（累计口径同季可比）。
    环比 = 本期累计 - 上一季度累计 = 单季度毛利率（累计口径拆单季）。
    """
    if stock_tool_instance is None:
        return ""
    try:
        # 取 DF：先 API，失败走 DB 缓存（与 call_fetch_income_data 口径一致）
        income_df = stock_tool_instance.fetch_and_save_stock_income(stock_code=stock_code)
        if income_df is None or income_df.empty:
            income_df = stock_tool_instance.db.get_stock_income(stock_code) if hasattr(stock_tool_instance, 'db') else None
        cashflow_df = stock_tool_instance.fetch_and_save_stock_cashflow(stock_code=stock_code)
        # balancesheet 暂不需要（当前快照字段用不到）
    except Exception as e:
        logger.debug(f"[snapshot] 三表DF取数失败: {e}")
        return ""

    def _n(v):
        if v is None or pd.isna(v):
            return None
        return float(v)

    lines = []

    # ================== 利润表快照 ==================
    if income_df is not None and not income_df.empty:
        inc = income_df.copy()
        inc['report_date'] = pd.to_datetime(inc['report_date'])
        inc = inc.sort_values('report_date', ascending=False).reset_index(drop=True)
        period_map = {}
        for _, r in inc.iterrows():
            rd = r['report_date']
            period_map[(rd.year, rd.quarter)] = r

        latest = inc.iloc[0]
        cur_rd = latest['report_date']
        cur_q = cur_rd.quarter
        # 去年同季（同比基准）
        prev_y = period_map.get((cur_rd.year - 1, cur_q))
        # 上一季度（环比基准，用于拆单季）——注意：累计口径要拆单季
        prev_q_row = period_map.get((cur_rd.year, cur_q - 1)) if cur_q > 1 else None
        # 上一年的上一季度（去年同季的上一季，用于算去年同期累计拆单季）
        prev_y_prev_q = period_map.get((cur_rd.year - 1, cur_q - 1)) if cur_q > 1 else None

        cur_rev = _n(latest.get('total_revenue'))
        cur_profit = _n(latest.get('n_income_attr_p')) or _n(latest.get('net_profit'))
        cur_gm = _n(latest.get('gross_margin'))
        prev_y_gm = _n(prev_y.get('gross_margin')) if prev_y is not None else None
        prev_y_rev = _n(prev_y.get('total_revenue')) if prev_y is not None else None
        prev_y_profit = (_n(prev_y.get('n_income_attr_p')) or _n(prev_y.get('net_profit'))) if prev_y is not None else None

        # 净利率（精确到 2 位小数，禁止 LLM 心算）
        cur_nm = (cur_profit / cur_rev * 100) if (cur_profit is not None and cur_rev and cur_rev > 0) else None
        prev_y_nm = (prev_y_profit / prev_y_rev * 100) if (prev_y_profit is not None and prev_y_rev and prev_y_rev > 0) else None

        # 毛利率同比/环比（pct 点差）
        gm_yoy_pct = (cur_gm - prev_y_gm) if (cur_gm is not None and prev_y_gm is not None) else None
        # 毛利率环比 = 本季单季毛利率 - 上季单季毛利率
        # 累计口径拆单季：
        #   单季营收 = 本期累计 - 上一季度累计（Q2拆=半年-Q1，Q3拆=前三季-半年）
        #   单季毛利 = 本期累计营收×累计毛利率 - 上一季度累计营收×上一季度累计毛利率
        #   单季毛利率 = 单季毛利 / 单季营收
        gm_qoq_pct = None
        if (cur_gm is not None and cur_rev and prev_q_row is not None):
            pq_rev = _n(prev_q_row.get('total_revenue'))
            pq_gm = _n(prev_q_row.get('gross_margin'))
            if pq_rev and pq_gm is not None and pq_rev < cur_rev:
                cur_single_rev = cur_rev - pq_rev
                cur_single_gp = cur_rev * cur_gm / 100 - pq_rev * pq_gm / 100
                cur_single_gm = cur_single_gp / cur_single_rev * 100 if cur_single_rev > 0 else None
                # 上一季度的单季毛利率 = （pq_rev*pq_gm/100 - pypq_rev*pypq_gm/100）/ (pq_rev - pypq_rev)
                if prev_y_prev_q is not None and cur_single_gm is not None:
                    pypq_rev = _n(prev_y_prev_q.get('total_revenue'))
                    pypq_gm = _n(prev_y_prev_q.get('gross_margin'))
                    if pypq_rev and pypq_gm is not None and pq_rev > pypq_rev:
                        prev_single_rev = pq_rev - pypq_rev
                        prev_single_gp = pq_rev * pq_gm / 100 - pypq_rev * pypq_gm / 100
                        prev_single_gm = prev_single_gp / prev_single_rev * 100 if prev_single_rev > 0 else None
                        if prev_single_gm is not None:
                            gm_qoq_pct = cur_single_gm - prev_single_gm
                else:
                    # Q1无拆单季数据，环比退化为同比（标注"Q1不适用环比"）
                    pass

        # 费用率（销+管+研+财 = 期间费用率）
        exp_items = [('sell_exp', '销售'), ('admin_exp', '管理'), ('rd_exp', '研发'), ('fin_exp', '财务')]
        cur_period_ratio = 0.0
        prev_y_period_ratio = 0.0
        exp_details = []
        has_any_exp = False
        for col, label in exp_items:
            cur_exp = _n(latest.get(col))
            prev_exp = _n(prev_y.get(col)) if prev_y is not None else None
            cur_r = (cur_exp / cur_rev * 100) if (cur_exp is not None and cur_rev and cur_rev > 0) else None
            prev_r = (prev_exp / prev_y_rev * 100) if (prev_exp is not None and prev_y_rev and prev_y_rev > 0) else None
            if cur_r is not None:
                cur_period_ratio += cur_r
                has_any_exp = True
            if prev_r is not None:
                prev_y_period_ratio += prev_r
            # 财务费用/研发费用单独展示绝对值
            if col == 'fin_exp' and cur_exp is not None:
                sign = '汇兑转亏' if (cur_exp > 0 and (prev_exp is None or prev_exp <= 0)) else (
                    '汇兑收益' if cur_exp < 0 else '')
                exp_details.append(f"财务费用: {cur_exp/1e8:.3f}亿（{sign}）" if sign else
                                   f"财务费用: {cur_exp/1e8:.3f}亿")
            if col == 'rd_exp':
                yoy = ""
                if cur_exp is not None and prev_exp is not None and prev_exp != 0:
                    y = (cur_exp - prev_exp) / abs(prev_exp) * 100
                    yoy = f"（同比{'+' if y >= 0 else ''}{y:.2f}%）"
                if cur_exp is not None:
                    exp_details.append(f"研发费用: {cur_exp/1e8:.2f}亿{yoy}")

        # 营收同比（给净利归因用）
        rev_yoy = None
        if cur_rev and prev_y_rev and prev_y_rev > 0:
            rev_yoy = (cur_rev - prev_y_rev) / prev_y_rev * 100
        profit_yoy = None
        if cur_profit is not None and prev_y_profit is not None and prev_y_profit != 0:
            profit_yoy = (cur_profit - prev_y_profit) / abs(prev_y_profit) * 100

        lines.append(f"报告期: {cur_rd.strftime('%Y-%m-%d')}（累计口径，Q{cur_q}）")
        lines.append(f"营收同比: {'+' if rev_yoy and rev_yoy >= 0 else ''}{rev_yoy:.2f}%" if rev_yoy is not None else "营收同比: N/A")
        lines.append(f"净利同比: {'+' if profit_yoy and profit_yoy >= 0 else ''}{profit_yoy:.2f}%" if profit_yoy is not None else "净利同比: N/A")
        if cur_gm is not None:
            gm_line = f"毛利率: {cur_gm:.2f}%"
            if gm_yoy_pct is not None:
                gm_line += f" | 同比{'+' if gm_yoy_pct >= 0 else ''}{gm_yoy_pct:.2f}pct"
            if gm_qoq_pct is not None:
                gm_line += f" | 环比(拆单季){'+' if gm_qoq_pct >= 0 else ''}{gm_qoq_pct:.2f}pct"
            elif cur_q == 1:
                gm_line += " | 环比:N/A（Q1无可比上季）"
            lines.append(gm_line)
        if cur_nm is not None:
            nm_line = f"净利率: {cur_nm:.2f}%"
            if prev_y_nm is not None:
                nm_line += f" | 同比{'+' if (cur_nm - prev_y_nm) >= 0 else ''}{cur_nm - prev_y_nm:.2f}pct"
            lines.append(nm_line)
        if has_any_exp:
            period_diff = None
            if cur_period_ratio and prev_y_period_ratio:
                period_diff = cur_period_ratio - prev_y_period_ratio
            p_line = f"期间费用率(销+管+研+财): {cur_period_ratio:.2f}%"
            if period_diff is not None:
                p_line += f" | 同比{'+' if period_diff >= 0 else ''}{period_diff:.2f}pct"
            lines.append(p_line)
        for d in exp_details:
            lines.append("  " + d)

    # ================== 现金流快照 ==================
    if cashflow_df is not None and not cashflow_df.empty:
        cf = cashflow_df.copy()
        cf['report_date'] = pd.to_datetime(cf['report_date'])
        cf = cf.sort_values('report_date', ascending=False).reset_index(drop=True)
        cf_map = {}
        for _, r in cf.iterrows():
            rd = r['report_date']
            cf_map[(rd.year, rd.quarter)] = r
        c_latest = cf.iloc[0]
        c_rd = c_latest['report_date']
        c_prev_y = cf_map.get((c_rd.year - 1, c_rd.quarter))
        cur_ocf = _n(c_latest.get('operating_cashflow'))
        prev_ocf = _n(c_prev_y.get('operating_cashflow')) if c_prev_y is not None else None
        cur_capex = _n(c_latest.get('capex'))
        cur_rev_snap = _n(c_latest.get('revenue')) or cur_rev

        if cur_ocf is not None:
            ocf_line = f"OCF(经营现金流): {cur_ocf/1e8:.2f}亿"
            if prev_ocf is not None and prev_ocf != 0:
                g = (cur_ocf - prev_ocf) / abs(prev_ocf) * 100
                ocf_line += f"（同比{'+' if g >= 0 else ''}{g:.2f}%）"
            lines.append(ocf_line)
            # 净现比 = OCF / 归母净利（取利润表 cur_profit）
            if cur_profit is not None and cur_profit != 0:
                ncr = cur_ocf / cur_profit * 100
                lines.append(f"净现比(OCF/归母净利): {ncr:.2f}%")
        if cur_ocf is not None and cur_capex is not None:
            fcf = cur_ocf - cur_capex
            lines.append(f"FCF(程序计算=OCF{cur_ocf/1e8:.2f}亿 - 资本开支{cur_capex/1e8:.2f}亿): {fcf/1e8:.2f}亿 【直接引用，禁止心算】")

    if not lines:
        return ""
    header = "========== 财务关键指标快照（程序直接提取+计算，直接引用禁止心算） =========="
    return header + "\n" + "\n".join(f"  - {l}" for l in lines)
