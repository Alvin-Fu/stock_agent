from dateutil.utils import today

from .stock.base import DataFetcherManager, DataFetchError
from storage.sqlite import get_db
import pandas as pd
from utils.logger import logger
from utils.common import TASK_NAME_DAILY_TASK, parse_row_date
from datetime import date
from .stock.tushare_fetcher import TushareFetcher
from .stock.akshare_fetcher import AkshareFetcher
from langchain_core.tools import StructuredTool
import traceback
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, date, timezone, timedelta



class StockTools:
    def __init__(self):
        """
        初始化管理器

        Args:
            fetchers: 数据源列表（可选，默认按优先级自动创建）
        """
        self.tushare = TushareFetcher()
        self.akshare = AkshareFetcher()
        self.data_manager = DataFetcherManager([self.tushare, self.akshare])
        self.db = get_db()

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

            df = self.tushare.income(stock_code, start_date, end_date_str)
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

    def _normalize_income_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化利润表数据
        Tushare income 返回的主要字段：
        ts_code, ann_date, end_date, total_revenue, operate_profit, n_income, basic_eps, oper_cost,
        sell_exp（销售费用）, admin_exp（管理费用）, rd_exp（研发费用）, fin_exp（财务费用）
        数据单位：元
        """
        df = df.copy()

        if 'update_flag' in df.columns:
            df = df.sort_values('update_flag', ascending=False)
        df = df.drop_duplicates(subset=['end_date'], keep='first')

        column_mapping = {
            'ts_code': 'code',
            'end_date': 'report_date',
            'total_revenue': 'total_revenue',
            'operate_profit': 'operating_profit',
            'n_income': 'net_profit',
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

        revenue_map = df.set_index(['report_year', 'report_quarter'])['total_revenue'].to_dict()
        profit_map = df.set_index(['report_year', 'report_quarter'])['net_profit'].to_dict()

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

        keep_cols = ['code', 'report_date', 'total_revenue', 'operating_profit', 'net_profit',
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
                     'total_equity', 'asset_liability_ratio', 'current_ratio', 'data_source']
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
            'c_pay_acq_const_fids': 'capex',
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
    try:
        import akshare as ak
        start = (today_d - timedelta(days=180)).strftime("%Y%m%d")
        idx = ak.index_zh_a_hist(symbol="000300", period="daily",
                                 start_date=start, end_date=today_d.strftime("%Y%m%d"))
        s = pd.Series(
            [float(v) for v in idx["收盘"].values],
            index=[parse_row_date(v) for v in idx["日期"].values],
        ).sort_index()
        _HS300_CACHE.update(date=today_d, series=s)
        return s
    except Exception as e:
        logger.warning(f"获取沪深300指数失败，跳过相对强弱计算: {e}")
        return None


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
                         turnover: Optional[float] = None, rs_text: str = "") -> str:
    """把带指标的K线 DataFrame 压缩成 LLM 友好的摘要文本（df 按日期降序，最新在前）"""
    latest = df.iloc[0]
    latest_date = parse_row_date(latest.get("date"))
    lines = [f"✅【{stock_code} {freq_label}数据】共 {len(df)} 根K线，数据截至 {latest_date}"]

    g = latest.get
    lines.append(
        f"【最新指标快照】收盘={_fmt(g('close'))} 涨跌幅={_fmt(g('pct_chg'))}% "
        f"量比={_fmt(g('volume_ratio'))}"
        + (f" 换手率={_fmt(turnover)}%" if turnover is not None else "")
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
            turnover=_get_latest_turnover(stock_code),
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
    lines.append(f"  - 资本开支（购建固定资产等支付现金）: {to_yi(latest.get('capex'))}")
    if _num(latest.get('free_cashflow')) is not None:
        lines.append(f"  - 自由现金流: {to_yi(latest.get('free_cashflow'))}")

    lines.append("\n📊 最近4个报告期（均为累计口径）:")
    for _, row in df.head(4).iterrows():
        rd = row['report_date'].strftime('%Y-%m-%d')
        lines.append(
            f"  {rd} | 经营:{to_yi(row.get('operating_cashflow'))} | "
            f"投资:{to_yi(row.get('investing_cashflow'))} | "
            f"筹资:{to_yi(row.get('financing_cashflow'))} | "
            f"资本开支:{to_yi(row.get('capex'))}"
        )

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
        return _format_income_data(df, stock_code)
    except Exception as e:
        logger.error(f"调用利润表工具失败: {e} {traceback.format_exc()}")
        return "❌ 获取利润表数据失败"


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


stock_analyst_tools = [
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
    )
]
