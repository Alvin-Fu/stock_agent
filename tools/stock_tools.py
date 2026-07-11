from dateutil.utils import today

from .stock.base import DataFetcherManager
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
        daily_datas,  fetcher_name = self.data_manager.get_daily_data(stock_code, old_daily_data, start_date_str, end_date_str)
        if daily_datas is None or daily_datas.empty:
            logger.error(f"获取股票[{stock_code}]数据为空")
            return  old_daily_data
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
        monthly_datas,  fetcher_name = self.data_manager.get_monthly_data(stock_code, old_monthly_data, start_date_str, end_date_str)
        if monthly_datas is None or monthly_datas.empty:
            logger.error(f"获取股票[{stock_code}]数据为空")
            return  old_monthly_data
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
        weekly_datas,  fetcher_name = self.data_manager.get_weekly_data(stock_code, old_weekly_data, start_date_str, end_date_str)
        if weekly_datas is None or weekly_datas.empty:
            logger.error(f"获取股票[{stock_code}]数据为空")
            return  old_weekly_data
        save_count = self.db.save_week_data(weekly_datas, stock_code, start_date, fetcher_name)
        logger.info(f"保存的数据为[{save_count}]")
        return weekly_datas

    def fetch_and_save_stock_basic_daily(self, stock_code: str)-> Union[pd.DataFrame, None]:
        """"
        获取股票的基本信息每日指标
        """
        if stock_code is None:
            logger.error(f"code is null")
            return None
        today = date.today()
        old_basic_data = self.db.get_latest_daily_basic_data(stock_code, 10)
        start_date = self.get_basic_daily_start_date(stock_code, old_basic_data)
        end_date_str = today.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")
        if end_date_str == start_date_str:
            logger.info(f"股票[{stock_code}]数据已经更新完成")
            return  old_basic_data
        new_basic_daily = self.tushare.stock_daily_basic( start_date=start_date_str, end_date=end_date_str, stock_code=stock_code)
        return old_basic_data

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
        if  len(old_basic_daily_data) == 0 or old_basic_daily_data[0].date is None:
            start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            if start_date is None :
                logger.error(f"股票的基本信息为空通过接口获取数据[{stock_code}]")
                # 全量加载一次
                self.save_stock_basic_by_tushare()
                start_date = self.get_stock_start_date_by_stock_basic(stock_code)
            return start_date
        return old_basic_daily_data.iloc[0].get('date')


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
            pdf_name_m, analyze_list = self.db.get_financial_analyze(code)

            logger.warning(f"已存在的研报[{pdf_name_m}]")
            need_analyze_list = pd.DataFrame(columns=["pdf_name", "pdf_url", "content", "code", "report_date"])
            for _, row in df.iterrows():
                report_date = row.get("date")
                if report_date is None:
                    logger.error(f"[{code}] 研报[{report_date}]无日期")
                    continue
                report_date = parse_row_date(report_date)

                half_year_ago = date.today() - timedelta(days=2)

                # 如果研报日期早于半年前，跳过
                if report_date < half_year_ago:
                    logger.debug(f"[{code}] 研报 {pdf_name_m} 日期 ({report_date}) 早于 ({half_year_ago})，已忽略")
                    continue

                if report_date in pdf_name_m:
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
                if content is None:
                    logger.error(f"[{code}] 获取股票研报内容失败")
                    continue
                need_analyze_list.append(
                    {
                        "pdf_name": pdf_name,
                        "pdf_url": pdf_url,
                        "content": content,
                        "code": code,
                        "report_date": report_date
                    }
                )



            return need_analyze_list
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

    def _normalize_income_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化利润表数据
        Tushare income 返回的主要字段：
        ts_code, ann_date, end_date, total_revenue, operate_profit, n_income, basic_eps, oper_cost
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
                     'basic_eps', 'revenue_growth', 'profit_growth', 'gross_margin', 'data_source']
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
        }

        df = df.rename(columns=column_mapping)

        if 'report_date' in df.columns:
            df['report_date'] = pd.to_datetime(df['report_date'], format='%Y%m%d')

        df['code'] = stock_code

        for col in ['total_assets', 'total_liabilities', 'current_assets', 'current_liabilities']:
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


stock_tool_instance = StockTools()  # 传入你的数据库连接

# ===================== 1. 注册：日线数据工具 =====================
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
        # 格式化输出（美观）
        return f"✅ 【{stock_code} 日线数据】\n{df.head(200).to_string()}"
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
        return f"✅ 【{stock_code} 周线数据】\n{df.head(200).to_string()}"
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
        return f"✅ 【{stock_code} 月线数据】\n{df.head(200).to_string()}"
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
    """格式化利润表数据为易于大模型理解的文本"""
    if df is None or df.empty:
        return f"❌ 未获取到 {stock_code} 的利润表数据"

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

    lines = [f"✅ 【{stock_code} 利润表数据】共 {len(df)} 条记录"]
    lines.append(f"\n📅 最新报告期: {report_date}")
    lines.append(f"  - 营业收入: {to_yi(latest.get('total_revenue'))}")
    lines.append(f"  - 营业利润: {to_yi(latest.get('operating_profit'))}")
    lines.append(f"  - 净利润: {to_yi(latest.get('net_profit'))}")
    lines.append(f"  - 基本每股收益: {latest.get('basic_eps', 'N/A')}")
    lines.append(f"  - 营收同比增长: {to_pct(latest.get('revenue_growth'))}")
    lines.append(f"  - 净利润同比增长: {to_pct(latest.get('profit_growth'))}")
    lines.append(f"  - 毛利率: {to_pct(latest.get('gross_margin'))}")

    lines.append("\n📊 最近4个季度趋势:")
    for _, row in df.head(4).iterrows():
        rd = row.get('report_date', '')
        if hasattr(rd, 'strftime'):
            rd = rd.strftime('%Y-%m-%d')
        lines.append(
            f"  {rd} | 营收:{to_yi(row.get('total_revenue'))} | "
            f"净利润:{to_yi(row.get('net_profit'))} | "
            f"EPS:{row.get('basic_eps', 'N/A')} | "
            f"毛利率:{to_pct(row.get('gross_margin'))}"
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
    )
]
