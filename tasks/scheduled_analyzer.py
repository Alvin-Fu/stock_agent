"""
定时分析任务管理器
负责定时分析特定行业中的股票的财务、K线等数据
"""
import schedule
import time
import threading
from datetime import datetime, date
from typing import List, Dict, Any

from storage.sqlite.stock_storage import DatabaseManager
from tools.stock_tools import stock_tool_instance
from orchestration.graph import get_default_graph
from agents.base import AgentState
from utils.constants import IntentType
from utils.logger import logger


class ScheduledAnalyzer:
    """
    定时分析任务管理器
    """
    
    def __init__(self):
        """
        初始化定时分析任务管理器
        """
        self.db = DatabaseManager.get_instance()
        self.graph = get_default_graph(enable_memory=False)
        self.running = False
        self.thread = None
    
    def get_stocks_by_industry(self, industry: str) -> List[Dict[str, Any]]:
        """
        根据行业获取股票列表
        
        Args:
            industry: 行业名称
            
        Returns:
            股票列表，每个元素包含股票代码、名称等信息
        """
        try:
            with self.db.get_session() as session:
                from storage.sqlite.stock_storage import StockBasic
                stocks = session.query(StockBasic).filter(StockBasic.industry == industry).all()
                return [stock.to_dict() for stock in stocks]
        except Exception as e:
            logger.error(f"获取行业股票失败: {e}")
            return []
    
    def analyze_stock(self, stock_code: str):
        """
        分析单个股票
        
        Args:
            stock_code: 股票代码
        """
        try:
            logger.info(f"开始分析股票: {stock_code}")
            
            # 1. 获取K线数据
            logger.info(f"获取股票 {stock_code} 的K线数据")
            stock_tool_instance.fetch_and_save_stock_daily_data(stock_code)
            stock_tool_instance.fetch_and_save_stock_weekly_data(stock_code)
            stock_tool_instance.fetch_and_save_stock_monthly_data(stock_code)
            
            # 2. 获取研报数据
            logger.info(f"获取股票 {stock_code} 的研报数据")
            stock_tool_instance.fetch_and_save_stock_research_report(stock_code)
            
            # 3. 进行综合分析（使用多Agent协作图）
            logger.info(f"分析股票 {stock_code} 的财务数据和走势")
            state = AgentState(
                messages=[],
                question=f"分析{stock_code}的财务状况、投资价值和股票走势",
                intent=IntentType.FINANCIAL_ANALYSIS,
                documents=[],
                financial_data=None,
                analysis_result=None,
                research_result=None,
                compliance_result=None,
                final_answer=None,
                intermediate_steps=[],
                next_agent=None,
                error=None
            )
            result = self.graph.invoke(state)
            
            logger.info(f"股票 {stock_code} 分析完成")
        except Exception as e:
            logger.error(f"分析股票 {stock_code} 失败: {e}")
    
    def analyze_industry(self, industry: str):
        """
        分析特定行业的所有股票
        
        Args:
            industry: 行业名称
        """
        logger.info(f"开始分析行业: {industry}")
        stocks = self.get_stocks_by_industry(industry)
        
        if not stocks:
            logger.warning(f"行业 {industry} 没有找到股票")
            return
        
        logger.info(f"行业 {industry} 共有 {len(stocks)} 只股票")
        
        for stock in stocks:
            stock_code = stock.get('code')
            stock_name = stock.get('name')
            logger.info(f"开始分析股票: {stock_code} - {stock_name}")
            self.analyze_stock(stock_code)
        
        logger.info(f"行业 {industry} 分析完成")
    
    def schedule_industry_analysis(self, industry: str, time_str: str):
        """
        安排行业分析定时任务
        
        Args:
            industry: 行业名称
            time_str: 定时时间，格式如 "10:00"（每天10点执行）
        """
        schedule.every().day.at(time_str).do(self.analyze_industry, industry=industry)
        logger.info(f"已安排行业 {industry} 的定时分析任务，执行时间: {time_str}")
    
    def start(self):
        """
        启动定时任务
        """
        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler)
        self.thread.daemon = True
        self.thread.start()
        logger.info("定时分析任务已启动")
    
    def stop(self):
        """
        停止定时任务
        """
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("定时分析任务已停止")
    
    def _run_scheduler(self):
        """
        运行调度器
        """
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次


# 示例用法
if __name__ == "__main__":
    analyzer = ScheduledAnalyzer()
    
    # 安排行业分析任务
    analyzer.schedule_industry_analysis("白酒", "10:00")
    analyzer.schedule_industry_analysis("半导体", "11:00")
    analyzer.schedule_industry_analysis("医药", "14:00")
    
    # 启动定时任务
    analyzer.start()
    
    # 保持运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        analyzer.stop()
