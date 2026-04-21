#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from agents.registry import AgentRegistry
from knowledge_bases.registry import KnowledgeRegistry
from utils.logger import logger
import traceback
from tools.stock import DataFetcherManager
from tools.stock_tools import stock_tool_instance

if __name__ == "__main__":
    logger.info(f"测试股票信息对应的接口")
    stock_tool_instance.fetch_and_save_stock_daily_data("002594")
