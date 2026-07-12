#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from utils.logger import logger
from tools.stock_tools import stock_tool_instance

if __name__ == "__main__":
    logger.info(f"测试股票信息对应的接口")
    stock_tool_instance.fetch_and_save_stock_daily_data("002594")
