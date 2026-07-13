#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主入口：多 Agent 股票/产业链分析系统
大脑（router）负责任务拆解分派，各专职 Agent 执行，responder 整合，compliance 收尾审查。
与 cli.py / web_ui.py 共用同一套 LangGraph 编排。
"""
import traceback

from orchestration.workflow import WorkflowExecutor
from utils.config import ensure_runtime_config
from utils.logger import logger

if __name__ == "__main__":
    ensure_runtime_config()  # 关键配置缺失时启动即报错，不等跑到 LLM 调用才炸
    logger.info("🚀 启动多 Agent 智能分析系统...")
    executor = WorkflowExecutor(enable_memory=True)
    logger.info("✅ 工作流加载完成")

    logger.info("\n🎉 我是你的智能助手，可直接提问！")
    logger.info("支持：个股分析（公司名或股票代码）、产业链上下游公司分析")

    while True:
        query = input("\n请输入你的问题：")
        if query.lower() in ["exit", "quit"]:
            logger.info("👋 再见！")
            break

        try:
            state = executor.run_sync(query)
            answer = executor.get_final_answer(state)
            logger.info(f"💡 智能回答：\n{answer}")
        except Exception as e:
            logger.error(f"处理失败：{str(e)}  {traceback.format_exc()}")
