#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from agent.registry import AgentRegistry
from knowledge_bases.registry import KnowledgeRegistry
from utils.logger import logger
import traceback

if __name__ == "__main__":
    logger.info("🚀 启动多知识库智能系统...")

    # 1. 加载所有知识库（从远程Docker Chroma读取）
    kb_registry = KnowledgeRegistry()
    logger.info("✅ 所有知识库加载完成")

    # 2. 加载所有Agent
    agent_registry = AgentRegistry()
    agents = agent_registry.get_all_agents(kb_registry)
    brain_agent = agents["router_brain"]  # 🔥 直接使用大脑
    logger.info("✅ 智能Agent加载完成")

    # ===================== 最终交互 =====================
    logger.info("\n🎉 我是你的智能助手，可直接提问！")
    logger.info("支持：股票知识、产品文档、技术方案")

    while True:
        query = input("\n请输入你的问题：")
        if query.lower() in ["exit", "quit"]:
            logger.info("👋 再见！")
            break

        try:
            # 🔥 只用大脑，全自动处理！
            result = brain_agent.run(query)
            # 输出答案
            logger.info(f"💡 智能回答：\n{result}")

        except Exception as e:
            logger.error(f"大脑处理失败：{str(e)}  {traceback.format_exc()}")