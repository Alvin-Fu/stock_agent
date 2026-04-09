#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from agent.registry import AgentRegistry
from knowledge_bases.registry import KnowledgeRegistry
from utils.logger import logger

if __name__ == "__main__":
    logger.info("🚀 启动多知识库智能问答系统...")

    # 1. 加载所有知识库（从远程Docker Chroma读取）
    kb_registry = KnowledgeRegistry()
    logger.info("✅ 所有知识库加载完成")

    # 2. 加载所有Agent
    agent_registry = AgentRegistry()
    agents = agent_registry.get_all_agents(kb_registry)
    qa_agent = agents["qa_agent"]
    logger.info("✅ 智能Agent加载完成")

    # 3. 启动交互
    logger.info("\n🎉 可以开始提问了！输入 exit 退出")
    logger.info("支持知识库：kb_product(产品)、kb_technical(技术)、kb_stock(股票)")

    while True:
        query = input("\n请输入问题：")
        if query.lower() == "exit":
            logger.info("👋 退出系统")
            break

        # 选择知识库（默认股票）
        kb_id = input("请指定知识库（默认kb_stock）：") or "kb_stock"

        try:
            result = qa_agent.run(query, kb_id)
            print("\n💡 答案：", result["result"])
            print("\n📄 参考来源：")
            for i, doc in enumerate(result["source_documents"]):
                print(f"{i + 1}. {doc.page_content[:120]}...")
        except Exception as e:
            logger.error(f"处理失败：{str(e)}")