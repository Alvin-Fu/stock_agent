#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔥 知识库初始化脚本
功能：将本地文档写入 Docker 远程 Chroma，生成向量知识库
执行：python init_knowledge_base.py
"""
from core.embeddings import get_embeddings
from core.vector_store import create_remote_chroma
from utils import load_documents_from_dir, split_documents
from utils.config import get_all_kb_config
from utils.logger import logger


def init_single_knowledge_base(kb_id: str, kb_config: dict):
    """
    初始化单个知识库（写入Chroma）
    :param kb_id: 知识库ID (kb_product)
    :param kb_config: 配置
    """
    logger.info(f"🚀 开始初始化知识库：{kb_config['name']}")

    # 1. 获取嵌入模型（转向量用）
    embeddings = get_embeddings()

    # 2. 加载本地文档
    docs = load_documents_from_dir(kb_config["data_path"])
    if not docs:
        logger.warning(f"⚠️ {kb_config['name']} 无文档，跳过写入")
        return

    # 3. 切分长文本
    split_docs = split_documents(
        documents=docs,
        chunk_size=kb_config["chunk_size"],
        chunk_overlap=kb_config["chunk_overlap"]
    )

    # 4. 🔥 写入远程 Docker Chroma（核心步骤）
    create_remote_chroma(
        split_docs=split_docs,
        embedding_function=embeddings,
        collection_name=kb_config["collection_name"]
    )

    logger.info(f"✅ {kb_config['name']} 写入 Chroma 完成！")


def init_all_knowledge_bases():
    """初始化所有知识库"""
    logger.info("🌟 开始批量初始化所有知识库...")

    all_kb_config = get_all_kb_config()

    # 遍历所有配置，依次写入Chroma
    for kb_id, kb_config in all_kb_config.items():
        init_single_knowledge_base(kb_id, kb_config)

    logger.info("🎉 所有知识库已成功写入 Chroma！")


if __name__ == "__main__":
    # 执行初始化
    init_all_knowledge_bases()