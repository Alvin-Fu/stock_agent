# core/vector_store.py
from langchain_chroma import Chroma  # 建议使用新包名
from utils.config import load_config
from utils.logger import logger

# 加载 Docker Chroma 服务配置（从config.yaml读取）
config = load_config()
CHROMA_SERVER_CFG = config["chroma_server"]


def get_remote_chroma_client(collection_name: str, embedding_function):
    """
    底层工具：连接 Docker 上的远程 Chroma 服务
    :param collection_name: 知识库唯一集合名（多知识库隔离）
    :param embedding_function: 向量模型（get_embeddings）
    :return: Chroma 客户端实例
    """
    try:
        # 🔥 关键：远程连接 Docker Chroma（无本地路径，只用IP+端口）
        chroma_client = Chroma(
            # Docker Chroma 连接信息
            host=CHROMA_SERVER_CFG["host"],
            port=CHROMA_SERVER_CFG["port"],
            ssl=CHROMA_SERVER_CFG["ssl"],

            # 多知识库 = 不同集合（完全隔离）
            collection_name=collection_name,

            # 向量模型
            embedding_function=embedding_function
        )
        logger.info(f"✅ 成功连接远程 Chroma | 集合：{collection_name}")
        return chroma_client

    except Exception as e:
        logger.error(f"❌ 连接 Chroma 失败：{str(e)}")
        raise ConnectionError("请检查 Docker Chroma 是否启动！")



def create_remote_chroma(split_docs, embedding_function, collection_name: str):
    """
    🔥 核心函数：将切分好的文档写入 Docker Chroma，生成知识库
    :param split_docs: 切分后的文本块
    :param embedding_function: 向量模型
    :param collection_name: 知识库集合名
    :return: Chroma 向量库实例
    """
    # 1. 获取远程 Chroma 连接
    chroma_db = get_remote_chroma_client(collection_name, embedding_function)

    # 2. 写入文档向量（不存在则创建集合，存在则追加）
    if split_docs and len(split_docs) > 0:
        chroma_db.add_documents(documents=split_docs)
        logger.info(f"✅ 写入 {len(split_docs)} 条向量到 Chroma | 集合：{collection_name}")
    else:
        logger.warning(f"⚠️ 无文本块可写入：{collection_name}")

    return chroma_db