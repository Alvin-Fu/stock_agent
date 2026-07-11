# core/vector_store.py
from utils.config import load_config
from utils.logger import logger


def _get_chroma_server_cfg():
    """延迟读取 Chroma 服务配置，缺失时给出明确提示而不是 import 期 KeyError"""
    cfg = load_config().get("chroma_server")
    if not cfg:
        raise KeyError("配置缺少 chroma_server 段（host/port/ssl），请在 config.yaml 或 local.yaml 中补充")
    return cfg


def get_remote_chroma_client(collection_name: str, embedding_function):
    """
    底层工具：连接 Docker 上的远程 Chroma 服务
    :param collection_name: 知识库唯一集合名（多知识库隔离）
    :param embedding_function: 向量模型（get_embeddings）
    :return: Chroma 客户端实例
    """
    try:
        # 延迟导入：未安装 langchain-chroma 时不影响纯分析链路的 import
        from langchain_chroma import Chroma
        server_cfg = _get_chroma_server_cfg()
        # 🔥 关键：远程连接 Docker Chroma（无本地路径，只用IP+端口）
        chroma_client = Chroma(
            # Docker Chroma 连接信息
            host=server_cfg["host"],
            port=server_cfg["port"],
            ssl=server_cfg.get("ssl", False),

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



def reset_remote_collection(collection_name: str):
    """删除远程 Chroma 中的集合（重建知识库前调用，避免重复追加同一批文档）"""
    import chromadb
    server_cfg = _get_chroma_server_cfg()
    client = chromadb.HttpClient(host=server_cfg["host"], port=server_cfg["port"])
    try:
        client.delete_collection(name=collection_name)
        logger.info(f"🗑️ 已删除旧集合：{collection_name}")
    except Exception:
        logger.info(f"集合 {collection_name} 不存在，无需删除")


def create_remote_chroma(split_docs, embedding_function, collection_name: str, rebuild: bool = False):
    """
    🔥 核心函数：将切分好的文档写入 Docker Chroma，生成知识库
    :param split_docs: 切分后的文本块
    :param embedding_function: 向量模型
    :param collection_name: 知识库集合名
    :param rebuild: True 时先删除旧集合再写入（重跑初始化脚本不会堆积重复向量）
    :return: Chroma 向量库实例
    """
    if rebuild:
        reset_remote_collection(collection_name)

    # 1. 获取远程 Chroma 连接
    chroma_db = get_remote_chroma_client(collection_name, embedding_function)

    # 2. 写入文档向量（不存在则创建集合，存在则追加）
    if split_docs and len(split_docs) > 0:
        chroma_db.add_documents(documents=split_docs)
        logger.info(f"✅ 写入 {len(split_docs)} 条向量到 Chroma | 集合：{collection_name}")
    else:
        logger.warning(f"⚠️ 无文本块可写入：{collection_name}")

    return chroma_db