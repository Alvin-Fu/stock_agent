# core/vector_store.py
import subprocess
import time

from utils.config import load_config
from utils.logger import logger


def _get_chroma_server_cfg():
    """延迟读取 Chroma 服务配置，缺失时给出明确提示而不是 import 期 KeyError"""
    cfg = load_config().get("chroma_server")
    if not cfg:
        raise KeyError("配置缺少 chroma_server 段（host/port/ssl），请在 config.yaml 或 local.yaml 中补充")
    return cfg


def _ensure_chroma_running() -> bool:
    """
    检查 Chroma 服务是否可连接，若不可用尝试自动启动 Docker 容器。
    返回 True=连接成功, False=连接失败（调用方决定跳过或 raise）。
    """
    server_cfg = _get_chroma_server_cfg()
    host = server_cfg["host"]
    port = server_cfg["port"]

    # 先尝试连接
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            return True  # 服务已在运行
    except Exception:
        pass
    finally:
        sock.close()

    # 连接失败，尝试启动 Docker 容器
    logger.info("Chroma 服务未响应，尝试自动启动 Docker 容器...")
    try:
        # 检查容器是否存在（可能 stopped）
        inspect = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", "chromadb"],
            capture_output=True, text=True, timeout=5
        )
        status = inspect.stdout.strip()
        if status == "exited" or status == "created":
            subprocess.run(["docker", "start", "chromadb"], capture_output=True, timeout=10)
            logger.info("已执行 docker start chromadb，等待服务就绪...")
            time.sleep(3)
            # 再次确认
            sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock2.settimeout(3)
            try:
                r2 = sock2.connect_ex((host, port))
                sock2.close()
                if r2 == 0:
                    logger.info("✅ Chroma 容器已自动启动")
                    return True
            except Exception:
                sock2.close()
        elif status == "running":
            logger.warning("Chroma 容器状态为 running 但端口不可达，可能容器内服务未就绪")
        else:
            logger.warning(f"Chroma 容器状态异常: {status}")
    except FileNotFoundError:
        logger.warning("Docker 命令不可用，无法自动启动 Chroma")
    except Exception as e:
        logger.warning(f"自动启动 Chroma 失败: {e}")

    return False


def get_remote_chroma_client(collection_name: str, embedding_function):
    """
    底层工具：连接 Docker 上的远程 Chroma 服务。
    自动检测服务状态，若未运行则尝试 docker start chromadb。
    :param collection_name: 知识库唯一集合名（多知识库隔离）
    :param embedding_function: 向量模型（get_embeddings）
    :return: Chroma 客户端实例，连接失败返回 None
    """
    if not _ensure_chroma_running():
        logger.warning("Chroma 服务不可用，跳过向量存储连接")
        return None

    try:
        from langchain_chroma import Chroma
        server_cfg = _get_chroma_server_cfg()
        chroma_client = Chroma(
            host=server_cfg["host"],
            port=server_cfg["port"],
            ssl=server_cfg.get("ssl", False),
            collection_name=collection_name,
            embedding_function=embedding_function
        )
        logger.info(f"✅ 成功连接远程 Chroma | 集合：{collection_name}")
        return chroma_client

    except Exception as e:
        logger.error(f"❌ 连接 Chroma 失败：{str(e)}")
        return None



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