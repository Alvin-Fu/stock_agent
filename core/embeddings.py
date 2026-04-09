# core/embeddings.py （远程Ollama嵌入模型）
# 替换为LangChain官方远程Ollama嵌入
from langchain_ollama import OllamaEmbeddings
from utils.config import get_model_config


def get_embeddings():
    """
    远程Ollama嵌入模型（调用另一台机器）
    """
    model_cfg = get_model_config()

    # 初始化远程嵌入模型
    embeddings = OllamaEmbeddings(
        model=model_cfg["embedding_model"],  # 可用：nomic-embed-text （Ollama专用嵌入模型）
        base_url=model_cfg["ollama_url"],  # 核心：指定远程地址
    )
    return embeddings