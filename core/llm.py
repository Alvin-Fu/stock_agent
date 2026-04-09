# core/llm.py
from langchain_ollama import OllamaLLM
from utils.config import get_model_config


def get_llm():
    """
    🔥 全局远程LLM大模型工厂
    自动连接另一台机器的Ollama服务
    :return: 远程Ollama模型实例
    """
    model_cfg = get_model_config()

    llm = OllamaLLM(
        model=model_cfg["embedding_model"],
        base_url=model_cfg["ollama_url"],  # 默认地址 http://127.0.0.1:11434,  # 🔥 指定远程Ollama地址
        temperature=0.1,  # 精准回答，不编造
        verbose=False
    )
    return llm