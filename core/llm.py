# core/llm.py
from langchain_ollama import OllamaLLM
from langchain_openai import ChatOpenAI
from utils.config import get_model_config, get_llm_model_config, get_deepseek_model_config, get_embedding_model_config, get_openai_model_config
import os
from typing import Optional, Dict, Any, Union
from functools import lru_cache
from utils.logger import logger

from langchain_core.language_models import BaseChatModel
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langchain_ollama import ChatOllama
from langchain_deepseek import ChatDeepSeek

LLM_CACHE_ENABLED = True

DeepseekFlashModule = "deepseek-v4-flash"
DeepseekProModule = "deepseek-v4-pro"

class LLMFactory:
    """LLM 工厂类，负责创建并缓存语言模型实例"""

    _instances: Dict[str, BaseChatModel] = {}

    # 支持的模型提供商
    PROVIDER_OPENAI = "openai"
    PROVIDER_DEEPSEEK = "deepseek"
    PROVIDER_AZURE = "azure"
    PROVIDER_OLLAMA = "ollama"
    PROVIDER_ANTHROPIC = "anthropic"

    @classmethod
    def get_llm(
        cls,
        model: Optional[str] = None,
        provider: str = PROVIDER_OPENAI,
        temperature: float = 0,
        max_tokens: Optional[int] = None,
        streaming: bool = False,
        **kwargs
    ) -> BaseChatModel:
        """
        获取 LLM 实例（带缓存）

        Args:
            model: 模型名称，None 时使用配置默认值
            provider: 提供商 ('openai', 'azure', 'ollama', 'anthropic')
            temperature: 温度参数
            max_tokens: 最大生成 token 数
            streaming: 是否启用流式输出
            **kwargs: 传递给具体 LLM 类的额外参数

        Returns:
            BaseChatModel 实例
        """
        cache_key = cls._build_cache_key(
            provider=provider,
            model=model or cls._get_default_model(provider),
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )

        if LLM_CACHE_ENABLED and cache_key in cls._instances:
            logger.debug(f"从缓存返回 LLM 实例: {cache_key}")
            return cls._instances[cache_key]

        llm = cls._create_llm(
            model=model,
            provider=provider,
            temperature=temperature,
            max_tokens=max_tokens,
            streaming=streaming,
            **kwargs
        )

        if LLM_CACHE_ENABLED:
            cls._instances[cache_key] = llm

        logger.info(f"创建新的 LLM 实例: provider={provider}, model={model or 'default'}")
        return llm

    @classmethod
    def _create_llm(
        cls,
        model: Optional[str],
        provider: str,
        temperature: float,
        max_tokens: Optional[int],
        streaming: bool,
        **kwargs
    ) -> BaseChatModel:
        """根据提供商创建具体 LLM 实例"""
        if provider == cls.PROVIDER_DEEPSEEK:
            return cls._create_deepseek_llm(
                model=model or DeepseekFlashModule,
                temperature=temperature,
                max_tokens=max_tokens,
                streaming=streaming,
                **kwargs
            )
        elif provider == cls.PROVIDER_OLLAMA:
            return cls._create_ollama_llm(
                model=model or "deepseek-r1:14b",
                temperature=temperature,
                max_tokens=max_tokens,
                streaming=streaming,
                **kwargs
            )
        else:
            raise ValueError(f"不支持的 LLM 提供商: {provider}")

    @classmethod
    def _create_openai_llm(
        cls,
        model: str,
        temperature: float,
        max_tokens: Optional[int],
        streaming: bool,
        **kwargs
    ) -> ChatOpenAI:
        """创建 OpenAI 模型实例"""
        openai_conf = get_openai_model_config()
        openai_api_key = openai_conf["api_key"]
        if not openai_api_key:
            raise ValueError("未配置 OPENAI_API_KEY")
        return ChatOpenAI(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            streaming=streaming,
            api_key=openai_api_key,
            **kwargs
        )

    @classmethod
    def _create_ollama_llm(
        cls,
        model: str,
        temperature: float,
        max_tokens: Optional[int],
        **kwargs
    ) -> ChatOllama:
        """创建本地 Ollama 模型实例"""
        llm_conf = get_llm_model_config()
        base_url = llm_conf["url"]
        return ChatOllama(
            model=model,
            temperature=temperature,
            num_predict=max_tokens,
            base_url=base_url,
            **kwargs
        )

    @classmethod
    def _create_deepseek_llm(
        cls,
        model: str,
        temperature: float,
        max_tokens: Optional[int],
        streaming: bool,
        **kwargs
    ) -> ChatDeepSeek:
        """创建 Deepseek 模型实例"""
        deepseek_conf = get_deepseek_model_config()
        api_key = deepseek_conf["api_key"]
        if not api_key:
            raise ValueError("未配置 DEEPSEEK_API_KEY")
        ds = ChatDeepSeek(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens or 14096,
            api_key=api_key,
            base_url=deepseek_conf["url"],
            verbose= False,
        )
        return ds

    @classmethod
    def _build_cache_key(cls, provider: str, model: str, temperature: float, **kwargs) -> str:
        """生成缓存键（忽略部分不稳定参数）"""
        # 只保留影响模型行为的关键参数
        key_parts = [
            provider,
            model,
            f"temp_{temperature}",
        ]
        if "max_tokens" in kwargs and kwargs["max_tokens"] is not None:
            key_parts.append(f"maxtok_{kwargs['max_tokens']}")
        if "timeout" in kwargs:
            key_parts.append(f"timeout_{kwargs['timeout']}")
        return "|".join(key_parts)

    @classmethod
    def _get_default_model(cls, provider: str) -> str:
        """返回各提供商的默认模型名称"""
        defaults = {
            cls.PROVIDER_OPENAI: DeepseekFlashModule,
            cls.PROVIDER_AZURE: "",
            cls.PROVIDER_OLLAMA: "deepseek-r1:14b",
            cls.PROVIDER_ANTHROPIC: "claude-3-sonnet-20240229",
            cls.PROVIDER_DEEPSEEK: DeepseekFlashModule,
        }
        return defaults.get(provider, "deepseek-r1:14b")

    @classmethod
    def clear_cache(cls) -> None:
        """清空 LLM 实例缓存"""
        cls._instances.clear()
        logger.info("LLM 实例缓存已清空")

    @classmethod
    def get_cached_instance_count(cls) -> int:
        """返回当前缓存的实例数量"""
        return len(cls._instances)


# 便捷函数，直接获取默认 LLM 实例
@lru_cache(maxsize=1)
def get_default_llm() -> BaseChatModel:
    """获取系统默认的 LLM 实例（基于配置文件）"""
    return LLMFactory.get_llm(provider=LLMFactory.PROVIDER_DEEPSEEK, temperature=0.1, model=DeepseekFlashModule)

# 为不同 Agent 预设特定参数的便捷函数
def get_router_llm() -> BaseChatModel:
    """获取路由 Agent 专用的 LLM（低温度，更确定性）"""
    return LLMFactory.get_llm(provider=LLMFactory.PROVIDER_DEEPSEEK, temperature=0.1, model=DeepseekProModule)


def get_analyst_llm() -> BaseChatModel:
    """获取财务分析 Agent 专用的 LLM（更强大的推理模型）"""
    return LLMFactory.get_llm(provider=LLMFactory.PROVIDER_DEEPSEEK, temperature=0.1, model=DeepseekFlashModule)


def get_technical_llm() -> BaseChatModel:
    """获取技术分析 Agent 专用的 LLM（适合技术分析推理）"""
    return LLMFactory.get_llm(provider=LLMFactory.PROVIDER_DEEPSEEK, temperature=0.1, model=DeepseekFlashModule)


def get_responder_llm() -> BaseChatModel:
    """获取回答生成 Agent 专用的 LLM（适中温度，表达自然）"""
    return LLMFactory.get_llm(provider=LLMFactory.PROVIDER_DEEPSEEK, temperature=0.1, model=DeepseekFlashModule)


def get_local_fast_llm() -> BaseChatModel:
    """获取本地快速响应 LLM（用于简单任务）"""
    return LLMFactory.get_llm(
        provider=LLMFactory.PROVIDER_OLLAMA,
        model="deepseek-r1:14b",
        temperature=0.0,
    )

def get_llm():
    """
    🔥 全局远程LLM大模型工厂
    自动连接另一台机器的Ollama服务
    :return: 远程Ollama模型实例
    """
    model_cfg = get_model_config()

    llm = OllamaLLM(
        model=model_cfg["llm_model"],
        base_url=model_cfg["ollama_url"],  # 默认地址 http://127.0.0.1:11434,  # 🔥 指定远程Ollama地址
        temperature=0.1,  # 精准回答，不编造
        verbose=False
    )
    return llm

def get_ds():
    """
    获取ds
    """
    model_cfg = get_model_config()
    llm = ChatOpenAI(
        model=model_cfg["deepseek_model"],
        base_url=model_cfg["deepseek_url"],  # 默认地址 http://127.0.0.1:11434,  # 🔥 指定远程Ollama地址
        api_key=model_cfg["deepseek_api_key"],
        temperature=0.1,  # 精准回答，不编造
        verbose=False
    )

    return llm