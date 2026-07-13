import os
from typing import Dict, Any

import yaml

# 项目根目录（自动获取，避免路径错误）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 配置文件路径：优先读 local.yaml（个人配置，不入库），没有则回退仓库自带的 config.yaml
_LOCAL_CONFIG = os.path.join(PROJECT_ROOT, "local.yaml")
_DEFAULT_CONFIG = os.path.join(PROJECT_ROOT, "config.yaml")
CONFIG_PATH = _LOCAL_CONFIG if os.path.exists(_LOCAL_CONFIG) else _DEFAULT_CONFIG

# 单例缓存（配置只加载一次，提升性能）
_CONFIG_CACHE: Dict[str, Any] = None

def load_config() -> Dict[str, Any]:
    """
    加载并返回全局配置（单例模式，重复调用只加载一次）
    :return: 完整配置字典
    """
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    # 异常处理：配置文件不存在
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"配置文件不存在！请检查路径：{CONFIG_PATH}")

    # 读取并解析YAML
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            _CONFIG_CACHE = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"配置文件YAML格式错误：{str(e)}")
    except Exception as e:
        raise RuntimeError(f"加载配置失败：{str(e)}")

    return _CONFIG_CACHE

def get_model_config() -> Dict[str, Any]:
    """获取全局模型配置（嵌入模型+LLM）"""
    config = load_config()
    return config.get("models", {})

def get_openai_model_config() -> Dict[str, Any]:
    """获取OpenAI模型配置"""
    config = load_config()
    return config.get("models", {}).get("openai", {})

def get_deepseek_model_config() -> Dict[str, Any]:
    """获取Deepseek模型配置"""
    config = load_config()
    return config.get("models", {}).get("deepseek", {})

def get_embedding_model_config() -> Dict[str, Any]:
    """获取嵌入模型配置"""
    config = load_config()
    return config.get("embedding", {})

def get_llm_model_config() -> Dict[str, Any]:
    """获取LLM模型配置"""
    config = load_config()
    return config.get("models", {}).get("ollama", {})

def get_all_kb_config() -> Dict[str, Any]:
    """获取所有知识库配置"""
    config = load_config()
    return config.get("knowledge_bases", {})

def get_kb_config(kb_id: str) -> Dict[str, Any]:
    """
    根据ID获取单个知识库配置
    :param kb_id: 知识库ID（如 kb_product）
    """
    all_kb = get_all_kb_config()
    kb_config = all_kb.get(kb_id)
    if not kb_config:
        raise ValueError(f"不存在该知识库配置：{kb_id}")
    return kb_config

def get_all_agent_config() -> Dict[str, Any]:
    """获取所有Agent配置"""
    config = load_config()
    return config.get("agents", {})

# 各 Agent LLM 的代码内兜底默认值（配置文件缺失时也能跑）
_AGENT_LLM_DEFAULTS: Dict[str, Any] = {
    "provider": "deepseek",
    "model": "deepseek-v4-flash",
    "temperature": 0.1,
    "max_tokens": None,
}

def get_agent_llm_config(agent_name: str) -> Dict[str, Any]:
    """
    获取指定 Agent 的 LLM 配置，三层合并（后者覆盖前者）：
    代码内兜底默认 ← agents.defaults ← agents.<agent_name>
    agent_name 传 "default" 时只应用全局默认（供未单列的组件使用）
    """
    agents_cfg = get_all_agent_config() or {}
    merged = dict(_AGENT_LLM_DEFAULTS)
    merged.update(agents_cfg.get("defaults") or {})
    if agent_name != "default":
        merged.update(agents_cfg.get(agent_name) or {})
    return merged

def get_db_config() -> Dict[str, Any]:
    """获取数据库配置"""
    config = load_config()
    return config.get("database", {})

def get_stock_tools_config() -> Dict[str, Any]:
    """获取股票工具配置"""
    config = load_config()
    return config.get("tools", {}).get("stock", {})

def get_search_config() -> Dict[str, Any]:
    """获取搜索配置"""
    config = load_config()
    return config.get("search", {})

def ensure_runtime_config() -> None:
    """
    启动即校验关键配置，缺什么当场报清楚——曾因 local.yaml 缺失静默回退到
    密钥全空的 config.yaml，一路跑到 LLM 调用才炸，浪费整段链路还不好定位。
    各入口（main/feishu_bot/golden_run）启动时调用；缺关键项抛 RuntimeError。
    """
    cfg = load_config() or {}
    problems = []
    ds_key = (((cfg.get("models") or {}).get("deepseek") or {}).get("api_key") or "").strip()
    if not ds_key:
        problems.append("models.deepseek.api_key 为空（LLM 无法调用，分析链路必挂）")
    if problems:
        hint = f"当前生效配置: {CONFIG_PATH}"
        if CONFIG_PATH == _DEFAULT_CONFIG:
            hint += "（未发现 local.yaml——个人密钥应放项目根目录 local.yaml，结构参照 config.yaml）"
        raise RuntimeError("配置检查未通过：\n- " + "\n- ".join(problems) + "\n" + hint)


def get_retriever_config() -> Dict[str, Any]:
    """获取检索配置"""
    config = load_config()
    return config.get("retriever", {"top_k": 3, "use_reranker": False})
