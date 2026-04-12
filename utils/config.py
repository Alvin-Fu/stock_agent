import os
from typing import Dict, Any

import yaml

# 项目根目录（自动获取，避免路径错误）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 配置文件路径
CONFIG_PATH = os.path.join(PROJECT_ROOT, "local.yaml")

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

def get_agent_config(agent_id: str) -> Dict[str, Any]:
    """
    根据ID获取单个Agent配置
    :param agent_id: AgentID（如 qa_agent）
    """
    all_agent = get_all_agent_config()
    agent_config = all_agent.get(agent_id)
    if not agent_config:
        raise ValueError(f"不存在该Agent配置：{agent_id}")
    return agent_config

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
