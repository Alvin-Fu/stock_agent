"""
API 密钥管理：多 key 轮换 + 配额耗尽自动跳过。

用法：
    from utils.keys import get_zhihu_secrets, mark_zhihu_key_dead
    for secret in get_zhihu_secrets():
        try:
            # 用 secret 请求
        except SomeQuotaError:
            mark_zhihu_key_dead(secret, "配额耗尽")
            continue
"""

import threading
from typing import List, Tuple
from utils.config import get_search_config
from utils.logger import logger

# ===== 知乎 API key 管理 =====

_zhihu_all_keys: List[str] = []
_zhihu_dead_keys: set = set()
_zhihu_lock = threading.Lock()


def _load_zhihu_keys() -> List[str]:
    """从配置加载知乎 key 列表（逗号分隔）"""
    raw = (get_search_config().get("zhihu_api_secret") or "").strip()
    if not raw:
        return []
    return [k.strip() for k in raw.split(",") if k.strip()]


def get_zhihu_secrets() -> List[str]:
    """返回当前所有可用的知乎 API secrets（自动过滤已耗尽的 key）"""
    global _zhihu_all_keys
    with _zhihu_lock:
        if not _zhihu_all_keys:
            _zhihu_all_keys = _load_zhihu_keys()
        return [k for k in _zhihu_all_keys if k not in _zhihu_dead_keys]


def mark_zhihu_key_dead(secret: str, reason: str = ""):
    """标记某个知乎 key 为已耗尽，后续请求将跳过它"""
    with _zhihu_lock:
        if secret not in _zhihu_dead_keys:
            _zhihu_dead_keys.add(secret)
    logger.warning(f"知乎 API key 已标记不可用（{reason}）: {secret[:12]}...")


def zhihu_keys_count() -> Tuple[int, int]:
    """返回 (可用 key 数, 总 key 数)"""
    with _zhihu_lock:
        if not _zhihu_all_keys:
            _zhihu_all_keys = _load_zhihu_keys()
        return len([k for k in _zhihu_all_keys if k not in _zhihu_dead_keys]), len(_zhihu_all_keys)


def reset_zhihu_dead_keys():
    """重置所有知乎 key 的死亡标记（进程内有需要时手动调用）"""
    with _zhihu_lock:
        _zhihu_dead_keys.clear()
