"""
数据缓存与增量更新模块
提供多级缓存策略和智能增量更新机制
"""

import time
import pickle
import os
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import deque
import hashlib
from utils.logger import logger


class DataCacheManager:
    """
    多级数据缓存管理器
    
    缓存层级：
    1. 内存缓存（L1）：最快，5分钟过期
    2. 文件缓存（L2）：较快，1小时过期
    3. 数据库缓存（L3）：持久化，手动清理
    
    缓存策略：
    - 优先使用内存缓存
    - 内存未命中时查找文件缓存
    - 文件未命中时查找数据库
    - 都未命中时从数据源获取并填充各级缓存
    """

    def __init__(self, cache_dir: str = "./data/cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # 内存缓存（L1）
        self.memory_cache: Dict[str, Dict] = {}
        self.memory_ttl = 300  # 5分钟
        
        # 文件缓存（L2）
        self.file_ttl = 3600  # 1小时
        
        # 访问频率统计（用于缓存淘汰）
        self.access_count: Dict[str, int] = {}
        self.max_memory_size = 1000  # 内存缓存最大条目数

    def _generate_key(self, stock_code: str, data_type: str, **kwargs) -> str:
        """生成唯一缓存键"""
        key_parts = [stock_code, data_type]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={v}")
        return hashlib.md5("|".join(key_parts).encode()).hexdigest()

    def get(self, stock_code: str, data_type: str, **kwargs) -> Optional[Any]:
        """获取缓存数据"""
        key = self._generate_key(stock_code, data_type, **kwargs)
        
        # 1. 检查内存缓存（L1）
        if key in self.memory_cache:
            entry = self.memory_cache[key]
            if time.time() - entry['timestamp'] < self.memory_ttl:
                self.access_count[key] = self.access_count.get(key, 0) + 1
                logger.debug(f"[缓存命中] L1内存缓存: {key}")
                return entry['data']
        
        # 2. 检查文件缓存（L2）
        file_path = os.path.join(self.cache_dir, f"{key}.pkl")
        if os.path.exists(file_path):
            mtime = os.path.getmtime(file_path)
            if time.time() - mtime < self.file_ttl:
                try:
                    with open(file_path, 'rb') as f:
                        data = pickle.load(f)
                        # 同时加载到内存缓存
                        self._set_memory(key, data)
                        self.access_count[key] = self.access_count.get(key, 0) + 1
                        logger.debug(f"[缓存命中] L2文件缓存: {key}")
                        return data
                except Exception as e:
                    logger.error(f"读取文件缓存失败: {e}")
        
        logger.debug(f"[缓存未命中] {key}")
        return None

    def set(self, stock_code: str, data_type: str, data: Any, **kwargs):
        """设置缓存数据"""
        key = self._generate_key(stock_code, data_type, **kwargs)
        
        # 1. 设置内存缓存（L1）
        self._set_memory(key, data)
        
        # 2. 设置文件缓存（L2）
        self._set_file(key, data)

    def _set_memory(self, key: str, data: Any):
        """设置内存缓存"""
        # 检查是否需要淘汰
        if len(self.memory_cache) >= self.max_memory_size:
            self._evict_least_used()
        
        self.memory_cache[key] = {
            'data': data,
            'timestamp': time.time()
        }

    def _set_file(self, key: str, data: Any):
        """设置文件缓存"""
        file_path = os.path.join(self.cache_dir, f"{key}.pkl")
        try:
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.error(f"写入文件缓存失败: {e}")

    def _evict_least_used(self):
        """淘汰访问频率最低的缓存"""
        if not self.access_count:
            # 如果没有访问记录，删除最早的
            oldest_key = min(self.memory_cache.keys(), 
                           key=lambda k: self.memory_cache[k]['timestamp'])
            del self.memory_cache[oldest_key]
        else:
            # 删除访问频率最低的
            least_used_key = min(self.access_count.keys(), key=self.access_count.get)
            del self.memory_cache[least_used_key]
            del self.access_count[least_used_key]
        logger.debug(f"[缓存淘汰] 已清理一条缓存")

    def invalidate(self, stock_code: str, data_type: str, **kwargs):
        """使指定缓存失效"""
        key = self._generate_key(stock_code, data_type, **kwargs)
        
        if key in self.memory_cache:
            del self.memory_cache[key]
        
        file_path = os.path.join(self.cache_dir, f"{key}.pkl")
        if os.path.exists(file_path):
            os.remove(file_path)
        
        if key in self.access_count:
            del self.access_count[key]
        
        logger.debug(f"[缓存失效] {key}")

    def clear_all(self):
        """清空所有缓存"""
        self.memory_cache.clear()
        self.access_count.clear()
        
        for filename in os.listdir(self.cache_dir):
            if filename.endswith('.pkl'):
                os.remove(os.path.join(self.cache_dir, filename))
        
        logger.info("[缓存清理] 所有缓存已清空")


class SmartIncrementalUpdater:
    """
    智能增量更新管理器
    
    更新策略：
    - 行情数据：实时更新（盘后）
    - 财务数据：财报发布日更新
    - 研报数据：每日更新
    - 股东数据：定期报告发布后更新
    """

    def __init__(self):
        self.update_history: Dict[str, Dict[str, float]] = {}  # {stock_code: {data_type: timestamp}}
        
    def needs_update(self, stock_code: str, data_type: str) -> Tuple[bool, str]:
        """
        判断是否需要更新
        
        Returns:
            (needs_update: bool, reason: str)
        """
        last_update = self._get_last_update(stock_code, data_type)
        now = time.time()
        
        # 根据数据类型决定更新频率
        update_rules = {
            'daily': {'interval': 86400, 'description': '每日更新'},  # 24小时
            'weekly': {'interval': 604800, 'description': '每周更新'},  # 7天
            'monthly': {'interval': 2592000, 'description': '每月更新'},  # 30天
            'financial': {'interval': 604800, 'description': '每周检查'},  # 7天
            'research_report': {'interval': 86400, 'description': '每日更新'},  # 24小时
            'realtime': {'interval': 60, 'description': '每分钟更新'},  # 1分钟
            'chip_distribution': {'interval': 3600, 'description': '每小时更新'},  # 1小时
        }
        
        rule = update_rules.get(data_type, {'interval': 86400, 'description': '默认每日更新'})
        
        if last_update is None:
            return True, "首次获取"
        
        if now - last_update >= rule['interval']:
            return True, f"超过{rule['description']}周期"
        
        return False, f"最近{rule['description']}内已更新"

    def record_update(self, stock_code: str, data_type: str):
        """记录更新时间"""
        if stock_code not in self.update_history:
            self.update_history[stock_code] = {}
        
        self.update_history[stock_code][data_type] = time.time()
        logger.debug(f"[更新记录] {stock_code} {data_type}")

    def get_update_status(self, stock_code: str) -> Dict[str, str]:
        """获取股票的更新状态"""
        status = {}
        
        if stock_code in self.update_history:
            for data_type, timestamp in self.update_history[stock_code].items():
                age = time.time() - timestamp
                if age < 3600:
                    status[data_type] = f"{int(age/60)}分钟前更新"
                elif age < 86400:
                    status[data_type] = f"{int(age/3600)}小时前更新"
                else:
                    status[data_type] = f"{int(age/86400)}天前更新"
        
        return status

    def _get_last_update(self, stock_code: str, data_type: str) -> Optional[float]:
        """获取上次更新时间"""
        return self.update_history.get(stock_code, {}).get(data_type)


# 全局实例
cache_manager = DataCacheManager()
incremental_updater = SmartIncrementalUpdater()
