"""
插件化架构和配置管理模块
支持动态加载Agent、工具和配置管理
"""

from typing import Dict, Any, List, Optional, Callable, Type, Tuple
from dataclasses import dataclass, field
from enum import Enum
import importlib
import inspect
from abc import ABC, abstractmethod
from datetime import datetime
from utils.logger import logger


class PluginType(Enum):
    """插件类型"""
    AGENT = "agent"
    TOOL = "tool"
    ANALYZER = "analyzer"
    VALIDATOR = "validator"


@dataclass
class PluginMetadata:
    """插件元数据"""
    name: str
    version: str
    plugin_type: PluginType
    description: str
    author: str
    dependencies: List[str] = field(default_factory=list)
    config_schema: Optional[Dict[str, Any]] = None
    loaded_at: Optional[datetime] = None


class PluginInterface(ABC):
    """
    插件接口基类
    所有Agent、工具都必须实现此接口
    """

    @abstractmethod
    def get_metadata(self) -> PluginMetadata:
        """获取插件元数据"""
        pass

    @abstractmethod
    def initialize(self, config: Dict[str, Any]):
        """初始化插件"""
        pass

    @abstractmethod
    def cleanup(self):
        """清理资源"""
        pass


class AgentPlugin(PluginInterface):
    """Agent插件基类"""

    @abstractmethod
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """执行Agent逻辑"""
        pass


class ToolPlugin(PluginInterface):
    """工具插件基类"""

    @abstractmethod
    def invoke(self, *args, **kwargs) -> Any:
        """调用工具"""
        pass


class PluginRegistry:
    """
    插件注册表
    管理所有插件的注册、加载和卸载
    """

    def __init__(self):
        self._plugins: Dict[str, PluginMetadata] = {}
        self._instances: Dict[str, PluginInterface] = {}
        self._factories: Dict[str, Callable] = {}

    def register_plugin(
        self,
        name: str,
        plugin_class: Type[PluginInterface],
        metadata: PluginMetadata
    ):
        """注册插件"""
        self._plugins[name] = metadata
        self._factories[name] = plugin_class
        logger.info(f"插件注册成功: {name} ({metadata.plugin_type.value})")

    def register_instance(self, name: str, instance: PluginInterface):
        """注册插件实例"""
        metadata = instance.get_metadata()
        self._plugins[name] = metadata
        self._instances[name] = instance
        logger.info(f"插件实例注册成功: {name}")

    def get_plugin(self, name: str) -> Optional[PluginInterface]:
        """获取插件实例"""
        if name in self._instances:
            return self._instances[name]

        if name in self._factories:
            try:
                instance = self._factories[name]()
                instance.initialize(self.get_plugin_config(name))
                self._instances[name] = instance
                return instance
            except Exception as e:
                logger.error(f"插件实例化失败 {name}: {e}")
                return None

        return None

    def list_plugins(self, plugin_type: Optional[PluginType] = None) -> List[PluginMetadata]:
        """列出插件"""
        if plugin_type:
            return [
                meta for meta in self._plugins.values()
                if meta.plugin_type == plugin_type
            ]
        return list(self._plugins.values())

    def get_plugin_config(self, name: str) -> Dict[str, Any]:
        """获取插件配置"""
        return config_manager.get_agent_config(name)

    def unregister_plugin(self, name: str):
        """卸载插件"""
        if name in self._instances:
            self._instances[name].cleanup()
            del self._instances[name]

        if name in self._factories:
            del self._factories[name]

        if name in self._plugins:
            del self._plugins[name]

        logger.info(f"插件卸载成功: {name}")

    def reload_plugin(self, name: str):
        """重新加载插件"""
        if name in self._instances:
            self._instances[name].cleanup()

        if name in self._factories:
            try:
                instance = self._factories[name]()
                instance.initialize(self.get_plugin_config(name))
                self._instances[name] = instance
                logger.info(f"插件重载成功: {name}")
            except Exception as e:
                logger.error(f"插件重载失败 {name}: {e}")


class DynamicLoader:
    """
    动态加载器
    支持运行时加载外部模块
    """

    @staticmethod
    def load_agent_from_module(module_path: str, class_name: str) -> Optional[Type[AgentPlugin]]:
        """
        从模块动态加载Agent类
        """
        try:
            module = importlib.import_module(module_path)
            agent_class = getattr(module, class_name)

            if not issubclass(agent_class, AgentPlugin):
                logger.warning(f"{class_name} 不是AgentPlugin的子类")
                return None

            return agent_class

        except ImportError as e:
            logger.error(f"模块导入失败 {module_path}: {e}")
            return None
        except AttributeError as e:
            logger.error(f"类未找到 {class_name}: {e}")
            return None

    @staticmethod
    def discover_plugins(
        package_path: str,
        base_class: Type[PluginInterface]
    ) -> List[Tuple[str, Type[PluginInterface]]]:
        """
        自动发现包中的插件
        """
        discovered = []

        try:
            package = importlib.import_module(package_path)

            for name, obj in inspect.getmembers(package):
                if inspect.isclass(obj) and issubclass(obj, base_class) and obj != base_class:
                    discovered.append((name, obj))

        except ImportError as e:
            logger.error(f"包导入失败 {package_path}: {e}")

        return discovered


class ConfigManager:
    """
    配置管理器
    统一管理系统配置
    """

    def __init__(self):
        self._configs: Dict[str, Dict[str, Any]] = {}
        self._defaults: Dict[str, Dict[str, Any]] = {}

    def register_default_config(self, category: str, config: Dict[str, Any]):
        """注册默认配置"""
        self._defaults[category] = config
        if category not in self._configs:
            self._configs[category] = config.copy()

    def update_config(self, category: str, updates: Dict[str, Any]):
        """更新配置"""
        if category not in self._configs:
            self._configs[category] = self._defaults.get(category, {}).copy()

        self._configs[category].update(updates)
        logger.info(f"配置更新: {category}")

    def get_config(self, category: str) -> Dict[str, Any]:
        """获取配置"""
        if category in self._configs:
            return self._configs[category].copy()

        if category in self._defaults:
            return self._defaults[category].copy()

        return {}

    def get_agent_config(self, agent_name: str) -> Dict[str, Any]:
        """获取Agent配置"""
        return self.get_config(f"agent:{agent_name}")

    def set_agent_config(self, agent_name: str, config: Dict[str, Any]):
        """设置Agent配置"""
        self.update_config(f"agent:{agent_name}", config)

    def load_from_file(self, file_path: str):
        """从文件加载配置"""
        import yaml

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                configs = yaml.safe_load(f)

            if configs:
                for category, config in configs.items():
                    self.register_default_config(category, config)

            logger.info(f"配置加载成功: {file_path}")

        except Exception as e:
            logger.error(f"配置加载失败 {file_path}: {e}")

    def save_to_file(self, file_path: str):
        """保存配置到文件"""
        import yaml

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(self._configs, f, allow_unicode=True)

            logger.info(f"配置保存成功: {file_path}")

        except Exception as e:
            logger.error(f"配置保存失败 {file_path}: {e}")


class AgentFactory:
    """
    Agent工厂
    支持创建和配置Agent实例
    """

    def __init__(self, registry: PluginRegistry, config_manager: ConfigManager):
        self.registry = registry
        self.config_manager = config_manager

    def create_agent(
        self,
        agent_type: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[AgentPlugin]:
        """
        创建Agent实例
        """
        agent_name = f"{agent_type}_agent"

        agent_class = self.registry.get_plugin(agent_name)

        if agent_class is None:
            from agents.registry import AgentRegistry

            agent_instance = AgentRegistry._instances.get(agent_type)

            if agent_instance:
                return agent_instance

            logger.error(f"Agent类型未注册: {agent_type}")
            return None

        final_config = self.config_manager.get_agent_config(agent_type)
        if config:
            final_config.update(config)

        agent = agent_class()
        agent.initialize(final_config)

        return agent


plugin_registry = PluginRegistry()
config_manager = ConfigManager()
agent_factory = AgentFactory(plugin_registry, config_manager)


class ABTestManager:
    """
    A/B测试管理器
    支持策略效果对比
    """

    def __init__(self):
        self.experiments: Dict[str, Dict[str, Any]] = {}
        self.results: Dict[str, List[Dict[str, Any]]] = {}

    def create_experiment(
        self,
        experiment_id: str,
        variants: Dict[str, Callable]
    ):
        """
        创建A/B测试实验
        """
        self.experiments[experiment_id] = {
            "variants": variants,
            "started_at": datetime.now(),
            "status": "running"
        }
        self.results[experiment_id] = []
        logger.info(f"A/B测试实验创建: {experiment_id}")

    def assign_variant(self, experiment_id: str, user_id: str) -> str:
        """分配测试变体"""
        if experiment_id not in self.experiments:
            return None

        variants = list(self.experiments[experiment_id]["variants"].keys())
        import hashlib

        hash_value = int(hashlib.md5(f"{experiment_id}:{user_id}".encode()).hexdigest(), 16)
        variant_index = hash_value % len(variants)

        return variants[variant_index]

    def record_result(
        self,
        experiment_id: str,
        variant: str,
        metrics: Dict[str, Any]
    ):
        """记录实验结果"""
        if experiment_id in self.results:
            self.results[experiment_id].append({
                "variant": variant,
                "metrics": metrics,
                "timestamp": datetime.now()
            })

    def get_experiment_summary(self, experiment_id: str) -> Dict[str, Any]:
        """获取实验摘要"""
        if experiment_id not in self.results:
            return {}

        results = self.results[experiment_id]

        summary = {}
        for result in results:
            variant = result["variant"]

            if variant not in summary:
                summary[variant] = {
                    "count": 0,
                    "metrics": {}
                }

            summary[variant]["count"] += 1

            for metric_name, metric_value in result["metrics"].items():
                if metric_name not in summary[variant]["metrics"]:
                    summary[variant]["metrics"][metric_name] = []

                summary[variant]["metrics"][metric_name].append(metric_value)

        for variant_data in summary.values():
            for metric_name, values in variant_data["metrics"].items():
                variant_data["metrics"][metric_name] = {
                    "mean": sum(values) / len(values),
                    "count": len(values)
                }

        return summary


ab_test_manager = ABTestManager()
