from abc import ABC, abstractmethod

class BaseAgent(ABC):
    """Agent抽象基类：所有智能体必须实现run方法"""
    def __init__(self, config, knowledge_registry):
        self.config = config
        self.knowledge_registry = knowledge_registry  # 注入所有知识库

    @abstractmethod
    def run(self, query: str):
        """执行Agent任务"""
        pass
class BaseAgent(ABC):
    """Agent抽象基类：所有智能体必须实现run方法"""
    def __init__(self, config, knowledge_registry):
        self.config = config
        self.knowledge_registry = knowledge_registry  # 注入所有知识库

    @abstractmethod
    def run(self, query: str):
        """执行Agent任务"""
        pass
