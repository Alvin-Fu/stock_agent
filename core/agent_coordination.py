"""
Agent协作模块
提供标准化的Agent消息格式和并行执行功能
"""

from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from core.state_manager import StateValidator
from utils.logger import logger


class MessageType(Enum):
    """消息类型"""
    REQUEST = "request"
    RESPONSE = "response"
    ERROR = "error"
    STATUS_UPDATE = "status_update"


class Priority(Enum):
    """任务优先级"""
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class AgentMessage:
    """
    标准化的Agent消息格式
    用于Agent间的信息传递
    """
    msg_type: MessageType
    sender: str
    receiver: str
    content: Dict[str, Any]
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())
    priority: Priority = Priority.NORMAL
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "msg_type": self.msg_type.value,
            "sender": self.sender,
            "receiver": self.receiver,
            "content": self.content,
            "timestamp": self.timestamp,
            "priority": self.priority.value,
            "metadata": self.metadata,
            "correlation_id": self.correlation_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentMessage':
        """从字典创建消息"""
        return cls(
            msg_type=MessageType(data["msg_type"]),
            sender=data["sender"],
            receiver=data["receiver"],
            content=data["content"],
            timestamp=data.get("timestamp", datetime.now().timestamp()),
            priority=Priority(data.get("priority", 2)),
            metadata=data.get("metadata", {}),
            correlation_id=data.get("correlation_id")
        )


@dataclass
class AgentTask:
    """Agent任务定义"""
    agent_name: str
    input_data: Dict[str, Any]
    dependencies: List[str] = field(default_factory=list)
    priority: Priority = Priority.NORMAL
    timeout: Optional[float] = None

    def can_execute(self, completed_agents: List[str]) -> bool:
        """检查是否可以执行（依赖是否满足）"""
        return all(dep in completed_agents for dep in self.dependencies)


class MessageBus:
    """
    Agent消息总线
    管理Agent间的消息传递
    """

    def __init__(self):
        self.message_queue: List[AgentMessage] = []
        self.subscribers: Dict[str, List[Callable]] = {}

    def publish(self, message: AgentMessage):
        """发布消息"""
        self.message_queue.append(message)

        if message.receiver in self.subscribers:
            for callback in self.subscribers[message.receiver]:
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"消息处理失败: {e}")

        logger.debug(f"消息发布: {message.sender} -> {message.receiver}")

    def subscribe(self, agent_name: str, callback: Callable):
        """订阅消息"""
        if agent_name not in self.subscribers:
            self.subscribers[agent_name] = []
        self.subscribers[agent_name].append(callback)

    def get_messages(self, agent_name: str) -> List[AgentMessage]:
        """获取指定Agent的消息"""
        return [msg for msg in self.message_queue if msg.receiver == agent_name]

    def clear(self):
        """清空消息队列"""
        self.message_queue.clear()


class ParallelExecutor:
    """
    并行任务执行器
    支持依赖管理的并行执行
    """

    def __init__(self, max_concurrent: int = 3):
        self.max_concurrent = max_concurrent
        self.running_tasks: List[AgentTask] = []
        self.completed_agents: List[str] = []

    def execute_parallel(
        self,
        tasks: List[AgentTask],
        executor_func: Callable[[AgentTask], Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        并行执行多个任务
        考虑依赖关系和并发限制
        """
        results = {}
        pending_tasks = tasks.copy()

        while pending_tasks or self.running_tasks:
            ready_tasks = [
                task for task in pending_tasks
                if task.can_execute(self.completed_agents)
            ]

            available_slots = self.max_concurrent - len(self.running_tasks)

            for task in ready_tasks[:available_slots]:
                pending_tasks.remove(task)
                self.running_tasks.append(task)

                try:
                    result = executor_func(task)
                    results[task.agent_name] = result
                    self.completed_agents.append(task.agent_name)
                    self.running_tasks.remove(task)

                except Exception as e:
                    logger.error(f"Agent {task.agent_name} 执行失败: {e}")
                    results[task.agent_name] = {"error": str(e)}
                    self.completed_agents.append(task.agent_name)
                    self.running_tasks.remove(task)

        return results


class AgentResultAggregator:
    """
    Agent结果聚合器
    合并多个Agent的输出结果
    """

    @staticmethod
    def aggregate(
        results: Dict[str, Dict[str, Any]],
        weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        聚合多个Agent的结果

        Args:
            results: {agent_name: result_dict}
            weights: {agent_name: weight} 权重配置

        Returns:
            聚合后的结果
        """
        if not results:
            return {}

        if weights is None:
            weights = {name: 1.0 for name in results.keys()}

        aggregated = {
            "agent_results": results,
            "metadata": {
                "total_agents": len(results),
                "successful": sum(1 for r in results.values() if "error" not in r),
                "failed": sum(1 for r in results.values() if "error" in r)
            }
        }

        text_results = []
        for agent_name, result in results.items():
            if "error" not in result and "text" in result:
                text_results.append({
                    "agent": agent_name,
                    "weight": weights.get(agent_name, 1.0),
                    "content": result["text"]
                })

        aggregated["text_summary"] = text_results

        return aggregated

    @staticmethod
    def merge_financial_data(data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        合并财务数据
        处理多个数据源的冲突
        """
        if not data_list:
            return {}

        merged = {}

        for data in data_list:
            if not isinstance(data, dict):
                continue

            for key, value in data.items():
                if key not in merged:
                    merged[key] = value
                elif isinstance(value, dict) and isinstance(merged[key], dict):
                    merged[key] = {**merged[key], **value}
                elif isinstance(value, list) and isinstance(merged[key], list):
                    merged[key].extend([v for v in value if v not in merged[key]])

        return merged

    @staticmethod
    def extract_insights(results: Dict[str, Dict[str, Any]]) -> List[str]:
        """
        从多个Agent结果中提取关键洞察
        """
        insights = []

        insight_keywords = [
            "关键发现", "重要发现", "核心观点", "主要结论",
            "值得注意", "建议关注", "风险提示", "机会提示"
        ]

        for agent_name, result in results.items():
            if "text" in result:
                text = result["text"]
                for keyword in insight_keywords:
                    if keyword in text:
                        insights.append(f"[{agent_name}] {keyword}")

        return insights


message_bus = MessageBus()
parallel_executor = ParallelExecutor()
result_aggregator = AgentResultAggregator()
