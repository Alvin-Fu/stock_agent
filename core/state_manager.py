"""
状态管理模块
提供状态验证、转换和中间结果管理功能
"""

from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import time
from utils.logger import logger


class AgentStatus(Enum):
    """Agent执行状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StateTransition:
    """状态转换记录"""
    from_state: str
    to_state: str
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentExecutionRecord:
    """Agent执行记录"""
    agent_name: str
    status: AgentStatus
    start_time: float
    end_time: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0


class StateValidator:
    """
    状态验证器
    确保状态转换的合法性和数据完整性
    """

    REQUIRED_FIELDS = [
        "messages",
        "question",
        "stock_code",
        "intent"
    ]

    OPTIONAL_FIELDS = [
        "industry_name",
        "chain_leaders",
        "documents",
        "financial_data",
        "analysis_result",
        "research_result",
        "compliance_result",
        "technical_result",
        "final_answer",
        "intermediate_steps",
        "next_agent",
        "error"
    ]

    @classmethod
    def validate_state(cls, state: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        验证状态合法性
        Returns: (is_valid, error_messages)
        """
        errors = []

        for field in cls.REQUIRED_FIELDS:
            if field not in state:
                errors.append(f"缺少必需字段: {field}")
            elif state[field] is None:
                errors.append(f"字段 {field} 不能为 None")

        if "messages" in state and not isinstance(state["messages"], list):
            errors.append("messages 必须是列表")

        if "question" in state and not isinstance(state["question"], str):
            errors.append("question 必须是字符串")

        return len(errors) == 0, errors

    @classmethod
    def validate_field_access(cls, state: Dict[str, Any], field: str) -> Any:
        """
        安全获取字段值，避免 'NoneType' object has no attribute 'get' 错误
        """
        if field not in state:
            logger.warning(f"状态中不存在字段: {field}")
            return None

        value = state[field]
        if value is None:
            logger.debug(f"字段 {field} 的值为 None")
            return None

        return value

    @classmethod
    def safe_get(cls, state: Dict[str, Any], field: str, default: Any = None) -> Any:
        """安全获取字段值，提供默认值"""
        return cls.validate_field_access(state, field) or default


class StateTransitionManager:
    """
    状态转换管理器
    跟踪和管理状态转换历史
    """

    def __init__(self):
        self.transitions: List[StateTransition] = []
        self.execution_records: Dict[str, AgentExecutionRecord] = {}

    def record_transition(self, from_state: str, to_state: str, metadata: Dict[str, Any] = None):
        """记录状态转换"""
        transition = StateTransition(
            from_state=from_state,
            to_state=to_state,
            metadata=metadata or {}
        )
        self.transitions.append(transition)
        logger.debug(f"状态转换: {from_state} -> {to_state}")

    def start_agent_execution(self, agent_name: str) -> AgentExecutionRecord:
        """记录Agent执行开始"""
        record = AgentExecutionRecord(
            agent_name=agent_name,
            status=AgentStatus.RUNNING,
            start_time=time.time()
        )
        self.execution_records[agent_name] = record
        return record

    def complete_agent_execution(
        self,
        agent_name: str,
        status: AgentStatus,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ):
        """记录Agent执行完成"""
        if agent_name in self.execution_records:
            record = self.execution_records[agent_name]
            record.status = status
            record.end_time = time.time()
            record.result = result
            record.error = error

            execution_time = record.end_time - record.start_time
            logger.info(f"Agent {agent_name} 执行{'成功' if status == AgentStatus.COMPLETED else '失败'}，耗时: {execution_time:.2f}秒")

    def get_execution_summary(self) -> Dict[str, Any]:
        """获取执行摘要"""
        total_time = 0
        completed = 0
        failed = 0

        for agent_name, record in self.execution_records.items():
            if record.end_time:
                total_time += record.end_time - record.start_time
            if record.status == AgentStatus.COMPLETED:
                completed += 1
            elif record.status == AgentStatus.FAILED:
                failed += 1

        return {
            "total_agents": len(self.execution_records),
            "completed": completed,
            "failed": failed,
            "total_execution_time": total_time,
            "execution_records": {
                name: {
                    "status": record.status.value,
                    "duration": (record.end_time - record.start_time) if record.end_time else None,
                    "error": record.error
                }
                for name, record in self.execution_records.items()
            }
        }

    def clear(self):
        """清空历史记录"""
        self.transitions.clear()
        self.execution_records.clear()


class StateCache:
    """
    状态缓存管理器
    用于避免重复计算和存储中间结果
    """

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.cache: Dict[str, Any] = {}
        self.access_times: Dict[str, float] = {}

    def set(self, key: str, value: Any):
        """设置缓存"""
        if len(self.cache) >= self.max_size:
            self._evict_oldest()

        self.cache[key] = value
        self.access_times[key] = time.time()

    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key in self.cache:
            self.access_times[key] = time.time()
            return self.cache[key]
        return None

    def has(self, key: str) -> bool:
        """检查缓存是否存在"""
        return key in self.cache

    def _evict_oldest(self):
        """清除最旧的缓存"""
        if not self.access_times:
            return

        oldest_key = min(self.access_times.items(), key=lambda x: x[1])[0]
        del self.cache[oldest_key]
        del self.access_times[oldest_key]

    def clear(self):
        """清空缓存"""
        self.cache.clear()
        self.access_times.clear()

    def invalidate(self, key: str):
        """使特定缓存失效"""
        if key in self.cache:
            del self.cache[key]
            del self.access_times[key]


state_validator = StateValidator()
state_transition_manager = StateTransitionManager()
state_cache = StateCache()
