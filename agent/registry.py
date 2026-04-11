from utils.config import get_all_agent_config

from .qa_agent.service import QAAgent
from .router_agent.service import RouterBrainAgent

AGENT_CONFIG = get_all_agent_config()

class AgentRegistry:
    _instances = {}

    @staticmethod
    def get_all_agents(knowledge_registry):
        if not AgentRegistry._instances:
            AgentRegistry._init_all(knowledge_registry)
        return AgentRegistry._instances

    @staticmethod
    def _init_all(knowledge_registry):
        # 注册问答Agent
        AgentRegistry._instances["qa_agent"] = QAAgent(
            AGENT_CONFIG["qa_agent"],
            knowledge_registry
        )

        # 注册大脑
        AgentRegistry._instances["router_brain"] = RouterBrainAgent(
            AGENT_CONFIG["router_agent"],
            knowledge_registry
        )