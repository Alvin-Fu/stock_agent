from utils.config import get_all_agent_config

from .qa_agent.service import QAAgent
from .router.service import RouterBrainAgent
from .retriever.retriever_agent import RetrieverAgent
from .financial_analyst.service import AnalystAgent
from .researcher.researcher_agent import ResearcherAgent
from .compliance.compliance_agent import ComplianceAgent

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
        # 注册大脑
        AgentRegistry._instances["router_brain"] = RouterBrainAgent(
            AGENT_CONFIG["router"],
            knowledge_registry
        )
        
        # 注册检索Agent
        AgentRegistry._instances["retriever"] = RetrieverAgent()
        
        # 注册财务分析Agent
        AgentRegistry._instances["analyst"] = AnalystAgent()
        
        # 注册研究Agent
        AgentRegistry._instances["researcher"] = ResearcherAgent()
        
        # 注册合规审查Agent
        AgentRegistry._instances["compliance"] = ComplianceAgent()