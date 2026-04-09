from knowledge_bases.kb_stock.service import StockKnowledge
from utils.config import load_config

config = load_config()
KB_CONFIG = config["knowledge_bases"]

class KnowledgeRegistry:
    """🌟 统一管理所有知识库：调用时直接获取"""
    _instances = {}

    @staticmethod
    def get_all_knowledge():
        """获取所有已初始化的知识库"""
        if not KnowledgeRegistry._instances:
            KnowledgeRegistry._init_all()
        return KnowledgeRegistry._instances

    @staticmethod
    def _init_all():
        """初始化所有知识库"""
        KnowledgeRegistry._instances["kb_stock"] = StockKnowledge(KB_CONFIG["kb_stock"])