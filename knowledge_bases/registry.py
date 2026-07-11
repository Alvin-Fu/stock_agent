from knowledge_bases.kb_stock.service import StockKnowledge
from utils.config import load_config


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
        """初始化所有知识库（延迟读配置，缺 kb_stock 段时给出明确报错）"""
        kb_config = load_config().get("knowledge_bases", {})
        if "kb_stock" not in kb_config:
            raise KeyError("配置缺少 knowledge_bases.kb_stock 段，请在 config.yaml 或 local.yaml 中补充")
        KnowledgeRegistry._instances["kb_stock"] = StockKnowledge(kb_config["kb_stock"])