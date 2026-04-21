"""
查询改写模块：将用户问题改写为更利于检索的形式
"""

from langchain_core.messages import SystemMessage, HumanMessage
from core.llm import get_default_llm

REWRITE_PROMPT = """你是一个查询改写专家。将用户关于财经的问题改写为更适合向量检索的关键词短语。
规则：
1. 提取核心实体和概念（公司名、财务指标、年份等）
2. 去除口语化表达，保持专业术语
3. 只返回改写后的查询语句，不要解释

原始问题：{question}
改写查询："""


class QueryRewriter:
    def __init__(self):
        self.llm = get_default_llm()

    def rewrite(self, question: str) -> str:
        prompt = REWRITE_PROMPT.format(question=question)
        response = self.llm.invoke(prompt)
        return response.content.strip()