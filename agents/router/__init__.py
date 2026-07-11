from .router import RouterAgent, create_router_node, route_next_agent, with_queue_pop
from .prompts import ROUTER_SYSTEM_PROMPT, ROUTER_USER_TEMPLATE

# 旧的 RouterBrainAgent（自研单体大脑）已退役，如需使用请显式
# from agents.router.service import RouterBrainAgent

__all__ = [
    "RouterAgent",
    "create_router_node",
    "route_next_agent",
    "with_queue_pop",
    "ROUTER_SYSTEM_PROMPT",
    "ROUTER_USER_TEMPLATE",
]
