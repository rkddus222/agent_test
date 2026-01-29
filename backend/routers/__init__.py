"""
Routers module
"""
from backend.routers.semantic_router import router as semantic_router
from backend.routers.semantic_router_v2 import router as semantic_router_v2

__all__ = [
    "semantic_router",
    "semantic_router_v2",
]
