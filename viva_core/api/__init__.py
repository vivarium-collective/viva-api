"""Core's HTTP surface: one router, the same paths whether core runs alone or inside an application."""

from viva_core.api.app import CORE_PREFIX, build_core_router, create_core_app

__all__ = ["CORE_PREFIX", "build_core_router", "create_core_app"]
