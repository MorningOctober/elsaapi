"""
API module for Elsa Crawler.
Provides FastAPI application and routes.
"""

from elsa_crawler.api.app import app
from elsa_crawler.api.routes import router

__all__ = ["app", "router"]
