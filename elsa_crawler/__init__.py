"""
Elsa Crawler Package.
Modular, type-safe ElsaPro document crawler following SOLID principles.
"""

from elsa_crawler.config import ElsaConfig, get_config
from elsa_crawler.models import (
    Category,
    CrawlerConfig,
    CrawlerStats,
    CrawlerStatus,
    Credentials,
    DocumentData,
    DocumentResponse,
    ExtractedDocument,
    FieldsetDetails,
    FieldsetRow,
    FieldsetSnapshot,
    InfomediaTreeNode,
    MessageBoxPayload,
)

__version__ = "1.0.0"

__all__ = [
    # Config
    "ElsaConfig",
    "get_config",
    # Models
    "Category",
    "Credentials",
    "CrawlerConfig",
    "CrawlerStats",
    "CrawlerStatus",
    "DocumentData",
    "DocumentResponse",
    "ExtractedDocument",
    "FieldsetDetails",
    "FieldsetRow",
    "FieldsetSnapshot",
    "InfomediaTreeNode",
    "MessageBoxPayload",
]
