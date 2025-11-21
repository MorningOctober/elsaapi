"""
Extractors module for Elsa Crawler.
Provides extractors for categories, documents, and fieldsets.
"""

from elsa_crawler.extractors.categories import CategoryExtractor
from elsa_crawler.extractors.documents import DocumentExtractor
from elsa_crawler.extractors.fieldsets import FieldsetExtractor

__all__ = [
    "CategoryExtractor",
    "DocumentExtractor",
    "FieldsetExtractor",
]
