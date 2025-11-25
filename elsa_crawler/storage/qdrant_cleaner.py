"""
Qdrant Collection Cleanup Service.

This module provides VIN-based cleanup for Qdrant collections.
Follows SOLID principles: Single Responsibility (cleanup only).
"""

from typing import Any

from qdrant_client import AsyncQdrantClient

from elsa_crawler.config import ElsaConfig


class QdrantCleanupService:
    """
    Service for cleaning up Qdrant collections by VIN.

    This service is responsible for deleting all collections that match
    a VIN pattern (elsadocs_{vin}_*), ensuring fresh data on each crawl.
    """

    def __init__(self, config: ElsaConfig):
        """
        Initialize Qdrant cleanup service.

        Args:
            config: Elsa configuration
        """
        self.config = config
        self.client: AsyncQdrantClient | None = None

    async def connect(self) -> None:
        """Connect to Qdrant."""
        self.client = AsyncQdrantClient(url=self.config.qdrant_url)
        print(f"✅ Connected to Qdrant at {self.config.qdrant_url}")

    async def disconnect(self) -> None:
        """Disconnect from Qdrant."""
        if self.client:
            await self.client.close()
            self.client = None
            print("✅ Disconnected from Qdrant")

    async def clear_vin_collections(self, vin: str) -> dict[str, Any]:
        """
        Delete all Qdrant collections for a specific VIN.

        This deletes all collections matching pattern: elsadocs_{vin}_*
        For example, for VIN WVWZZZ3HZPE507713:
        - elsadocs_wvwzzz3hzpe507713_123
        - elsadocs_wvwzzz3hzpe507713_456
        - etc.

        Args:
            vin: Vehicle VIN

        Returns:
            Dict with deletion statistics:
            {
                "collections_deleted": int,
                "deleted_collections": list[str],
                "errors": list[str]
            }
        """
        if not self.client:
            raise RuntimeError("Qdrant client not connected")

        deleted_collections: list[str] = []
        errors: list[str] = []

        # Normalize VIN to lowercase for pattern matching
        vin_lower = vin.lower()
        prefix = f"elsadocs_{vin_lower}_"

        print(f"   🗑️  Searching for Qdrant collections with prefix: {prefix}")

        try:
            # Get all collections
            collections_response = await self.client.get_collections()
            all_collections = [c.name for c in collections_response.collections]

            # Filter collections matching VIN pattern
            matching_collections = [c for c in all_collections if c.startswith(prefix)]

            if not matching_collections:
                print(f"      ℹ️  No collections found for VIN {vin}")
                return {
                    "collections_deleted": 0,
                    "deleted_collections": [],
                    "errors": [],
                }

            print(f"      Found {len(matching_collections)} collections to delete")

            # Delete each matching collection
            for collection_name in matching_collections:
                try:
                    await self.client.delete_collection(collection_name=collection_name)
                    deleted_collections.append(collection_name)
                    print(f"      • Deleted collection: {collection_name}")
                except Exception as exc:
                    error_msg = f"Failed to delete {collection_name}: {exc}"
                    errors.append(error_msg)
                    print(f"      ⚠️  {error_msg}")

            return {
                "collections_deleted": len(deleted_collections),
                "deleted_collections": deleted_collections,
                "errors": errors,
            }

        except Exception as exc:
            error_msg = f"Failed to list collections: {exc}"
            print(f"   ⚠️  {error_msg}")
            return {
                "collections_deleted": 0,
                "deleted_collections": [],
                "errors": [error_msg],
            }

    async def __aenter__(self) -> "QdrantCleanupService":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()
