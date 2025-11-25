"""
Redis storage module for Elsa Crawler.
Provides async Redis operations for document storage and retrieval.
"""

import json
from typing import Any, Awaitable, Optional, cast

import redis.asyncio as aioredis

from elsa_crawler.config import ElsaConfig
from elsa_crawler.models import DocumentData


class RedisStorage:
    """Async Redis client wrapper for document storage."""

    def __init__(self, config: ElsaConfig) -> None:
        """
        Initialize Redis storage.

        Args:
            config: ElsaConfig instance with Redis settings
        """
        self.config = config
        self.client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Connect to Redis server."""
        self.client = cast(
            aioredis.Redis,
            await aioredis.from_url(  # type: ignore[no-untyped-call]
                self.config.redis_url,
                db=self.config.redis_db,
                decode_responses=True,
                encoding="utf-8",
            ),
        )
        print(f"✅ Redis connected: {self.config.redis_url}")

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            print("🔌 Redis disconnected")

    async def save_document(self, doc: DocumentData) -> None:
        """
        Save document to Redis.

        Args:
            doc: DocumentData instance to save
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        # Key format: doc:{vin}:{category}
        key = f"doc:{doc.vin}:{doc.category}"

        # Store document data as JSON
        await self.client.set(
            key,
            json.dumps(doc.model_dump(), ensure_ascii=False),
            ex=86400 * 30,  # 30 days TTL
        )

        # Add to VIN index set
        await cast(Awaitable[int], self.client.sadd(f"vin:{doc.vin}:docs", key))

    async def save_vehicle_history(self, vin: str, history: dict[str, Any]) -> None:
        """
        Save vehicle history for a VIN.

        Args:
            vin: Vehicle VIN
            history: VehicleHistory dict

        Stored at: vin:{vin}:history (JSON, 30-day TTL)
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"vin:{vin}:history"
        await self.client.set(
            key,
            json.dumps(history, ensure_ascii=False),
            ex=86400 * 30,  # 30 days TTL
        )

    async def save_fieldsets(
        self,
        vin: str,
        customer: dict[str, Any],
        vehicle: dict[str, Any],
        message: dict[str, Any] | None = None,
    ) -> None:
        """
        Save customer/vehicle fieldsets for a VIN.

        Stored at: vin:{vin}:fieldset (JSON)
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        payload = {
            "vin": vin,
            "customer": customer,
            "vehicle": vehicle,
            "message": message,
        }
        key = f"vin:{vin}:fieldset"
        await self.client.set(
            key, json.dumps(payload, ensure_ascii=False), ex=86400 * 30
        )

    async def get_document(self, vin: str, category: str) -> Optional[DocumentData]:
        """
        Retrieve document by VIN and category.

        Args:
            vin: Vehicle VIN
            category: Document category

        Returns:
            DocumentData if found, None otherwise
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"doc:{vin}:{category}"
        data = await self.client.get(key)

        if not data:
            return None

        return DocumentData.model_validate_json(data)

    async def get_documents_by_vin(self, vin: str) -> list[DocumentData]:
        """
        Retrieve all documents for a VIN.

        Args:
            vin: Vehicle VIN

        Returns:
            List of DocumentData instances
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        # Get all document keys for this VIN
        keys = cast(set[str], self.client.smembers(f"vin:{vin}:docs"))

        if not keys:
            return []

        # Fetch all documents in parallel
        documents: list[DocumentData] = []
        for key in keys:
            data = await self.client.get(key)
            if data:
                documents.append(DocumentData.model_validate_json(data))

        return documents

    async def document_exists(self, vin: str, category: str) -> bool:
        """
        Check if document exists.

        Args:
            vin: Vehicle VIN
            category: Document category

        Returns:
            True if document exists
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"doc:{vin}:{category}"
        return bool(await self.client.exists(key))

    async def delete_document(self, vin: str, category: str) -> bool:
        """
        Delete document from Redis.

        Args:
            vin: Vehicle VIN
            category: Document category

        Returns:
            True if document was deleted
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"doc:{vin}:{category}"

        # Remove from VIN index
        await cast(Awaitable[int], self.client.srem(f"vin:{vin}:docs", key))

        # Delete document
        deleted = await self.client.delete(key)
        return bool(deleted)

    async def clear_vin_documents(self, vin: str) -> int:
        """
        Clear all documents for a VIN.

        Args:
            vin: Vehicle VIN

        Returns:
            Number of documents deleted
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        # Get all document keys
        keys: set[str] = await cast(
            Awaitable[set[str]], self.client.smembers(f"vin:{vin}:docs")
        )

        if not keys:
            return 0

        # Delete all documents
        deleted_raw = await self.client.delete(*keys)
        deleted = int(deleted_raw or 0)

        # Delete index set
        await self.client.delete(f"vin:{vin}:docs")

        return deleted

    async def __aenter__(self) -> "RedisStorage":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()
