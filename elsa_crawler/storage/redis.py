"""
Redis storage module for Elsa Crawler.
Provides async Redis operations for document storage and retrieval.
Uses RedisJSON for structured data storage with JSONPath query support.
"""

from typing import Any, Awaitable, Optional, cast

import redis.asyncio as aioredis
from redis.commands.json.path import Path

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
        """Connect to Redis server and ensure RediSearch indexes exist."""
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

        # Ensure RediSearch indexes are created
        await self._ensure_search_indexes()

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            print("🔌 Redis disconnected")

    async def save_document(self, doc: DocumentData) -> None:
        """
        Save document to Redis using RedisJSON.

        Args:
            doc: DocumentData instance to save
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        # Key format: doc:{vin}:{category}:{vorgangs_nr}
        key = f"doc:{doc.vin}:{doc.category}:{doc.vorgangs_nr}"

        # Store document as RedisJSON with JSONPath queries
        result = await self.client.json().set(key, Path.root_path(), doc.model_dump())  # type: ignore[misc]
        if result is None:
            raise RuntimeError(f"Failed to save document to Redis: {key}")

        # Set TTL (30 days)
        await self.client.expire(key, 86400 * 30)

        # Add to VIN index set
        await cast(Awaitable[int], self.client.sadd(f"vin:{doc.vin}:docs", key))

    async def save_vehicle_history(self, vin: str, history: dict[str, Any]) -> None:
        """
        Save vehicle history for a VIN using RedisJSON.

        Args:
            vin: Vehicle VIN
            history: VehicleHistory dict

        Stored at: vin:{vin}:history (RedisJSON, 30-day TTL)
        Supports JSONPath queries like: $.entries[?(@.type=='Beanstandung')]
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"vin:{vin}:history"
        result = await self.client.json().set(key, Path.root_path(), history)  # type: ignore[misc]
        if result is None:
            raise RuntimeError(f"Failed to save vehicle history to Redis: {key}")
        await self.client.expire(key, 86400 * 30)

    async def save_fieldsets(
        self,
        vin: str,
        customer: dict[str, Any],
        vehicle: dict[str, Any],
        message: dict[str, Any] | None = None,
    ) -> None:
        """
        Save customer/vehicle fieldsets for a VIN using RedisJSON.

        Stored at: vin:{vin}:fieldset (RedisJSON)
        Supports queries like: $.vehicle.model or $.customer.name
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
        result = await self.client.json().set(key, Path.root_path(), payload)  # type: ignore[misc]
        if result is None:
            raise RuntimeError(f"Failed to save fieldsets to Redis: {key}")
        await self.client.expire(key, 86400 * 30)

    async def get_document(self, vin: str, category: str, vorgangs_nr: str) -> Optional[DocumentData]:
        """
        Retrieve document by VIN, category and vorgangs_nr using RedisJSON.

        Args:
            vin: Vehicle VIN
            category: Document category
            vorgangs_nr: Document vorgangs number

        Returns:
            DocumentData if found, None otherwise
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"doc:{vin}:{category}:{vorgangs_nr}"
        data = self.client.json().get(key)

        if not data:
            return None

        return DocumentData.model_validate(data)

    async def get_documents_by_vin(self, vin: str) -> list[DocumentData]:
        """
        Retrieve all documents for a VIN using RedisJSON.

        Args:
            vin: Vehicle VIN

        Returns:
            List of DocumentData instances
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        # Get all document keys for this VIN
        keys = await cast(Awaitable[set[str]], self.client.smembers(f"vin:{vin}:docs"))

        if not keys:
            return []

        # Fetch all documents using RedisJSON
        documents: list[DocumentData] = []
        for key in keys:
            data = self.client.json().get(key)
            if data:
                documents.append(DocumentData.model_validate(data))

        return documents

    async def document_exists(self, vin: str, category: str, vorgangs_nr: str) -> bool:
        """
        Check if document exists.

        Args:
            vin: Vehicle VIN
            category: Document category
            vorgangs_nr: Document vorgangs number

        Returns:
            True if document exists
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"doc:{vin}:{category}:{vorgangs_nr}"
        return bool(await self.client.exists(key))

    async def delete_document(self, vin: str, category: str, vorgangs_nr: str) -> bool:
        """
        Delete document from Redis.

        Args:
            vin: Vehicle VIN
            category: Document category
            vorgangs_nr: Document vorgangs number

        Returns:
            True if document was deleted
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        key = f"doc:{vin}:{category}:{vorgangs_nr}"

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
        keys: set[str] = await cast(Awaitable[set[str]], self.client.smembers(f"vin:{vin}:docs"))

        if not keys:
            return 0

        # Delete all documents
        deleted_raw = await self.client.delete(*keys)
        deleted = int(deleted_raw or 0)

        # Delete index set
        await self.client.delete(f"vin:{vin}:docs")

        return deleted

    async def clear_vin_data(self, vin: str) -> dict[str, Any]:
        """
        Clear ALL VIN-related data from Redis (documents + metadata).

        This ensures fresh data on each crawl by removing:
        - All documents (doc:{vin}:*)
        - Vehicle history (vin:{vin}:history)
        - Fieldsets (vin:{vin}:fieldset)
        - Document index (vin:{vin}:docs)
        - Active marker (vin:{vin}:active)

        Args:
            vin: Vehicle VIN

        Returns:
            Dict with deletion statistics:
            {
                "documents_deleted": int,
                "metadata_keys_deleted": int,
                "deleted_keys": list[str],
                "total_deleted": int
            }
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        deleted_keys: list[str] = []
        documents_deleted = 0
        metadata_deleted = 0

        # 1. Delete all documents for this VIN
        print(f"   🗑️  Clearing documents for VIN {vin}...")
        doc_keys: set[str] = await cast(Awaitable[set[str]], self.client.smembers(f"vin:{vin}:docs"))

        if doc_keys:
            for key in doc_keys:
                if await self.client.delete(key):
                    deleted_keys.append(key)
                    documents_deleted += 1
                    print(f"      • Deleted: {key}")

        # 2. Delete metadata keys
        print(f"   🗑️  Clearing metadata for VIN {vin}...")
        metadata_keys = [
            f"vin:{vin}:history",
            f"vin:{vin}:fieldset",
            f"vin:{vin}:docs",
            f"vin:{vin}:active",
        ]

        for key in metadata_keys:
            if await self.client.exists(key):
                await self.client.delete(key)
                deleted_keys.append(key)
                metadata_deleted += 1
                print(f"      • Deleted: {key}")

        total_deleted = documents_deleted + metadata_deleted

        return {
            "documents_deleted": documents_deleted,
            "metadata_keys_deleted": metadata_deleted,
            "deleted_keys": deleted_keys,
            "total_deleted": total_deleted,
        }

    # ========================================================================
    # RediSearch Index Management
    # ========================================================================

    async def _ensure_search_indexes(self) -> None:
        """Create RediSearch indexes if they don't exist."""
        if not self.client:
            return

        try:
            await self._create_document_index()
            await self._create_history_index()
            await self._create_fieldset_index()
        except Exception as exc:
            print(f"⚠️  RediSearch index creation warning: {exc}")

    async def _create_document_index(self) -> None:
        """Create RediSearch index for documents."""
        if not self.client:
            return

        index_name = "idx:documents"

        try:
            # Check if index exists
            await self.client.execute_command("FT.INFO", index_name)
            print(f"✅ RediSearch index '{index_name}' already exists")
            return
        except Exception:
            # Index doesn't exist, create it
            pass

        try:
            await self.client.execute_command(
                "FT.CREATE",
                index_name,
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "$.vin",
                "AS",
                "vin",
                "TAG",
                "SORTABLE",
                "$.category",
                "AS",
                "category",
                "TAG",
                "SORTABLE",
                "$.vorgangs_nr",
                "AS",
                "vorgangs_nr",
                "TAG",
                "$.title",
                "AS",
                "title",
                "TEXT",
                "WEIGHT",
                "2.0",
                "$.content",
                "AS",
                "content",
                "TEXT",
                "$.timestamp",
                "AS",
                "timestamp",
                "NUMERIC",
                "SORTABLE",
            )
            print(f"✅ Created RediSearch index: {index_name}")
        except Exception as exc:
            print(f"⚠️  Failed to create index '{index_name}': {exc}")

    async def _create_history_index(self) -> None:
        """Create RediSearch index for vehicle history."""
        if not self.client:
            return

        index_name = "idx:vehicle_history"

        try:
            await self.client.execute_command("FT.INFO", index_name)
            print(f"✅ RediSearch index '{index_name}' already exists")
            return
        except Exception:
            pass

        try:
            await self.client.execute_command(
                "FT.CREATE",
                index_name,
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "vin:",
                "FILTER",
                "@__key=='vin:*:history'",
                "SCHEMA",
                "$.vin",
                "AS",
                "vin",
                "TAG",
                "SORTABLE",
                "$.total_entries",
                "AS",
                "total_entries",
                "NUMERIC",
                "$.entries[*].type",
                "AS",
                "entry_type",
                "TAG",
                "$.entries[*].mileage",
                "AS",
                "mileage",
                "NUMERIC",
                "SORTABLE",
                "$.entries[*].acceptance_date",
                "AS",
                "acceptance_date",
                "TEXT",
                "SORTABLE",
                "$.entries[*].workshop",
                "AS",
                "workshop",
                "TEXT",
            )
            print(f"✅ Created RediSearch index: {index_name}")
        except Exception as exc:
            print(f"⚠️  Failed to create index '{index_name}': {exc}")

    async def _create_fieldset_index(self) -> None:
        """Create RediSearch index for fieldsets."""
        if not self.client:
            return

        index_name = "idx:fieldsets"

        try:
            await self.client.execute_command("FT.INFO", index_name)
            print(f"✅ RediSearch index '{index_name}' already exists")
            return
        except Exception:
            pass

        try:
            await self.client.execute_command(
                "FT.CREATE",
                index_name,
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "vin:",
                "FILTER",
                "@__key=='vin:*:fieldset'",
                "SCHEMA",
                "$.vin",
                "AS",
                "vin",
                "TAG",
                "SORTABLE",
                "$.vehicle.model",
                "AS",
                "model",
                "TAG",
                "$.vehicle.modelyear",
                "AS",
                "modelyear",
                "NUMERIC",
                "SORTABLE",
                "$.customer.name",
                "AS",
                "customer_name",
                "TEXT",
            )
            print(f"✅ Created RediSearch index: {index_name}")
        except Exception as exc:
            print(f"⚠️  Failed to create index '{index_name}': {exc}")

    # ========================================================================
    # RediSearch Query Methods
    # ========================================================================

    async def search_documents(
        self,
        query: str = "*",
        vin: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_desc: bool = True,
    ) -> dict[str, Any]:
        """
        Search documents using RediSearch.

        Args:
            query: Search query (default: "*" for all)
            vin: Filter by VIN
            category: Filter by category
            limit: Max results to return
            offset: Pagination offset
            sort_by: Field to sort by (e.g., "timestamp")
            sort_desc: Sort descending if True

        Returns:
            Dict with 'total', 'results' (list of dicts)
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        # Build query string
        query_parts: list[str] = []

        if vin:
            query_parts.append(f"@vin:{{{vin}}}")
        if category:
            query_parts.append(f"@category:{{{category}}}")
        if query != "*":
            query_parts.append(f"@content:({query})")

        final_query = " ".join(query_parts) if query_parts else "*"

        # Build command
        cmd = [
            "FT.SEARCH",
            "idx:documents",
            final_query,
            "LIMIT",
            str(offset),
            str(limit),
        ]

        if sort_by:
            cmd.extend(["SORTBY", sort_by, "DESC" if sort_desc else "ASC"])

        try:
            raw_result: Any = await self.client.execute_command(*cmd)  # type: ignore[reportUnknownVariableType]
            result = cast(list[Any], raw_result)

            if not result:
                return {"total": 0, "results": []}

            total = int(result[0])

            # Parse results (format: [total, key1, data1, key2, data2, ...])
            results: list[Any] = []
            for i in range(1, len(result), 2):
                # Data is in result[i+1][1] (after '$' path)
                if i + 1 < len(result) and isinstance(result[i + 1], list) and len(result[i + 1]) > 1:
                    results.append(result[i + 1][1])

            return {"total": total, "results": results}
        except Exception as exc:
            print(f"⚠️  Search failed: {exc}")
            return {"total": 0, "results": []}

    async def search_vehicle_history(
        self,
        vin: Optional[str] = None,
        entry_type: Optional[str] = None,
        min_mileage: Optional[int] = None,
        max_mileage: Optional[int] = None,
        workshop: Optional[str] = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """
        Search vehicle history entries using RediSearch.

        Args:
            vin: Filter by VIN
            entry_type: Filter by entry type (ServicePlan, Complaint, Invoice)
            min_mileage: Minimum mileage filter
            max_mileage: Maximum mileage filter
            workshop: Filter by workshop name
            limit: Max results to return

        Returns:
            Dict with 'total', 'results'
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        query_parts: list[str] = []

        if vin:
            query_parts.append(f"@vin:{{{vin}}}")
        if entry_type:
            query_parts.append(f"@entry_type:{{{entry_type}}}")
        if min_mileage is not None or max_mileage is not None:
            min_val = min_mileage if min_mileage is not None else 0
            max_val = str(max_mileage) if max_mileage is not None else "+inf"
            query_parts.append(f"@mileage:[{min_val} {max_val}]")
        if workshop:
            query_parts.append(f"@workshop:({workshop})")

        final_query = " ".join(query_parts) if query_parts else "*"

        try:
            raw_result: Any = await self.client.execute_command(  # type: ignore[reportUnknownVariableType]
                "FT.SEARCH",
                "idx:vehicle_history",
                final_query,
                "LIMIT",
                "0",
                str(limit),
            )
            result = cast(list[Any], raw_result)

            if not result:
                return {"total": 0, "results": []}

            total = int(result[0])

            results: list[Any] = []
            for i in range(1, len(result), 2):
                if i + 1 < len(result) and isinstance(result[i + 1], list) and len(result[i + 1]) > 1:
                    results.append(result[i + 1][1])

            return {"total": total, "results": results}
        except Exception as exc:
            print(f"⚠️  History search failed: {exc}")
            return {"total": 0, "results": []}

    async def aggregate_history_by_workshop(self) -> list[dict[str, Any]]:
        """
        Aggregate vehicle history entries by workshop.

        Returns:
            List of dicts with 'workshop' and 'count'
        """
        if not self.client:
            raise RuntimeError("Redis client not connected")

        try:
            raw_result: Any = await self.client.execute_command(  # type: ignore[reportUnknownVariableType]
                "FT.AGGREGATE",
                "idx:vehicle_history",
                "*",
                "GROUPBY",
                "1",
                "@workshop",
                "REDUCE",
                "COUNT",
                "0",
                "AS",
                "count",
            )
            result = cast(list[Any], raw_result)

            if not result:
                return []

            # Parse aggregation results
            aggregations: list[dict[str, Any]] = []

            for i in range(1, len(result)):
                item = cast(list[Any], result[i])
                # Format: ['workshop', 'Workshop Name', 'count', '5']
                if len(item) >= 4:
                    aggregations.append(
                        {
                            "workshop": str(item[1]),
                            "count": int(item[3]),
                        }
                    )

            return aggregations
        except Exception as exc:
            print(f"⚠️  Aggregation failed: {exc}")
            return []

    async def __aenter__(self) -> "RedisStorage":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()
