"""
Kafka to Qdrant consumer module for Elsa Crawler.
Consumes documents from Kafka and indexes them in Qdrant vector database.
Supports dynamic VIN-based topic subscription and collection management.
"""

import asyncio
import re
from typing import Any, AsyncIterable, Optional, cast

from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

from elsa_crawler.config import ElsaConfig
from elsa_crawler.models import DocumentData, VehicleHistory
from elsa_crawler.storage.sanitizer import sanitize_html


class KafkaQdrantConsumer:
    """Async Kafka consumer that indexes documents to Qdrant with VIN-based collections."""

    def __init__(self, config: ElsaConfig, topic_pattern: Optional[str] = None) -> None:
        """
        Initialize Kafka-Qdrant consumer.

        Args:
            config: ElsaConfig instance with Kafka and Qdrant settings
            topic_pattern: Regex pattern for topic subscription (e.g., '^elsadocs_.*')
                          If None and config.vin is set, subscribes to elsadocs_{vin}
                          If None and no VIN, subscribes to all elsadocs_* topics
        """
        self.config = config
        self.topic_pattern = topic_pattern or self._determine_topic_pattern()
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.qdrant_client: Optional[AsyncQdrantClient] = None
        self.embedding_model: Optional[SentenceTransformer] = None
        # Cache for created collections to avoid repeated checks
        self._collection_cache: set[str] = set()

    def _determine_topic_pattern(self) -> str:
        """
        Determine topic pattern based on config.

        Returns:
            Regex pattern for Kafka topic subscription
        """
        if self.config.vin:
            # Subscribe to specific VIN topic
            return f"^elsadocs_{self.config.vin.lower()}$"
        # Subscribe to all VIN topics
        return "^elsadocs_.*"

    @staticmethod
    def _deserialize_message(message: bytes) -> dict[str, Any]:
        """Deserialize message to dict (validation happens in consume())."""
        import json

        return json.loads(message.decode("utf-8"))

    @staticmethod
    def _encode_content(model: SentenceTransformer, text: str) -> list[float]:
        result = model.encode(text)
        return cast(list[float], result.tolist())

    async def connect(self) -> None:
        """Initialize Kafka consumer, Qdrant client and embedding model."""
        print("🚀 Initializing Kafka-Qdrant Consumer...")

        # Load embedding model (blocking, run in executor)
        print(f"📥 Loading embedding model: {self.config.embedding_model}")
        loop = asyncio.get_event_loop()
        self.embedding_model = await loop.run_in_executor(None, SentenceTransformer, self.config.embedding_model)
        print("✅ Embedding model loaded")

        # Connect to Qdrant
        self.qdrant_client = AsyncQdrantClient(url=self.config.qdrant_url)
        print(f"✅ Connected to Qdrant: {self.config.qdrant_url}")

        # Initialize Kafka consumer with pattern subscription
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            group_id="qdrant-consumer-group",
            value_deserializer=self._deserialize_message,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )
        await self.consumer.start()

        # Subscribe to topics matching pattern
        self.consumer.subscribe(pattern=re.compile(self.topic_pattern))
        print(f"✅ Kafka Consumer subscribed to pattern: {self.topic_pattern}")

    async def disconnect(self) -> None:
        """Close Kafka consumer and Qdrant client."""
        if self.consumer:
            await self.consumer.stop()
            print("✅ Kafka Consumer stopped")

        if self.qdrant_client:
            await self.qdrant_client.close()
            print("✅ Qdrant Client closed")

    async def _ensure_collection(self, collection_name: str) -> None:
        """
        Create Qdrant collection if it doesn't exist.
        Always checks current state (no caching) to handle external deletions.

        Args:
            collection_name: Name of the collection to create (e.g., 'elsadocs_vin_category')
        """
        if not self.qdrant_client:
            return

        # Always check if collection exists (no cache to avoid stale state)
        collections = await self.qdrant_client.get_collections()
        collection_names = [c.name for c in collections.collections]

        if collection_name not in collection_names:
            print(f"📦 Creating Qdrant collection: {collection_name}")
            await self.qdrant_client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=384,  # all-MiniLM-L6-v2 embedding dimension
                    distance=Distance.COSINE,
                ),
            )
            print(f"✅ Collection created: {collection_name}")

    async def consume(self) -> None:
        """Consume messages from Kafka and index to Qdrant."""
        if not self.consumer or not self.qdrant_client or not self.embedding_model:
            raise RuntimeError("Consumer not properly initialized")

        print("\n🔄 Starting message consumption...\n")

        try:
            async for msg in cast(AsyncIterable[Any], self.consumer):
                try:
                    # Message is already deserialized to dict by value_deserializer
                    data: dict[str, Any] = msg.value

                    # Check message type and validate
                    if data.get("type") == "vehicle_history":
                        # Vehicle history - skip (stored in Redis only, not Qdrant)
                        vehicle_history = VehicleHistory.model_validate(data)
                        print(f"⏭️  Skipping vehicle_history for VIN {vehicle_history.vin} (stored in Redis only)")
                        continue
                    else:
                        # Document data - validate and process
                        doc = DocumentData.model_validate(data)
                        await self._process_document(doc)
                except Exception as exc:
                    print(f"⚠️  Error processing message: {exc}")
        except Exception as exc:
            print(f"❌ Consumer error: {exc}")

    async def _process_document(self, doc: DocumentData) -> None:
        """
        Process document and index to Qdrant using VIN-category based collection.
        Collection schema: elsadocs_{vin}_{category}

        Args:
            doc: DocumentData instance to process
        """
        if not self.embedding_model or not self.qdrant_client:
            return

        if not doc.content:
            print(f"⚠️  Skipping {doc.title}: No content")
            return

        # Generate collection name based on VIN and category
        collection_name = self._get_collection_name(doc.vin, doc.category)

        # Ensure collection exists (always check, no caching)
        await self._ensure_collection(collection_name)

        # Sanitize HTML content to clean Markdown text
        loop = asyncio.get_event_loop()
        sanitized_content = await loop.run_in_executor(None, sanitize_html, doc.content)

        if not sanitized_content or len(sanitized_content.strip()) < 50:
            print(f"⚠️  Skipping {doc.title}: Content too short after sanitization")
            return

        # Generate embedding with sanitized content (blocking, run in executor)
        vector = await loop.run_in_executor(None, self._encode_content, self.embedding_model, sanitized_content)

        # Prepare payload with sanitized content (remove html_preview from metadata)
        clean_metadata = {k: v for k, v in doc.metadata.items() if k != "html_preview"}

        payload = {
            "vin": doc.vin,
            "category": doc.category,
            "title": doc.title,
            "content": sanitized_content[:5000],  # Limit for Qdrant
            "url": doc.url or "",
            "metadata": {
                **clean_metadata,
                "sanitization": {
                    "original_length": len(doc.content),
                    "sanitized_length": len(sanitized_content),
                    "reduction_percent": round((1 - len(sanitized_content) / len(doc.content)) * 100, 2)
                    if len(doc.content) > 0
                    else 0,
                },
            },
            "timestamp": doc.timestamp,
        }

        # Generate point ID from vin + category + timestamp for uniqueness
        point_id = abs(hash(f"{doc.vin}:{doc.category}:{doc.timestamp}")) % (2**63 - 1)

        point = PointStruct(
            id=point_id,
            vector=vector,
            payload=payload,
        )

        # Upsert with Try-Catch for auto-recovery if collection was deleted externally
        try:
            await self.qdrant_client.upsert(collection_name=collection_name, points=[point])
        except Exception as exc:
            # Handle case where collection was deleted after _ensure_collection check
            if "doesn't exist" in str(exc).lower() or "not found" in str(exc).lower():
                print(f"⚠️  Collection deleted externally, recreating: {collection_name}")
                await self._ensure_collection(collection_name)
                # Retry upsert after recreation
                await self.qdrant_client.upsert(collection_name=collection_name, points=[point])
            else:
                raise

        # Safe metadata access with type checking
        metadata = payload.get("metadata")
        reduction: float = 0.0
        if isinstance(metadata, dict) and "sanitization" in metadata:
            sanitization = metadata["sanitization"]
            if isinstance(sanitization, dict):
                reduction_value = sanitization.get("reduction_percent", 0.0)  # type: ignore[misc]
                if isinstance(reduction_value, (int, float)):
                    reduction = float(reduction_value)

        print(
            f"✅ Indexed to {collection_name}: {doc.title} "
            f"(sanitized: {len(doc.content)}→{len(sanitized_content)} chars, -{reduction}%)"
        )

    def _get_collection_name(self, vin: str, category: str) -> str:
        """
        Generate Qdrant collection name following schema: elsadocs_{vin}_{category}
        Sanitizes category to ensure valid collection name.

        Args:
            vin: Vehicle identification number
            category: Document category

        Returns:
            Sanitized collection name
        """
        # Sanitize category: remove special chars, replace spaces with underscores
        sanitized_category = re.sub(r"[^a-zA-Z0-9_]", "_", category.lower())
        sanitized_category = re.sub(r"_+", "_", sanitized_category).strip("_")

        return f"elsadocs_{vin.lower()}_{sanitized_category}"

    async def __aenter__(self) -> "KafkaQdrantConsumer":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type, exc_val: Exception, exc_tb: type) -> None:
        """Async context manager exit."""
        await self.disconnect()


async def main() -> None:
    """Run Kafka-Qdrant consumer as standalone application."""
    import sys

    try:
        # Load config from environment
        config = ElsaConfig()

        print("🔧 Configuration:")
        print(f"   Kafka: {config.kafka_bootstrap_servers}")
        print(f"   Qdrant: {config.qdrant_url}")
        print(f"   Embedding Model: {config.embedding_model}")
        if config.vin:
            print(f"   VIN Filter: {config.vin}")
        else:
            print("   VIN Filter: All (pattern: elsadocs_*)")

        # Create and run consumer
        async with KafkaQdrantConsumer(config) as consumer:
            print("\n🚀 Starting Kafka-Qdrant Consumer...\n")
            await consumer.consume()

    except KeyboardInterrupt:
        print("\n⚠️  Shutdown requested by user")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
