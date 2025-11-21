"""
Kafka to Qdrant consumer module for Elsa Crawler.
Consumes documents from Kafka and indexes them in Qdrant vector database.
"""

import asyncio
from typing import Any, AsyncIterable, Optional, cast

from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

from elsa_crawler.config import ElsaConfig
from elsa_crawler.models import DocumentData


class KafkaQdrantConsumer:
    """Async Kafka consumer that indexes documents to Qdrant."""

    def __init__(self, config: ElsaConfig) -> None:
        """
        Initialize Kafka-Qdrant consumer.

        Args:
            config: ElsaConfig instance with Kafka and Qdrant settings
        """
        self.config = config
        # Align topic to VIN if provided
        if config.vin:
            self.config.kafka_topic = f"elsadocs_{config.vin.lower()}"
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.qdrant_client: Optional[AsyncQdrantClient] = None
        self.embedding_model: Optional[SentenceTransformer] = None

    @staticmethod
    def _deserialize_message(message: bytes) -> DocumentData:
        return DocumentData.model_validate_json(message.decode("utf-8"))

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
        self.embedding_model = await loop.run_in_executor(
            None, SentenceTransformer, self.config.embedding_model
        )
        print("✅ Embedding model loaded")

        # Connect to Qdrant
        self.qdrant_client = AsyncQdrantClient(url=self.config.qdrant_url)
        print(f"✅ Connected to Qdrant: {self.config.qdrant_url}")

        # Ensure collection exists
        await self._ensure_collection()

        # Initialize Kafka consumer
        self.consumer = AIOKafkaConsumer(
            self.config.kafka_topic,
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            group_id="qdrant-consumer-group",
            value_deserializer=self._deserialize_message,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )
        await self.consumer.start()
        print(f"✅ Kafka Consumer subscribed: {self.config.kafka_topic}")

    async def disconnect(self) -> None:
        """Close Kafka consumer and Qdrant client."""
        if self.consumer:
            await self.consumer.stop()
            print("✅ Kafka Consumer stopped")

        if self.qdrant_client:
            await self.qdrant_client.close()
            print("✅ Qdrant Client closed")

    async def _ensure_collection(self) -> None:
        """Create Qdrant collection if it doesn't exist."""
        if not self.qdrant_client:
            return

        collections = await self.qdrant_client.get_collections()
        collection_names = [c.name for c in collections.collections]

        if self.config.qdrant_collection not in collection_names:
            print(f"📦 Creating Qdrant collection: {self.config.qdrant_collection}")
            await self.qdrant_client.create_collection(
                collection_name=self.config.qdrant_collection,
                vectors_config=VectorParams(
                    size=384,  # all-MiniLM-L6-v2 embedding dimension
                    distance=Distance.COSINE,
                ),
            )
            print(f"✅ Collection created: {self.config.qdrant_collection}")
        else:
            print(f"✅ Collection exists: {self.config.qdrant_collection}")

    async def consume(self) -> None:
        """Consume messages from Kafka and index to Qdrant."""
        if not self.consumer or not self.qdrant_client or not self.embedding_model:
            raise RuntimeError("Consumer not properly initialized")

        print("\n🔄 Starting message consumption...\n")

        try:
            async for msg in cast(AsyncIterable[Any], self.consumer):
                try:
                    # msg.value is already DocumentData from deserializer
                    doc = cast(DocumentData, msg.value)
                    await self._process_document(doc)
                except Exception as exc:
                    print(f"⚠️  Error processing message: {exc}")
        except Exception as exc:
            print(f"❌ Consumer error: {exc}")

    async def _process_document(self, doc: DocumentData) -> None:
        """
        Process document and index to Qdrant.

        Args:
            doc: DocumentData instance to process
        """
        if not self.embedding_model or not self.qdrant_client:
            return

        if not doc.content:
            print(f"⚠️  Skipping {doc.title}: No content")
            return

        # Generate embedding (blocking, run in executor)
        loop = asyncio.get_event_loop()
        vector = await loop.run_in_executor(
            None, self._encode_content, self.embedding_model, doc.content
        )

        # Prepare payload
        payload = {
            "vin": doc.vin,
            "category": doc.category,
            "title": doc.title,
            "content": doc.content[:5000],  # Limit for Qdrant
            "url": doc.url or "",
            "metadata": doc.metadata,
            "timestamp": doc.timestamp,
        }

        # Generate point ID from vin + category
        point_id = abs(hash(f"{doc.vin}:{doc.category}")) % (2**63 - 1)

        point = PointStruct(
            id=point_id,
            vector=vector,
            payload=payload,
        )

        await self.qdrant_client.upsert(
            collection_name=self.config.qdrant_collection, points=[point]
        )

        print(f"✅ Indexed: {doc.title}")

    async def __aenter__(self) -> "KafkaQdrantConsumer":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type, exc_val: Exception, exc_tb: type) -> None:
        """Async context manager exit."""
        await self.disconnect()
