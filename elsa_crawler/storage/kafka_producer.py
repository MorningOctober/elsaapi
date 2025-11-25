"""
Kafka producer module for Elsa Crawler.
Streams documents to Kafka for real-time processing.
"""

import json
from typing import Any, Mapping, Optional

from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]

from elsa_crawler.config import ElsaConfig
from elsa_crawler.models import DocumentData


class KafkaProducer:
    """Async Kafka producer for document streaming."""

    def __init__(self, config: ElsaConfig) -> None:
        """
        Initialize Kafka producer.

        Args:
            config: ElsaConfig instance with Kafka settings
        """
        self.config = config
        self.producer: Optional[AIOKafkaProducer] = None

    async def connect(self) -> None:
        """Start Kafka producer."""

        def serialize_value(value: Mapping[str, Any]) -> bytes:
            return json.dumps(value, ensure_ascii=False).encode("utf-8")

        def serialize_key(key: Optional[str]) -> Optional[bytes]:
            return key.encode("utf-8") if key is not None else None

        topic = self._topic_for_vin(self.config.vin)

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            value_serializer=serialize_value,
            key_serializer=serialize_key,
            compression_type="gzip",
            acks="all",  # Wait for all replicas
        )
        await self.producer.start()

        # Store resolved topic
        self.config.kafka_topic = topic
        print(f"✅ Kafka Producer connected: {self.config.kafka_bootstrap_servers}")

    async def disconnect(self) -> None:
        """Stop Kafka producer."""
        if self.producer:
            await self.producer.stop()
            print("🔌 Kafka Producer disconnected")

    async def send_document(self, doc: DocumentData) -> None:
        """
        Send document to Kafka topic.

        Args:
            doc: DocumentData instance to send
        """
        if not self.producer:
            raise RuntimeError("Kafka producer not connected")

        # Key: VIN:category for partitioning
        key = f"{doc.vin}:{doc.category}"

        # Send to configured topic
        await self.producer.send(
            self.config.kafka_topic, value=doc.model_dump(), key=key
        )

    async def send_vehicle_history(self, history: dict[str, Any]) -> None:
        """
        Send complete vehicle history to Kafka topic.

        Args:
            history: VehicleHistory model dict
        """
        if not self.producer:
            raise RuntimeError("Kafka producer not connected")

        # Key: VIN:history for partitioning
        key = f"{history['vin']}:history"

        # Wrap in message envelope
        message = {
            "type": "vehicle_history",
            "vin": history["vin"],
            "timestamp": history["extraction_timestamp"],
            "status": history["extraction_status"],
            "metadata": {
                "total_entries": history["total_entries"],
                "successful": history["successful_entries"],
                "failed": history["failed_entries"],
            },
            "entries": history["entries"],
        }

        # Send to configured topic
        await self.producer.send(self.config.kafka_topic, value=message, key=key)

    def _topic_for_vin(self, vin: Optional[str]) -> str:
        suffix = (vin or "unknown").lower()
        return f"elsadocs_{suffix}"

    async def flush(self) -> None:
        """Flush pending messages."""
        if self.producer:
            await self.producer.flush()

    async def __aenter__(self) -> "KafkaProducer":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type, exc_val: Exception, exc_tb: type) -> None:
        """Async context manager exit."""
        await self.disconnect()
