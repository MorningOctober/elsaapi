"""
Storage module for Elsa Crawler.
Provides Redis, Kafka Producer and Kafka Consumer implementations.
"""

from elsa_crawler.storage.kafka_consumer import KafkaQdrantConsumer
from elsa_crawler.storage.kafka_producer import KafkaProducer
from elsa_crawler.storage.redis import RedisStorage

__all__ = [
    "RedisStorage",
    "KafkaProducer",
    "KafkaQdrantConsumer",
]
