"""Entry point for running Kafka-Qdrant consumer as a module."""

import asyncio

from .kafka_consumer import main

if __name__ == "__main__":
    asyncio.run(main())
