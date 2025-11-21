"""
Crawler orchestrator module for Elsa Crawler.
Manages multiple workers and coordinates crawling process.
"""

import asyncio
from datetime import UTC, datetime
from typing import Any, Optional

from playwright.async_api import Browser, BrowserContext, async_playwright

from elsa_crawler.auth.credentials import AuthHandler
from elsa_crawler.browser.manager import BrowserManager
from elsa_crawler.config import ElsaConfig
from elsa_crawler.crawler.worker import CrawlerWorker
from elsa_crawler.extractors.fieldsets import FieldsetExtractor
from elsa_crawler.models import Category, CrawlerStats, Credentials
from elsa_crawler.storage.kafka_producer import KafkaProducer
from elsa_crawler.storage.redis import RedisStorage


class CrawlerOrchestrator:
    """Orchestrates multiple crawler workers for parallel processing."""

    def __init__(self, config: ElsaConfig, credentials: Credentials, vin: str) -> None:
        """
        Initialize crawler orchestrator.

        Args:
            config: ElsaConfig instance
            credentials: Authentication credentials
            vin: Vehicle VIN to crawl
        """
        self.config = config
        self.credentials = credentials
        self.vin = vin
        # Align Kafka topic to VIN for producer/consumer
        self.config.kafka_topic = f"elsadocs_{vin.lower()}"

        self.redis: Optional[RedisStorage] = None
        self.kafka: Optional[KafkaProducer] = None
        self.browser: Optional[Browser] = None
        self.contexts: list[BrowserContext] = []
        self.workers: list[CrawlerWorker] = []

        self.all_categories: list[Category] = []
        self.stats = CrawlerStats()

    async def initialize(self) -> None:
        """Initialize storage, browser, and workers."""
        print(
            f"\n🚀 Initializing Crawler Orchestrator ({self.config.max_workers} workers)"
        )

        # Connect to Redis
        try:
            self.redis = RedisStorage(self.config)
            await self.redis.connect()
        except Exception as exc:
            print(f"⚠️  Redis connection failed: {exc}")
            self.redis = None

        # Connect to Kafka
        try:
            self.kafka = KafkaProducer(self.config)
            await self.kafka.connect()
        except Exception as exc:
            print(f"⚠️  Kafka connection failed: {exc}")
            self.kafka = None

        # Start Playwright browser (single instance for login + workers)
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.config.headless)

        # Enforce single worker for current test phase
        self.config.max_workers = 1

        # Perform login in first context, reuse session for workers
        print("\n🔐 Performing initial login...")
        auth_handler = AuthHandler(self.credentials)
        browser_manager = BrowserManager(self.config, auth_handler)

        await browser_manager.start(self.browser)
        await browser_manager.navigate_and_login()
        await browser_manager.open_vehicle_search()
        vin_frame = await browser_manager.detect_vin_frame()
        await browser_manager.fill_vin(vin_frame, self.vin)

        # Extract and cache fieldsets (customer/vehicle) before navigation
        try:
            await FieldsetExtractor.wait_for_job_details(vin_frame)
            customer_fs = await FieldsetExtractor.extract_fieldset(
                vin_frame, "fieldsetCustomer"
            )
            vehicle_fs = await FieldsetExtractor.extract_fieldset(
                vin_frame, "fieldsetVehicle"
            )
            if self.redis:
                await self.redis.save_fieldsets(
                    self.vin, customer_fs.model_dump(), vehicle_fs.model_dump()
                )
                print("✅ Fieldsets cached in Redis")
        except Exception as exc:
            print(f"⚠️  Fieldset extraction failed: {exc}")

        await browser_manager.navigate_manual_section()

        if browser_manager.context:
            await browser_manager.context.storage_state(path="state.json")
            print("✅ Session state saved")

        # Create workers with saved session (same browser, new context)
        for worker_id in range(self.config.max_workers):
            context = await self.browser.new_context(storage_state="state.json")
            page = await context.new_page()

            worker = CrawlerWorker(
                worker_id=worker_id,
                page=page,
                config=self.config,
                redis=self.redis,
                kafka=self.kafka,
            )

            await worker.initialize(self.vin)

            self.contexts.append(context)
            self.workers.append(worker)

        print(f"✅ {self.config.max_workers} workers ready")

    async def crawl_all(self) -> dict[str, Any]:
        """
        Start crawling with all workers.

        Returns:
            Crawling statistics
        """
        print("\n📁 Collecting categories...")

        self.stats.start_time = datetime.now(UTC).timestamp()

        # Collect categories from first worker
        if self.workers:
            self.all_categories = await self.workers[0].collect_categories()

        if not self.all_categories:
            print("❌ No categories found")
            return self._get_summary()

        print(f"✅ Found {len(self.all_categories)} categories\n")

        # Distribute categories among workers
        print("🔄 Starting parallel crawling...")

        category_chunks = self._distribute_categories()

        tasks = [
            self._worker_crawl_task(worker, chunk)
            for worker, chunk in zip(self.workers, category_chunks)
        ]

        await asyncio.gather(*tasks)

        self.stats.end_time = datetime.now(UTC).timestamp()

        return self._get_summary()

    def _distribute_categories(self) -> list[list[Category]]:
        """Distribute categories evenly among workers."""
        chunks: list[list[Category]] = [[] for _ in range(self.config.max_workers)]

        for i, category in enumerate(self.all_categories):
            worker_idx = i % self.config.max_workers
            chunks[worker_idx].append(category)

        return chunks

    async def _worker_crawl_task(
        self, worker: CrawlerWorker, categories: list[Category]
    ) -> None:
        """Crawling task for a single worker."""
        print(f"[Worker {worker.worker_id}] 📋 Assigned {len(categories)} categories")

        for category in categories:
            try:
                await worker.crawl_category(category)
            except Exception as exc:
                print(f"[Worker {worker.worker_id}] ❌ Error: {exc}")
                worker.stats.errors += 1

    def _get_summary(self) -> dict[str, Any]:
        """Get aggregated statistics from all workers."""
        total_categories = 0
        total_documents = 0
        total_errors = 0

        for worker in self.workers:
            stats = worker.get_stats()
            total_categories += stats.categories_crawled
            total_documents += stats.documents_extracted
            total_errors += stats.errors

        self.stats.categories_crawled = total_categories
        self.stats.documents_extracted = total_documents
        self.stats.errors = total_errors

        duration = 0.0
        if self.stats.start_time and self.stats.end_time:
            duration = self.stats.end_time - self.stats.start_time

        return {
            "vin": self.vin,
            "workers": self.config.max_workers,
            "categories_crawled": total_categories,
            "documents_extracted": total_documents,
            "errors": total_errors,
            "duration_seconds": round(duration, 2),
        }

    async def cleanup(self) -> None:
        """Close all connections and browser instances."""
        print("\n🧹 Cleaning up...")

        for context in self.contexts:
            await context.close()

        if self.browser:
            await self.browser.close()

        if self.kafka:
            await self.kafka.disconnect()

        if self.redis:
            await self.redis.disconnect()

        print("✅ Cleanup complete")

    async def __aenter__(self) -> "CrawlerOrchestrator":
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: type, exc_val: Exception, exc_tb: type) -> None:
        """Async context manager exit."""
        await self.cleanup()
