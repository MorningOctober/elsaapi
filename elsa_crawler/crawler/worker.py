"""
Crawler worker module for Elsa Crawler.
Individual worker that crawls categories and extracts documents.
"""

from typing import Optional

from playwright.async_api import Frame, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from elsa_crawler.config import ElsaConfig
from elsa_crawler.extractors.categories import CategoryExtractor
from elsa_crawler.extractors.documents import DocumentExtractor
from elsa_crawler.models import Category, CrawlerStats, DocumentData, ExtractedDocument
from elsa_crawler.storage.kafka_producer import KafkaProducer
from elsa_crawler.storage.redis import RedisStorage


class CrawlerWorker:
    """Individual crawler worker for parallel processing."""

    def __init__(
        self,
        worker_id: int,
        page: Page,
        config: ElsaConfig,
        redis: Optional[RedisStorage],
        kafka: Optional[KafkaProducer],
    ) -> None:
        """
        Initialize crawler worker.

        Args:
            worker_id: Unique worker identifier
            page: Playwright page instance
            config: ElsaConfig instance
            redis: Optional Redis storage
            kafka: Optional Kafka producer
        """
        self.worker_id = worker_id
        self.page = page
        self.config = config
        self.redis = redis
        self.kafka = kafka

        self.stats = CrawlerStats()
        self.visited_categories: set[str] = set()
        self.visited_documents: set[str] = set()
        self.vin: str = ""

        # Frame references
        self.navigation_frame: Optional[Frame] = None
        self.content_frame: Optional[Frame] = None
        self.document_frame: Optional[Frame] = None

        # Cached categories
        self.all_categories: list[Category] = []

    async def initialize(self, vin: str) -> None:
        """
        Initialize worker by navigating to ElsaPro and detecting frames.

        Args:
            vin: Vehicle VIN for this worker
        """
        print(f"[Worker {self.worker_id}] 🔧 Initializing...")
        self.vin = vin

        await self.page.goto(self.config.elsa_base_url)
        await self.page.wait_for_load_state("networkidle")

        await self._open_vehicle_search()
        vin_frame = await self._detect_vin_frame()
        await self._fill_vin(vin_frame, vin)

        # Cache active VIN marker (best-effort)
        if self.redis and self.redis.client:
            try:
                await self.redis.client.set(f"vin:{vin}:active", vin, ex=3600)
            except Exception:
                pass

        await self._navigate_manual_section()
        await self._detect_frames()

        print(f"[Worker {self.worker_id}] ✅ Initialized")

    async def _open_vehicle_search(self) -> None:
        """Open the vehicle search dialog from toolbar."""
        await self.page.wait_for_selector("#barFs")
        button = self.page.locator("#toolbar\\.button\\.new\\.job")
        if await button.count() == 0:
            raise RuntimeError("Vehicle search button not found")
        await button.click()
        await self.page.wait_for_timeout(800)

    async def _detect_vin_frame(self) -> Frame:
        """Detect iframe that contains VIN input."""
        await self.page.wait_for_timeout(1200)

        # Prefer frames that look like VIN search
        for frame in self.page.frames:
            url = (frame.url or "").lower()
            if "search" in url or "veh" in url:
                return frame

        # Fallback: first non-main frame without children
        for frame in self.page.frames:
            if frame != self.page.main_frame and not frame.child_frames:
                return frame

        raise RuntimeError("Could not detect VIN iframe")

    async def _fill_vin(self, frame: Frame, vin: str) -> None:
        """Fill VIN in given frame and submit."""
        try:
            await frame.wait_for_selector("input[name='vin']", timeout=6000)
        except PlaywrightTimeoutError as exc:
            raise RuntimeError("VIN input field not found") from exc

        vin_input = frame.locator("input[name='vin']")
        await vin_input.fill(vin)
        await vin_input.press("Enter")

        await frame.wait_for_load_state("networkidle")
        await frame.wait_for_timeout(1200)

    async def _navigate_manual_section(self) -> None:
        """Navigate to 'Handbuch Service Technik' (TPL) module."""
        button = self.page.locator("#infomedia\\.button\\.TPL")
        if await button.count() == 0:
            raise RuntimeError("Handbuch Service Technik button not found")

        await button.click()
        await self.page.wait_for_load_state("networkidle")
        await self.page.wait_for_timeout(1000)

    async def _detect_frames(self) -> None:
        """Detect and cache navigation, content, and document frames."""
        await self.page.wait_for_timeout(1000)

        for frame in self.page.frames:
            try:
                frame_info = await frame.evaluate("""
                    () => {
                        const text = document.body?.textContent || '';
                        const lis = document.querySelectorAll('li');
                        
                        // Navigation frame markers
                        let hasNeuheiten = false;
                        let hasFeldmassnahmen = false;
                        for (const li of lis) {
                            const liText = li.textContent || '';
                            if (liText.includes('Neuheiten')) hasNeuheiten = true;
                            if (liText.includes('Feldmaßnahmen')) hasFeldmassnahmen = true;
                        }
                        
                        const hasNavigation = hasNeuheiten && hasFeldmassnahmen;
                        
                        // Content frame markers (look for rows with Vorgangs-Nr like 123/45)
                        const rows = document.querySelectorAll('tr');
                        let hasDocs = false;
                        for (const row of rows) {
                            const text = row.textContent || '';
                            if (/\\d+\\/\\d+/.test(text)) {
                                hasDocs = true;
                                break;
                            }
                        }

                        return {
                            hasNavigation,
                            hasContent: hasDocs,
                            textLength: text.length
                        };
                    }
                """)

                if frame_info.get("hasNavigation"):
                    self.navigation_frame = frame
                    print(f"[Worker {self.worker_id}] 📂 Navigation frame: {frame.url}")

                if frame_info.get("hasContent"):
                    self.content_frame = frame
                    print(f"[Worker {self.worker_id}] 📄 Content frame: {frame.url}")

            except Exception:
                continue

    async def _refresh_content_frame(self) -> None:
        """Re-detect content frame after navigation."""
        self.content_frame = None
        for frame in self.page.frames:
            try:
                has_docs = await frame.evaluate("""
                    () => {
                        const rows = document.querySelectorAll('tr');
                        for (const row of rows) {
                            const text = row.textContent || '';
                            if (/\\d+\\/\\d+/.test(text)) {
                                return true;
                            }
                        }
                        return false;
                    }
                """)
                if has_docs:
                    self.content_frame = frame
                    print(
                        f"[Worker {self.worker_id}] 📄 Content frame refreshed: {frame.url}"
                    )
                    break
            except Exception:
                continue

    async def collect_categories(self) -> list[Category]:
        """
        Collect all categories from navigation tree.

        Returns:
            List of Category instances
        """
        if not self.navigation_frame:
            print(f"[Worker {self.worker_id}] ⚠️  No navigation frame")
            return []

        categories = await CategoryExtractor.collect_all_categories(
            self.navigation_frame
        )
        self.all_categories = categories

        print(f"[Worker {self.worker_id}] 📁 Collected {len(categories)} categories")
        return categories

    async def crawl_category(self, category: Category) -> int:
        """
        Crawl a single category and extract documents.

        Args:
            category: Category to crawl

        Returns:
            Number of documents extracted
        """
        if category.id in self.visited_categories:
            return 0

        self.visited_categories.add(category.id)

        print(f"[Worker {self.worker_id}] 📂 Crawling: {category.name}")

        # Navigate by clicking category in navigation frame
        clicked = await self._click_category(category)
        await self.page.wait_for_timeout(800)
        if not clicked:
            print(f"[Worker {self.worker_id}] ⚠️  Could not navigate to {category.name}")
            return 0
        # Refresh content frame after navigation
        await self._refresh_content_frame()
        if not self.content_frame:
            print(f"[Worker {self.worker_id}] ⚠️  No content frame after navigation")
            return 0

        documents_extracted = await self._extract_documents_from_content(category)

        self.stats.categories_crawled += 1

        return documents_extracted

    async def _click_category(self, category: Category) -> bool:
        """Click a category inside the navigation frame."""
        if not self.navigation_frame:
            print(f"[Worker {self.worker_id}] ⚠️  No navigation frame to click category")
            return False

        target = category.url or category.id or category.name
        try:
            clicked = await self.navigation_frame.evaluate(
                """
                (catId) => {
                    // Strategy: href match or text match
                    const links = document.querySelectorAll('a');
                    for (const link of links) {
                        const href = link.getAttribute('href') || '';
                        const text = (link.textContent || '').trim();
                        if (href.includes(catId) || text === catId) {
                            link.click();
                            return true;
                        }
                    }
                    return false;
                }
                """,
                target,
            )
            if not clicked:
                print(f"[Worker {self.worker_id}] ⚠️  Could not click category {target}")
            return bool(clicked)
        except Exception as exc:
            print(
                f"[Worker {self.worker_id}] ⚠️  Error clicking category {target}: {exc}"
            )
            return False

    async def _extract_documents_from_content(self, category: Category) -> int:
        """Extract document links from content frame and process them."""
        if not self.content_frame:
            await self._refresh_content_frame()
        if not self.content_frame:
            return 0

        try:
            doc_links = await self._extract_document_list()

            count = 0
            for idx, doc_link in enumerate(doc_links[:50], 1):  # Limit per category
                vorgangs_nr = doc_link.get("vorgangs_nr", "")

                if vorgangs_nr in self.visited_documents:
                    continue

                self.visited_documents.add(vorgangs_nr)

                # Click document link in content frame
                success = await self._click_document_link(vorgangs_nr)
                if not success:
                    continue

                await self._refresh_document_frame()

                doc = await DocumentExtractor.extract_document_content(
                    self.page,
                    self.document_frame,
                    vorgangs_nr,
                    category.id,
                    category.name,
                )

                if doc:
                    await self._save_document(doc)
                    count += 1
                    self.stats.documents_extracted += 1

                # Return to document list by re-clicking the category for next doc
                if idx < len(doc_links):
                    await self._click_category(category)
                    await self._refresh_content_frame()

            return count

        except Exception as exc:
            print(f"[Worker {self.worker_id}] ⚠️  Document extraction failed: {exc}")
            return 0

    async def _click_document_link(self, vorgangs_nr: str) -> bool:
        """Click document link inside frames by Vorgangs-Nr match."""
        for frame in self.page.frames:
            try:
                clicked = await frame.evaluate(
                    """
                    (vnr) => {
                        const rows = document.querySelectorAll('tr');
                        for (const row of rows) {
                            if ((row.innerText || '').includes(vnr)) {
                                const link = row.querySelector('a');
                                if (link) {
                                    link.click();
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                    """,
                    vorgangs_nr,
                )
                if clicked:
                    await self.page.wait_for_timeout(600)
                    return True
            except Exception:
                continue
        return False

    async def _extract_document_list(self) -> list[dict[str, str]]:
        """Extract document links from content frame or any frame."""

        async def _eval(frame: Frame) -> list[dict[str, str]]:
            result = await frame.evaluate("""
                () => {
                    const docs = [];
                    const rows = document.querySelectorAll('tr');
                    rows.forEach((row, index) => {
                        const link = row.querySelector('a');
                        if (!link) return;
                        const text = (row.innerText || '').trim();
                        const match = text.match(/(\\d+\\/\\d+)/);
                        if (!match) return;
                        docs.push({
                            id: `doc_${index}`,
                            vorgangs_nr: match[1],
                            href: link.getAttribute('href') || ''
                        });
                    });
                    return docs;
                }
            """)
            return list(result or [])

        if self.content_frame:
            try:
                docs = await _eval(self.content_frame)
                if docs:
                    return docs
            except Exception:
                pass

        for frame in self.page.frames:
            try:
                docs = await _eval(frame)
                if docs:
                    return docs
            except Exception:
                continue
        return []

    async def _refresh_document_frame(self) -> None:
        """Find document frame after clicking a document link."""
        self.document_frame = None
        for frame in self.page.frames:
            try:
                is_doc = await frame.evaluate("""
                    () => {
                        const text = document.body?.innerText || '';
                        if (text.length < 200) return false;
                        return text.includes('Vorgangs-Nr') || text.includes('Kundenaussage') ||
                               text.includes('Kundenbemerkung') || text.includes('Lösung') ||
                               text.includes('Datum:');
                    }
                """)
                if is_doc:
                    self.document_frame = frame
                    break
            except Exception:
                continue

    async def _save_document(self, doc: ExtractedDocument) -> None:
        """Save extracted document to Redis and Kafka."""
        doc_data = DocumentData(
            vin=self.config.vin or "UNKNOWN",
            category=doc.category_name,
            title=doc.title,
            content=doc.content,
            url=doc.url,
            metadata=doc.metadata,
        )

        # Save to Redis
        if self.redis:
            try:
                await self.redis.save_document(doc_data)
            except Exception as exc:
                print(f"[Worker {self.worker_id}] ⚠️  Redis save failed: {exc}")

        # Stream to Kafka
        if self.kafka:
            try:
                await self.kafka.send_document(doc_data)
            except Exception as exc:
                print(f"[Worker {self.worker_id}] ⚠️  Kafka send failed: {exc}")

        print(f"[Worker {self.worker_id}] ✅ Saved: {doc.title}")

    def get_stats(self) -> CrawlerStats:
        """Get worker statistics."""
        return self.stats
