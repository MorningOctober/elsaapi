"""
Browser manager module for Elsa Crawler.
Handles Playwright browser lifecycle and frame detection.
"""

from typing import Optional

from playwright.async_api import Browser, BrowserContext, Frame, Page, async_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from elsa_crawler.auth.credentials import AuthHandler
from elsa_crawler.config import ElsaConfig


class BrowserManager:
    """Manages Playwright browser instance and page navigation."""

    def __init__(self, config: ElsaConfig, auth_handler: AuthHandler) -> None:
        """
        Initialize browser manager.

        Args:
            config: ElsaConfig instance
            auth_handler: AuthHandler for login
        """
        self.config = config
        self.auth_handler = auth_handler
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    async def start(self, browser: Optional[Browser] = None) -> Page:
        """
        Start browser and create page.

        Returns:
            Playwright Page instance
        """
        if browser is None:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=self.config.headless,
                slow_mo=70 if not self.config.headless else 0,
            )
        else:
            self.browser = browser

        if not self.browser:
            raise RuntimeError("Browser could not be started")

        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()

        print(f"🌐 Browser started (headless={self.config.headless})")
        return self.page

    async def stop(self) -> None:
        """Close browser and cleanup."""
        if self.context:
            await self.context.close()

        if self.browser:
            await self.browser.close()

        print("🔌 Browser stopped")

    async def navigate_and_login(self) -> None:
        """Navigate to ElsaPro and perform login."""
        if not self.page:
            raise RuntimeError("Browser not started")

        print(f"🌐 Opening ElsaPro: {self.config.elsa_base_url}")
        await self.page.goto(self.config.elsa_base_url, wait_until="domcontentloaded")

        # Perform login
        await self.auth_handler.wait_for_login(self.page)

    async def open_vehicle_search(self) -> None:
        """Open vehicle search dialog from toolbar."""
        if not self.page:
            raise RuntimeError("Browser not started")

        print("\n🔍 Opening vehicle search...")
        await self.page.wait_for_selector("#barFs")

        toolbar_button = self.page.locator("#toolbar\\.button\\.new\\.job")
        if await toolbar_button.count() == 0:
            raise RuntimeError("Vehicle search button not found")

        await toolbar_button.click()
        print("✅ Vehicle search opened")

    async def detect_vin_frame(self) -> Frame:
        """Detect iframe containing VIN input."""
        if not self.page:
            raise RuntimeError("Browser not started")

        print("\n🔎 Detecting VIN iframe...")
        await self.page.wait_for_timeout(1500)

        # Prefer frames with VIN/search hints
        for frame in self.page.frames:
            if self._looks_like_vin_frame(frame):
                print(f"✅ VIN iframe detected: {frame.url}")
                return frame

        # Fallback: first non-main leaf frame
        for frame in self.page.frames:
            if frame != self.page.main_frame and not frame.child_frames:
                print(f"✅ VIN iframe (fallback) detected: {frame.url}")
                return frame

        raise RuntimeError("Could not detect VIN iframe")

    async def fill_vin(self, frame: Frame, vin: str) -> None:
        """Fill VIN in the detected frame."""
        print(f"\n📝 Filling VIN: {vin}")
        try:
            await frame.wait_for_selector("input[name='vin']", timeout=6000)
        except PlaywrightTimeoutError as exc:
            raise RuntimeError("VIN input field not found") from exc

        vin_input = frame.locator("input[name='vin']")
        await vin_input.fill(vin)
        await vin_input.press("Enter")

        await frame.wait_for_load_state("networkidle")
        await frame.wait_for_timeout(1500)
        print("✅ VIN submitted")

    async def navigate_manual_section(self) -> None:
        """Navigate to 'Handbuch Service Technik' (TPL) module."""
        if not self.page:
            raise RuntimeError("Browser not started")

        tpl_button = self.page.locator("#infomedia\\.button\\.TPL")
        if await tpl_button.count() == 0:
            raise RuntimeError("Handbuch Service Technik button not found")

        await tpl_button.click()
        await self.page.wait_for_load_state("networkidle")
        await self.page.wait_for_timeout(1000)
        print("✅ Switched to Handbuch Service Technik (TPL)")

    @staticmethod
    def _looks_like_vin_frame(frame: Frame) -> bool:
        """Check if frame looks like VIN search frame."""
        url = (frame.url or "").lower()
        return "search" in url or "veh" in url

    async def __aenter__(self) -> "BrowserManager":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: type, exc_val: Exception, exc_tb: type) -> None:
        """Async context manager exit."""
        await self.stop()
