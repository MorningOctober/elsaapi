"""
Authentication module for Elsa Crawler.
Handles ElsaPro login with TOTP/OTP authentication.
"""

import pyotp
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from elsa_crawler.config import get_otp_from_user
from elsa_crawler.models import Credentials

OTP_SELECTORS = ("#otp", 'input[name*="otp"]')


class AuthHandler:
    """Handles ElsaPro authentication flow."""

    def __init__(self, credentials: Credentials) -> None:
        """
        Initialize auth handler.

        Args:
            credentials: Credentials instance with username/password/TOTP
        """
        self.credentials = credentials

    def generate_otp(self) -> str:
        """
        Generate OTP code from TOTP secret or return manual code.

        Returns:
            6-digit OTP code

        Raises:
            RuntimeError: If no TOTP secret or OTP code available
        """
        if self.credentials.totp_secret:
            try:
                return pyotp.TOTP(self.credentials.totp_secret).now()
            except Exception as exc:
                raise RuntimeError(f"Invalid TOTP secret: {exc}") from exc

        if self.credentials.otp_code:
            return self.credentials.otp_code

        raise RuntimeError("No TOTP secret or OTP code available for MFA")

    async def wait_for_login(self, page: Page, login_timeout: float = 60.0) -> None:
        """
        Handle two-step login including TOTP challenge.

        Args:
            page: Playwright page instance
            login_timeout: Timeout in seconds

        Raises:
            TimeoutError: If login times out
        """
        import asyncio

        creds_submitted = False
        totp_started = False
        totp_submitted = False
        deadline = asyncio.get_event_loop().time() + login_timeout

        print("🔐 Logging in...")

        while asyncio.get_event_loop().time() < deadline:
            if not creds_submitted:
                creds_submitted = await self._fill_credentials_if_ready(page)
                if creds_submitted:
                    await page.wait_for_timeout(1200)

            if creds_submitted and not totp_started:
                totp_started = await self._start_totp_flow(page)

            if creds_submitted and not totp_submitted:
                totp_submitted = await self._try_submit_totp(page)

            url = page.url or ""
            if totp_submitted and "elsaweb" in url and "/isam/" not in url:
                print("✅ Login successful")
                return

            await page.wait_for_timeout(200)

        raise TimeoutError("Login timeout")

    async def _fill_credentials_if_ready(self, page: Page) -> bool:
        """Fill username and password if login form is ready."""
        username_field = page.locator("#username")

        try:
            await username_field.wait_for(state="visible", timeout=1000)
        except PlaywrightTimeoutError:
            return False

        if await username_field.count() == 0:
            return False

        await page.fill("#username", self.credentials.username)
        await page.fill("#password", self.credentials.password)
        await page.press("#password", "Enter")

        print("📝 Credentials submitted")
        return True

    async def _start_totp_flow(self, page: Page) -> bool:
        """Start TOTP flow if button is present."""
        start_button = page.locator("#start_totp_login")

        try:
            await start_button.wait_for(state="visible", timeout=1000)
        except PlaywrightTimeoutError:
            return False

        if await start_button.count() == 0:
            return False

        await start_button.click()
        await page.wait_for_timeout(1200)

        print("🔑 TOTP flow started")
        return True

    async def _try_submit_totp(self, page: Page) -> bool:
        """Try to submit TOTP code."""
        for selector in OTP_SELECTORS:
            field = page.locator(selector)

            try:
                await field.wait_for(state="visible", timeout=1000)
            except PlaywrightTimeoutError:
                continue

            if await field.count() == 0:
                continue

            # Try to generate OTP, fallback to manual input
            try:
                code = self.generate_otp()
            except RuntimeError:
                # No TOTP secret, ask user
                code = await get_otp_from_user()

            await field.fill(code)
            await field.press("Enter")
            await page.wait_for_timeout(2500)

            print("✅ OTP submitted")
            return True

        return False
