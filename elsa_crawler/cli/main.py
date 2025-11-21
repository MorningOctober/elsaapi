"""
CLI main entry point for Elsa Crawler.
Command-line interface for running the crawler.
"""

import asyncio
import sys

from elsa_crawler.config import (
    ensure_credentials,
    ensure_vin,
    get_config,
)
from elsa_crawler.crawler.orchestrator import CrawlerOrchestrator
from elsa_crawler.models import Credentials


async def main() -> int:
    """
    Main CLI entry point.

    Returns:
        Exit code (0 = success, 1 = error)
    """
    print("=" * 60)
    print("🚀 Elsa Crawler - ElsaPro Document Crawler")
    print("=" * 60)
    print()

    # Load config
    config = get_config()

    # Get credentials
    try:
        username, password = await ensure_credentials(config)
        credentials = Credentials(
            username=username,
            password=password,
            totp_secret=config.totp_secret,
            otp_code=config.otp_code,
        )
    except Exception as exc:
        print(f"❌ Failed to get credentials: {exc}")
        return 1

    # Get VIN
    try:
        vin = await ensure_vin(config)
    except Exception as exc:
        print(f"❌ Failed to get VIN: {exc}")
        return 1

    print()
    print("📋 Configuration:")
    print(f"   VIN: {vin}")
    print(f"   Workers: {config.max_workers}")
    print(f"   Headless: {config.headless}")
    print(f"   Timeout: {config.timeout}ms")
    print()

    # Start crawler
    try:
        async with CrawlerOrchestrator(config, credentials, vin) as orchestrator:
            print("🔄 Starting crawler...")
            print()

            summary = await orchestrator.crawl_all()

            print()
            print("=" * 60)
            print("✅ Crawling Complete!")
            print("=" * 60)
            print(f"VIN: {summary['vin']}")
            print(f"Workers: {summary['workers']}")
            print(f"Categories: {summary['categories_crawled']}")
            print(f"Documents: {summary['documents_extracted']}")
            print(f"Errors: {summary['errors']}")
            print(f"Duration: {summary['duration_seconds']}s")
            print("=" * 60)

            return 0

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        return 130
    except Exception as exc:
        print(f"\n❌ Crawler failed: {exc}")
        import traceback

        traceback.print_exc()
        return 1


def cli_main() -> None:
    """CLI entry point wrapper for setup.py."""
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    cli_main()
