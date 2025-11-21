"""
FastAPI routes for Elsa Crawler API.
Provides REST endpoints for crawler control and document retrieval.
"""

import asyncio
from typing import Any, Optional

from fastapi import APIRouter, HTTPException

from elsa_crawler.config import ensure_credentials, get_config
from elsa_crawler.crawler.orchestrator import CrawlerOrchestrator
from elsa_crawler.models import (
    ApiResponse,
    CrawlerStatus,
    DocumentResponse,
    StartCrawlerRequest,
)
from elsa_crawler.storage.redis import RedisStorage

# Global state
_orchestrator: Optional[CrawlerOrchestrator] = None
_crawler_task: Optional[asyncio.Task[Any]] = None
_is_running = False

router = APIRouter()


@router.get("/health")
async def health_check() -> ApiResponse:
    """
    Health check endpoint.

    Returns:
        API health status
    """
    config = get_config()
    redis_connected = False

    try:
        redis = RedisStorage(config)
        await redis.connect()
        await redis.disconnect()
        redis_connected = True
    except Exception:
        pass

    return ApiResponse(
        success=True,
        message="API is healthy",
        data={"redis_connected": redis_connected, "crawler_running": _is_running},
    )


@router.get("/status")
async def get_status() -> CrawlerStatus:
    """
    Get current crawler status.

    Returns:
        Current crawler status and statistics
    """
    global _orchestrator, _is_running

    if not _is_running or not _orchestrator:
        return CrawlerStatus(is_running=False)

    # Aggregate worker stats
    total_categories = 0
    total_documents = 0
    total_errors = 0

    for worker in _orchestrator.workers:
        stats = worker.get_stats()
        total_categories += stats.categories_crawled
        total_documents += stats.documents_extracted
        total_errors += stats.errors

    from elsa_crawler.models import CrawlerStats

    aggregated_stats = CrawlerStats(
        categories_crawled=total_categories,
        documents_extracted=total_documents,
        errors=total_errors,
    )

    return CrawlerStatus(
        is_running=True,
        current_vin=_orchestrator.vin,
        num_workers=_orchestrator.config.max_workers,
        stats=aggregated_stats,
    )


@router.post("/start")
async def start_crawler(request: StartCrawlerRequest) -> ApiResponse:
    """
    Start crawler with given VIN.

    Args:
        request: Crawler start request

    Returns:
        Success response

    Raises:
        HTTPException: If crawler already running
    """
    global _orchestrator, _crawler_task, _is_running

    if _is_running:
        raise HTTPException(status_code=400, detail="Crawler already running")

    config = get_config()
    config.vin = request.vin
    config.max_workers = request.max_workers

    # Get credentials
    username, password = await ensure_credentials(config)

    from elsa_crawler.models import Credentials

    credentials = Credentials(
        username=username,
        password=password,
        totp_secret=config.elsa_username,  # Will be None, trigger manual OTP
    )

    async def run_crawler() -> None:
        """Background crawler task."""
        global _orchestrator, _is_running

        try:
            _orchestrator = CrawlerOrchestrator(config, credentials, request.vin)
            await _orchestrator.initialize()
            await _orchestrator.crawl_all()
        except Exception as exc:
            print(f"❌ Crawler error: {exc}")
        finally:
            _is_running = False
            if _orchestrator:
                await _orchestrator.cleanup()

    _is_running = True
    _crawler_task = asyncio.create_task(run_crawler())

    return ApiResponse(
        success=True,
        message="Crawler started",
        data={"vin": request.vin, "workers": request.max_workers},
    )


@router.post("/stop")
async def stop_crawler() -> ApiResponse:
    """
    Stop running crawler.

    Returns:
        Success response

    Raises:
        HTTPException: If crawler not running
    """
    global _orchestrator, _crawler_task, _is_running

    if not _is_running:
        raise HTTPException(status_code=400, detail="Crawler not running")

    if _crawler_task and not _crawler_task.done():
        _crawler_task.cancel()

    if _orchestrator:
        await _orchestrator.cleanup()

    _is_running = False

    return ApiResponse(success=True, message="Crawler stopped")


@router.get("/documents/{vin}")
async def get_documents(vin: str) -> DocumentResponse:
    """
    Get all documents for a VIN.

    Args:
        vin: Vehicle VIN

    Returns:
        Documents response with all documents

    Raises:
        HTTPException: If retrieval fails
    """
    config = get_config()

    try:
        redis = RedisStorage(config)
        await redis.connect()

        documents = await redis.get_documents_by_vin(vin)

        await redis.disconnect()

        return DocumentResponse(vin=vin, documents=documents, total=len(documents))

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve documents: {exc}"
        ) from exc
