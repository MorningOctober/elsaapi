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
    SearchDocumentsRequest,
    SearchHistoryRequest,
    SearchResponse,
    StartCrawlerRequest,
)
from elsa_crawler.storage.qdrant_cleaner import QdrantCleanupService
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


@router.post("/search/documents")
async def search_documents(request: SearchDocumentsRequest) -> SearchResponse:
    """
    Search documents using RediSearch.

    Args:
        request: Search parameters

    Returns:
        Search results with total count

    Raises:
        HTTPException: If search fails
    """
    config = get_config()

    try:
        redis = RedisStorage(config)
        await redis.connect()

        result = await redis.search_documents(
            query=request.query,
            vin=request.vin,
            category=request.category,
            limit=request.limit,
            offset=request.offset,
            sort_by=request.sort_by,
            sort_desc=request.sort_desc,
        )

        await redis.disconnect()

        return SearchResponse(
            total=result["total"],
            results=result["results"],
            query=request.query,
            filters={
                "vin": request.vin,
                "category": request.category,
                "sort_by": request.sort_by,
                "sort_desc": request.sort_desc,
            },
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Document search failed: {exc}"
        ) from exc


@router.post("/search/history")
async def search_history(request: SearchHistoryRequest) -> SearchResponse:
    """
    Search vehicle history using RediSearch.

    Args:
        request: Search parameters

    Returns:
        Search results with total count

    Raises:
        HTTPException: If search fails
    """
    config = get_config()

    try:
        redis = RedisStorage(config)
        await redis.connect()

        result = await redis.search_vehicle_history(
            vin=request.vin,
            entry_type=request.entry_type,
            min_mileage=request.min_mileage,
            max_mileage=request.max_mileage,
            workshop=request.workshop,
            limit=request.limit,
        )

        await redis.disconnect()

        return SearchResponse(
            total=result["total"],
            results=result["results"],
            query="*",
            filters={
                "vin": request.vin,
                "entry_type": request.entry_type,
                "min_mileage": request.min_mileage,
                "max_mileage": request.max_mileage,
                "workshop": request.workshop,
            },
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"History search failed: {exc}"
        ) from exc


@router.get("/analytics/workshops")
async def get_workshop_analytics() -> dict[str, Any]:
    """
    Get aggregated statistics by workshop.

    Returns:
        List of workshops with repair counts

    Raises:
        HTTPException: If aggregation fails
    """
    config = get_config()

    try:
        redis = RedisStorage(config)
        await redis.connect()

        aggregations = await redis.aggregate_history_by_workshop()

        await redis.disconnect()

        return {
            "success": True,
            "workshops": aggregations,
            "total": len(aggregations),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Workshop analytics failed: {exc}"
        ) from exc


@router.delete("/vin/{vin}/data")
async def clear_vin_data(vin: str) -> ApiResponse:
    """
    Clear all data for a specific VIN from Redis and Qdrant.

    This endpoint performs a comprehensive cleanup:
    - Deletes all documents from Redis (doc:{vin}:*)
    - Deletes all metadata (history, fieldsets, etc.)
    - Deletes all Qdrant collections (elsadocs_{vin}_*)

    Use this before re-crawling a VIN to ensure fresh data, or
    to manually clean up after testing.

    Args:
        vin: Vehicle Identification Number (17 characters)

    Returns:
        Deletion statistics

    Raises:
        HTTPException: If cleanup fails
    """
    config = get_config()

    try:
        # Validate VIN format
        if len(vin) != 17:
            raise HTTPException(
                status_code=400, detail="VIN must be exactly 17 characters"
            )

        redis_stats: dict[str, Any] = {}
        qdrant_stats: dict[str, Any] = {}

        # Clear Redis data
        redis = RedisStorage(config)
        await redis.connect()
        redis_stats = await redis.clear_vin_data(vin)
        await redis.disconnect()

        # Clear Qdrant collections
        qdrant = QdrantCleanupService(config)
        await qdrant.connect()
        qdrant_stats = await qdrant.clear_vin_collections(vin)
        await qdrant.disconnect()

        return ApiResponse(
            success=True,
            message=f"Successfully cleared all data for VIN {vin}",
            data={
                "vin": vin,
                "redis": {
                    "documents_deleted": redis_stats.get("documents_deleted", 0),
                    "metadata_keys_deleted": redis_stats.get(
                        "metadata_keys_deleted", 0
                    ),
                    "total_deleted": redis_stats.get("total_deleted", 0),
                },
                "qdrant": {
                    "collections_deleted": qdrant_stats.get("collections_deleted", 0),
                    "deleted_collections": qdrant_stats.get("deleted_collections", []),
                },
                "errors": qdrant_stats.get("errors", []),
            },
        )

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to clear VIN data: {exc}"
        ) from exc
