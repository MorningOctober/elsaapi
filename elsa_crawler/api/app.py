"""
FastAPI application for Elsa Crawler.
Provides REST API for crawler control.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from elsa_crawler.api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan manager."""
    # Startup
    print("🚀 Starting Elsa Crawler API...")
    yield
    # Shutdown
    print("🛑 Shutting down Elsa Crawler API...")


# Create FastAPI app
app = FastAPI(
    title="Elsa Crawler API",
    description="ElsaPro document crawler with Redis & Kafka integration",
    version="2.0.0",
    lifespan=lifespan,
)

# Include routes
app.include_router(router, prefix="/api/v1", tags=["crawler"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
