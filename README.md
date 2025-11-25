# Elsa Crawler (ElsaPro)

Asynchronous Playwright crawler for ElsaPro that extracts workshop documents, streams them to Kafka, and indexes vectors in Qdrant for semantic search. A small FastAPI layer exposes health/status and start/stop controls.

## Repository Layout
- `elsa_crawler/config.py` – Pydantic settings (env-driven, prompts if missing).
- `elsa_crawler/cli/main.py` – CLI entry point that runs the crawler end-to-end (`python -m elsa_crawler.cli`).
- `elsa_crawler/crawler/` – Orchestrator + worker logic for Playwright sessions.
- `elsa_crawler/browser/` – Browser/context manager.
- `elsa_crawler/extractors/` – Category/document parsing helpers (categories, documents, fieldsets).
- `elsa_crawler/storage/` – Redis cache, Kafka producer, Kafka→Qdrant consumer.
- `elsa_crawler/api/` – FastAPI app (`router` with health/status/start/stop/documents).
- `.old/` – Legacy scripts (e.g., `elsa_crawler_async.py`, `api.py`, old configs); kept for reference.
- `docker-compose.yml` – Redis, Kafka (KRaft), Kafka UI, Qdrant, Kafka→Qdrant consumer.
- `Makefile` – Convenience targets (some still point to legacy entrypoints; prefer commands below).

## Prerequisites
- Python 3.12+ and [`uv`](https://github.com/astral-sh/uv) installed.
- Docker + Docker Compose for Kafka/Redis/Qdrant.
- Chromium for Playwright (`uv run playwright install chromium`).

## Installation
```bash
uv sync
uv run playwright install chromium
```

## Environment Variables (current code)
Set these in `.env`:
- `ELSA_USERNAME`, `ELSA_PASSWORD` – ElsaPro credentials (prompts if missing).
- `ELSA_VIN` – VIN to crawl (prompts if missing).
- `ELSA_SECRET` – Base32 TOTP secret for MFA (optional).
- `OTP_CODE` – One-time OTP fallback when no TOTP secret (optional).
- `REDIS_URL` / `REDIS_DB` – default `redis://localhost:6379` / `0`.
- `KAFKA_BOOTSTRAP_SERVERS` – default `localhost:9092`. Topic is derived from VIN: `elsadocs_<vin>` and must exist or the broker must allow autocreate.
- `QDRANT_URL` / `QDRANT_COLLECTION` – default `http://localhost:6333` / `elsa_documents`.
- `EMBEDDING_MODEL` – default `sentence-transformers/all-MiniLM-L6-v2`.
- `API_HOST` / `API_PORT` – defaults `0.0.0.0:8000`.

## Running Services
Start dependencies (Redis, Kafka, Qdrant, consumer, Kafka UI, RedisInsight):
```bash
docker-compose up -d redis redisinsight kafka kafka-ui qdrant kafka-qdrant-consumer
```
**Web Interfaces:**
- Kafka UI: http://localhost:8080  
- Qdrant Dashboard: http://localhost:6333/dashboard  
- RedisInsight: http://localhost:5540

## Run the Crawler (CLI)
```bash
uv run python -m elsa_crawler.cli
```
Flow: login → vehicle search → VIN input → fieldsets cached to Redis → switch to “Handbuch Service Technik” → crawl categories/documents → send to Redis + Kafka (`elsadocs_<vin>`), Qdrant consumer indexes embeddings.

## Run the API
```bash
uv run uvicorn elsa_crawler.api.app:app --host 0.0.0.0 --port 8000 --reload
```
Endpoints (prefix `/api/v1`):
- `GET /health` – basic readiness.
- `GET /status` – crawler stats.
- `POST /start` – body `StartCrawlerRequest`; kicks off crawl in background.
- `POST /stop` – stops active crawl.
- `GET /documents/{vin}` – fetch documents for a VIN.

## Development Tasks
- Lint: `uv run ruff check .`
- Format: `uv run ruff format .`
- Type check: `uv run mypy .` and `uv run pyright elsa_crawler`
- Tests: `uv run pytest`
- Clean caches: `make clean`

## Notes & Legacy
- Legacy `.old/` scripts exist for reference; Makefile/CLI use the current module layout.
- Kafka→Qdrant consumer also ships as a Docker service (`kafka-qdrant-consumer`); production runs should keep it enabled.
- OTP/TOTP: preferred is `ELSA_SECRET` (Base32). For manual codes use `OTP_CODE`. If neither is set, login will fail when MFA is required.

## Troubleshooting
- If Playwright fails to launch, rerun `uv run playwright install chromium`.
- Model downloads (sentence-transformers) happen on first run; ensure network access for that step.
- Kafka/Qdrant connection errors: verify `KAFKA_BOOTSTRAP_SERVERS` and `QDRANT_URL` match container addresses (use `kafka:29092` / `qdrant:6333` inside Docker).
