.PHONY: help install setup docker-up docker-down docker-logs crawl api consumer lint format typecheck test clean

help:
	@printf "\nElsa Crawler - Commands\n"
	@printf "-----------------------\n"
	@printf "Setup:\n"
	@printf "  make install        Install Python deps via uv\n"
	@printf "  make setup          Install deps + Playwright Chromium\n"
	@printf "\nServices (Docker):\n"
	@printf "  make docker-up      Start Redis, Kafka (KRaft), Qdrant, Kafka consumer\n"
	@printf "  make docker-down    Stop all services\n"
	@printf "  make docker-logs    Follow service logs\n"
	@printf "\nRun:\n"
	@printf "  make crawl          Run crawler via CLI (prompts for creds/VIN if missing)\n"
	@printf "  make api            Start FastAPI server (0.0.0.0:8000)\n"
	@printf "  make consumer       Run Kafka->Qdrant consumer locally\n"
	@printf "\nDev:\n"
	@printf "  make lint           Ruff lint\n"
	@printf "  make format         Ruff format\n"
	@printf "  make typecheck      mypy + pyright\n"
	@printf "  make test           Pytest suite\n"
	@printf "  make clean          Remove caches\n\n"

install:
	uv sync

setup: install
	uv run playwright install chromium
	@printf "✅ Setup complete. Configure .env then run make docker-up + make crawl\n"

docker-up:
	docker-compose up -d redis kafka kafka-ui qdrant kafka-qdrant-consumer

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

crawl:
	uv run python -m elsa_crawler.cli.main

api:
	uv run uvicorn elsa_crawler.api.app:app --host 0.0.0.0 --port 8000 --reload

consumer:
	uv run python -m elsa_crawler.storage.kafka_consumer

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy .
	uv run pyright elsa_crawler

test:
	uv run pytest

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@printf "✅ Cache cleaned\n"
