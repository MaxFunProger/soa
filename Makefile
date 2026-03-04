.PHONY: generate
generate:
	python scripts/generate_models.py

.PHONY: run
run: generate
	uvicorn src.marketplace.main:app --reload --host 0.0.0.0 --port 8000

.PHONY: migrate
migrate:
	alembic upgrade head

.PHONY: docker-up
docker-up:
	docker compose up -d

.PHONY: docker-down
docker-down:
	docker compose down
