.PHONY: up down logs ps test clean restart-consumer scenario-basic scenario-idempotent scenario-ordering scenario-dlq scenario-multinode scenario-evolution

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

restart-consumer:
	docker compose restart consumer

# Запустить интеграционный smoke-тест в отдельном контейнере (общая сеть compose).
test:
	docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm --build tests

clean:
	docker compose down -v --remove-orphans

# Удобные шорткаты для проверки сценариев из README.
scenario-basic:
	./tests/scenarios/01_basic.sh

scenario-idempotent:
	./tests/scenarios/02_idempotency.sh

scenario-ordering:
	./tests/scenarios/04_ordering.sh

scenario-dlq:
	./tests/scenarios/05_dlq.sh

scenario-multinode:
	./tests/scenarios/06_multinode.sh

scenario-evolution:
	./tests/scenarios/08_schema_evolution.sh
