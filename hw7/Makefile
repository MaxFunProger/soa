COMPOSE = docker compose

.PHONY: up down clean logs ps test-unit test-int load sli alerts demo-alert

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

test-unit:
	cd producer && PYTHONPATH=. python -m pytest tests -v
	cd aggregation && PYTHONPATH=. python -m pytest tests -v

test-int:
	$(COMPOSE) -f docker-compose.yml -f docker-compose.test.yml run --rm --build tests

load:
	mkdir -p load/results
	$(COMPOSE) -f docker-compose.yml -f docker-compose.load.yml run --rm k6

sli:
	python3 scripts/check_sli.py --prometheus http://localhost:9090 --wait 0

demo-alert:
	./scripts/demo_alert.sh
