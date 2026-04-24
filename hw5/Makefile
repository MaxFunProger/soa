.PHONY: up down logs test clean producer-logs aggregation-logs

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

producer-logs:
	docker compose logs -f --tail=200 producer

aggregation-logs:
	docker compose logs -f --tail=200 aggregation

test:
	docker compose -f docker-compose.yml -f docker-compose.test.yml run --rm --build tests
