.PHONY: tests_all, serve, reset, clear_cache

tests_all:
	poetry run pytest -v

serve:
	poetry run python app/app.py

reset:
	rm cache/*.json

clear_cache:
	rm cache/*

build:
	docker build -t deduper:latest .

docker-run:
	docker run -t -d --rm --name deduper -p 8007:8080 deduper 