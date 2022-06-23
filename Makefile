.PHONY: tests_all, serve, reset, clear_cache, build, docker-run, lint

tests_all:
	poetry run pytest -v

serve:
	poetry run python run.py

reset:
	rm cache/*.json

clear_cache:
	rm cache/*

build:
	docker build -t deduper:latest .

docker-run:
	docker run -t -d --rm --name deduper -p 8080:8081 deduper 

lint:
	flake8 --ignore W291 dedupe --max-line-length=180
	flake8 --ignore W291 app --max-line-length=180