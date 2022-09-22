.PHONY: tests_all, serve, reset, clear_cache, build, docker-run, lint, label-studio, book, serve

tests_all:
	poetry run pytest -v

clear_cache:
	rm cache/*
	
lint:
	flake8 --ignore W291 dedupe --max-line-length=180
	flake8 --ignore W291 app --max-line-length=180

postgres:
	. dedupe/postgres/postgres.sh;
	python3 dedupe/postgres/postgres.py;

label-studio:
	docker run -it -p $(port):8080 -v `pwd`/cache/mydata:/label-studio/data \
	--env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
	--env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
	-v `pwd`/cache/myfiles:/label-studio/files \
	heartexlabs/label-studio:latest label-studio

fast-api:
	python dedupe/fastapi/main.py 

book:
	poetry run jb build book

serve:
	python -m http.server -d book/_build/html $(port)