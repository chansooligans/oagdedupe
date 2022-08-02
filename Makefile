.PHONY: tests_all, serve, reset, clear_cache, build, docker-run, lint, label-studio

tests_all:
	poetry run pytest -v

serve:
	mkdir -p cache
	cp tests/test.csv cache/
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

label-studio:
	docker run -it -p 8001:8080 -v `pwd`/cache/mydata:/label-studio/data \
	--env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
	--env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
	-v `pwd`/cache/myfiles:/label-studio/files \
	heartexlabs/label-studio:latest label-studio

fast-api:
	python dedupe/fastapi/main.py --model /mnt/Research.CF/References\ \&\ Training/Satchel/dedupe_rl/active_models/test_df.pkl --cache ../../cache/test.db