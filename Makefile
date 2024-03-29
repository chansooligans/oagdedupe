export DATABASE_URL=postgresql+psycopg2://username:password@0.0.0.0:8088/db
.PHONY: tests_all, test-file, mypy, lint, serve, postgres, test-postgres, label-studio, book, serve

tests_all:
	poetry run pytest -v -rP

test-file:
	poetry run pytest -v -rP $(file)

mypy:
	mypy --show-error-codes --config-file=.mypy.ini $(file)
	
lint:
	flake8 --ignore W291 oagdedupe --max-line-length=180

postgres:
	docker run --rm -dp 8000:5432 \
      --name oagdedupe-postgres \
      --env POSTGRES_USER=username \
      --env POSTGRES_PASSWORD=password \
      --env POSTGRES_DB=db \
      --env PGDATA=/var/lib/pgsql/data/pgdata \
      -v "`pwd`/.dedupe:/var/lib/pgsql/data" \
      chansoosong/oagdedupe-postgres 

test-postgres:
	docker run --rm -dp 8088:5432 \
      --name test-oagdedupe-postgres \
      --env POSTGRES_USER=username \
      --env POSTGRES_PASSWORD=password \
      --env POSTGRES_DB=db \
      --env PGDATA=/var/lib/pgsql/data/pgdata \
      -v "`pwd`/.dedupe_test:/var/lib/pgsql/data" \
      chansoosong/oagdedupe-postgres 

label-studio:
	docker run --rm -it -dp $(port):8080 \
	--name oagdedupe-labelstudio \
	--add-host host.docker.internal:host-gateway \
	--env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
	--env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
	-v "`pwd`/.dedupe:/label-studio/data" \
	-v "`pwd`/.dedupe:/label-studio/files" \
	heartexlabs/label-studio:latest label-studio

fast-api:
	python oagdedupe/fastapi/main.py --settings='.dedupe/.env'

book:
	poetry run jb build book

serve:
	python -m http.server -d book/_build/html $(port)

