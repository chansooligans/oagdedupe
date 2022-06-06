.PHONY: tests_all

tests_all:
	poetry run pytest -v

serve:
	poetry run python app/app.py

reset:
	rm cache/*
	poetry run python app/app.py

