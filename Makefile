.PHONY: book clean

book:
	b build book

clean:
	jb clean book

tests_all:
	pytest -v
