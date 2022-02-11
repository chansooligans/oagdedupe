.PHONY: book clean

book:
	poetry run jb build book

clean:
	poetry run jb clean book