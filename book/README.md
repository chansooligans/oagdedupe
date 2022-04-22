# Jupyter book documentation

## How to build

### Step 1 set up packages
This was required in dumpling as of 02/02/21 to fix this [bug](https://github.com/executablebooks/jupyter-book/issues/1137):
```sh
pip install sphinxcontrib-bibtex==1.0.0
```
All other required packages come with dumpling.

### Step 2 clean
To clean out previous builds, run
```sh
jb clean book/
```

### Step 3 build
To build the book, run
```sh
jb build book/
```

This creates output html files in book/_build.

## How to view
Once you've built the book, you can serve it with
```sh
python -m http.server -d book/_build/html 8000
```
or pick another port if 8000 is not open. Now navigate to http://pdcprladsci01:8000/ (or whichever port you ended up using).

## How to add

## Resources
- [jupyter book documentation](https://jupyterbook.org/intro.html)

#### restart
jb clean book/; jb build book/; python -m http.server -d book/_build/html 8000