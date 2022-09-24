# Configuration file for the Sphinx documentation builder.

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

# -- Project information
project = 'oagdedupe'
author = 'Chansoo Song, Gautam Sisodia'

release = '0.1'
version = '0.1.0'

# -- General configuration

extensions = [
    'myst_parser',
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autosectionlabel'
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output
html_theme = 'sphinx_rtd_theme'

html_theme_options = {
    "collapse_navigation" : False,
    "navigation_depth": 2
}

# -- Options for EPUB output
epub_show_urls = 'footnote'