import os
import sys

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
]

html_theme = 'sphinx_rtd_theme'
sys.path.insert(0, os.path.abspath('../'))
sys.path.insert(0, os.path.abspath('../tests/sample_app'))
