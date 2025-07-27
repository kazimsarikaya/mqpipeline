import os
import sys

extensions = [
    "myst_parser",       # Enable Markdown
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

# Recognize Markdown and reStructuredText
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

project = 'MQPipeline'
version = '0.1.0'
author = 'Kazım SARIKAYA'

html_title = f'{project} {version} Documentation'
html_theme = 'sphinx_rtd_theme'


sys.path.insert(0, os.path.abspath('../'))
sys.path.insert(0, os.path.abspath('../tests/sample_app'))
