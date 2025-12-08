"""Sphinx configuration for Minions documentation."""
from __future__ import annotations

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(".."))

project = ""
author = "Chris"
release = "0.0.2"
copyright = f"{datetime.now():%Y}, {author}"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_design",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "private/**"]

autosummary_generate = True
autosummary_imported_members = True
autodoc_member_order = "bysource"
autodoc_typehints = "description"

myst_enable_extensions = ["colon_fence", "deflist"]
myst_heading_anchors = 3

html_theme = "sphinx_book_theme"
html_theme_options = {
    "repository_url": "https://github.com/crios1/minions",
    "announcement": "Status: Pre-alpha (`0.0.x`). APIs and docs are still evolving and may change without notice.",
    "use_repository_button": False,
    "use_issues_button": False,
    "use_edit_page_button": False,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/crios1/minions",
            "icon": "fab fa-github",
            "type": "fontawesome",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/minions/",
            "icon": "_static/pypi-logo-small.8998e9d1.svg",
            "type": "local",
        },
    ],
    "show_navbar_depth": 1,
    # Disable PDF/Markdown download buttons to keep the header clean.
    "use_download_button": False,
    # Remove the fullscreen toggle button.
    "use_fullscreen_button": False,
    "logo": {
        "image_light": "_static/mascot-871-shadowed.png",
        "image_dark": "_static/mascot-856.png",
        "alt_text": "Minions Documentation - Home",
    },
}


html_static_path = ["_static"]
# html_logo = "_static/mascot-256.png"
html_favicon = "_static/favicon.ico"

# Load a small custom stylesheet so the logo can be sized/positioned neatly.
html_css_files = ["custom.css"]
