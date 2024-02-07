# -*- coding: utf-8 -*-

import os
import re
import sys

import getml

# If we are building locally, or the build on Read the Docs looks like a PR
# build, prefer to use the version of the theme in this repo, not the installed
# version of the theme.
# def is_development_build():
#     # PR builds have an interger version
#     re_version = re.compile(r'^[\d]+$')
#     if 'READTHEDOCS' in os.environ:
#         version = os.environ.get('READTHEDOCS_VERSION', '')
#         if re_version.match(version):
#             return True
#         return False
#     return True

# if is_development_build():
sys.path.insert(1, os.path.abspath(".."))
# sys.path.append(os.path.abspath('./demo/'))


import sphinx_rtd_theme
from sphinx.locale import _

# project = u'Read the Docs Sphinx Theme'
# version = '0.5.0'
# release = '0.5.0'
# author = u'Dave Snider, Read the Docs, Inc. & contributors'
# copyright = author


project = "getML"
slug = re.sub(r"\W+", "-", project.lower())
version = getml.__version__
release = getml.__version__
author = "The SQLNet Company GmbH"
copyright = author
language = "en"

extensions = [
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "sphinxcontrib.httpdomain",
    "sphinx_rtd_theme",
    "sphinx.ext.autosummary",
    "sphinx_automodapi.automodapi",
    'sphinx.ext.autosectionlabel'
]

autosummary_generate = True
autosummary_imported_members = True

autosectionlabel_prefix_document = True

numpydoc_show_class_members = False

templates_path = ["_templates"]
source_suffix = ".rst"
exclude_patterns = []
locale_dirs = ["locale/"]
gettext_compact = False

master_doc = "index"
suppress_warnings = ["image.nonlocal_uri"]
pygments_style = "trac"

intersphinx_mapping = {
    "rtd": ("https://docs.readthedocs.io/en/latest/", None),
    "sphinx": ("http://www.sphinx-doc.org/en/stable/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "numpy": ("https://docs.scipy.org/doc/numpy", None),
}


html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "logo_only": True,
    "style_nav_header_background": "#ffffff",
    "navigation_depth": 5,
    "prev_next_buttons_location": None,
}
html_context = {}

html_css_files = [
    # [#420] - temporarily disabled
    # "https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.css",
    "css/theme.css",
    "css/custom.css",
]

html_js_files = [
    # [#420] - temporarily disabled
    # "https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.js",
    ("https://getml.com/assets/js/tags.js", {"defer": "defer"}),
    ("js/getml.js", {"defer": "defer"}),
]

html_static_path = ["_static/"]

# if not 'READTHEDOCS' in os.environ:
#     html_js_files = ['debug.js']

#     # Add fake versions for local QA of the menu
#     html_context['test_versions'] = list(map(
#         lambda x: str(x / 10),
#         range(1, 100)
#     ))

html_favicon = "_static/img/favicon.png"
html_logo = "_static/img/shape-main.png"

# html_logo = 'theme/getml_simple/static/logo-scaled.png'
# html_baseurl = 'https://docs.getml.com/'

html_show_sphinx = False

html_show_sourcelink = False

htmlhelp_basename = slug


# latex_documents = [
#   ('index', '{0}.tex'.format(slug), project, author, 'manual'),
# ]

# man_pages = [
#     ('index', slug, project, [author], 1)
# ]

# texinfo_documents = [
#   ('index', slug, project, author, slug, project, 'Miscellaneous'),
# ]


# # Extensions to theme docs
# def setup(app):
#     from sphinx.domains.python import PyField
#     from sphinx.util.docfields import Field

#     app.add_object_type(
#         'confval',
#         'confval',
#         objname='configuration value',
#         indextemplate='pair: %s; configuration value',
#         doc_field_types=[
#             PyField(
#                 'type',
#                 label=_('Type'),
#                 has_arg=False,
#                 names=('type',),
#                 bodyrolename='class'
#             ),
#             Field(
#                 'default',
#                 label=_('Default'),
#                 has_arg=False,
#                 names=('default',),
#             ),
#         ]
#     )
