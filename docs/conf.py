# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import re
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

import sphinx
from sphinx.ext.inheritance_diagram import InheritanceGraph

from dbt_contracts import MODULE_ROOT, PROGRAM_OWNER_NAME, PROGRAM_OWNER_USER, PROGRAM_NAME, PROGRAM_URL


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = PROGRAM_NAME
# noinspection PyShadowingBuiltins
copyright = f"{datetime.now().year}, {PROGRAM_OWNER_NAME}"
author = PROGRAM_OWNER_NAME


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

# noinspection SpellCheckingInspection
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.inheritance_diagram",
    "sphinx_rtd_theme",
    "sphinx_design",
    "sphinx_new_tab_link",
]
# noinspection SpellCheckingInspection
autodoc_member_order = "bysource"
# noinspection SpellCheckingInspection
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "inherited-members": False,
    "special-members": False,
    "show-inheritance": True,
    # "member-order": "bysource",
}
autosummary_generate = True
# document the classes each package re-exports from its private modules via __all__
autosummary_ignore_module_all = False
# the templates strip the package name from page titles
autosummary_context = {"project": MODULE_ROOT}

templates_path = ["_templates"]
exclude_patterns = ["_build",]


def skip_pydantic_internals(app, what: str, name: str, obj, skip: bool, options) -> bool | None:
    """
    Skip pydantic internals which autodoc would otherwise document:

    * the parametrised generic models e.g. ``Result[ModelNode, NoneType]`` which pydantic registers
      on their module, and would be documented as duplicates of their generic class.
    * ``model_config``, which only carries pydantic's own markdown docstring.
    """
    return True if "[" in name or name == "model_config" else None


def link_generic_bases(app, name: str, obj, options, bases: list) -> None:
    """
    Show pydantic's parametrised generic bases e.g. ``Result[ModelNode, NoneType]`` as their generic class
    so the bases shown for a class link to their documentation.
    """
    for idx, base in enumerate(bases):
        metadata = getattr(base, "__pydantic_generic_metadata__", None) or {}
        if (origin := metadata.get("origin")) is not None:
            bases[idx] = origin


def _strip_generic_params(name: str) -> str:
    return re.sub(r"\[.*]$", "", name)


def _class_info_without_generics(self, *args, **kwargs) -> list[tuple[str, str, Sequence[str], str | None]]:
    """
    Remove pydantic's parametrised generic classes e.g. ``ContractTerm[TypeVar]`` from inheritance diagrams,
    linking their subclasses directly to the generic class instead.

    Wraps the private ``InheritanceGraph._class_info`` as Sphinx provides no hook for the classes in diagrams.
    """
    info = []
    for name, fullname, bases, tooltip in _class_info(self, *args, **kwargs):
        if _strip_generic_params(fullname) != fullname:
            continue
        bases = tuple(dict.fromkeys(_strip_generic_params(base) for base in bases))
        info.append((name, fullname, bases, tooltip))
    return info


_class_info = InheritanceGraph._class_info
InheritanceGraph._class_info = _class_info_without_generics


def setup(app) -> None:
    app.connect("autodoc-skip-member", skip_pydantic_internals)
    app.connect("autodoc-process-bases", link_generic_bases)


# -- Linkcheck configuration -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-the-linkcheck-builder

# some sites e.g. graphviz.org reject Sphinx's default browser-like user agent
user_agent = f"Sphinx/{sphinx.__version__} linkcheck (+{PROGRAM_URL})"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_static_path = ["_static"]
html_title = project

html_theme = "sphinx_rtd_theme"
html_theme_options = dict(
    collapse_navigation=False,
    sticky_navigation=True,
    navigation_depth=-1,
    includehidden=True,
    titles_only=False,
)
# html_logo = "_images/dbt-contracts-logo.png"
html_css_files = [
    "style.css",
]
html_context = dict(
    display_github=True,
    github_user=PROGRAM_OWNER_USER,
    github_repo=MODULE_ROOT,
    github_version="HEAD",
    conf_py_path=f"/{Path(__file__).parent.name}/",
)


# -- OpenGraph configuration --------------------------------------------------

ogp_site_url = f"https://{PROGRAM_OWNER_USER}.github.io/{PROGRAM_NAME.lower()}/"


# -- GraphViz configuration --------------------------------------------------
# https://graphviz.org/doc/info/attrs.html

graphviz_output_format = "svg"
inheritance_graph_attrs = dict(
    rankdir="TB", size='""', fontsize=10, ratio="auto", center="true", style="solid"
)
inheritance_node_attrs = dict(shape="ellipse", fontsize=10, fontname="monospace")
