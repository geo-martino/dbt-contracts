import os
from pathlib import Path
from typing import Any

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.flags import get_flags
from dbt_common.contracts.metadata import CatalogTable

from dbt_contracts.types import NodeT


def get_matching_catalog_table(item: NodeT, catalog: CatalogArtifact) -> CatalogTable | None:
    """
    Check whether the given `item` exists in the database.

    :param item: The resource to match.
    :param catalog: The catalog of tables.
    :return: The matching catalog table.
    """
    if isinstance(item, SourceDefinition):
        return catalog.sources.get(item.unique_id)
    return catalog.nodes.get(item.unique_id)


def to_tuple(value: Any) -> tuple:
    """Convert the given value to a tuple"""
    if value is None:
        return tuple()
    elif isinstance(value, tuple):
        return value
    elif isinstance(value, str):
        value = (value,)
    return tuple(value)


def get_absolute_project_path(path: str | Path) -> Path | None:
    """
    Get the absolute path of the given relative `path` in the project directory.
    Only returns the path if it exists.

    :param path: The relative path.
    :return: The absolute project path.
    """
    flags = get_flags()
    project_dir = getattr(flags, "PROJECT_DIR", None) or ""

    if project_dir and (path_in_project := Path(project_dir, path)).exists():
        return path_in_project
    elif (path_in_cwd := Path(os.getcwd(), path)).exists():
        return path_in_cwd
    return Path(path)
