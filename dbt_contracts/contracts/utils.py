import os
from collections.abc import Collection, Mapping, MutableMapping
from pathlib import Path
from typing import Any

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.flags import get_flags
from dbt_common.contracts.metadata import CatalogTable

from dbt_contracts.types import NodeT


def to_tuple(value: Any) -> tuple:
    """Convert the given value to a tuple"""
    if value is None:
        return tuple()
    elif isinstance(value, tuple):
        return value
    elif isinstance(value, str):
        value = (value,)
    return tuple(value)


def merge_maps[T: MutableMapping](source: T, new: Mapping, extend: bool = True, overwrite: bool = False) -> T:
    """
    Recursively update a given ``source`` map in place with a ``new`` map.

    :param source: The source map.
    :param new: The new map with values to update for the source map.
    :param extend: When a value is a list and a list is already present in the source map, extend the list when True.
        When False, only replace the list if overwrite is True.
    :param overwrite: When True, overwrite any value in the source destructively.
    :return: The updated dict.
    """
    def is_collection(value: Any) -> bool:
        """Return True if ``value`` is of type ``Collection`` and not a string or map"""
        return isinstance(value, Collection) and not isinstance(value, str) and not isinstance(value, Mapping)

    for k, v in new.items():
        if isinstance(v, Mapping) and isinstance(source.get(k, {}), Mapping):
            source[k] = merge_maps(source.get(k, {}), v, extend=extend, overwrite=overwrite)
        elif extend and is_collection(v) and is_collection(source.get(k, [])):
            source[k] = list(to_tuple(source.get(k, []))) + list(to_tuple(v))
        elif overwrite or source.get(k) is None:
            source[k] = v

    return source


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
        return path_in_project.resolve()
    elif (path_in_cwd := Path(os.getcwd(), path)).exists():
        return path_in_cwd.resolve()

    raise FileNotFoundError(f"Could not find absolute path for: {path}")
