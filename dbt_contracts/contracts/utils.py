from collections.abc import Collection, Mapping, MutableMapping
from typing import Any

from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts._compat import CatalogArtifact, CatalogTable
# defined outside this package so dbt_contracts.properties can import it without a circular import
from dbt_contracts._core import get_absolute_project_path
from dbt_contracts.types import NodeT

__all__ = ["to_tuple", "merge_maps", "get_matching_catalog_table", "get_absolute_project_path"]


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
    Check whether the given ``item`` exists in the database.

    :param item: The resource to match.
    :param catalog: The catalog of tables.
    :return: The matching catalog table.
    """
    if isinstance(item, SourceDefinition):
        return catalog.sources.get(item.unique_id)
    return catalog.nodes.get(item.unique_id)
