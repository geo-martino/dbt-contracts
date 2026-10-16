"""
Re-exports dbt symbols which moved in dbt-core 1.8 so the package can support dbt-core <1.8.

Delete this module and import directly from dbt again once support for dbt-core <1.8 is dropped,
replacing :py:func:`get_project_name` with ``manifest.metadata.project_name``.
"""
import os
from hashlib import md5
from itertools import chain

from dbt.contracts.graph.manifest import Manifest

try:  # dbt>=1.8 split resources and artifact schemas into dbt.artifacts
    from dbt.artifacts.resources import BaseResource
    from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
    from dbt.artifacts.resources.v1.macro import MacroArgument
    from dbt.artifacts.schemas.catalog import CatalogArtifact
except ImportError:  # dbt<1.8
    from dbt.contracts.graph.nodes import BaseNode as BaseResource
    from dbt.contracts.graph.nodes import ColumnInfo, ParsedNode as ParsedResource
    from dbt.contracts.graph.unparsed import MacroArgument
    from dbt.contracts.results import CatalogArtifact

try:  # dbt>=1.8 moved shared contracts into dbt-common
    from dbt_common.contracts.metadata import CatalogTable, ColumnMetadata
    from dbt_common.dataclass_schema import dbtClassMixin
    from dbt_common.context import set_invocation_context
except ImportError:  # dbt<1.8
    from dbt.contracts.results import CatalogTable, ColumnMetadata
    from dbt.dataclass_schema import dbtClassMixin

    def set_invocation_context(env: os._Environ) -> None:
        """No-op: dbt<1.8 has no invocation context to set."""

try:  # dbt>=1.8 moved adapter utils into dbt-adapters
    from dbt.adapters.utils import classproperty
except ImportError:  # dbt<1.8
    from dbt.utils import classproperty

try:  # dbt>=1.8
    from dbt.constants import MANIFEST_FILE_NAME
except ImportError:  # dbt<1.8
    from dbt.parser.manifest import MANIFEST_FILE_NAME

try:  # dbt>=1.8 moved the docs tasks into dbt.task.docs
    from dbt.task.docs.generate import CATALOG_FILENAME
except ImportError:  # dbt<1.8
    from dbt.task.generate import CATALOG_FILENAME


def get_project_name(manifest: Manifest) -> str | None:
    """Get the name of the root project of the given ``manifest``."""
    if (project_name := getattr(manifest.metadata, "project_name", None)) is not None:
        return project_name

    # dbt<1.7 only stores an md5 hash of the project name as the project_id
    items = chain(manifest.nodes.values(), manifest.sources.values(), manifest.macros.values())
    packages = {item.package_name for item in items}
    hashed_packages = {md5(name.encode(), usedforsecurity=False).hexdigest(): name for name in packages}
    return hashed_packages.get(manifest.metadata.project_id)


__all__ = [
    "BaseResource",
    "ColumnInfo",
    "ParsedResource",
    "MacroArgument",
    "CatalogArtifact",
    "CatalogTable",
    "ColumnMetadata",
    "dbtClassMixin",
    "set_invocation_context",
    "classproperty",
    "MANIFEST_FILE_NAME",
    "CATALOG_FILENAME",
    "get_project_name",
]
