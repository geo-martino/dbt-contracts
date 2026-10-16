"""
Re-exports dbt symbols which moved in dbt-core 1.8 that only the tests need.
See :py:mod:`dbt_contracts._compat` for the symbols the package itself uses.

Delete this module and import directly from dbt again once support for dbt-core <1.8 is dropped.
"""
try:  # dbt>=1.8 split resources and artifact schemas into dbt.artifacts
    from dbt.artifacts.resources import FileHash, TestMetadata
    from dbt.artifacts.resources.types import NodeType, TimePeriod
    from dbt.artifacts.resources.v1.components import FreshnessThreshold, Time
    from dbt.artifacts.schemas.catalog import CatalogMetadata
except ImportError:  # dbt<1.8
    from dbt.contracts.files import FileHash
    from dbt.contracts.graph.nodes import TestMetadata
    from dbt.node_types import NodeType
    from dbt.contracts.graph.unparsed import FreshnessThreshold, Time, TimePeriod
    from dbt.contracts.results import CatalogMetadata

try:  # dbt>=1.8 moved shared contracts into dbt-common
    from dbt_common.contracts.constraints import ConstraintType, ModelLevelConstraint
    from dbt_common.contracts.metadata import TableMetadata
except ImportError:  # dbt<1.8
    from dbt.contracts.graph.nodes import ConstraintType, ModelLevelConstraint
    from dbt.contracts.results import TableMetadata

__all__ = [
    "FileHash",
    "TestMetadata",
    "NodeType",
    "TimePeriod",
    "FreshnessThreshold",
    "Time",
    "CatalogMetadata",
    "ConstraintType",
    "ModelLevelConstraint",
    "TableMetadata",
]
