"""
Contract configuration for columns.
"""
import itertools
from collections.abc import Iterable
from typing import Generic, TypeVar

from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts_old._core import ChildContract, CatalogContract

ColumnParentT = TypeVar('ColumnParentT', ParsedResource, SourceDefinition)


class ColumnContract(
    CatalogContract[ColumnInfo, ColumnParentT],
    ChildContract[ColumnInfo, ColumnParentT],
    Generic[ColumnParentT]
):
    """Configures a contract configuration for columns."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def config_key(cls) -> str:
        return "columns"

    @property
    def items(self) -> Iterable[tuple[ColumnInfo, ColumnParentT]]:
        arguments = map(lambda parent: [(column, parent) for column in parent.columns.values()], self.parents)
        return self._filter_items(itertools.chain.from_iterable(arguments))
