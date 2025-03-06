from collections.abc import Mapping, Sequence
from random import choice
from typing import Literal, Annotated, get_args

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt_common.contracts.metadata import ColumnMetadata
from pydantic import Field, BeforeValidator

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators._core import ParentGenerator, CORE_FIELDS
from dbt_contracts.contracts.utils import get_matching_catalog_table, to_tuple
from dbt_contracts.types import NodeT

NODE_FIELDS = Literal[CORE_FIELDS, "columns"]


class NodeGenerator[I: NodeT](ParentGenerator[I]):
    exclude: Annotated[Sequence[NODE_FIELDS], BeforeValidator(to_tuple)] = Field(
        description="The fields to exclude from the generated properties.",
        default=(),
        examples=[choice(get_args(NODE_FIELDS)), list(get_args(NODE_FIELDS))]
    )
    ordered_columns: bool = Field(
        description=(
            "Reorder the columns to match the order found in the database object. "
            "Ignored when 'columns' is excluded."
        ),
        default=False,
        examples=[True, False],
    )

    def _set_columns(self, item: I, columns: Mapping[str, ColumnMetadata]) -> bool:
        if "columns" in self.exclude:
            return False
        if not columns:
            return False

        return any(self._set_column(item, column) for column in columns.values())

    @staticmethod
    def _set_column(item: I, column: ColumnMetadata) -> bool:
        if any(col.name == column.name for col in item.columns.values()):
            return False

        item.columns[column.name] = ColumnInfo(name=column.name)
        return True

    def _reorder_columns(self, item: I, columns: Mapping[str, ColumnMetadata]) -> bool:
        if "columns" in self.exclude:
            return False
        if not self.ordered_columns or not columns:
            return False

        index_map = {col.name: col.index for col in columns.values()}
        columns_in_order = dict(
            sorted(item.columns.items(), key=lambda col: index_map.get(col[1].name, len(index_map)))
        )
        if columns_in_order == item.columns:
            return False

        item.columns = columns_in_order
        return True

    def merge(self, item: I, context: ContractContext) -> bool:
        if (table := get_matching_catalog_table(item, catalog=context.catalog)) is None:
            return False

        modified = False
        modified |= self._set_description(item, table.metadata.comment)
        modified |= self._set_columns(item, table.columns)
        modified |= self._reorder_columns(item, table.columns)

        return modified
