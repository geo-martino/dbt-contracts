from abc import ABCMeta
from collections.abc import Mapping, Sequence
from random import choice, sample
from typing import Literal, Annotated, get_args

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt_common.contracts.metadata import ColumnMetadata
from pydantic import Field, BeforeValidator

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.generators._core import ParentPropertiesGenerator, CORE_FIELDS
from dbt_contracts.contracts.utils import get_matching_catalog_table, to_tuple
from dbt_contracts.types import NodeT

NODE_FIELDS = Literal[CORE_FIELDS, "columns"]


class NodePropertiesGenerator[I: NodeT](ParentPropertiesGenerator[I], metaclass=ABCMeta):
    exclude: Annotated[Sequence[NODE_FIELDS], BeforeValidator(to_tuple)] = Field(
        description="The fields to exclude from the generated properties.",
        default=(),
        examples=[choice(get_args(NODE_FIELDS)), sample(get_args(NODE_FIELDS), k=2)]
    )
    remove_columns: bool = Field(
        description=(
            "Remove columns from the properties file which are not found in the database object. "
            "Ignored when 'columns' is excluded."
        ),
        default=False,
        examples=[True, False],
    )
    order_columns: bool = Field(
        description=(
            "Reorder columns in the properties file to match the order found in the database object. "
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

        added = any([self._set_column(item, column=column) for column in columns.values()])
        removed = any([
            self._drop_column(item, column=column, columns=columns) for column in list(item.columns.values())
        ])
        return added or removed

    @staticmethod
    def _set_column(item: I, column: ColumnMetadata) -> bool:
        if any(col.name == column.name for col in item.columns.values()):
            return False

        item.columns[column.name] = ColumnInfo(name=column.name)
        return True

    def _drop_column(self, item: I, column: ColumnInfo, columns: Mapping[str, ColumnMetadata]) -> bool:
        if not self.remove_columns or any(col.name == column.name for col in columns.values()):
            return False

        item.columns.pop(column.name)
        return True

    def _reorder_columns(self, item: I, columns: Mapping[str, ColumnMetadata]) -> bool:
        if "columns" in self.exclude:
            return False
        if not self.order_columns or not columns:
            return False

        index_map = {col.name: col.index for col in columns.values()}
        columns_in_order = dict(
            sorted(item.columns.items(), key=lambda col: index_map.get(col[1].name, len(index_map)))
        )
        if list(columns_in_order) == list(item.columns):
            return False

        item.columns.clear()
        item.columns.update(columns_in_order)
        return True

    def merge(self, item: I, context: ContractContext) -> bool:
        if (table := get_matching_catalog_table(item, catalog=context.catalog)) is None:
            return False

        modified = False
        modified |= self._set_description(item, description=table.metadata.comment)
        modified |= self._set_columns(item, columns=table.columns)
        modified |= self._reorder_columns(item, columns=table.columns)

        return modified
