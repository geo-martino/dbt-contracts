from collections.abc import Sequence
from random import choice
from typing import Literal, Annotated, get_args

from dbt.artifacts.resources.v1.components import ColumnInfo
from pydantic import BeforeValidator, Field

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators._core import ChildGenerator, CORE_FIELDS
from dbt_contracts.contracts.utils import get_matching_catalog_table, to_tuple
from dbt_contracts.types import NodeT

COLUMN_FIELDS = Literal[CORE_FIELDS, "data_type"]


class ColumnGenerator[P: NodeT](ChildGenerator[ColumnInfo, P]):
    exclude: Annotated[Sequence[COLUMN_FIELDS], BeforeValidator(to_tuple)] = Field(
        description="The fields to exclude from the generated properties.",
        default=(),
        examples=[choice(get_args(COLUMN_FIELDS)), list(get_args(COLUMN_FIELDS))]
    )

    def _set_data_type(self, item: ColumnInfo, data_type: str | None) -> bool:
        if "data_type" in self.exclude:
            return False
        if not data_type:
            return False
        if item.data_type and not self.overwrite:
            return False
        if item.data_type == data_type:
            return False

        item.data_type = data_type
        return True

    def merge(self, item: ColumnInfo, context: ContractContext, parent: P = None) -> bool:
        if (table := get_matching_catalog_table(parent, catalog=context.catalog)) is None:
            return False
        if (column := next(col for col in table.columns.values() if col.name == item.name)) is None:
            return False

        modified = False
        modified |= self._set_description(item, column.comment)
        modified |= self._set_data_type(item, column.type)

        return modified
