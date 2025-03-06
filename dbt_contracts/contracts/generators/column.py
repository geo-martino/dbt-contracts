from dbt.artifacts.resources.v1.components import ColumnInfo

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators._core import ChildGenerator
from dbt_contracts.contracts.utils import get_matching_catalog_table
from dbt_contracts.types import NodeT


class ColumnGenerator[P: NodeT](ChildGenerator[ColumnInfo, P]):

    def _set_data_type(self, item: ColumnInfo, data_type: str | None) -> bool:
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
