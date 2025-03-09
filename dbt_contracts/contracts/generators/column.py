from abc import ABCMeta
from typing import Literal, Any

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt_common.contracts.metadata import ColumnMetadata
from pydantic import Field

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.generators._core import ChildPropertiesGenerator, CORE_FIELDS, PropertyGenerator
from dbt_contracts.contracts.generators.properties import SetDescription
from dbt_contracts.contracts.utils import get_matching_catalog_table
from dbt_contracts.types import NodeT

COLUMN_FIELDS = Literal[CORE_FIELDS, "data_type"]


class ColumnPropertyGenerator(PropertyGenerator[ColumnInfo, ColumnMetadata], metaclass=ABCMeta):
    pass


class SetColumnDescription(ColumnPropertyGenerator, SetDescription[ColumnInfo, ColumnMetadata]):
    def run(self, source: ColumnInfo, target: ColumnMetadata) -> bool:
        return self._set_description(source, description=target.comment)


class SetDataType(ColumnPropertyGenerator):
    @classmethod
    def _name(cls) -> str:
        return "data_type"

    def _set_data_type(self, source: ColumnInfo, data_type: str | None) -> bool:
        if not data_type:
            return False
        if source.data_type and not self.overwrite:
            return False
        if source.data_type == data_type:
            return False

        source.data_type = data_type
        return True

    def run(self, source: ColumnInfo, target: ColumnMetadata) -> bool:
        return self._set_data_type(source, data_type=target.type)


class ColumnPropertiesGenerator[P: NodeT](ChildPropertiesGenerator[ColumnInfo, P, ColumnPropertyGenerator]):
    __supported_generators__ = (
        SetColumnDescription,
        SetDataType,
    )

    description: SetColumnDescription = Field(
        description="Configuration for setting the column description",
        default=SetColumnDescription(),
    )
    data_type: SetDataType = Field(
        description="Configuration for setting the column data type",
        default=SetDataType(),
    )

    def merge(self, item: ColumnInfo, context: ContractContext, parent: P = None) -> bool:
        if (table := get_matching_catalog_table(parent, catalog=context.catalog)) is None:
            return False
        if (column := next((col for col in table.columns.values() if col.name == item.name), None)) is None:
            return False

        return any([generator.run(item, column) for generator in self.generators])
