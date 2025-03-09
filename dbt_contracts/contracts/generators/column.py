from abc import ABCMeta
from collections.abc import Sequence
from random import choice, sample
from typing import Literal, Annotated, get_args

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt_common.contracts.metadata import ColumnMetadata
from pydantic import Field, BeforeValidator

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.generators._core import ChildPropertiesGenerator, CORE_FIELDS, PropertyGenerator
from dbt_contracts.contracts.generators.properties import SetDescription
from dbt_contracts.contracts.utils import get_matching_catalog_table, to_tuple
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


EXCLUDE_TYPES = Literal["description", "data_type"]


class ColumnPropertiesGenerator[P: NodeT](ChildPropertiesGenerator[ColumnInfo, P, ColumnPropertyGenerator]):
    exclude: Annotated[Sequence[EXCLUDE_TYPES], BeforeValidator(to_tuple)] = Field(
        description="The fields to exclude from the generated properties.",
        default=(),
        examples=[choice(get_args(EXCLUDE_TYPES)), sample(get_args(EXCLUDE_TYPES), k=2)]
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
