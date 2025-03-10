from abc import ABCMeta
from collections.abc import Mapping, Sequence
from random import choice, sample
from typing import Literal, Any, Annotated, get_args

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt_common.contracts.metadata import ColumnMetadata, CatalogTable
from pydantic import Field, BeforeValidator

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.generators._core import ParentPropertiesGenerator, CORE_FIELDS, PropertyGenerator
from dbt_contracts.contracts.generators.properties import SetDescription
from dbt_contracts.contracts.utils import get_matching_catalog_table, merge_maps, to_tuple
from dbt_contracts.types import NodeT

NODE_FIELDS = Literal[CORE_FIELDS, "columns"]


class NodePropertyGenerator[S: NodeT](PropertyGenerator[S, CatalogTable], metaclass=ABCMeta):
    pass


class SetNodeDescription[S: NodeT](SetDescription[S, CatalogTable], NodePropertyGenerator[S]):
    def run(self, source: S, target: CatalogTable) -> bool:
        return self._set_description(source, description=target.metadata.comment)


class SetNodeColumns[S: NodeT](NodePropertyGenerator[S]):
    add: bool = Field(
        description=(
            "Add columns to the properties file which are in the database object but missing from the properties."
        ),
        default=True,
        examples=[True, False],
    )
    remove: bool = Field(
        description=(
            "Remove columns from the properties file which are in the properties file but not in the database object."
        ),
        default=True,
        examples=[True, False],
    )
    order: bool = Field(
        description="Reorder columns in the properties file to match the order found in the database object.",
        default=True,
        examples=[True, False],
    )

    @classmethod
    def _name(cls) -> str:
        return "columns"

    @staticmethod
    def _set_column(source: S, column: ColumnMetadata) -> bool:
        if any(col.name == column.name for col in source.columns.values()):
            return False

        source.columns[column.name] = ColumnInfo(name=column.name)
        return True

    @staticmethod
    def _drop_column(source: S, column: ColumnInfo, columns: Mapping[str, ColumnMetadata]) -> bool:
        if any(col.name == column.name for col in columns.values()):
            return False

        source.columns.pop(column.name)
        return True

    @staticmethod
    def _order_columns(source: S, columns: Mapping[str, ColumnMetadata]):
        index_map = {col.name: col.index for col in columns.values()}
        columns_in_order = dict(
            sorted(source.columns.items(), key=lambda col: index_map.get(col[1].name, len(index_map)))
        )
        if list(columns_in_order) == list(source.columns):
            return False

        source.columns.clear()
        source.columns.update(columns_in_order)
        return True

    def run(self, source: S, target: CatalogTable) -> bool:
        if not target.columns:
            return False

        columns = target.columns

        added = False
        if self.add:
            added = any([self._set_column(source, column=column) for column in columns.values()])

        removed = False
        if self.remove:
            removed = any([
                self._drop_column(source, column=column, columns=columns) for column in source.columns.copy().values()
            ])

        ordered = False
        if self.order:
            ordered = self._order_columns(source, target.columns)

        return added or removed or ordered


EXCLUDE_TYPES = Literal["description", "columns"]


class NodePropertiesGenerator(ParentPropertiesGenerator[NodeT, NodePropertyGenerator], metaclass=ABCMeta):
    exclude: Annotated[Sequence[EXCLUDE_TYPES], BeforeValidator(to_tuple)] = Field(
        description="The fields to exclude from the generated properties.",
        default=(),
        examples=[choice(get_args(EXCLUDE_TYPES)), sample(get_args(EXCLUDE_TYPES), k=2)]
    )
    description: SetNodeDescription = Field(
        description="Configuration for setting the description",
        default=SetNodeDescription(),
    )
    columns: SetNodeColumns = Field(
        description="Configuration for setting the columns",
        default=SetNodeColumns(),
    )

    def merge(self, item: NodeT, context: ContractContext) -> bool:
        if (table := get_matching_catalog_table(item, catalog=context.catalog)) is None:
            return False

        return any([generator.run(item, table) for generator in self.generators])

    @classmethod
    def _merge_columns(cls, item: NodeT, table: dict[str, Any]) -> None:
        if "columns" not in table:
            table["columns"] = []

        for column in table["columns"].copy():
            if column["name"] not in item.columns:
                table["columns"].remove(column)

        for index, column_info in enumerate(item.columns.values()):
            column = cls._generate_column_properties(column_info)
            index_in_props, column_in_props = next(
                ((i, col) for i, col in enumerate(table["columns"]) if col["name"] == column_info.name),
                (None, None)
            )

            if column_in_props is not None:
                merge_maps(column_in_props, column, overwrite=True, extend=False)
            else:
                table["columns"].insert(index, column)

            if index_in_props is not None and index_in_props != index:
                table["columns"].pop(index_in_props)
                table["columns"].insert(index, column_in_props)

    @staticmethod
    def _generate_column_properties(column: ColumnInfo) -> dict[str, Any]:
        column = {
            "name": column.name,
            "description": column.description,
            "data_type": column.data_type,
        }

        return {key: val for key, val in column.items() if val}
