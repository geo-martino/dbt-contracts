from abc import ABCMeta
from collections.abc import Iterable
from typing import Any

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode
from dbt_common.contracts.metadata import CatalogTable

from dbt_contracts.contracts import ContractContext, ContractTerm
from dbt_contracts.contracts.terms._node import get_matching_catalog_table
from dbt_contracts.types import NodeT


def _get_tests(column: ColumnInfo, node: NodeT, manifest: Manifest) -> Iterable[TestNode]:
    def _filter_nodes(test: Any) -> bool:
        return isinstance(test, TestNode) and all((
            test.attached_node == node.unique_id,
            test.column_name == column.name,
        ))

    return filter(_filter_nodes, manifest.nodes.values())


def _column_in_node(column: ColumnInfo, node: NodeT, term_name: str, context: ContractContext) -> bool:
    missing_column = column not in node.columns.values()
    if missing_column:
        message = f"The column cannot be found in the {node.resource_type.lower()}"
        context.add_result(name=term_name, message=message, item=column, parent=node)

    return not missing_column


def _column_in_table(
        column: ColumnInfo, node: NodeT, table: CatalogTable, term_name: str, context: ContractContext
) -> bool:
    missing_column = column.name not in table.columns.keys()
    if missing_column and term_name:
        message = f"The column cannot be found in the {table.metadata.type} {table.unique_id!r}"
        context.add_result(name=term_name, message=message, item=column, parent=node)

    return not missing_column


class ColumnContractTerm[T: NodeT](ContractTerm[ColumnInfo, T], metaclass=ABCMeta):
    pass


class Exists[T: NodeT](ColumnContractTerm[T]):
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not _column_in_node(column=item, node=parent, term_name=self._term_name, context=context):
            return False

        table = get_matching_catalog_table(item=parent, catalog=context.catalog)
        if table is None:
            message = f"The {parent.resource_type.lower()} cannot be found in the database"
            self._add_result(name=self._term_name, message=message, item=item, parent=parent)
            return False

        return _column_in_table(column=item, node=parent, table=table, term_name=self._term_name, context=context)
