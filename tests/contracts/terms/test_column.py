from random import choice
from unittest import mock

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.nodes import TestNode, SourceDefinition
from dbt_common.contracts.metadata import CatalogTable
from distlib.manifest import Manifest

from dbt_contracts.contracts import ContractContext
# noinspection PyProtectedMember
from dbt_contracts.contracts.terms.column import _get_tests, _column_in_node, _column_in_table
from dbt_contracts.types import NodeT


# noinspection PyTestUnpassedFixture
def test_get_tests(node: NodeT, node_column: ColumnInfo, manifest: Manifest):
    tests = list(_get_tests(column=node_column, node=node, manifest=manifest))
    assert tests
    assert all(isinstance(test, TestNode) for test in tests)
    assert all(test.attached_node == node.unique_id and test.column_name == node_column.name for test in tests)


def test_get_no_tests(column: ColumnInfo, simple_resource: BaseResource, manifest: Manifest):
    assert not list(_get_tests(column=column, node=simple_resource, manifest=manifest))


# noinspection PyTestUnpassedFixture
def test_column_in_node(node: NodeT, node_column: ColumnInfo, context: ContractContext):
    assert _column_in_node(node=node, column=node_column, context=context, term_name="test")

    node.columns.pop(node_column.name)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not _column_in_node(node=node, column=node_column, context=context, term_name="test")
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
def test_column_in_table(node: NodeT, node_column: ColumnInfo, node_table: CatalogTable, context: ContractContext):
    assert _column_in_table(node=node, column=node_column, table=node_table, context=context, term_name="test")

    node_table.columns.pop(node_column.name)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not _column_in_table(node=node, column=node_column, table=node_table, context=context, term_name="test")
        mock_add_result.assert_called_once()

