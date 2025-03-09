from copy import copy, deepcopy
from random import choice
from unittest import mock

import pytest
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode, ModelNode, CompiledNode
from dbt_common.contracts.metadata import CatalogTable
from faker import Faker

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.terms.column import ColumnContractTerm, Exists, HasTests, HasExpectedName, HasDataType, \
    HasMatchingDescription, HasMatchingDataType, HasMatchingIndex
from dbt_contracts.contracts.utils import get_matching_catalog_table
from dbt_contracts.types import NodeT


# noinspection PyTestUnpassedFixture
def test_validate_node(node: NodeT, node_column: ColumnInfo, context: ContractContext):
    assert Exists()._validate_node(node=node, column=node_column, context=context)

    node.columns.pop(node_column.name)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not Exists()._validate_node(node=node, column=node_column, context=context)
        mock_add_result.assert_called_once()


def test_get_and_validate_table(
        node: NodeT, node_column: ColumnInfo, catalog_table: CatalogTable, context: ContractContext
):
    assert Exists()._get_and_validate_table(node=node, column=node_column, context=context) == catalog_table


def test_get_and_validate_table_unmatched_table(
        node: NodeT, node_column: ColumnInfo, catalog_table: CatalogTable, context: ContractContext
):
    node.unique_id = "unknown_id"
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert Exists()._get_and_validate_table(node=node, column=node_column, context=context) is None
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
def test_get_and_validate_table_missing_column(
        node: NodeT, node_column: ColumnInfo, catalog_table: CatalogTable, context: ContractContext
):
    # add a new column to the node to simulate a column being remove
    # preferred way to set up test as the catalog_table should not be modified to ensure other tests pass
    new_column = deepcopy(node_column)
    new_column.name = "new_columns"
    node.columns[new_column.name] = new_column

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert Exists()._get_and_validate_table(node=node, column=new_column, context=context) is None
        mock_add_result.assert_called_once()


@pytest.mark.parametrize("term", [
    Exists(),
    HasTests(),
    HasExpectedName(),
    HasDataType(),
    HasMatchingDescription(),
    HasMatchingDataType(),
    HasMatchingIndex()
])
def test_terms_validate_column_in_node(term: ColumnContractTerm, model: ModelNode, context: ContractContext):
    column = choice(list(model.columns.values()))

    with mock.patch.object(term.__class__, "_validate_node", return_value=False) as mock_validate_node:
        assert not term.run(item=column, parent=model, context=context)
        mock_validate_node.assert_called_once()

    model.columns.pop(column.name)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not term.run(item=column, parent=model, context=context)
        mock_add_result.assert_called_once()
        assert "The column cannot be found" in mock_add_result.mock_calls[0].kwargs["message"]


def test_exists(node: CompiledNode, node_column: ColumnInfo, context: ContractContext):
    assert get_matching_catalog_table(item=node, catalog=context.catalog) is not None
    assert Exists().run(node_column, parent=node, context=context)

    node.columns.pop(node_column.name)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not Exists().run(node_column, parent=node, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
def test_get_tests(node: NodeT, node_column: ColumnInfo, simple_resource: BaseResource, manifest: Manifest):
    tests = list(HasTests._get_tests(column=node_column, node=node, manifest=manifest))
    assert tests
    assert all(isinstance(test, TestNode) for test in tests)
    assert all(test.attached_node == node.unique_id and test.column_name == node_column.name for test in tests)

    # noinspection PyTypeChecker
    assert not list(HasTests._get_tests(column=node_column, node=simple_resource, manifest=manifest))


def test_has_tests(node: CompiledNode, node_column: ColumnInfo, context: ContractContext):
    assert 0 < len(list(HasTests._get_tests(column=node_column, node=node, manifest=context.manifest))) < 10
    assert HasTests().run(node_column, parent=node, context=context)

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasTests(min_count=10).run(node_column, parent=node, context=context)
        mock_add_result.assert_called_once()


def test_get_column_data_type(
        node: CompiledNode, node_column: ColumnInfo, catalog_table: CatalogTable, context: ContractContext
):
    node_column.data_type = "int"
    catalog_table.columns[node_column.name].type = "str"

    # gets data type from node
    assert HasExpectedName()._get_column_data_type(column=node_column, node=node, context=context) == "int"

    # gets data type from table when node data type is not available
    node_column.data_type = None
    assert HasExpectedName()._get_column_data_type(column=node_column, node=node, context=context) == "str"

    # returns safely when table or catalog are not available
    node.unique_id = "unknown_id"
    assert HasExpectedName()._get_column_data_type(column=node_column, node=node, context=context) is None
    context = copy(context)
    assert HasExpectedName()._get_column_data_type(column=node_column, node=node, context=context) is None


def test_has_expected_name(
        node: CompiledNode, node_column: ColumnInfo, catalog_table: CatalogTable, context: ContractContext
):
    node_column.data_type = "boolean"
    # always returns true when data type is defined in patterns
    assert HasExpectedName(patterns={"str": ".*"}).run(node_column, parent=node, context=context)

    node_column.name = "has_name"
    assert HasExpectedName(patterns={"boolean": "has_.*"}).run(node_column, parent=node, context=context)

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasExpectedName(patterns={"boolean": "is_.*"}).run(node_column, parent=node, context=context)
        mock_add_result.assert_called_once()


def test_has_data_type(
        node: CompiledNode, node_column: ColumnInfo, catalog_table: CatalogTable, context: ContractContext
):
    node_column.data_type = "int"
    assert HasDataType().run(node_column, parent=node, context=context)

    node_column.data_type = None
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasDataType().run(node_column, parent=node, context=context)
        mock_add_result.assert_called_once()


def test_has_matching_description(
        node: CompiledNode, node_column: ColumnInfo, catalog_table: CatalogTable, faker: Faker, context: ContractContext
):
    node_column.description = catalog_table.columns[node_column.name].comment
    assert HasMatchingDescription().run(node_column, parent=node, context=context)

    node_column.description = faker.sentence()
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasMatchingDescription().run(node_column, parent=node, context=context)
        mock_add_result.assert_called_once()


def test_has_matching_data_type(
        node: CompiledNode, node_column: ColumnInfo, catalog_table: CatalogTable, faker: Faker, context: ContractContext
):
    node_column.data_type = catalog_table.columns[node_column.name].type
    assert HasMatchingDataType().run(node_column, parent=node, context=context)

    node_column.data_type = "_".join(faker.words())
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasMatchingDataType().run(node_column, parent=node, context=context)
        mock_add_result.assert_called_once()


def test_has_matching_index(
        node: CompiledNode, node_column: ColumnInfo, catalog_table: CatalogTable, context: ContractContext
):
    assert list(node.columns).index(node_column.name) == catalog_table.columns[node_column.name].index
    assert HasMatchingIndex().run(node_column, parent=node, context=context)

    # reorder columns
    if list(node.columns.keys()).index(node_column.name) == (len(node.columns) - 1):
        first_column = node.columns.pop(list(node.columns.keys())[0])
        node.columns[first_column.name] = first_column
    else:
        node.columns.pop(node_column.name)
        node.columns[node_column.name] = node_column

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasMatchingIndex().run(node_column, parent=node, context=context)
        mock_add_result.assert_called_once()
