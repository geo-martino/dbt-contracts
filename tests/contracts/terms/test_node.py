from random import sample
from unittest import mock

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode
from faker import Faker

from dbt_contracts.contracts import ContractContext
# noinspection PyProtectedMember
from dbt_contracts.contracts.terms._node import _get_matching_catalog_table, _get_tests, Exists, HasTests, \
    HasAllColumns, HasExpectedColumns, HasMatchingDescription
from dbt_contracts.types import NodeT


@pytest.mark.parametrize("item", ["model", "source"])
def test_get_matching_catalog_table(item: str, catalog: CatalogArtifact, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)

    table = _get_matching_catalog_table(item=item, catalog=catalog)
    assert table is not None
    assert table.metadata.name == item.name


def test_get_no_matching_catalog_table(simple_resource: BaseResource, catalog: CatalogArtifact):
    assert _get_matching_catalog_table(item=simple_resource, catalog=catalog) is None


@pytest.mark.parametrize("item", ["model", "source"])
def test_get_tests(item: str, manifest: Manifest, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)

    tests = list(_get_tests(node=item, manifest=manifest))
    assert tests
    assert all(isinstance(test, TestNode) for test in tests)


def test_get_no_tests(simple_resource: BaseResource, manifest: Manifest):
    assert not list(_get_tests(node=simple_resource, manifest=manifest))


@pytest.mark.parametrize("item", ["model", "source"])
def test_exists(item: str, context: ContractContext, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)
    assert Exists().run(item, context=context)


def test_does_not_exist(simple_resource: BaseResource, context: ContractContext):
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not Exists().run(simple_resource, context=context)
        mock_add_result.assert_called_once()


@pytest.mark.parametrize("item", ["model", "source"])
def test_has_tests(item: str, context: ContractContext, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)
    assert HasTests().run(item, context=context)
    assert not HasTests(min_count=10).run(item, context=context)


def test_has_no_tests(simple_resource: BaseResource, context: ContractContext):
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasTests().run(simple_resource, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
@pytest.mark.parametrize("item", ["model", "source"])
def test_has_all_columns(item: str, context: ContractContext, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)

    assert HasAllColumns().run(item, context=context)  # fixtures are set up to match

    item.columns.clear()
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasAllColumns().run(item, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
@pytest.mark.parametrize("item", ["model", "source"])
def test_has_expected_column_names(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)

    assert HasExpectedColumns().run(item, context=context)
    assert HasExpectedColumns(columns=sample(list(item.columns), k=2)).run(item, context=context)

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasExpectedColumns(columns=faker.words(10)).run(item, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
@pytest.mark.parametrize("item", ["model", "source"])
def test_has_expected_column_types(item: str, context: ContractContext, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)

    columns = {column.name: column.data_type for column in item.columns.values() if column.data_type}
    assert HasExpectedColumns(columns=columns).run(item, context=context)

    columns = {column.name: "incorrect type" for column in item.columns.values()}
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasExpectedColumns(columns=columns).run(item, context=context)
        mock_add_result.assert_called_once()


@pytest.mark.parametrize("item", ["model", "source"])
def test_has_matching_description(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: NodeT = request.getfixturevalue(item)

    assert HasMatchingDescription().run(item, context=context)  # fixtures are set up to match

    item.description = faker.sentence()
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasMatchingDescription().run(item, context=context)
        mock_add_result.assert_called_once()
