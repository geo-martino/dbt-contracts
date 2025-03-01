from random import sample
from unittest import mock

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode, CompiledNode
from faker import Faker

from dbt_contracts.contracts import ContractContext
# noinspection PyProtectedMember
from dbt_contracts.contracts.terms._node import _get_matching_catalog_table, _get_tests, Exists, HasTests, \
    HasAllColumns, HasExpectedColumns, HasMatchingDescription, HasContract, HasValidUpstreamDependencies, \
    HasValidRefDependencies, HasValidSourceDependencies, HasValidMacroDependencies, HasNoFinalSemiColon, \
    HasNoHardcodedRefs


@pytest.fixture(params=["model", "source"])
def node(request: FixtureRequest) -> CompiledNode:
    return request.getfixturevalue(request.param)


def test_get_matching_catalog_table(node: CompiledNode, catalog: CatalogArtifact):
    table = _get_matching_catalog_table(item=node, catalog=catalog)
    assert table is not None
    assert table.metadata.name == node.name


def test_get_no_matching_catalog_table(simple_resource: BaseResource, catalog: CatalogArtifact):
    assert _get_matching_catalog_table(item=simple_resource, catalog=catalog) is None


def test_get_tests(node: CompiledNode, manifest: Manifest, catalog: CatalogArtifact):
    tests = list(_get_tests(node=node, manifest=manifest))
    assert tests
    assert all(isinstance(test, TestNode) for test in tests)


def test_get_no_tests(simple_resource: BaseResource, manifest: Manifest):
    assert not list(_get_tests(node=simple_resource, manifest=manifest))


def test_exists(node: CompiledNode, context: ContractContext):
    assert Exists().run(node, context=context)


def test_does_not_exist(simple_resource: BaseResource, context: ContractContext):
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not Exists().run(simple_resource, context=context)
        mock_add_result.assert_called_once()


def test_has_tests(node: CompiledNode, context: ContractContext):
    assert HasTests().run(node, context=context)
    assert not HasTests(min_count=10).run(node, context=context)


def test_has_no_tests(simple_resource: BaseResource, context: ContractContext):
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasTests().run(simple_resource, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
def test_has_all_columns(node: CompiledNode, context: ContractContext):
    assert HasAllColumns().run(node, context=context)  # fixtures are set up to match

    node.columns.clear()
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasAllColumns().run(node, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
def test_has_expected_column_names(node: CompiledNode, context: ContractContext, faker: Faker):
    assert HasExpectedColumns().run(node, context=context)
    assert HasExpectedColumns(columns=sample(list(node.columns), k=2)).run(node, context=context)

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasExpectedColumns(columns=faker.words(10)).run(node, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
def test_has_expected_column_types(node: CompiledNode, context: ContractContext):
    columns = {column.name: column.data_type for column in node.columns.values() if column.data_type}
    assert HasExpectedColumns(columns=columns).run(node, context=context)

    columns = {column.name: "incorrect type" for column in node.columns.values()}
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasExpectedColumns(columns=columns).run(node, context=context)
        mock_add_result.assert_called_once()


def test_has_matching_description(node: CompiledNode, context: ContractContext, faker: Faker):
    assert HasMatchingDescription().run(node, context=context)  # fixtures are set up to match

    node.description = faker.sentence()
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasMatchingDescription().run(node, context=context)
        mock_add_result.assert_called_once()


# noinspection PyTestUnpassedFixture
@pytest.fixture(params=["model"])
def compiled_node(request: FixtureRequest) -> CompiledNode:
    return request.getfixturevalue(request.param)


# noinspection PyTestUnpassedFixture
@pytest.fixture
def contract_node(compiled_node: CompiledNode) -> CompiledNode:
    compiled_node.contract.enforced = True
    for column in compiled_node.columns.values():
        column.data_type = "int"

    return compiled_node


def test_has_valid_contract(contract_node: CompiledNode, context: ContractContext):
    with mock.patch.object(HasAllColumns, "run", return_value=True) as mock_has_all_columns:
        assert HasContract().run(contract_node, context=context)
        mock_has_all_columns.assert_called_once_with(contract_node, parent=None, context=context)


def test_has_invalid_contract_not_enforced(contract_node: CompiledNode, context: ContractContext):
    contract_node.contract.enforced = False

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasContract().run(contract_node, context=context)
        mock_add_result.assert_called_once()
        assert mock_add_result.mock_calls[0].kwargs["message"] == "Contract not enforced"


def test_has_invalid_contract_missing_data_types(contract_node: CompiledNode, context: ContractContext):
    for column in sample(list(contract_node.columns.values()), k=2):
        column.data_type = None

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasContract().run(contract_node, context=context)
        mock_add_result.assert_called_once()
        assert "all data types must be declared" in mock_add_result.mock_calls[0].kwargs["message"]


def test_add_result_for_invalid_dependencies(compiled_node: CompiledNode, context: ContractContext):
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasValidUpstreamDependencies._add_result_for_invalid_dependencies(
            item=compiled_node, kind="ref", context=context, missing=["1", "2", "3"]
        )
        mock_add_result.assert_called_once()


def test_has_valid_ref_dependencies(compiled_node: CompiledNode, context: ContractContext):
    assert HasValidRefDependencies().run(compiled_node, context=context)

    upstream_deps = sample([node for node in context.manifest.nodes if node.startswith("model")], k=3)
    assert upstream_deps
    compiled_node.depends_on_nodes.extend(upstream_deps)
    assert HasValidRefDependencies().run(compiled_node, context=context)

    compiled_node.depends_on_nodes.append("model.invalid_ref")
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasValidRefDependencies().run(compiled_node, context=context)
        mock_add_result.assert_called_once()


def test_has_valid_source_dependencies(compiled_node: CompiledNode, context: ContractContext):
    assert HasValidSourceDependencies().run(compiled_node, context=context)

    upstream_deps = sample(list(context.manifest.sources), k=3)
    assert upstream_deps
    compiled_node.depends_on_nodes.extend(upstream_deps)
    assert HasValidSourceDependencies().run(compiled_node, context=context)

    compiled_node.depends_on_nodes.append("source.invalid_ref")
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasValidSourceDependencies().run(compiled_node, context=context)
        mock_add_result.assert_called_once()


def test_has_valid_macro_dependencies(compiled_node: CompiledNode, context: ContractContext):
    assert HasValidMacroDependencies().run(compiled_node, context=context)

    upstream_deps = sample(list(context.manifest.macros), k=3)
    assert upstream_deps
    compiled_node.depends_on_macros.extend(upstream_deps)
    assert HasValidMacroDependencies().run(compiled_node, context=context)

    compiled_node.depends_on_macros.append("macro.invalid_ref")
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasValidMacroDependencies().run(compiled_node, context=context)
        mock_add_result.assert_called_once()


def test_has_no_final_semicolon(compiled_node: CompiledNode, context: ContractContext, faker: Faker):
    compiled_node.path = faker.file_path(extension="sql", absolute=False)
    compiled_node.raw_code = "SELECT * FROM table"
    assert HasNoFinalSemiColon().run(compiled_node, context=context)

    compiled_node.raw_code = "SELECT * FROM table \n;   \n \n "
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasNoFinalSemiColon().run(compiled_node, context=context)
        mock_add_result.assert_called_once()


def test_has_no_hardcoded_refs(compiled_node: CompiledNode, context: ContractContext, faker: Faker):
    compiled_node.path = faker.file_path(extension="sql", absolute=False)
    compiled_node.raw_code = """
        WITH cte1 as (SELECT * FROM VALUES(1,2,3)), cte2 as (SELECT * FROM VALUES(1,2,3))
        SELECT * FROM {{ ref('model_ref')}} ref1
        JOIN cte1 ON cte1.a = ref.a
        JOIN cte2 ON cte2.a = ref.a
    """
    assert HasNoHardcodedRefs().run(compiled_node, context=context)

    compiled_node.raw_code = "SELECT * FROM table"
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasNoHardcodedRefs().run(compiled_node, context=context)
        mock_add_result.assert_called_once()
