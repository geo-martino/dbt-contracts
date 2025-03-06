from random import sample
from unittest import mock

import pytest
from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.source import SourcePropertiesGenerator
from tests.contracts.generators.test_node import NodePropertiesGeneratorTester


class TestSourcePropertiesGenerator(NodePropertiesGeneratorTester[SourceDefinition]):
    @pytest.fixture
    def generator(self) -> SourcePropertiesGenerator:
        return SourcePropertiesGenerator()

    @pytest.fixture
    def item(self, source: SourceDefinition) -> SourceDefinition:
        return source

    def test_generate_source_patch(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        table = generator._generate_source_patch(item)
        assert all(val for val in table.values())

    def test_generate_table_patch(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        table = generator._generate_table_patch(item)
        assert all(val for val in table.values())

    def test_generate_new_patch(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        patch = generator._generate_new_patch(item)
        assert item.resource_type.pluralize() in patch

        source = generator._generate_full_patch(item)
        assert source in patch[item.resource_type.pluralize()]
        for key, val in generator._patch_defaults.items():
            assert patch[key] == val

    def test_update_existing_patch_with_empty_patch(
            self, generator: SourcePropertiesGenerator, item: SourceDefinition, context: ContractContext
    ):
        key = item.resource_type.pluralize()
        patch = {}
        expected_source = generator._generate_full_patch(item)

        with mock.patch.object(ContractContext, "get_patch_file", return_value=patch):
            generator._update_existing_patch(item, context)
            assert len(patch[key]) == 1
            assert expected_source in patch[key]

    def test_update_existing_patch_with_new_source(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        patch = {key: list(map(generator._generate_full_patch, sources))}
        assert not any(source["name"] == item.source_name for source in patch[key])

        original_sources_count = len(patch[key])
        expected_source = generator._generate_full_patch(item)

        with mock.patch.object(ContractContext, "get_patch_file", return_value=patch):
            generator._update_existing_patch(item, context)
            assert len(patch[key]) == original_sources_count + 1
            assert expected_source in patch[key]

    def test_update_existing_patch_with_new_table(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        patch = {key: list(map(generator._generate_full_patch, sources))}
        patch[key].append(generator._generate_source_patch(item))
        assert sum(source["name"] == item.source_name for source in patch[key]) == 1

        original_sources_count = len(patch[key])
        expected_table = generator._generate_table_patch(item)

        with mock.patch.object(ContractContext, "get_patch_file", return_value=patch):
            generator._update_existing_patch(item, context)
            assert len(patch[key]) == original_sources_count

            actual_sources = [source for source in patch[key] if source["name"] == item.source_name]
            assert len(actual_sources) == 1
            assert expected_table in actual_sources[0]["tables"]

    def test_update_existing_patch_with_existing_table(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        source = generator._generate_full_patch(item)
        patch = {key: list(map(generator._generate_full_patch, sources)) + [source]}
        assert sum(source["name"] == item.source_name for source in patch[key]) == 1
        assert len(source["tables"]) == 1

        # should update the description in the patch
        original_sources_count = len(patch[key])
        item.description = "a brand new description"
        expected_table = generator._generate_table_patch(item)

        with mock.patch.object(ContractContext, "get_patch_file", return_value=patch):
            generator._update_existing_patch(item, context)
            assert len(patch[key]) == original_sources_count

            actual_sources = [source for source in patch[key] if source["name"] == item.source_name]
            assert len(actual_sources) == 1

            actual_tables = [prop for prop in actual_sources[0]["tables"] if prop["name"] == item.name]
            assert len(actual_tables) == 1
            assert actual_tables[0]["description"] == item.description
            assert expected_table == actual_tables[0]
