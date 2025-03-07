from random import sample
from unittest import mock

import pytest
from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.source import SourcePropertiesGenerator
from dbt_contracts.properties import PropertiesIO
from tests.contracts.generators.test_node import NodePropertiesGeneratorTester


class TestSourcePropertiesGenerator(NodePropertiesGeneratorTester[SourceDefinition]):
    @pytest.fixture
    def generator(self) -> SourcePropertiesGenerator:
        return SourcePropertiesGenerator()

    @pytest.fixture
    def item(self, source: SourceDefinition) -> SourceDefinition:
        return source

    def test_generate_source_properties(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        table = generator._generate_source_properties(item)
        assert all(val for val in table.values())

    def test_generate_table_properties(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        table = generator._generate_table_properties(item)
        assert all(val for val in table.values())

    def test_generate_new_properties(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        properties = generator._generate_new_properties(item)
        assert item.resource_type.pluralize() in properties

        source = generator._generate_full_properties(item)
        assert source in properties[item.resource_type.pluralize()]
        for key, val in generator._properties_defaults.items():
            assert properties[key] == val

    def test_update_existing_properties_with_empty_properties(
            self, generator: SourcePropertiesGenerator, item: SourceDefinition, context: ContractContext
    ):
        key = item.resource_type.pluralize()
        properties = {}
        expected_source = generator._generate_full_properties(item)

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == 1
        assert expected_source in properties[key]

    def test_update_existing_properties_with_new_source(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        properties = {key: list(map(generator._generate_full_properties, sources))}
        assert not any(source["name"] == item.source_name for source in properties[key])

        original_sources_count = len(properties[key])
        expected_source = generator._generate_full_properties(item)

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_sources_count + 1
        assert expected_source in properties[key]

    def test_update_existing_properties_with_new_table(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        properties = {key: list(map(generator._generate_full_properties, sources))}
        properties[key].append(generator._generate_source_properties(item))
        assert sum(source["name"] == item.source_name for source in properties[key]) == 1

        original_sources_count = len(properties[key])
        expected_table = generator._generate_table_properties(item)

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_sources_count

        actual_sources = [source for source in properties[key] if source["name"] == item.source_name]
        assert len(actual_sources) == 1
        assert expected_table in actual_sources[0]["tables"]

    def test_update_existing_properties_with_existing_table(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        source = generator._generate_full_properties(item)
        properties = {key: list(map(generator._generate_full_properties, sources)) + [source]}
        assert sum(source["name"] == item.source_name for source in properties[key]) == 1
        assert len(source["tables"]) == 1

        # should update the description in the properties
        original_sources_count = len(properties[key])
        item.description = "a brand new description"
        expected_table = generator._generate_table_properties(item)

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_sources_count

        actual_sources = [source for source in properties[key] if source["name"] == item.source_name]
        assert len(actual_sources) == 1

        actual_tables = [prop for prop in actual_sources[0]["tables"] if prop["name"] == item.name]
        assert len(actual_tables) == 1
        assert actual_tables[0]["description"] == item.description
        assert expected_table == actual_tables[0]
