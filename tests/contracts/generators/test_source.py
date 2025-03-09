from random import sample

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

    def test_generate_source_properties(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        table = generator._generate_source_properties(item)
        assert all(val for val in table.values())

    def test_generate_table_properties(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        table = generator._generate_table_properties(item)
        assert all(val for val in table.values())

    def test_generate_properties(self, generator: SourcePropertiesGenerator, item: SourceDefinition):
        properties = generator._generate_properties(item)
        assert item.resource_type.pluralize() in properties

        columns = list(map(generator._generate_column_properties, item.columns.values()))
        table = generator._generate_table_properties(item) | {"columns": columns}
        source = generator._generate_source_properties(item) | {"tables": [table]}

        assert source in properties[item.resource_type.pluralize()]
        for key, val in generator._properties_defaults.items():
            assert properties[key] == val

    def test_update_existing_properties_with_empty_properties(
            self, generator: SourcePropertiesGenerator, item: SourceDefinition, context: ContractContext
    ):
        key = item.resource_type.pluralize()
        properties = {}
        expected_source = generator._generate_properties(item)[key][0]

        assert generator._update_existing_properties(item, properties=properties) is properties
        assert len(properties[key]) == 1
        assert expected_source in properties[key]

    # noinspection PyTestUnpassedFixture
    def test_update_existing_properties_with_new_source(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        properties = {key: [sources[key][0] for sources in map(generator._generate_properties, sources)]}
        assert not any(source["name"] == item.source_name for source in properties[key])

        original_sources_count = len(properties[key])
        expected_source = generator._generate_properties(item)[key][0]

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_sources_count + 1
        assert expected_source in properties[key]

    # noinspection PyTestUnpassedFixture
    def test_update_existing_properties_with_new_table(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        properties = {key: [sources[key][0] for sources in map(generator._generate_properties, sources)]}
        properties[key].append(generator._generate_properties(item)[key][0])
        assert sum(source["name"] == item.source_name for source in properties[key]) == 1

        original_sources_count = len(properties[key])
        expected_columns = list(map(generator._generate_column_properties, item.columns.values()))
        expected_table = generator._generate_table_properties(item) | {"columns": expected_columns}

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_sources_count

        actual_sources = [source for source in properties[key] if source["name"] == item.source_name]
        assert len(actual_sources) == 1
        assert expected_table in actual_sources[0]["tables"]

    # noinspection PyTestUnpassedFixture
    def test_update_existing_properties_with_existing_table(
            self,
            generator: SourcePropertiesGenerator,
            item: SourceDefinition,
            sources: list[SourceDefinition],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        sources = sample([source for source in sources if source.name != item.name], k=5)
        source = generator._generate_properties(item)[key][0]
        properties = {key: [sources[key][0] for sources in map(generator._generate_properties, sources)] + [source]}
        assert sum(source["name"] == item.source_name for source in properties[key]) == 1
        assert len(source["tables"]) == 1

        # should update the description in the properties
        original_sources_count = len(properties[key])
        item.description = "a brand new description"

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_sources_count

        actual_sources = [source for source in properties[key] if source["name"] == item.source_name]
        assert len(actual_sources) == 1

        actual_tables = [prop for prop in actual_sources[0]["tables"] if prop["name"] == item.name]
        assert len(actual_tables) == 1
        assert actual_tables[0]["description"] == item.description
