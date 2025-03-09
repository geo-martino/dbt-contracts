from random import sample

import pytest
from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.model import ModelPropertiesGenerator
from tests.contracts.generators.test_node import NodePropertiesGeneratorTester


class TestModelPropertiesGenerator(NodePropertiesGeneratorTester[ModelNode]):
    @pytest.fixture
    def generator(self) -> ModelPropertiesGenerator:
        return ModelPropertiesGenerator()

    @pytest.fixture
    def item(self, model: ModelNode) -> ModelNode:
        return model

    def test_generate_table_properties(self, generator: ModelPropertiesGenerator, item: ModelNode):
        table = generator._generate_table_properties(item)
        assert all(val for val in table.values())

    def test_generate_properties(self, generator: ModelPropertiesGenerator, item: ModelNode):
        properties = generator._generate_properties(item)
        assert item.resource_type.pluralize() in properties

        columns = list(map(generator._generate_column_properties, item.columns.values()))
        table = generator._generate_table_properties(item) | {"columns": columns}

        assert table in properties[item.resource_type.pluralize()]
        for key, val in generator._properties_defaults.items():
            assert properties[key] == val

    def test_update_existing_properties_with_empty_properties(
            self, generator: ModelPropertiesGenerator, item: ModelNode, context: ContractContext
    ):
        key = item.resource_type.pluralize()
        properties = {}
        expected_table = generator._generate_properties(item)[key][0]

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == 1
        assert expected_table in properties[key]

    def test_update_existing_properties_with_new_table(
            self,
            generator: ModelPropertiesGenerator,
            item: ModelNode,
            models: list[ModelNode],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        models = sample([model for model in models if model.name != item.name], k=5)
        properties = {key: [models[key][0] for models in map(generator._generate_properties, models)]}
        original_count = len(properties[key])

        expected_columns = list(map(generator._generate_column_properties, item.columns.values()))
        expected_table = generator._generate_table_properties(item) | {"columns": expected_columns}

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_count + 1
        assert expected_table in properties[key]

    def test_update_existing_properties_with_existing_table(
            self,
            generator: ModelPropertiesGenerator,
            item: ModelNode,
            models: list[ModelNode],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        models = sample([model for model in models if model.name != item.name], k=5)
        table = generator._generate_table_properties(item)
        properties = {key: [models[key][0] for models in map(generator._generate_properties, models)] + [table]}
        original_count = len(properties[key])

        # should update the description in the properties
        item.description = "a brand new description"

        generator._update_existing_properties(item, properties=properties)
        assert len(properties[key]) == original_count

        actual_tables = [prop for prop in properties[key] if prop["name"] == item.name]
        assert len(actual_tables) == 1
        assert actual_tables[0]["description"] == item.description
