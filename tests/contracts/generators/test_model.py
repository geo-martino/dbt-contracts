from random import sample
from unittest import mock

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

    def test_generate_table_patch(self, generator: ModelPropertiesGenerator, item: ModelNode):
        table = generator._generate_table_patch(item)
        assert all(val for val in table.values())

    def test_generate_new_patch(self, generator: ModelPropertiesGenerator, item: ModelNode):
        patch = generator._generate_new_patch(item)
        assert item.resource_type.pluralize() in patch

        table = generator._generate_table_patch(item)
        assert table in patch[item.resource_type.pluralize()]
        for key, val in generator._patch_defaults.items():
            assert patch[key] == val

    def test_update_existing_patch_with_empty_patch(
            self, generator: ModelPropertiesGenerator, item: ModelNode, context: ContractContext
    ):
        key = item.resource_type.pluralize()
        patch = {}
        expected_table = generator._generate_table_patch(item)

        with mock.patch.object(ContractContext, "get_patch_file", return_value=patch):
            generator._update_existing_patch(item, context)
            assert len(patch[key]) == 1
            assert expected_table in patch[key]

    def test_update_existing_patch_with_new_table(
            self,
            generator: ModelPropertiesGenerator,
            item: ModelNode,
            models: list[ModelNode],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        models = sample([model for model in models if model.name != item.name], k=5)
        patch = {key: list(map(generator._generate_table_patch, models))}
        original_count = len(patch[key])
        expected_table = generator._generate_table_patch(item)

        with mock.patch.object(ContractContext, "get_patch_file", return_value=patch):
            generator._update_existing_patch(item, context)
            assert len(patch[key]) == original_count + 1
            assert expected_table in patch[key]

    def test_update_existing_patch_with_existing_table(
            self,
            generator: ModelPropertiesGenerator,
            item: ModelNode,
            models: list[ModelNode],
            context: ContractContext
    ):
        key = item.resource_type.pluralize()
        models = sample([model for model in models if model.name != item.name], k=5)
        table = generator._generate_table_patch(item)
        patch = {key: list(map(generator._generate_table_patch, models)) + [table]}
        original_count = len(patch[key])

        # should update the description in the patch
        item.description = "a brand new description"
        expected_table = generator._generate_table_patch(item)

        with mock.patch.object(ContractContext, "get_patch_file", return_value=patch):
            generator._update_existing_patch(item, context)
            assert len(patch[key]) == original_count
            assert expected_table in patch[key]

            actual_tables = [prop for prop in patch[key] if prop["name"] == item.name]
            assert len(actual_tables) == 1
            assert actual_tables[0]["description"] == item.description
