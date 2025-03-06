from copy import deepcopy
from unittest import mock

import pytest
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.nodes import CompiledNode
from dbt_common.contracts.metadata import CatalogTable

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.column import ColumnPropertiesGenerator
from tests.contracts.generators.test_core import ChildPropertiesGeneratorTester


class TestColumnPropertiesGenerator(ChildPropertiesGeneratorTester):

    @pytest.fixture
    def generator(self) -> ColumnPropertiesGenerator:
        return ColumnPropertiesGenerator()

    @pytest.fixture
    def item(self, node_column: ColumnInfo) -> ColumnInfo:
        return node_column

    @pytest.fixture
    def parent(self, node: CompiledNode) -> CompiledNode:
        return node

    def test_set_data_type_skips_on_exclude(
            self, generator: ColumnPropertiesGenerator, item: ColumnInfo, parent: CompiledNode
    ) -> None:
        original_data_type = "str"
        item.data_type = original_data_type
        data_type = "int"
        assert item.data_type != data_type

        generator.exclude = ["data_type"]
        generator.overwrite = True

        assert not generator._set_data_type(item, data_type=data_type)
        assert item.data_type == original_data_type

    def test_set_data_type_skips_on_empty_data_type(
            self, generator: ColumnPropertiesGenerator, item: ColumnInfo, parent: CompiledNode
    ) -> None:
        original_data_type = item.data_type

        assert not generator.exclude
        generator.overwrite = True

        assert not generator._set_data_type(item, data_type=None)
        assert not generator._set_data_type(item, data_type="")
        assert item.data_type == original_data_type

    def test_set_data_type_skips_on_not_overwrite(
            self, generator: ColumnPropertiesGenerator, item: ColumnInfo, parent: CompiledNode
    ) -> None:
        original_data_type = "old data_type"
        item.data_type = original_data_type
        data_type = "new data_type"

        assert not generator.exclude
        generator.overwrite = False

        assert not generator._set_data_type(item, data_type=data_type)
        assert item.data_type == original_data_type

    def test_set_data_type_skips_on_matching_data_type(
            self, generator: ColumnPropertiesGenerator, item: ColumnInfo, parent: CompiledNode
    ) -> None:
        original_data_type = "int"
        item.data_type = original_data_type

        assert not generator.exclude
        generator.overwrite = True

        assert not generator._set_data_type(item, data_type=original_data_type)
        assert item.data_type == original_data_type

    def test_set_data_type(
            self, generator: ColumnPropertiesGenerator, item: ColumnInfo, parent: CompiledNode
    ) -> None:
        original_data_type = "int"
        item.data_type = original_data_type
        data_type = "timestamp"

        assert not generator.exclude
        generator.overwrite = True

        assert generator._set_data_type(item, data_type=data_type)
        assert item.data_type == data_type

    def test_merge_skips_on_no_table_in_database(
            self,
            generator: ColumnPropertiesGenerator,
            item: ColumnInfo,
            parent: CompiledNode,
            context: ContractContext,
    ):
        with (
            mock.patch("dbt_contracts.contracts.generators.column.get_matching_catalog_table", return_value=None),
            mock.patch.object(generator.__class__, "_set_description") as mock_description,
            mock.patch.object(generator.__class__, "_set_data_type") as mock_data_type,
        ):
            assert not generator.merge(item, context=context, parent=parent)

            mock_description.assert_not_called()
            mock_data_type.assert_not_called()

    def test_merge_skips_on_no_column_in_database(
            self,
            generator: ColumnPropertiesGenerator,
            item: ColumnInfo,
            parent: CompiledNode,
            context: ContractContext,
            node_table: CatalogTable,
    ):
        table = deepcopy(node_table)
        table.columns.pop(item.name)

        with (
            mock.patch("dbt_contracts.contracts.generators.column.get_matching_catalog_table", return_value=table),
            mock.patch.object(generator.__class__, "_set_description") as mock_description,
            mock.patch.object(generator.__class__, "_set_data_type") as mock_data_type,
        ):
            assert not generator.merge(item, context=context, parent=parent)

            mock_description.assert_not_called()
            mock_data_type.assert_not_called()

    def test_merge(
            self,
            generator: ColumnPropertiesGenerator,
            item: ColumnInfo,
            parent: CompiledNode,
            context: ContractContext,
            node_table: CatalogTable,
    ):
        with (
            mock.patch("dbt_contracts.contracts.generators.column.get_matching_catalog_table", return_value=node_table),
            mock.patch.object(generator.__class__, "_set_description", return_value=False) as mock_description,
            mock.patch.object(generator.__class__, "_set_data_type", return_value=True) as mock_data_type,
        ):
            assert generator.merge(item, context=context, parent=parent)

            mock_description.assert_called_once_with(item, description=node_table.columns[item.name].comment)
            mock_data_type.assert_called_once_with(item, data_type=node_table.columns[item.name].type)
