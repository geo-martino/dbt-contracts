from copy import deepcopy
from unittest import mock

import pytest
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.nodes import CompiledNode
from dbt_common.contracts.metadata import CatalogTable, ColumnMetadata
from faker import Faker

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.column import ColumnPropertyGenerator, ColumnPropertiesGenerator, \
    SetColumnDescription, SetDataType
from tests.contracts.generators.test_core import ChildPropertiesGeneratorTester
from tests.contracts.generators.test_properties import SetDescriptionTester


class TestSetColumnDescription(SetDescriptionTester[ColumnInfo, ColumnMetadata]):
    @pytest.fixture
    def generator(self) -> SetColumnDescription:
        return SetColumnDescription()

    @pytest.fixture
    def item(self, node_column: ColumnInfo) -> ColumnInfo:
        return node_column

    def test_run(self, generator: SetDataType, item: ColumnInfo, catalog_column: ColumnMetadata, faker: Faker):
        item.description = faker.sentence()
        catalog_column.comment = faker.sentence()

        generator.overwrite = True

        assert generator.run(item, catalog_column)
        assert item.description == catalog_column.comment


class TestSetDataType:

    @pytest.fixture
    def generator(self) -> SetDataType:
        """Fixture for the property generator to test."""
        return SetDataType()

    def test_skips_on_empty_data_type(self, generator: SetDataType, node_column: ColumnInfo):
        original_data_type = node_column.data_type

        generator.overwrite = True

        assert not generator._set_data_type(node_column, data_type=None)
        assert not generator._set_data_type(node_column, data_type="")
        assert node_column.data_type == original_data_type

    def test_skips_on_not_overwrite(self, generator: SetDataType, node_column: ColumnInfo):
        original_data_type = "old data_type"
        node_column.data_type = original_data_type
        data_type = "new data_type"

        generator.overwrite = False

        assert not generator._set_data_type(node_column, data_type=data_type)
        assert node_column.data_type == original_data_type

    def test_skips_on_matching_data_type(self, generator: SetDataType, node_column: ColumnInfo):
        original_data_type = "int"
        node_column.data_type = original_data_type

        generator.overwrite = True

        assert not generator._set_data_type(node_column, data_type=original_data_type)
        assert node_column.data_type == original_data_type

    def test_valid_set(self, generator: SetDataType, node_column: ColumnInfo):
        original_data_type = "int"
        node_column.data_type = original_data_type
        data_type = "timestamp"

        generator.overwrite = True

        assert generator._set_data_type(node_column, data_type=data_type)
        assert node_column.data_type == data_type

    def test_run(self, generator: SetDataType, node_column: ColumnInfo, catalog_column: ColumnMetadata):
        node_column.data_type = "int"
        catalog_column.type = "timestamp"

        generator.overwrite = True

        assert generator.run(node_column, catalog_column)
        assert node_column.data_type == catalog_column.type


class TestColumnPropertiesGenerator(
    ChildPropertiesGeneratorTester[ColumnInfo, ColumnMetadata, ColumnPropertyGenerator]
):

    @pytest.fixture
    def generator(self) -> ColumnPropertiesGenerator:
        return ColumnPropertiesGenerator()

    @pytest.fixture
    def item(self, node_column: ColumnInfo) -> ColumnInfo:
        return node_column

    @pytest.fixture
    def parent(self, node: CompiledNode) -> CompiledNode:
        return node

    def test_merge_skips_on_no_table_in_database(
            self,
            generator: ColumnPropertiesGenerator,
            item: ColumnInfo,
            parent: CompiledNode,
            context: ContractContext,
    ):
        with (
            mock.patch("dbt_contracts.contracts.generators.column.get_matching_catalog_table", return_value=None),
            mock.patch.object(
                generator.__class__, "generators", new_callable=mock.PropertyMock, return_value=[]
            ) as mock_generators,
        ):
            assert not generator.merge(item, context=context, parent=parent)
            mock_generators.assert_not_called()

    def test_merge_skips_on_no_column_in_database(
            self,
            generator: ColumnPropertiesGenerator,
            item: ColumnInfo,
            parent: CompiledNode,
            context: ContractContext,
            catalog_table: CatalogTable,
    ):
        table = deepcopy(catalog_table)
        table.columns.pop(item.name)

        with (
            mock.patch("dbt_contracts.contracts.generators.column.get_matching_catalog_table", return_value=table),
            mock.patch.object(
                generator.__class__, "generators", new_callable=mock.PropertyMock, return_value=[]
            ) as mock_generators,
        ):
            assert not generator.merge(item, context=context, parent=parent)
            mock_generators.assert_not_called()

    def test_merge(
            self,
            generator: ColumnPropertiesGenerator,
            item: ColumnInfo,
            parent: CompiledNode,
            context: ContractContext,
            catalog_table: CatalogTable,
    ):
        with (
            mock.patch(
                "dbt_contracts.contracts.generators.column.get_matching_catalog_table",
                return_value=catalog_table
            ),
            mock.patch.object(
                generator.__class__, "generators", new_callable=mock.PropertyMock, return_value=[]
            ) as mock_generators,
            mock.patch.object(generator.description.__class__, "run", return_value=True) as mock_description,
            mock.patch.object(generator.data_type.__class__, "run", return_value=False) as mock_data_type,
        ):
            assert not generator.merge(item, context=context, parent=parent)

            mock_generators.return_value = [generator.description, generator.data_type]
            assert generator.merge(item, context=context, parent=parent)

            mock_description.assert_called_once_with(item, catalog_table.columns[item.name])
            mock_data_type.assert_called_once_with(item, catalog_table.columns[item.name])
