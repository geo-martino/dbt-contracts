from abc import ABCMeta, abstractmethod
from copy import deepcopy
from random import choice, sample, shuffle
from unittest import mock

import pytest
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.nodes import CompiledNode
from dbt_common.contracts.metadata import CatalogTable, ColumnMetadata
from faker import Faker

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.column import ColumnPropertiesGenerator
from dbt_contracts.contracts.generators.node import NodePropertyGenerator, SetNodeDescription, SetNodeColumns, \
    NodePropertiesGenerator
from dbt_contracts.types import NodeT
from tests.contracts.generators.test_core import ParentPropertiesGeneratorTester
from tests.contracts.generators.test_properties import SetDescriptionTester


class TestSetNodeDescription(SetDescriptionTester[ColumnInfo, ColumnMetadata]):
    @pytest.fixture
    def generator(self) -> SetNodeDescription:
        return SetNodeDescription()

    @pytest.fixture
    def item(self, node: CompiledNode) -> CompiledNode:
        return node

    def test_run(
            self, generator: SetNodeDescription, item: CompiledNode, catalog_table: CatalogTable, faker: Faker
    ):
        item.description = faker.sentence()
        catalog_table.metadata.comment = faker.sentence()

        generator.overwrite = True

        assert generator.run(item, catalog_table)
        assert item.description == catalog_table.metadata.comment


class TestSetColumns:

    @pytest.fixture
    def generator(self) -> SetNodeColumns:
        """Fixture for the property generator to test."""
        return SetNodeColumns()

    def test_skips_on_empty_columns(self, generator: SetNodeColumns, node: CompiledNode, catalog_table: CatalogTable):
        generator.add = True
        generator.remove = True
        generator.order = True

        catalog_table = deepcopy(catalog_table)
        catalog_table.columns.clear()

        with (
                mock.patch.object(generator.__class__, "_set_column") as mock_set,
                mock.patch.object(generator.__class__, "_drop_column") as mock_drop,
                mock.patch.object(generator.__class__, "_order_columns") as mock_order,
        ):
            assert not generator.run(node, catalog_table)

            mock_set.assert_not_called()
            mock_drop.assert_not_called()
            mock_order.assert_not_called()

    def test_skips_add_when_disabled(self, generator: SetNodeColumns, node: CompiledNode, catalog_table: CatalogTable):
        assert catalog_table.columns

        with mock.patch.object(generator.__class__, "_set_column") as mock_set:
            generator.add = False
            generator.run(node, catalog_table)
            mock_set.assert_not_called()

            generator.add = True
            generator.run(node, catalog_table)
            mock_set.assert_called()

    def test_skips_remove_when_disabled(
            self, generator: SetNodeColumns, node: CompiledNode, catalog_table: CatalogTable
    ):
        assert catalog_table.columns

        with mock.patch.object(generator.__class__, "_drop_column") as mock_drop:
            generator.remove = False
            generator.run(node, catalog_table)
            mock_drop.assert_not_called()

            generator.remove = True
            generator.run(node, catalog_table)
            mock_drop.assert_called()

    def test_skips_order_when_disabled(
            self, generator: SetNodeColumns, node: CompiledNode, catalog_table: CatalogTable
    ):
        assert catalog_table.columns

        with mock.patch.object(generator.__class__, "_order_columns") as mock_order:
            generator.order = False
            generator.run(node, catalog_table)
            mock_order.assert_not_called()

            generator.order = True
            generator.run(node, catalog_table)
            mock_order.assert_called()

    def test_set_column_skips_on_matched_column(self, generator: SetNodeColumns, node: CompiledNode, faker: Faker):
        node_column = choice(list(node.columns.values()))
        column = ColumnMetadata(name=node_column.name, index=faker.random_int(), type=node_column.data_type)
        assert column.name in node.columns

        assert not generator._set_column(node, column=column)

    def test_set_column(self, generator: SetNodeColumns, node: CompiledNode, faker: Faker):
        column = ColumnMetadata(name=faker.word(), index=faker.random_int(), type="int")
        assert column.name not in node.columns

        assert generator._set_column(node, column=column)
        assert column.name in node.columns
        assert node.columns[column.name].name == column.name

    def test_drop_column_skips_on_matched_column(self, generator: SetNodeColumns, node: CompiledNode):
        columns = {
            col.name: ColumnMetadata(name=col.name, index=i, type="int")
            for i, col in enumerate(node.columns.values())
        }
        column = choice(list(node.columns.values()))
        assert column.name in columns
        assert column.name in node.columns

        assert not generator._drop_column(node, column=column, columns=columns)
        assert column.name in node.columns

    def test_drop_column(self, generator: SetNodeColumns, node: CompiledNode, faker: Faker):
        columns = {name: ColumnMetadata(name=name, index=i, type="int") for i, name in enumerate(faker.words())}
        column = choice(list(node.columns.values()))
        assert column.name not in columns
        assert column.name in node.columns

        assert generator._drop_column(node, column=column, columns=columns)
        assert column.name not in node.columns

    def test_order_columns_skips_on_empty_columns(self, generator: SetNodeColumns, node: CompiledNode):
        original_order = list(node.columns)

        assert not generator._order_columns(node, columns={})
        assert list(node.columns) == original_order

    def test_order_columns_skips_when_columns_already_in_order(
            self, generator: SetNodeColumns, node: CompiledNode, faker: Faker
    ):
        columns = {
            col.name: ColumnMetadata(name=col.name, index=faker.random_int(), type="int")
            for col in node.columns.values()
        }
        index_map = {col.name: col.index for col in columns.values()}
        node.columns = dict(
            sorted(node.columns.items(), key=lambda col: index_map.get(col[1].name, len(index_map)))
        )
        original_order = list(node.columns)

        assert not generator._order_columns(node, columns=columns)
        assert list(node.columns) == original_order

    # TODO: flakey test - fix me
    def test_order_columns(self, generator: SetNodeColumns, node: CompiledNode, faker: Faker):
        node.columns |= {name: ColumnInfo(name=name) for name in faker.words()}
        original_order = list(node.columns)
        columns = {
            col.name: ColumnMetadata(name=col.name, index=faker.random_int(max=len(node.columns)), type="int")
            for col in sample(list(node.columns.values()), k=3)
        }

        assert list(columns) != original_order

        assert generator._order_columns(node, columns=columns)
        assert list(node.columns) != original_order


class NodePropertiesGeneratorTester[I: NodeT](
    ParentPropertiesGeneratorTester[I, NodePropertyGenerator], metaclass=ABCMeta
):
    @abstractmethod
    def generator(self) -> NodePropertiesGenerator[I]:
        raise NotImplementedError

    @staticmethod
    def test_generate_column_properties(generator: ColumnPropertiesGenerator, item: I):
        column = choice(list(item.columns.values()))
        table = generator._generate_column_properties(column)
        assert all(val for val in table.values())

    # TODO: flakey test - fix me
    @staticmethod
    def test_merge_columns_merges_and_sorts(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        table = {"columns": list(map(generator._generate_column_properties, item.columns.values()))}
        modified_columns = {col["name"]: col for col in sample(table["columns"], k=3)}
        for column in modified_columns.values():
            column["description"] = faker.sentence()
            column["new_property"] = faker.random_int()

        while table["columns"] == list(modified_columns.values()):
            shuffle(table["columns"])

        expected_columns = list(map(generator._generate_column_properties, item.columns.values()))
        for column in expected_columns:
            if not (modified_column := modified_columns.get(column["name"])):
                continue
            column["new_property"] = modified_column["new_property"]

        generator._merge_columns(item, table)
        assert table["columns"] == expected_columns
        assert table["columns"] != list(modified_columns.values())  # sorted back to expected order after shuffle

    @staticmethod
    def test_merge_columns_drops(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        table = {"columns": list(map(generator._generate_column_properties, item.columns.values()))}
        added_columns = [
            generator._generate_column_properties(ColumnInfo(name=faker.word())) for _ in range(3)
        ]
        table["columns"].extend(added_columns)

        expected_columns = list(map(generator._generate_column_properties, item.columns.values()))

        generator._merge_columns(item, table)
        assert table["columns"] == expected_columns

    @staticmethod
    def test_merge_skips_on_no_table_in_database(
            generator: NodePropertiesGenerator[I], item: I, context: ContractContext
    ):
        with (
            mock.patch("dbt_contracts.contracts.generators.node.get_matching_catalog_table", return_value=None),
            mock.patch.object(
                generator.__class__, "generators", new_callable=mock.PropertyMock, return_value=[]
            ) as mock_generators,
        ):
            assert not generator.merge(item, context=context)
            mock_generators.assert_not_called()

    @staticmethod
    def test_merge(
            generator: NodePropertiesGenerator[I], item: I, context: ContractContext, catalog_table: CatalogTable
    ):
        with (
            mock.patch(
                "dbt_contracts.contracts.generators.node.get_matching_catalog_table", return_value=catalog_table
            ),
            mock.patch.object(
                generator.__class__, "generators", new_callable=mock.PropertyMock, return_value=[]
            ) as mock_generators,
            mock.patch.object(generator.description.__class__, "run", return_value=True) as mock_description,
            mock.patch.object(generator.columns.__class__, "run", return_value=False) as mock_columns,
        ):
            assert not generator.merge(item, context=context)

            mock_generators.return_value = [generator.description, generator.columns]
            assert generator.merge(item, context=context)

            mock_description.assert_called_once_with(item, catalog_table)
            mock_columns.assert_called_once_with(item, catalog_table)
