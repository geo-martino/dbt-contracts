from abc import ABCMeta, abstractmethod
from random import choice, sample, shuffle
from unittest import mock

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt_common.contracts.metadata import CatalogTable, ColumnMetadata
from faker import Faker

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.node import NodePropertiesGenerator
from dbt_contracts.types import NodeT
from tests.contracts.generators.test_core import ParentPropertiesGeneratorTester


class NodePropertiesGeneratorTester[I: NodeT](ParentPropertiesGeneratorTester, metaclass=ABCMeta):
    @abstractmethod
    def generator(self) -> NodePropertiesGenerator[I]:
        raise NotImplementedError

    @staticmethod
    def test_generate_column_properties(generator: NodePropertiesGenerator[I], item: I):
        column = choice(list(item.columns.values()))
        table = generator._generate_column_properties(column)
        assert all(val for val in table.values())

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
    def test_set_columns_skips_on_exclude(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        columns = {name: ColumnMetadata(name=name, index=i, type="int") for i, name in enumerate(faker.words())}

        generator.exclude = ["columns"]
        generator.overwrite = True

        with (
                mock.patch.object(generator.__class__, "_set_column") as mock_set,
                mock.patch.object(generator.__class__, "_drop_column") as mock_drop,
        ):
            assert not generator._set_columns(item, columns=columns)
            mock_set.assert_not_called()
            mock_drop.assert_not_called()

    @staticmethod
    def test_set_columns_skips_on_empty_columns(generator: NodePropertiesGenerator[I], item: I):
        assert not generator.exclude
        generator.overwrite = True

        with (
                mock.patch.object(generator.__class__, "_set_column") as mock_set,
                mock.patch.object(generator.__class__, "_drop_column") as mock_drop,
        ):
            assert not generator._set_columns(item, columns={})
            mock_set.assert_not_called()
            mock_drop.assert_not_called()

    @staticmethod
    def test_set_columns(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        columns = {name: ColumnMetadata(name=name, index=i, type="int") for i, name in enumerate(faker.words())}

        assert not generator.exclude
        generator.overwrite = True

        with (
                mock.patch.object(generator.__class__, "_set_column", return_value=False) as mock_set,
                mock.patch.object(generator.__class__, "_drop_column", return_value=True) as mock_drop,
        ):
            assert generator._set_columns(item, columns=columns)
            assert len(mock_set.mock_calls) == len(columns)
            assert len(mock_drop.mock_calls) == len(item.columns)

    @staticmethod
    def test_set_column_skips_on_matched_column(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        item_column = choice(list(item.columns.values()))
        column = ColumnMetadata(name=item_column.name, index=faker.random_int(), type=item_column.data_type)
        assert column.name in item.columns

        assert not generator._set_column(item, column=column)

    @staticmethod
    def test_set_column(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        column = ColumnMetadata(name=faker.word(), index=faker.random_int(), type="int")
        assert column.name not in item.columns

        assert generator._set_column(item, column=column)
        assert column.name in item.columns
        assert item.columns[column.name].name == column.name

    @staticmethod
    def test_drop_column_skips_on_exclude(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        columns = {name: ColumnMetadata(name=name, index=i, type="int") for i, name in enumerate(faker.words())}
        column = choice(list(item.columns.values()))
        assert column.name not in columns
        assert column.name in item.columns

        generator.remove_columns = False

        assert not generator._drop_column(item, column=column, columns=columns)
        assert column.name in item.columns

    @staticmethod
    def test_drop_column_skips_on_matched_column(generator: NodePropertiesGenerator[I], item: I):
        columns = {
            col.name: ColumnMetadata(name=col.name, index=i, type="int")
            for i, col in enumerate(item.columns.values())
        }
        column = choice(list(item.columns.values()))
        assert column.name in columns
        assert column.name in item.columns

        generator.remove_columns = True

        assert not generator._drop_column(item, column=column, columns=columns)
        assert column.name in item.columns

    @staticmethod
    def test_drop_column(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        columns = {name: ColumnMetadata(name=name, index=i, type="int") for i, name in enumerate(faker.words())}
        column = choice(list(item.columns.values()))
        assert column.name not in columns
        assert column.name in item.columns

        generator.remove_columns = True

        assert generator._drop_column(item, column=column, columns=columns)
        assert column.name not in item.columns

    @staticmethod
    def test_reorder_columns_skips_on_exclude(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        original_order = list(item.columns)
        columns = {
            col.name: ColumnMetadata(name=col.name, index=faker.random_int(), type="int")
            for col in sample(list(item.columns.values()), k=3)
        }

        generator.exclude = ["columns"]
        generator.overwrite = True
        generator.order_columns = True

        assert not generator._reorder_columns(item, columns=columns)
        assert list(item.columns) == original_order

    @staticmethod
    def test_reorder_columns_skips_on_empty_columns(generator: NodePropertiesGenerator[I], item: I):
        original_order = list(item.columns)

        assert not generator.exclude
        generator.overwrite = True
        generator.order_columns = True

        assert not generator._reorder_columns(item, columns={})
        assert list(item.columns) == original_order

    @staticmethod
    def test_reorder_columns_skips_when_columns_already_in_order(
            generator: NodePropertiesGenerator[I], item: I, faker: Faker
    ):
        original_order = list(item.columns)
        columns = {
            col.name: ColumnMetadata(name=col.name, index=faker.random_int(), type="int")
            for col in sample(list(item.columns.values()), k=3)
        }

        generator.exclude = ["columns"]
        generator.overwrite = True
        generator.order_columns = True

        assert not generator._reorder_columns(item, columns=columns)
        assert list(item.columns) == original_order

    @staticmethod
    def test_reorder_columns(generator: NodePropertiesGenerator[I], item: I, faker: Faker):
        item.columns |= {name: ColumnInfo(name=name) for name in faker.words()}
        original_order = list(item.columns)
        columns = {
            col.name: ColumnMetadata(name=col.name, index=faker.random_int(max=len(item.columns)), type="int")
            for col in sample(list(item.columns.values()), k=3)
        }

        assert list(columns) != original_order

        assert not generator.exclude
        generator.overwrite = True
        generator.order_columns = True

        assert generator._reorder_columns(item, columns=columns)
        assert list(item.columns) != original_order

    @staticmethod
    def test_merge_skips_on_no_table_in_database(
            generator: NodePropertiesGenerator[I], item: I, context: ContractContext
    ):
        with (
            mock.patch("dbt_contracts.contracts.generators.node.get_matching_catalog_table", return_value=None),
            mock.patch.object(generator.__class__, "_set_description") as mock_description,
            mock.patch.object(generator.__class__, "_set_columns") as mock_set_columns,
            mock.patch.object(generator.__class__, "_reorder_columns") as mock_reorder_columns,
        ):
            assert not generator.merge(item, context=context)

            mock_description.assert_not_called()
            mock_set_columns.assert_not_called()
            mock_reorder_columns.assert_not_called()

    @staticmethod
    def test_merge(generator: NodePropertiesGenerator[I], item: I, context: ContractContext, node_table: CatalogTable):
        with (
            mock.patch("dbt_contracts.contracts.generators.node.get_matching_catalog_table", return_value=node_table),
            mock.patch.object(generator.__class__, "_set_description", return_value=False) as mock_description,
            mock.patch.object(generator.__class__, "_set_columns", return_value=True) as mock_set_columns,
            mock.patch.object(generator.__class__, "_reorder_columns", return_value=False) as mock_reorder_columns,
        ):
            assert generator.merge(item, context=context)

            mock_description.assert_called_once_with(item, description=node_table.metadata.comment)
            mock_set_columns.assert_called_once_with(item, columns=node_table.columns)
            mock_reorder_columns.assert_called_once_with(item, columns=node_table.columns)
