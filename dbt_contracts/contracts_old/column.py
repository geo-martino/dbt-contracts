"""
Contract configuration for columns.
"""
import inspect
import itertools
import re
from collections.abc import Collection, Iterable
from typing import Any, Generic, TypeVar

from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.artifacts.schemas.catalog import CatalogTable
from dbt.contracts.graph.nodes import TestNode, SourceDefinition
from dbt_contracts.contracts_old._comparisons import match_strings, is_not_in_range
from dbt_contracts.contracts_old._properties import DescriptionPropertyContract, TagContract, MetaContract

from dbt_contracts.contracts_old._core import enforce_method, ChildContract, CatalogContract

ColumnParentT = TypeVar('ColumnParentT', ParsedResource, SourceDefinition)


class ColumnContract(
    DescriptionPropertyContract[ColumnInfo, ColumnParentT],
    TagContract[ColumnInfo, ColumnParentT],
    MetaContract[ColumnInfo, ColumnParentT],
    CatalogContract[ColumnInfo, ColumnParentT],
    ChildContract[ColumnInfo, ColumnParentT],
    Generic[ColumnParentT]
):
    """Configures a contract configuration for columns."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def config_key(cls) -> str:
        return "columns"

    @property
    def items(self) -> Iterable[tuple[ColumnInfo, ColumnParentT]]:
        arguments = map(lambda parent: [(column, parent) for column in parent.columns.values()], self.parents)
        return self._filter_items(itertools.chain.from_iterable(arguments))

    def get_tests(self, column: ColumnInfo, parent: ColumnParentT) -> Iterable[TestNode]:
        """
        Get the tests from the manifest that test the given `column` of the given `parent`.

        :param column: The column for which to get tests.
        :param parent: The parent node for which to get tests.
        :return: The matching test nodes.
        """

        def _filter_nodes(test: Any) -> bool:
            return isinstance(test, TestNode) and all((
                test.attached_node == parent.unique_id,
                test.column_name is not None,
                test.column_name == column.name,
            ))

        return filter(_filter_nodes, self.manifest.nodes.values())

    def _is_column_in_node(self, column: ColumnInfo, parent: ColumnParentT) -> bool:
        """
        Checks whether the given `column` is not a part of the given `parent` node.

        :param column: The column to check.
        :param parent: The parent node to check against.
        """
        missing_column = column not in parent.columns.values()
        if missing_column:
            message = f"The column cannot be found in the {parent.resource_type.lower()}"
            self._add_result(item=column, parent=parent, name="exists_in_node", message=message)

        return not missing_column

    def _is_column_in_table(
            self,
            column: ColumnInfo,
            parent: ColumnParentT,
            table: CatalogTable,
            test_name: str | None = None
    ) -> bool:
        """
        Checks whether the given `column` exists in the given `table`.

        :param column: The column to check.
        :param parent: The column's parent node.
        :param table: The table to check against.
        :return: True if the column exists, False otherwise.
        """
        missing_column = column.name not in table.columns.keys()
        if missing_column and test_name:
            message = f"The column cannot be found in {table.unique_id!r}"
            self._add_result(item=column, parent=parent, name=test_name, message=message)

        return not missing_column

    @enforce_method
    def has_expected_name(
            self,
            column: ColumnInfo,
            parent: ColumnParentT,
            ignore_whitespace: bool = False,
            case_insensitive: bool = False,
            compare_start_only: bool = False,
            **patterns: Collection[str] | str
    ) -> bool:
        """
        Check whether the given `column` of the given `parent` has a name that matches some expectation.
        This expectation can be generic or specific to only columns of a certain data type.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :param ignore_whitespace: When True, ignore any whitespaces when comparing data type keys.
        :param case_insensitive: When True, ignore cases and compare data type keys only case-insensitively.
        :param compare_start_only: When True, match data type keys when the two values start with the same value.
            Ignore the rest of the data type definition in this case.
        :param patterns: A map of data types to regex patterns for which to
            validate names of columns which have the matching data type.
            To define a generic contract which can apply to all unmatched data types,
            specify the data type key as an empty key.
            e.g. {"BOOLEAN": "(is|has|do)_.*", "TIMESTAMP": ".*_at", "": "name_.*", ...}
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        test_name = inspect.currentframe().f_code.co_name

        data_type = column.data_type
        if not data_type:
            data_type = ""
            if self.catalog_is_set:
                table = self.get_matching_catalog_table(parent, test_name=test_name)
                if not table:
                    return False

                if not self._is_column_in_table(column, parent=parent, table=table, test_name=test_name):
                    return False
                data_type = table.columns[column.name].type

        pattern_key = next((
            key for key in patterns if match_strings(
                key,
                data_type,
                ignore_whitespace=ignore_whitespace,
                case_insensitive=case_insensitive,
                compare_start_only=compare_start_only
            )
        ), "")

        pattern_values = patterns.get(pattern_key)
        if not pattern_values:
            return True
        if not isinstance(pattern_values, Collection) or isinstance(pattern_values, str):
            pattern_values = tuple(str(pattern_values))

        unexpected_name = not all(re.match(pattern, column.name) for pattern in pattern_values)
        if unexpected_name:
            patterns_log = ', '.join(pattern_values)
            if pattern_key:
                message = f"Column name does not match expected pattern for type {data_type!r}: {patterns_log}"
            else:
                message = f"Column name does not match expected patterns: {patterns_log}"
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unexpected_name

    @enforce_method
    def has_data_type(self, column: ColumnInfo, parent: ColumnParentT) -> bool:
        """
        Check whether the given `column` of the given `parent` has a data type set.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        missing_data_type = not column.data_type
        if missing_data_type:
            name = inspect.currentframe().f_code.co_name
            message = "Data type not configured for this column"
            self._add_result(column, parent=parent, name=name, message=message)

        return not missing_data_type

    @enforce_method
    def has_tests(self, column: ColumnInfo, parent: ColumnParentT, min_count: int = 1, max_count: int = None) -> bool:
        """
        Check whether the given `column` of the given `parent` has an appropriate number of tests.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :param min_count: The minimum number of tests allowed.
        :param max_count: The maximum number of tests allowed.
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        count = len(tuple(self.get_tests(column, parent)))
        too_small, too_large = is_not_in_range(count=count, min_count=min_count, max_count=max_count)

        if too_small or too_large:
            test_name = inspect.currentframe().f_code.co_name
            quantifier = 'few' if too_small else 'many'
            expected = min_count if too_small else max_count
            message = f"Too {quantifier} tests found: {count}. Expected: {expected}."

            self._add_result(column, parent=parent, name=test_name, message=message)

        return not too_small and not too_large

    @enforce_method(needs_catalog=True)
    def exists(self, column: ColumnInfo, parent: ColumnParentT) -> bool:
        """
        Check whether the column exists in the database.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :return: True if the node's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        test_name = inspect.currentframe().f_code.co_name
        table = self.get_matching_catalog_table(parent)
        if table is None:
            message = f"The {parent.resource_type.lower()} cannot be found in the database"
            self._add_result(column, parent=parent, name=test_name, message=message)
            return False

        return self._is_column_in_table(column, parent=parent, table=table, test_name=test_name)

    @enforce_method(needs_catalog=True)
    def has_matching_description(
            self,
            column: ColumnInfo,
            parent: ColumnParentT,
            case_insensitive: bool = False,
            compare_start_only: bool = False
    ) -> bool:
        """
        Check whether the given `column` of the given `parent`
        has a description configured which matches the remote resource.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :param case_insensitive: When True, ignore cases and compare only case-insensitively.
        :param compare_start_only: When True, match when the two values start with the same value.
            Ignore the rest of the text in this case.
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        test_name = inspect.currentframe().f_code.co_name

        table = self.get_matching_catalog_table(parent, test_name=test_name)
        if not table:
            return False
        if not self._is_column_in_table(column, parent=parent, table=table, test_name=test_name):
            return False

        unmatched_description = not match_strings(
            column.description,
            table.columns[column.name].comment,
            case_insensitive=case_insensitive,
            compare_start_only=compare_start_only
        )

        if unmatched_description:
            message = (
                f"Description does not match remote entity: "
                f"{column.description!r} != {table.columns[column.name].comment!r}"
            )
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unmatched_description

    @enforce_method(needs_catalog=True)
    def has_matching_data_type(
            self,
            column: ColumnInfo,
            parent: ColumnParentT,
            ignore_whitespace: bool = False,
            case_insensitive: bool = False,
            compare_start_only: bool = False,
    ) -> bool:
        """
        Check whether the given `column` of the given `parent`
        has a data type configured which matches the remote resource.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :param ignore_whitespace: When True, ignore any whitespaces when comparing.
        :param case_insensitive: When True, ignore cases and compare only case-insensitively.
        :param compare_start_only: When True, match when the two values start with the same value.
            Ignore the rest of the text in this case.
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        test_name = inspect.currentframe().f_code.co_name

        table = self.get_matching_catalog_table(parent, test_name=test_name)
        if not table:
            return False
        if not self._is_column_in_table(column, parent=parent, table=table, test_name=test_name):
            return False

        unmatched_type = not match_strings(
            column.data_type,
            table.columns[column.name].type,
            ignore_whitespace=ignore_whitespace,
            case_insensitive=case_insensitive,
            compare_start_only=compare_start_only
        )

        if unmatched_type:
            message = (
                f"Data type does not match remote entity: "
                f"{column.data_type!r} != {table.columns[column.name].type!r}"
            )
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unmatched_type

    @enforce_method(needs_catalog=True)
    def has_matching_index(self, column: ColumnInfo, parent: ColumnParentT) -> bool:
        """
        Check whether the given `column` of the given `parent`
        is in the same position in the dbt config as the remote resource.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        test_name = inspect.currentframe().f_code.co_name

        table = self.get_matching_catalog_table(parent, test_name=test_name)
        if not table:
            return False
        if not self._is_column_in_table(column, parent=parent, table=table, test_name=test_name):
            return False

        node_index = list(parent.columns).index(column.name)
        table_index = table.columns[column.name].index

        unmatched_index = node_index != table_index
        if unmatched_index:
            message = f"Column index does not match remote entity: {node_index} != {table_index}"
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unmatched_index
