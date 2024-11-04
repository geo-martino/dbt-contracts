"""
Contract configuration for columns.
"""
import inspect
import itertools
import re
from collections.abc import Collection, Mapping, Iterable
from typing import Any, Generic, TypeVar

from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.artifacts.schemas.catalog import CatalogTable
from dbt.contracts.graph.nodes import TestNode, SourceDefinition

from dbt_contracts.contracts._core import validation_method, ChildContract
from dbt_contracts.contracts._properties import DescriptionPropertyContract, TagContract, MetaContract

ParentT = TypeVar('ParentT', ParsedResource, SourceDefinition)


class ColumnContract(
    DescriptionPropertyContract[ColumnInfo, ParentT],
    TagContract[ColumnInfo, ParentT],
    MetaContract[ColumnInfo, ParentT],
    ChildContract[ColumnInfo, ParentT],
    Generic[ParentT]
):
    """Configures a contract configuration for columns."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def config_key(cls) -> str:
        return "columns"

    @property
    def items(self) -> Iterable[tuple[ColumnInfo, ParentT]]:
        arguments = map(lambda parent: [(column, parent) for column in parent.columns.values()], self.parents)
        return self._filter_items(itertools.chain.from_iterable(arguments))

    def get_tests(self, column: ColumnInfo, parent: ParentT) -> Iterable[TestNode]:
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

    def _is_column_in_node(self, column: ColumnInfo, parent: ParentT) -> bool:
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

    def _is_column_in_table(self, column: ColumnInfo, parent: ParentT, table: CatalogTable) -> bool:
        """
        Checks whether the given `column` exists in the given `table`.

        :param column: The column to check.
        :param parent: The column's parent node.
        :param table: The table to check against.
        :return: True if the column exists, False otherwise.
        """
        missing_column = column.name not in table.columns.keys()
        if missing_column:
            message = f"The column cannot be found in {table.key()!r}"
            self._add_result(item=column, parent=parent, name="exists_in_table", message=message)

        return not missing_column

    @validation_method
    def has_expected_name(
            self, column: ColumnInfo, parent: ParentT, contract: Mapping[str | None, Collection[str] | str]
    ) -> bool:
        """
        Check whether the given `column` of the given `parent` has a name that matches some expectation.
        This expectation can be generic or specific to only columns of a certain data type.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :param contract: A map of data types to regex patterns for which to
            validate names of columns which have the matching data type.
            To define a generic contract which can apply to all unmatched data types,
            specify the data type key as 'None'.
            e.g. {"BOOLEAN": "(is|has|do)_.*", "TIMESTAMP": ".*_at", None: "name_.*", ...}
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

                if not self._is_column_in_table(column, parent=parent, table=table):
                    return False
                data_type = table.columns[column.name].type

        pattern_key = next((key for key in contract if key.casefold() == data_type.casefold()), None)
        patterns = contract.get(pattern_key)
        if not patterns:
            return True
        if not isinstance(patterns, Collection) or isinstance(patterns, str):
            patterns = tuple(str(patterns))

        unexpected_name = not all(re.match(pattern, column.name) for pattern in patterns)
        if unexpected_name:
            if pattern_key:
                message = f"Column name does not match expected pattern for type {data_type}: {', '.join(patterns)}"
            else:
                message = f"Column name does not match expected patterns: {', '.join(patterns)}"
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unexpected_name

    @validation_method
    def has_data_type(self, column: ColumnInfo, parent: ParentT) -> bool:
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

    @validation_method
    def has_tests(self, column: ColumnInfo, parent: ParentT, min_count: int = 1, max_count: int = None) -> bool:
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
        return self._is_in_range(
            item=column, parent=parent, kind="tests", count=count, min_count=min_count, max_count=max_count
        )

    @validation_method(needs_catalog=True)
    def has_matching_description(self, column: ColumnInfo, parent: ParentT, case_sensitive: bool = False) -> bool:
        """
        Check whether the given `column` of the given `parent`
        has a description configured which matches the remote resource.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :param case_sensitive: When True, cases must match. When False, apply case-insensitive match.
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        test_name = inspect.currentframe().f_code.co_name

        table = self.get_matching_catalog_table(parent, test_name=test_name)
        if not table:
            return False
        if not self._is_column_in_table(column, parent=parent, table=table):
            return False

        table_comment = table.columns[column.name].comment
        if not table_comment:
            unmatched_description = True
        elif case_sensitive:
            unmatched_description = column.description != table_comment
        else:
            unmatched_description = column.description.casefold() != table_comment.casefold()

        if unmatched_description:
            message = f"Description does not match remote entity: {column.description!r} != {table_comment!r}"
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unmatched_description

    @validation_method(needs_catalog=True)
    def has_matching_data_type(self, column: ColumnInfo, parent: ParentT, exact: bool = False) -> bool:
        """
        Check whether the given `column` of the given `parent`
        has a data type configured which matches the remote resource.

        :param column: The column to check.
        :param parent: The parent node that the column belongs to.
        :param exact: When True, type must match exactly including cases.
        :return: True if the column's properties are valid, False otherwise.
        """
        if not self._is_column_in_node(column, parent):
            return False

        test_name = inspect.currentframe().f_code.co_name

        table = self.get_matching_catalog_table(parent, test_name=test_name)
        if not table:
            return False
        if not self._is_column_in_table(column, parent=parent, table=table):
            return False

        table_type = table.columns[column.name].type
        if exact:
            unmatched_type = column.data_type != table_type
        else:
            column_type = column.data_type.casefold().replace(" ", "")
            unmatched_type = column_type != table_type.casefold().replace(" ", "")

        if unmatched_type:
            message = f"Data type does not match remote entity: {column.data_type} != {table_type}"
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unmatched_type

    @validation_method(needs_catalog=True)
    def has_matching_index(self, column: ColumnInfo, parent: ParentT) -> bool:
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
        if not self._is_column_in_table(column, parent=parent, table=table):
            return False

        node_index = list(parent.columns).index(column.name)
        table_index = table.columns[column.name].index

        unmatched_index = node_index != table_index
        if unmatched_index:
            message = f"Column index does not match remote entity: {node_index} != {table_index}"
            self._add_result(column, parent=parent, name=test_name, message=message)

        return not unmatched_index
