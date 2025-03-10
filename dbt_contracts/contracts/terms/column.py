import re
from abc import ABCMeta
from collections.abc import Iterable, Mapping, Collection, Sequence
from copy import copy
from typing import Any

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode
from dbt_common.contracts.metadata import CatalogTable
from pydantic import Field, field_validator

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.matchers import StringMatcher, RangeMatcher
from dbt_contracts.contracts.terms._core import ChildContractTerm, validate_context
from dbt_contracts.contracts.utils import get_matching_catalog_table
from dbt_contracts.types import NodeT


class ColumnContractTerm[T: NodeT](ChildContractTerm[ColumnInfo, T], metaclass=ABCMeta):
    def _validate_node(self, column: ColumnInfo, node: NodeT, context: ContractContext) -> bool:
        has_column = column in node.columns.values()
        if not has_column:
            message = f"The column cannot be found in the {node.resource_type.lower()}"
            context.add_result(name=self.name, message=message, item=column, parent=node)

        return has_column

    def _get_and_validate_table(self, column: ColumnInfo, node: T, context: ContractContext) -> CatalogTable | None:
        table = get_matching_catalog_table(item=node, catalog=context.catalog)
        if table is None:
            message = f"The {node.resource_type.lower()} cannot be found in the database"
            context.add_result(name=self.name, message=message, item=column, parent=node)
            return

        has_column = column.name in table.columns.keys()
        if not has_column:
            message = f"The column cannot be found in the {table.metadata.type} {table.unique_id!r}"
            context.add_result(name=self.name, message=message, item=column, parent=node)
            return

        return table


class Exists[T: NodeT](ColumnContractTerm[T]):
    """Check whether the columns exist in the database."""
    needs_catalog = True

    @validate_context
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not self._validate_node(column=item, node=parent, context=context):
            return False

        return self._get_and_validate_table(column=item, node=parent, context=context) is not None


class HasTests[T: NodeT](ColumnContractTerm[T], RangeMatcher):
    """Check whether columns have an appropriate number of tests configured."""
    needs_manifest = True

    @staticmethod
    def _get_tests(column: ColumnInfo, node: NodeT, manifest: Manifest) -> Iterable[TestNode]:
        def _filter_nodes(test: Any) -> bool:
            return isinstance(test, TestNode) and all((
                test.attached_node == node.unique_id,
                test.column_name == column.name,
            ))

        return filter(_filter_nodes, manifest.nodes.values())

    @validate_context
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not self._validate_node(column=item, node=parent, context=context):
            return False

        count = len(tuple(self._get_tests(column=item, node=parent, manifest=context.manifest)))
        log_message = self._match(count=count, kind="tests")

        if log_message:
            context.add_result(name=self.name, message=log_message, item=item, parent=parent)
        return not log_message


class HasExpectedName[T: NodeT](ColumnContractTerm[T], StringMatcher):
    """
    Check whether columns have an expected name based on their data type.
    Matches the column names against a set of patterns mapped to specific data types.
    """
    patterns: Mapping[str, Sequence[str, ...]] = Field(
        description=(
            "A map of data types to regex patterns for which to "
            "validate names of columns which have the matching data type."
            "To define a generic contract which can apply to all unmatched data types, "
            "specify the data type key as `null`."
        ),
        default_factory=dict,
        examples=[{"BOOLEAN": ["(is|has|do)_.*"], "TIMESTAMP": [".*_at"], None: ["name_.*"]}]
    )

    # noinspection PyNestedDecorators
    @field_validator("patterns", mode="before")
    @classmethod
    def make_pattern_values_tuple(cls, patterns: Mapping[str, str | Sequence[str]]) -> dict[str, tuple[str]]:
        """Convert all meta values to tuples"""
        patterns = dict(copy(patterns))

        for key, val in patterns.items():
            if not isinstance(val, Collection) or isinstance(val, str):
                patterns[key] = (val,)
            else:
                patterns[key] = tuple(val)
        # noinspection PyTypeChecker
        return patterns

    def _get_column_data_type(self, column: ColumnInfo, node: T, context: ContractContext) -> str | None:
        if column.data_type:
            return column.data_type

        if context.catalog is None:
            return
        if (table := self._get_and_validate_table(column=column, node=node, context=context)) is None:
            return

        return table.columns[column.name].type

    @validate_context
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not self._validate_node(column=item, node=parent, context=context):
            return False
        if not (data_type := self._get_column_data_type(column=item, node=parent, context=context)):
            return False

        data_type = next((key for key in self.patterns if self._match(key, data_type)), None)
        patterns = self.patterns.get(data_type)
        if not patterns:  # no patterns defined for this data type
            return True

        expected_name = any(re.match(pattern, item.name) for pattern in patterns)
        if not expected_name:
            patterns_log = ', '.join(patterns)
            message = "Column name does not match expected patterns "
            message += f"for type {data_type!r}: {patterns_log}" if data_type else f": {patterns_log}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return expected_name


class HasDataType[T: NodeT](ColumnContractTerm[T]):
    """Check whether columns have a data type configured in their properties."""
    @validate_context
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not self._validate_node(column=item, node=parent, context=context):
            return False

        has_data_type = bool(item.data_type)
        if not has_data_type:
            message = "Data type not configured for this column"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return has_data_type


class HasMatchingDescription[T: NodeT](ColumnContractTerm[T], StringMatcher):
    """Check whether the descriptions configured in columns' properties matches the descriptions in the database."""
    needs_catalog = True

    @validate_context
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not self._validate_node(column=item, node=parent, context=context):
            return False
        if (table := self._get_and_validate_table(column=item, node=parent, context=context)) is None:
            return False

        node_description = item.description
        table_description = table.columns[item.name].comment

        matched_description = self._match(node_description, table_description)
        if not matched_description:
            message = f"Description does not match remote entity: {node_description!r} != {table_description!r}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return matched_description


class HasMatchingDataType[T: NodeT](ColumnContractTerm[T], StringMatcher):
    """Check whether the data type configured in a column's properties matches the data type in the database."""
    needs_catalog = True

    @validate_context
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not self._validate_node(column=item, node=parent, context=context):
            return False
        if (table := self._get_and_validate_table(column=item, node=parent, context=context)) is None:
            return False

        node_type = item.data_type
        table_type = table.columns[item.name].type

        matched_type = self._match(node_type, table_type)
        if not matched_type:
            message = f"Data type does not match remote entity: {node_type!r} != {table_type!r}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return matched_type


class HasMatchingIndex[T: NodeT](ColumnContractTerm[T], StringMatcher):
    """
    Check whether the index position within the properties of a column's table
    matches the index position in the database.
    """
    needs_catalog = True

    @validate_context
    def run(self, item: ColumnInfo, context: ContractContext, parent: T = None) -> bool:
        if not self._validate_node(column=item, node=parent, context=context):
            return False
        if (table := self._get_and_validate_table(column=item, node=parent, context=context)) is None:
            return False

        node_index = list(parent.columns).index(item.name)
        table_index = table.columns[item.name].index

        matched_index = node_index == table_index
        if not matched_index:
            message = f"Column index does not match remote entity: {node_index} != {table_index}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return matched_index
