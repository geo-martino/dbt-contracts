from abc import ABCMeta
from collections.abc import Iterable, Sequence, Mapping
from typing import Any

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode, SourceDefinition
from dbt_common.contracts.metadata import CatalogTable
from pydantic import Field

from dbt_contracts.contracts import ContractTerm, RangeMatcher, StringMatcher
from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.types import NodeT


def _get_tests(node: NodeT, manifest: Manifest) -> Iterable[TestNode]:
    def _filter_nodes(test: Any) -> bool:
        return isinstance(test, TestNode) and all((
            test.attached_node == node.unique_id,
            test.column_name is None,
        ))

    return filter(_filter_nodes, manifest.nodes.values())


def _get_matching_catalog_table(resource: BaseResource, catalog: CatalogArtifact) -> CatalogTable | None:
    if isinstance(resource, SourceDefinition):
        return catalog.sources.get(resource.unique_id)
    return catalog.nodes.get(resource.unique_id)


class NodeContractTerm[T: NodeT](ContractTerm[T, None], metaclass=ABCMeta):
    pass


class HasTests[T: NodeT](NodeContractTerm[T], RangeMatcher):
    def run(self, item: T, context: ContractContext, parent: None = None) -> bool:
        if context.manifest is None:
            raise Exception("Must provide a manifest to run this operation")

        count = len(tuple(_get_tests(item, manifest=context.manifest)))
        too_small, too_large = self._match(count)

        # if too_small or too_large:
        #     test_name = inspect.currentframe().f_code.co_name
        #     quantifier = 'few' if too_small else 'many'
        #     expected = self.min_count if too_small else self.max_count
        #     message = f"Too {quantifier} tests found: {count}. Expected: {expected}."
        #
        #     self._add_result(item, name=test_name, message=message)

        return not too_small and not too_large


class Exists[T: NodeT](NodeContractTerm[T]):
    def run(self, item: T, context: ContractContext, parent: None = None) -> bool:
        table = _get_matching_catalog_table(item, catalog=context.catalog)
        # if table is None:
        #     test_name = inspect.currentframe().f_code.co_name
        #     message = f"The {item.resource_type.lower()} cannot be found in the database"
        #     self._add_result(item, name=test_name, message=message)

        return table is not None


class HasAllColumns[T: NodeT](NodeContractTerm[T]):
    def run(self, item: T, context: ContractContext, parent: None = None) -> bool:
        table = _get_matching_catalog_table(item, catalog=context.catalog)
        if not table:
            return False

        actual_columns = {column.name for column in item.columns.values()}
        expected_columns = {column.name for column in table.columns.values()}

        missing_columns = expected_columns - actual_columns
        # if missing_columns:
        #     test_name = inspect.currentframe().f_code.co_name
        #     message = (
        #         f"{item.resource_type.title()} config does not contain all columns. "
        #         f"Missing {', '.join(missing_columns)}"
        #     )
        #     self._add_result(item, name=test_name, message=message)

        return not missing_columns


class HasExpectedColumns[T: NodeT](NodeContractTerm[T]):
    columns: str | Sequence[str] | Mapping[str, str] = Field(
        description="A sequence of the names of the columns that should exist in the node, "
                    "or a mapping of the column names and their associated data types that should exist.",
        default=tuple()
    )

    def run(self, item: T, context: ContractContext, parent: None = None) -> bool:
        # test_name = inspect.currentframe().f_code.co_name
        node_columns = {column.name: column.data_type for column in item.columns.values()}

        missing_columns = set()
        if self.columns:
            missing_columns = set(self.columns) - set(node_columns)
        # if missing_columns:
        #     message = (
        #         f"{item.resource_type.title()} does not have all expected columns. "
        #         f"Missing: {', '.join(missing_columns)}"
        #     )
        #     self._add_result(item, name=test_name, message=message)

        unexpected_types = {}
        if isinstance(self.columns, Mapping):
            unexpected_types = {
                name: (node_columns[name], data_type) for name, data_type in self.columns.items()
                if name in node_columns and node_columns[name] != data_type
            }
        # if unexpected_types:
        #     message = f"{item.resource_type.title()} has unexpected column types."
        #     for name, (actual, expected) in unexpected_types.items():
        #         message += f"\n- {actual!r} should be {expected!r}"
        #
        #     self._add_result(item, name=test_name, message=message)

        return not missing_columns and not unexpected_types


class HasMatchingDescription[T: NodeT](NodeContractTerm[T], StringMatcher):
    def run(self, item: T, context: ContractContext, parent: None = None) -> bool:
        table = _get_matching_catalog_table(item, catalog=context.catalog)
        if not table:
            return False

        unmatched_description = not self._match(item.description, table.metadata.comment)
        # if unmatched_description:
        #     test_name = inspect.currentframe().f_code.co_name
        #     message = f"Description does not match remote entity: {item.description!r} != {table.metadata.comment!r}"
        #     self._add_result(item, name=test_name, message=message)

        return not unmatched_description
