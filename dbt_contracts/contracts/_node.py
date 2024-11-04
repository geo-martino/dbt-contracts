"""
Contract configuration for nodes.
"""
import inspect
import re
from abc import ABCMeta
from collections.abc import Collection, Iterable
from pathlib import Path
from typing import TypeVar, Generic, Any

from dbt.contracts.graph.nodes import TestNode, SourceDefinition, CompiledNode, BaseNode

from dbt_contracts.contracts._core import validation_method, ParentContract
from dbt_contracts.contracts._properties import PatchContract, TagContract, MetaContract
from dbt_contracts.contracts.column import ColumnContract

NodeT = TypeVar('NodeT', BaseNode, SourceDefinition)


class NodeContract(
    PatchContract[NodeT, None],
    TagContract[NodeT, None],
    MetaContract[NodeT, None],
    ParentContract[NodeT, ColumnContract[NodeT]],
    Generic[NodeT],
    metaclass=ABCMeta
):
    """Configures a contract for nodes."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def child_type(cls) -> type[ColumnContract[NodeT]]:
        return ColumnContract

    def get_tests(self, node: NodeT) -> Iterable[TestNode]:
        """
        Get the tests from the manifest that test the given `node` directly.

        :param node: The node for which to get tests.
        :return: The matching test nodes.
        """
        def _filter_nodes(test: Any) -> bool:
            return isinstance(test, TestNode) and all((
                test.attached_node == node.unique_id,
                test.column_name is None,
            ))
        return filter(_filter_nodes, self.manifest.nodes.values())

    @validation_method
    def has_tests(self, node: NodeT, min_count: int = 1, max_count: int = None) -> bool:
        """
        Check whether the given `node` has an appropriate number of tests.

        :param node: The node to check.
        :param min_count: The minimum number of tests allowed.
        :param max_count: The maximum number of tests allowed.
        :return: True if the node's properties are valid, False otherwise.
        """
        count = len(tuple(self.get_tests(node)))
        return self._is_in_range(item=node, kind="tests", count=count, min_count=min_count, max_count=max_count)

    @validation_method(needs_catalog=True)
    def has_all_columns(self, node: NodeT) -> bool:
        """
        Check whether the node properties contain all available columns of the node.

        :param node: The node to check.
        :return: True if the node's properties are valid, False otherwise.
        """
        test_name = inspect.currentframe().f_code.co_name

        table = self.get_matching_catalog_table(node, test_name=test_name)
        if not table:
            return False

        actual_columns = {column.name for column in table.columns.values()}
        expected_columns = {column.name for column in node.columns.values()}

        missing_columns = expected_columns - actual_columns
        if missing_columns:
            message = (
                f"{node.resource_type.title()} config does not contain all columns. "
                f"Missing {', '.join(missing_columns)}"
            )
            self._add_result(node, name=test_name, message=message)

        return not missing_columns

    @validation_method
    def has_expected_columns(self, node: NodeT, *columns: str, **column_data_types: str) -> bool:
        """
        Check whether the node properties contain the expected set of `columns`.

        :param node: The node to check.
        :param columns: The names of the columns that should exist in the node.
        :param column_data_types: The column names and associated data types that should exist.
        :return: True if the node's properties are valid, False otherwise.
        """
        test_name = inspect.currentframe().f_code.co_name
        node_columns = {column.name: column.data_type for column in node.columns.values()}

        missing_columns = set()
        if columns or column_data_types:
            missing_columns = (set(columns) | set(column_data_types)) - set(node_columns)
        if missing_columns:
            message = (
                f"{node.resource_type.title()} does not have all expected columns. "
                f"Missing: {', '.join(missing_columns)}"
            )
            self._add_result(node, name=test_name, message=message)

        unexpected_types = {} if not column_data_types else {
            name: (node_columns[name], data_type) for name, data_type in column_data_types.items()
            if name in node_columns and node_columns[name] != data_type
        }
        if unexpected_types:
            message = f"{node.resource_type.title()} has unexpected column types."
            for name, (actual, expected) in unexpected_types.items():
                message += f"\n- {actual!r} should be {expected!r}"

            self._add_result(node, name=test_name, message=message)

        return not missing_columns and not unexpected_types


CompiledNodeT = TypeVar('CompiledNodeT', bound=CompiledNode)


class CompiledNodeContract(NodeContract[CompiledNodeT], metaclass=ABCMeta):
    """Configures a contract for compiled nodes."""

    @validation_method(needs_catalog=True)
    def has_contract(self, node: CompiledNodeT) -> bool:
        """
        Check whether the node properties define a contract.

        :param node: The node to check.
        :return: True if the node's properties are valid, False otherwise.
        """
        test_name = inspect.currentframe().f_code.co_name

        missing_contract = not node.contract.enforced
        if missing_contract:
            self._add_result(node, name=test_name, message="Contract not enforced")

        # node must have all columns defined for contract to be valid
        missing_columns = not self.has_all_columns(node)

        missing_data_types = any(not column.data_type for column in node.columns.values())
        if missing_data_types:
            self._add_result(node, name=test_name, message="To enforce a contract, all data types must be declared")

        return not any((missing_columns, missing_columns, missing_data_types))

    def _has_valid_upstream_dependencies(self, node: CompiledNodeT, missing: Collection, kind: str) -> bool:
        if missing:
            kind = kind.rstrip("s")
            message = (
                f"{node.resource_type.title()} has missing upstream {kind} dependencies declared: "
                f"{', '.join(missing)}"
            )
            self._add_result(node, name=f"has_valid_{kind}_dependencies", message=message)

        return not missing

    @validation_method
    def has_valid_ref_dependencies(self, node: CompiledNodeT) -> bool:
        """
        Check whether the given `node` has valid upstream ref dependencies.
        i.e. do the declared upstream dependencies exist within the project.

        :param node: The node to check.
        :return: True if the node's properties are valid, False otherwise.
        """
        upstream_dependencies = {ref for ref in node.depends_on_nodes if ref.startswith("model")}
        missing_dependencies = upstream_dependencies - set(self.manifest.nodes.keys())
        return self._has_valid_upstream_dependencies(node, missing=missing_dependencies, kind="ref")

    @validation_method
    def has_valid_source_dependencies(self, node: CompiledNodeT) -> bool:
        """
        Check whether the given `node` has valid upstream source dependencies.
        i.e. do the declared upstream dependencies exist within the project's configuration.

        :param node: The node to check.
        :return: True if the node's properties are valid, False otherwise.
        """
        upstream_dependencies = {ref for ref in node.depends_on_nodes if ref.startswith("source")}
        missing_dependencies = upstream_dependencies - set(self.manifest.sources.keys())
        return self._has_valid_upstream_dependencies(node, missing=missing_dependencies, kind="source")

    @validation_method
    def has_valid_macro_dependencies(self, node: CompiledNodeT) -> bool:
        """
        Check whether the given `node` has valid upstream macro dependencies.
        i.e. do the declared upstream dependencies exist within the project's configuration.

        :param node: The node to check.
        :return: True if the node's properties are valid, False otherwise.
        """
        upstream_dependencies = set(node.depends_on_macros)
        missing_dependencies = upstream_dependencies - set(self.manifest.macros.keys())
        return self._has_valid_upstream_dependencies(node, missing=missing_dependencies, kind="macro")

    @validation_method
    def has_no_final_semicolon(self, node: CompiledNodeT) -> bool:
        """
        Check whether the given `node` has a no closing semicolon at the end of the script.

        :param node: The node to check.
        :return: True if the node's properties are valid, False otherwise.
        """
        has_final_semicolon = node.raw_code.strip().endswith(";")
        if has_final_semicolon:
            name = inspect.currentframe().f_code.co_name
            self._add_result(node, name=name, message="Script has final semicolon")

        return not has_final_semicolon

    @validation_method
    def has_no_hardcoded_refs(self, node: CompiledNodeT) -> bool:
        """
        Check whether the given `node` has a no hardcoded upstream references.
        i.e. does not hardcode table/view name references.

        :param node: The node to check.
        :return: True if the node's properties are valid, False otherwise.
        """
        # ignore non-SQL models
        if Path(node.path).suffix.casefold() != ".sql":
            return True

        pattern_cte = r"^[\w\d_-]+$"
        pattern_macro = r"^(ref|source)\(\s*(['\"][^'\"]+['\"],?\s*){1,2}\s*\)$"
        pattern_comments = r"(?<=(\/\*|\{#))((.|[\r\n])+?)(?=(\*+\/|#\}))|[ \t]*--.*"

        code = re.sub(pattern_comments, "", node.raw_code)
        words = iter(code.split())

        def _format_ref() -> str:
            ref = next(words)
            if ref.startswith("{{"):
                while not ref.endswith("}}"):
                    ref += next(words)
                ref = ref.strip("{}").strip()

            return ref

        # noinspection SpellCheckingInspection
        ctes = set()
        refs = set()
        while (word := next(words, None)) is not None:
            if word.casefold() in ["from", "join"]:
                refs.add(_format_ref())

            if word.casefold() in ["with", ","] and re.match(pattern_cte, word := next(words, None), re.I):
                next_word = next(words).casefold()
                if next_word == "(" or (next_word == "as" and next(words) == "("):
                    ctes.add(word)

        hardcoded_refs = {ref for ref in refs if ref not in ctes and not re.match(pattern_macro, ref, re.I)}
        if hardcoded_refs:
            name = inspect.currentframe().f_code.co_name
            message = f"Script has hardcoded refs: {', '.join(hardcoded_refs)}"
            self._add_result(node, name=name, message=message)

        return not hardcoded_refs
