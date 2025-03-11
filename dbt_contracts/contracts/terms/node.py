import re
from abc import ABCMeta
from collections.abc import Iterable, Sequence, Mapping, Collection, Generator
from pathlib import Path
from typing import Any, ClassVar

from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import TestNode, CompiledNode
from pydantic import Field

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.matchers import RangeMatcher, StringMatcher
from dbt_contracts.contracts.terms._core import ParentContractTerm, validate_context
from dbt_contracts.contracts.utils import get_matching_catalog_table
from dbt_contracts.types import NodeT


class NodeContractTerm[T: NodeT](ParentContractTerm[T], metaclass=ABCMeta):
    pass


class Exists[T: NodeT](NodeContractTerm[T]):
    """Check whether the {kind} exist in the database."""
    needs_catalog = True

    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        table = get_matching_catalog_table(item, catalog=context.catalog)
        if table is None:
            message = f"The {item.resource_type.lower()} cannot be found in the database"
            context.add_result(name=self.name, message=message, item=item)

        return table is not None


class HasTests[T: NodeT](NodeContractTerm[T], RangeMatcher):
    """Check whether {kind} have an appropriate number of tests configured."""
    needs_manifest = True

    @staticmethod
    def _get_tests(item: NodeT, manifest: Manifest) -> Iterable[TestNode]:
        def _filter_nodes(test: Any) -> bool:
            return isinstance(test, TestNode) and all((
                test.attached_node == item.unique_id,
                test.column_name is None,
            ))

        return filter(_filter_nodes, manifest.nodes.values())

    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        count = len(tuple(self._get_tests(item, manifest=context.manifest)))
        log_message = self._match(count=count, kind="tests")

        if log_message:
            context.add_result(name=self.name, message=log_message, item=item)
        return not log_message


class HasAllColumns[T: NodeT](NodeContractTerm[T]):
    """
    Check whether {kind} have all columns set in their properties.
    Ensures that all columns present in the database are present in dbt project properties.
    """
    needs_catalog = True

    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        table = get_matching_catalog_table(item, catalog=context.catalog)
        if not table:
            return False

        actual_columns = {column.name for column in item.columns.values()}
        expected_columns = {column.name for column in table.columns.values()}

        missing_columns = expected_columns - actual_columns
        if missing_columns:
            message = (
                f"{item.resource_type.title()} config does not contain all columns. "
                f"Missing {', '.join(missing_columns)}"
            )
            context.add_result(name=self.name, message=message, item=item)

        extra_columns = actual_columns - expected_columns
        if extra_columns:
            message = (
                f"{item.resource_type.title()} config contains too many columns. "
                f"Extra {', '.join(extra_columns)}"
            )
            context.add_result(name=self.name, message=message, item=item)

        return not missing_columns and not extra_columns


class HasExpectedColumns[T: NodeT](NodeContractTerm[T]):
    """
    Check whether {kind} have the expected names of columns set in their properties.
    Also checks if those columns have the expected data types if configured to do so.
    """
    columns: str | Sequence[str] | Mapping[str, str] = Field(
        description="A sequence of the names of the columns that should exist in the node, "
                    "or a mapping of the column names and their associated data types that should exist.",
        default=tuple(),
        examples=["column1", ["column1", "column2", "column3"], {"column1": "VARCHAR", "column2": "INT"}]
    )

    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        node_column_types = {column.name: column.data_type for column in item.columns.values()}

        missing_columns = set()
        if self.columns:
            missing_columns = set(self.columns) - set(node_column_types)
        if missing_columns:
            message = (
                f"{item.resource_type.title()} does not have all expected columns. "
                f"Missing: {', '.join(missing_columns)}"
            )
            context.add_result(name=self.name, message=message, item=item)

        unexpected_types = {}
        if isinstance(self.columns, Mapping):
            unexpected_types = {
                name: (node_column_types[name], data_type) for name, data_type in self.columns.items()
                if name in node_column_types and node_column_types[name] != data_type
            }
        if unexpected_types:
            message = f"{item.resource_type.title()} has unexpected column types."
            for name, (actual, expected) in unexpected_types.items():
                message += f"\n- {actual!r} should be {expected!r}"

            context.add_result(name=self.name, message=message, item=item)

        return not missing_columns and not unexpected_types


class HasMatchingDescription[T: NodeT](NodeContractTerm[T], StringMatcher):
    """Check whether the descriptions configured in {kind}' properties match the descriptions in the database."""
    needs_catalog = True

    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        table = get_matching_catalog_table(item, catalog=context.catalog)
        if not table:
            return False

        matched_description = self._match(item.description, table.metadata.comment)
        if not matched_description:
            message = f"Description does not match remote entity: {item.description!r} != {table.metadata.comment!r}"
            context.add_result(name=self.name, message=message, item=item)

        return matched_description


class HasContract[T: CompiledNode](NodeContractTerm[T]):
    """Check whether {kind} have appropriate configuration for a contract in their properties."""
    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        has_contract = bool(item.contract.enforced)
        if not has_contract:
            message = "Contract not enforced"
            context.add_result(name=self.name, message=message, item=item)

        # node must have all columns defined for contract to be valid
        has_all_columns = HasAllColumns().run(item, context=context)

        has_data_types = all(column.data_type for column in item.columns.values())
        if not has_data_types:
            message = "To enforce a contract, all data types must be declared"
            context.add_result(name=self.name, message=message, item=item)

        return all((has_contract, has_all_columns, has_data_types))


class HasValidUpstreamDependencies[T: CompiledNode](NodeContractTerm[T], metaclass=ABCMeta):
    needs_manifest = True

    @staticmethod
    def _add_result_for_invalid_dependencies(
            item: T, kind: str, context: ContractContext, missing: Collection
    ) -> bool:
        if missing:
            kind = kind.rstrip("s")
            message = (
                f"{item.resource_type.title()} has missing upstream {kind} dependencies declared: "
                f"{', '.join(missing)}"
            )
            context.add_result(name=f"has_valid_{kind}_dependencies", message=message, item=item)

        return not missing


class HasValidRefDependencies[T: CompiledNode](HasValidUpstreamDependencies[T]):
    """
    Check whether {kind} have an appropriate number of upstream dependencies
    i.e. the number of `ref` macros present in the query.
    """
    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        upstream_dependencies = {ref for ref in item.depends_on_nodes if ref.startswith("model")}
        missing_dependencies = upstream_dependencies - set(context.manifest.nodes.keys())

        return self._add_result_for_invalid_dependencies(
            item, kind="ref", context=context, missing=missing_dependencies
        )


class HasValidSourceDependencies[T: CompiledNode](HasValidUpstreamDependencies[T]):
    """
    Check whether {kind} have an appropriate number of upstream dependencies for sources
    i.e. the number of `source` macros present in the query.
    """
    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        upstream_dependencies = {ref for ref in item.depends_on_nodes if ref.startswith("source")}
        missing_dependencies = upstream_dependencies - set(context.manifest.sources.keys())

        return self._add_result_for_invalid_dependencies(
            item, kind="source", context=context, missing=missing_dependencies
        )


class HasValidMacroDependencies[T: CompiledNode](HasValidUpstreamDependencies[T]):
    """
    Check whether {kind} have an appropriate number of upstream dependencies for macros
    i.e. the number of custom macros present in the query.
    """
    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        upstream_dependencies = set(item.depends_on_macros)
        missing_dependencies = upstream_dependencies - set(context.manifest.macros.keys())

        return self._add_result_for_invalid_dependencies(
            item, kind="macro", context=context, missing=missing_dependencies
        )


class HasNoFinalSemicolon[T: CompiledNode](NodeContractTerm[T]):
    """Check if {kind} have a final semicolon present in their queries."""
    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        # ignore non-SQL models
        if Path(item.path).suffix.casefold() != ".sql":
            return True

        has_final_semicolon = item.raw_code.strip().endswith(";")
        if has_final_semicolon:
            message = "Script has a final semicolon"
            context.add_result(name=self.name, message=message, item=item)

        return not has_final_semicolon


class HasNoHardcodedRefs[T: CompiledNode](NodeContractTerm[T]):
    """Check if {kind} have any hardcoded references to database objects in their queries."""
    comments_pattern: ClassVar[str] = r"(?<=(\/\*|\{#))((.|[\r\n])+?)(?=(\*+\/|#\}))|[ \t]*--.*"

    cte_keywords: ClassVar[frozenset[str]] = frozenset({"with"})
    cte_pattern: ClassVar[str] = r"^[\w\d_-]+$"

    ref_keywords: ClassVar[frozenset[str]] = frozenset({"from", "join"})
    ref_ignore: ClassVar[frozenset[str]] = frozenset({"values"})

    @classmethod
    def _remove_comments(cls, script: str) -> str:
        return re.sub(cls.comments_pattern, "", script)

    @classmethod
    def _add_spacing(cls, script: str) -> str:
        script = re.sub(r"\{\s*\{", "{{ ", script)
        script = re.sub(r"}\s*}", " }}", script)
        script = re.sub(r"([()])", r" \1 ", script)
        return script

    @classmethod
    def _iter_script_tokens(cls, script: str) -> Generator[tuple[str, str, str]]:
        sql = script.split(";")[0]
        sql = cls._remove_comments(sql)
        sql = cls._add_spacing(sql)
        tokens = iter(sql.split())

        prev_token = None
        curr_token = next(tokens, None)
        if curr_token is not None:
            curr_token = curr_token.casefold()

        while (next_token := next(tokens, None)) is not None:
            next_token = next_token.casefold()
            yield prev_token, curr_token, next_token
            prev_token = curr_token
            curr_token = next_token

        yield prev_token, curr_token, None

    def _get_ref(self, prev_token: str, curr_token: str) -> str | None:
        if prev_token not in self.ref_keywords:
            return
        if curr_token in self.ref_ignore:
            return
        if any(curr_token.startswith(char) for char in {"{", "("}):
            return
        return curr_token.strip(",")

    def _get_cte(self, prev_token: str, curr_token: str, next_token: str) -> str | None:
        if prev_token in self.cte_keywords and re.match(self.cte_pattern, curr_token, re.I):
            return curr_token

        if curr_token == "as" and next_token.startswith("(") and re.match(self.cte_pattern, prev_token, re.I):
            return prev_token

    @validate_context
    def run(self, item: T, context: ContractContext) -> bool:
        # ignore non-SQL models
        if Path(item.path).suffix.casefold() != ".sql":
            return True

        refs = set()
        ctes = set()
        for prev_token, curr_token, next_token in self._iter_script_tokens(item.raw_code):
            if not prev_token or not next_token:
                continue

            if ref := self._get_ref(curr_token, next_token):
                refs.add(ref)
            elif cte := self._get_cte(prev_token, curr_token, next_token):
                ctes.add(cte)

        hardcoded_refs = refs - ctes
        if hardcoded_refs:
            message = f"Script has hardcoded refs: {', '.join(hardcoded_refs)}"
            context.add_result(name=self.name, message=message, item=item)

        return not hardcoded_refs
