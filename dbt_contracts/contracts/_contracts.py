from __future__ import annotations
from typing import Iterable, Sequence

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts.contracts import ContractCondition, ContractTerm
from dbt_contracts.contracts._core import ParentContract, ChildContract
from dbt_contracts.contracts.result import Result, ModelResult, SourceResult, ColumnResult, MacroResult, \
    MacroArgumentResult
from dbt_contracts.types import ParentT


class ModelContract(ParentContract[ModelNode]):
    @property
    def result_processor(self) -> type[Result]:
        return ModelResult

    @property
    def items(self) -> Iterable[tuple[ModelNode, None]]:
        return (node, None for node in self.manifest.nodes.values() if isinstance(node, ModelNode))

    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> ColumnContract[ModelNode]:
        return ColumnContract(parent_contract=self, conditions=conditions, terms=terms)


class SourceContract(ParentContract[SourceDefinition, None]):
    @property
    def result_processor(self) -> type[Result]:
        return SourceResult

    @property
    def items(self) -> Iterable[tuple[SourceDefinition, None]]:
        return (source, None for source in self.manifest.sources.values())

    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> ColumnContract[SourceDefinition]:
        return ColumnContract(parent_contract=self, conditions=conditions, terms=terms)


class ColumnContract[T: ParentT](ChildContract[ColumnInfo, T]):
    @property
    def result_processor(self) -> type[Result]:
        return ColumnResult

    @property
    def items(self) -> Iterable[tuple[ColumnInfo, T]]:
        return (
            column, parent for parent in self.parent_contract.filtered_items for column in parent.columns.values()
        )


class MacroContract(ParentContract[Macro]):
    @property
    def result_processor(self) -> type[Result]:
        return MacroResult

    @property
    def items(self) -> Iterable[tuple[Macro, None]]:
        return (
            macro, None for macro in self.manifest.macros.values()
            if macro.package_name == self.manifest.metadata.project_name
        )

    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> MacroArgumentContract:
        return MacroArgumentContract(parent_contract=self, conditions=conditions, terms=terms)


class MacroArgumentContract(ChildContract[MacroArgument, Macro]):
    @property
    def result_processor(self) -> type[Result]:
        return MacroArgumentResult

    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        return (
            arg, macro for macro in self.parent_contract.filtered_items
            if macro.package_name == self.manifest.metadata.project_name
            for arg in macro.arguments
        )
