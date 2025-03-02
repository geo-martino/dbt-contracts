from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Generator, Collection
from typing import Iterable, Sequence

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts.contracts._core import ContractContext, ContractTerm, ContractCondition
from dbt_contracts.contracts.result import Result, ModelResult, SourceResult, ColumnResult, MacroResult, \
    MacroArgumentResult
from dbt_contracts.types import ItemT, ParentT


class Contract[I: ItemT](metaclass=ABCMeta):
    """
    Composes the terms and conditions that make a contract for specific types of dbt objects within a manifest.
    """
    @property
    @abstractmethod
    def items(self) -> Iterable[I]:
        """Get all the items that this contract can process from the manifest."""
        raise NotImplementedError

    @property
    def filtered_items(self) -> Generator[I]:
        """
        Get all the items that this contract can process from the manifest
        filtered according to the given conditions.
        """
        for item in self.items:
            if all(condition.validate(item) for condition in self.conditions):
                yield item

    @property
    def context(self) -> ContractContext:
        """Generate a context object from the current loaded dbt artifacts"""
        return ContractContext(manifest=self.manifest, catalog=self.catalog)

    def __init__(
            self,
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            conditions: Collection[ContractCondition] = (),
            terms: Collection[ContractTerm] = ()
    ):
        #: The dbt manifest to extract items from
        self.manifest = manifest
        #: The dbt catalog to extract information on database objects from
        self.catalog = catalog

        #: The conditions to apply to items when filtering items to process
        self.conditions = conditions
        #: The terms to apply to items when validating items
        self.terms = terms


class ParentContract[I: ParentT](Contract[I], metaclass=ABCMeta):
    @abstractmethod
    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> ChildContract[I] | None:
        """Create a child contract from this parent contract if available"""
        raise NotImplementedError


class ChildContract[I: ItemT, P: ParentT](metaclass=ABCMeta):
    """
    Composes the terms and conditions that make a contract for specific types of dbt child objects within a manifest.
    """
    @property
    @abstractmethod
    def items(self) -> Iterable[I]:
        """Get all the items that this contract can process from the manifest."""
        raise NotImplementedError

    @property
    def filtered_items(self) -> Generator[I]:
        """
        Get all the items that this contract can process from the manifest
        filtered according to the given conditions.
        """
        for item in self.items:
            if all(condition.validate(item) for condition in self.conditions):
                yield item

    def __init__(
            self,
            parent: Contract[ParentT],
            conditions: Collection[ContractCondition] = (),
            terms: Collection[ContractTerm] = (),
    ):
        #: The contract object representing the parent contract for this child contract.
        self.parent = parent

        #: The conditions to apply to items when filtering items to process
        self.conditions = conditions
        #: The terms to apply to items when validating items
        self.terms = terms


class ModelContract(ParentContract[ModelNode]):
    @property
    def items(self) -> Iterable[tuple[ModelNode, None]]:
        return ((node, None) for node in self.manifest.nodes.values() if isinstance(node, ModelNode))

    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> ColumnContract[ModelNode]:
        return ColumnContract(parent_contract=self, conditions=conditions, terms=terms)


class SourceContract(ParentContract[SourceDefinition]):
    @property
    def items(self) -> Iterable[tuple[SourceDefinition, None]]:
        return ((source, None) for source in self.manifest.sources.values())

    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> ColumnContract[SourceDefinition]:
        return ColumnContract(parent_contract=self, conditions=conditions, terms=terms)


class ColumnContract[T: ParentT](ChildContract[ColumnInfo, T]):
    @property
    def items(self) -> Iterable[tuple[ColumnInfo, T]]:
        return (
            (column, parent) for parent in self.parent_contract.filtered_items for column in parent.columns.values()
        )


class MacroContract(ParentContract[Macro]):
    @property
    def items(self) -> Iterable[tuple[Macro, None]]:
        return (
            (macro, None) for macro in self.manifest.macros.values()
            if macro.package_name == self.manifest.metadata.project_name
        )

    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> MacroArgumentContract:
        return MacroArgumentContract(parent_contract=self, conditions=conditions, terms=terms)


class MacroArgumentContract(ChildContract[MacroArgument, Macro]):
    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        return (
            (arg, macro) for macro in self.parent_contract.filtered_items
            if macro.package_name == self.manifest.metadata.project_name
            for arg in macro.arguments
        )
