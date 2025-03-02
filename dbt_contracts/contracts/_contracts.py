from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Generator, Collection, Iterable

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts.contracts._core import ContractContext, ContractTerm, ContractCondition
from dbt_contracts.types import ItemT, ParentT


class Contract[I: ItemT | tuple[ItemT, ParentT]](metaclass=ABCMeta):
    """
    Composes the terms and conditions that make a contract for specific types of dbt objects within a manifest.
    """
    @property
    @abstractmethod
    def items(self) -> Iterable[I]:
        """Get all the items that this contract can process from the manifest."""
        raise NotImplementedError

    @property
    @abstractmethod
    def filtered_items(self) -> Generator[I]:
        """
        Get all the items that this contract can process from the manifest
        filtered according to the given conditions.
        """
        raise NotImplementedError

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
    @property
    def filtered_items(self) -> Generator[I]:
        for item in self.items:
            if not self.conditions or all(condition.validate(item) for condition in self.conditions):
                yield item

    @abstractmethod
    def create_child_contract(
            self, conditions: Collection[ContractCondition], terms: Collection[ContractTerm]
    ) -> ChildContract[I] | None:
        """Create a child contract from this parent contract if available"""
        raise NotImplementedError


class ChildContract[I: ItemT, P: ParentT](Contract[tuple[I, P]], metaclass=ABCMeta):
    """
    Composes the terms and conditions that make a contract for specific types of dbt child objects within a manifest.
    """
    @property
    @abstractmethod
    def items(self) -> Iterable[tuple[I, P]]:
        """Get all the items that this contract can process from the manifest."""
        raise NotImplementedError

    @property
    def filtered_items(self) -> Generator[tuple[I, P]]:
        for item, parent in self.items:
            if not self.conditions or all(condition.validate(item) for condition in self.conditions):
                yield item, parent

    def __init__(
            self,
            parent: ParentContract[ParentT],
            conditions: Collection[ContractCondition] = (),
            terms: Collection[ContractTerm] = (),
    ):
        super().__init__(manifest=parent.manifest, catalog=parent.catalog, conditions=conditions, terms=terms)

        #: The contract object representing the parent contract for this child contract.
        self.parent = parent


class ModelContract(ParentContract[ModelNode]):
    @property
    def items(self) -> Iterable[ModelNode]:
        return (node for node in self.manifest.nodes.values() if isinstance(node, ModelNode))

    def create_child_contract(
            self, conditions: Collection[ContractCondition], terms: Collection[ContractTerm]
    ) -> ColumnContract[ModelNode]:
        return ColumnContract[ModelNode](parent=self, conditions=conditions, terms=terms)


class SourceContract(ParentContract[SourceDefinition]):
    @property
    def items(self) -> Iterable[SourceDefinition]:
        return iter(self.manifest.sources.values())

    def create_child_contract(
            self, conditions: Collection[ContractCondition], terms: Collection[ContractTerm]
    ) -> ColumnContract[SourceDefinition]:
        return ColumnContract[SourceDefinition](parent=self, conditions=conditions, terms=terms)


class ColumnContract[T: ParentT](ChildContract[ColumnInfo, T]):
    @property
    def items(self) -> Iterable[tuple[ColumnInfo, T]]:
        return (
            (column, parent) for parent in self.parent.filtered_items for column in parent.columns.values()
        )


class MacroContract(ParentContract[Macro]):
    @property
    def items(self) -> Iterable[Macro]:
        return (
            macro for macro in self.manifest.macros.values()
            if macro.package_name == self.manifest.metadata.project_name
        )

    def create_child_contract(
            self, conditions: Collection[ContractCondition], terms: Collection[ContractTerm]
    ) -> MacroArgumentContract:
        return MacroArgumentContract(parent=self, conditions=conditions, terms=terms)


class MacroArgumentContract(ChildContract[MacroArgument, Macro]):
    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        return (
            (arg, macro) for macro in self.parent.filtered_items
            if macro.package_name == self.manifest.metadata.project_name
            for arg in macro.arguments
        )
