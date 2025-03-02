from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Collection, Iterable
from functools import cached_property

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts.contracts._core import ContractContext, ContractTerm, ContractCondition
from dbt_contracts.contracts.conditions import NameCondition, PathCondition, TagCondition, MetaCondition
from dbt_contracts.contracts.terms import properties, node, model, source, column, macro
from dbt_contracts.types import ItemT, ParentT, NodeT


class Contract[I: ItemT | tuple[ItemT, ParentT]](metaclass=ABCMeta):
    """
    Composes the terms and conditions that make a contract for specific types of dbt objects within a manifest.
    """
    __supported_terms__: frozenset[type[ContractTerm]]
    __supported_conditions__: frozenset[type[ContractTerm]]

    @property
    @abstractmethod
    def items(self) -> Iterable[I]:
        """Get all the items that this contract can process from the manifest."""
        raise NotImplementedError

    @property
    @abstractmethod
    def filtered_items(self) -> Iterable[I]:
        """
        Get all the items that this contract can process from the manifest
        filtered according to the given conditions.
        """
        raise NotImplementedError

    @cached_property
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
        if not self.validate_terms(terms):
            raise Exception("Unsupported terms for this contract.")
        if not self.validate_conditions(conditions):
            raise Exception("Unsupported conditions for this contract.")

        #: The dbt manifest to extract items from
        self.manifest = manifest
        #: The dbt catalog to extract information on database objects from
        self.catalog = catalog

        #: The conditions to apply to items when filtering items to process
        self.conditions = conditions
        #: The terms to apply to items when validating items
        self.terms = terms

    @classmethod
    def validate_terms(cls, terms: Collection[ContractTerm]) -> bool:
        """.Validate that all the given ``terms`` are supported by this contract."""
        if not cls.__supported_terms__:
            raise Exception("No supported terms set for this contract.")
        return all(term.__class__ in cls.__supported_terms__ for term in terms)

    @classmethod
    def validate_conditions(cls, conditions: Collection[ContractCondition]) -> bool:
        """.Validate that all the given ``conditions`` are supported by this contract."""
        if not cls.__supported_conditions__:
            raise Exception("No supported conditions set for this contract.")
        return all(condition.__class__ in cls.__supported_conditions__ for condition in conditions)

    @abstractmethod
    def validate(self) -> list[I]:
        """
        Validate the terms of this contract against the filtered items.

        :return: The valid items.
        """
        raise NotImplementedError


class ParentContract[I: ItemT, P: ParentT](Contract[P], metaclass=ABCMeta):
    __child_contract__: type[ChildContract[I: ItemT, P: ParentT]]

    @property
    def filtered_items(self) -> Iterable[P]:
        for item in self.items:
            if not self.conditions or all(condition.run(item) for condition in self.conditions):
                yield item

    def create_child_contract(
            self, conditions: Collection[ContractCondition] = (), terms: Collection[ContractTerm] = ()
    ) -> ChildContract[I: ItemT, P: ParentT] | None:
        """Create a child contract from this parent contract."""
        if self.__child_contract__ is None:
            return
        return self.__child_contract__(parent=self, conditions=conditions, terms=terms)

    def validate(self) -> list[P]:
        if not self.terms:
            print("go")
            return list(self.filtered_items)
        return [
            item for item in self.filtered_items
            if all(term.run(item, context=self.context) for term in self.terms)
        ]


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
    def filtered_items(self) -> Iterable[tuple[I, P]]:
        for item, parent in self.items:
            if not self.conditions or all(condition.run(item) for condition in self.conditions):
                yield item, parent

    def __init__(
            self,
            parent: ParentContract[I, P],
            conditions: Collection[ContractCondition] = (),
            terms: Collection[ContractTerm] = (),
    ):
        super().__init__(manifest=parent.manifest, catalog=parent.catalog, conditions=conditions, terms=terms)

        #: The contract object representing the parent contract for this child contract.
        self.parent = parent

    def validate(self) -> list[tuple[I, P]]:
        if not self.terms:
            return list(self.filtered_items)
        return [
            (item, parent) for item, parent in self.filtered_items
            if all(term.run(item, parent=parent, context=self.context) for term in self.terms)
        ]


class ColumnContract[T: NodeT](ChildContract[ColumnInfo, T]):

    __supported_terms__ = frozenset({
        properties.HasProperties,
        properties.HasDescription,
        properties.HasRequiredTags,
        properties.HasAllowedTags,
        properties.HasRequiredMetaKeys,
        properties.HasAllowedMetaKeys,
        properties.HasAllowedMetaValues,
        column.Exists,
        column.HasTests,
        column.HasExpectedName,
        column.HasDataType,
        column.HasMatchingDescription,
        column.HasMatchingDataType,
        column.HasMatchingIndex,
    })
    __supported_conditions__ = frozenset({
        NameCondition, TagCondition, MetaCondition
    })

    @property
    def items(self) -> Iterable[tuple[ColumnInfo, T]]:
        return (
            (col, parent) for parent in self.parent.filtered_items for col in parent.columns.values()
        )


class MacroArgumentContract(ChildContract[MacroArgument, Macro]):

    __supported_terms__ = frozenset({
        properties.HasDescription,
        macro.HasType,
    })
    __supported_conditions__ = frozenset({
        NameCondition
    })

    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        return (
            (arg, mac) for mac in self.parent.filtered_items
            if mac.package_name == self.manifest.metadata.project_name
            for arg in mac.arguments
        )


class ModelContract(ParentContract[ColumnInfo, ModelNode]):

    __child_contract__ = ColumnContract
    __supported_terms__ = frozenset({
        properties.HasProperties,
        properties.HasDescription,
        properties.HasRequiredTags,
        properties.HasAllowedTags,
        properties.HasRequiredMetaKeys,
        properties.HasAllowedMetaKeys,
        properties.HasAllowedMetaValues,
        node.Exists,
        node.HasTests,
        node.HasAllColumns,
        node.HasExpectedColumns,
        node.HasMatchingDescription,
        node.HasContract,
        node.HasValidRefDependencies,
        node.HasValidSourceDependencies,
        node.HasValidMacroDependencies,
        node.HasNoFinalSemiColon,
        node.HasNoHardcodedRefs,
        model.HasConstraints,
    })
    __supported_conditions__ = frozenset({
        NameCondition, PathCondition, TagCondition, MetaCondition
    })

    @property
    def items(self) -> Iterable[ModelNode]:
        return (n for n in self.manifest.nodes.values() if isinstance(n, ModelNode))


class SourceContract(ParentContract[ColumnInfo, SourceDefinition]):

    __child_contract__ = ColumnContract
    __supported_terms__ = frozenset({
        properties.HasProperties,
        properties.HasDescription,
        properties.HasRequiredTags,
        properties.HasAllowedTags,
        properties.HasRequiredMetaKeys,
        properties.HasAllowedMetaKeys,
        properties.HasAllowedMetaValues,
        node.Exists,
        node.HasTests,
        node.HasAllColumns,
        node.HasExpectedColumns,
        node.HasMatchingDescription,
        source.HasLoader,
        source.HasFreshness,
        source.HasDownstreamDependencies,
    })
    __supported_conditions__ = frozenset({
        NameCondition, PathCondition, TagCondition, MetaCondition
    })

    @property
    def items(self) -> Iterable[SourceDefinition]:
        return iter(self.manifest.sources.values())


class MacroContract(ParentContract[MacroArgument, Macro]):

    __child_contract__ = MacroArgumentContract
    __supported_terms__ = frozenset({
        properties.HasProperties,
        properties.HasDescription,
    })
    __supported_conditions__ = frozenset({
        NameCondition, PathCondition
    })

    @property
    def items(self) -> Iterable[Macro]:
        return (
            mac for mac in self.manifest.macros.values()
            if mac.package_name == self.manifest.metadata.project_name
        )