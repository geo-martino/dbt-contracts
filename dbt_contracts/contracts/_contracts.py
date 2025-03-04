from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Collection, Iterable, Mapping
from functools import cached_property
from typing import Any, Self, Type

from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.conditions import ContractCondition, properties as c_properties, source as c_source
from dbt_contracts.contracts.terms import ContractTerm, properties as t_properties, node as t_node, model as t_model, \
    source as t_source, column as t_column, macro as t_macro
from dbt_contracts.types import ItemT, ParentT, NodeT


class Contract[I: ItemT | tuple[ItemT, ParentT]](metaclass=ABCMeta):
    """
    Composes the terms and conditions that make a contract for specific types of dbt objects within a manifest.
    """
    __config_key__: str
    __supported_terms__: frozenset[type[ContractTerm]]
    __supported_conditions__: frozenset[type[ContractCondition]]

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

    @classmethod
    def from_dict(cls, config: Mapping[str, Any], manifest: Manifest, catalog: CatalogArtifact) -> Self:
        """
        Create a contract from a given configuration.

        :param config: The configuration to create the contract from.
        :param manifest: The dbt manifest to extract items from.
        :param catalog: The dbt catalog to extract information on database objects from.
        :return: The contract.
        """
        # noinspection PyProtectedMember
        conditions_map = {condition._name(): condition for condition in cls.__supported_conditions__}
        conditions_config = config.get("filter", [])
        conditions = tuple(
            cls._create_contract_part_from_dict(conf, part_map=conditions_map) for conf in conditions_config
        )

        # noinspection PyProtectedMember
        terms_map = {term._name(): term for term in cls.__supported_terms__}
        terms_config = config.get("terms", [])
        terms = tuple(
            cls._create_contract_part_from_dict(conf, part_map=terms_map) for conf in terms_config
        )

        return cls(
            manifest=manifest,
            catalog=catalog,
            conditions=tuple(condition for condition in conditions if condition is not None),
            terms=tuple(term for term in terms if term is not None),
        )

    @classmethod
    def _create_contract_part_from_dict[T: ContractTerm | ContractCondition](
            cls, config: str | Mapping[str, Any], part_map: Mapping[str, Type[T]]
    ) -> T | None:
        if isinstance(config, str):
            part_cls = part_map.get(config)
            kwargs = {}
        elif isinstance(config, Mapping):
            name, kwargs = next(iter(config.items()))
            part_cls = part_map.get(name)
        else:
            return

        if part_cls is None:
            return

        return part_cls(**kwargs)

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
    __child_contract__: type[ChildContract[I, P]] | None = None

    @property
    def filtered_items(self) -> Iterable[P]:
        for item in self.items:
            if not self.conditions or all(condition.run(item) for condition in self.conditions):
                yield item

    def create_child_contract(
            self, conditions: Collection[ContractCondition] = (), terms: Collection[ContractTerm] = ()
    ) -> ChildContract[I, P] | None:
        """Create a child contract from this parent contract."""
        if self.__child_contract__ is None:
            return
        return self.__child_contract__(parent=self, conditions=conditions, terms=terms)

    def create_child_contract_from_dict(self, config: Mapping[str, Any]) -> ChildContract[I, P] | None:
        """Create a child contract from this parent contract."""
        if self.__child_contract__ is None:
            return
        if (config := config.get(self.__child_contract__.__config_key__)) is None:
            return

        contract = self.__child_contract__.from_dict(config=config, manifest=self.manifest, catalog=self.catalog)
        contract.parent = self
        return contract

    def validate(self) -> list[P]:
        if not self.terms:
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
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            conditions: Collection[ContractCondition] = (),
            terms: Collection[ContractTerm] = (),
            parent: ParentContract[I, P] = None,
    ):
        super().__init__(
            manifest=manifest or parent.manifest,
            catalog=catalog or parent.catalog,
            conditions=conditions,
            terms=terms
        )

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

    __config_key__ = "columns"

    __supported_terms__ = frozenset({
        t_properties.HasDescription,
        t_properties.HasRequiredTags,
        t_properties.HasAllowedTags,
        t_properties.HasRequiredMetaKeys,
        t_properties.HasAllowedMetaKeys,
        t_properties.HasAllowedMetaValues,
        t_column.Exists,
        t_column.HasTests,
        t_column.HasExpectedName,
        t_column.HasDataType,
        t_column.HasMatchingDescription,
        t_column.HasMatchingDataType,
        t_column.HasMatchingIndex,
    })
    __supported_conditions__ = frozenset({
        c_properties.NameCondition,
        c_properties.TagCondition,
        c_properties.MetaCondition,
    })

    @property
    def items(self) -> Iterable[tuple[ColumnInfo, T]]:
        return (
            (col, parent) for parent in self.parent.filtered_items for col in parent.columns.values()
        )


class MacroArgumentContract(ChildContract[MacroArgument, Macro]):

    __config_key__ = "arguments"

    __supported_terms__ = frozenset({
        t_properties.HasDescription,
        t_macro.HasType,
    })
    __supported_conditions__ = frozenset({
        c_properties.NameCondition
    })

    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        return (
            (arg, mac) for mac in self.parent.filtered_items
            if mac.package_name == self.manifest.metadata.project_name
            for arg in mac.arguments
        )


class ModelContract(ParentContract[ColumnInfo, ModelNode]):

    __config_key__ = "models"
    __child_contract__ = ColumnContract

    __supported_terms__ = frozenset({
        t_properties.HasProperties,
        t_properties.HasDescription,
        t_properties.HasRequiredTags,
        t_properties.HasAllowedTags,
        t_properties.HasRequiredMetaKeys,
        t_properties.HasAllowedMetaKeys,
        t_properties.HasAllowedMetaValues,
        t_node.Exists,
        t_node.HasTests,
        t_node.HasAllColumns,
        t_node.HasExpectedColumns,
        t_node.HasMatchingDescription,
        t_node.HasContract,
        t_node.HasValidRefDependencies,
        t_node.HasValidSourceDependencies,
        t_node.HasValidMacroDependencies,
        t_node.HasNoFinalSemiColon,
        t_node.HasNoHardcodedRefs,
        t_model.HasConstraints,
    })
    __supported_conditions__ = frozenset({
        c_properties.NameCondition,
        c_properties.PathCondition,
        c_properties.TagCondition,
        c_properties.MetaCondition,
        c_properties.IsMaterializedCondition,
    })

    @property
    def items(self) -> Iterable[ModelNode]:
        return (n for n in self.manifest.nodes.values() if isinstance(n, ModelNode))


class SourceContract(ParentContract[ColumnInfo, SourceDefinition]):

    __config_key__ = "sources"
    __child_contract__ = ColumnContract

    __supported_terms__ = frozenset({
        t_properties.HasProperties,
        t_properties.HasDescription,
        t_properties.HasRequiredTags,
        t_properties.HasAllowedTags,
        t_properties.HasRequiredMetaKeys,
        t_properties.HasAllowedMetaKeys,
        t_properties.HasAllowedMetaValues,
        t_node.Exists,
        t_node.HasTests,
        t_node.HasAllColumns,
        t_node.HasExpectedColumns,
        t_node.HasMatchingDescription,
        t_source.HasLoader,
        t_source.HasFreshness,
        t_source.HasDownstreamDependencies,
    })
    __supported_conditions__ = frozenset({
        c_properties.NameCondition,
        c_properties.PathCondition,
        c_properties.TagCondition,
        c_properties.MetaCondition,
        c_source.IsEnabledCondition,
    })

    @property
    def items(self) -> Iterable[SourceDefinition]:
        return iter(self.manifest.sources.values())


class MacroContract(ParentContract[MacroArgument, Macro]):

    __config_key__ = "macros"
    __child_contract__ = MacroArgumentContract

    __supported_terms__ = frozenset({
        t_properties.HasProperties,
        t_properties.HasDescription,
    })
    __supported_conditions__ = frozenset({
        c_properties.NameCondition,
        c_properties.PathCondition
    })

    @property
    def items(self) -> Iterable[Macro]:
        return (
            mac for mac in self.manifest.macros.values()
            if mac.package_name == self.manifest.metadata.project_name
        )


CONTRACT_CLASSES = [ModelContract, SourceContract, MacroContract]
CONTRACT_MAP = {contract.__config_key__: contract for contract in CONTRACT_CLASSES}
