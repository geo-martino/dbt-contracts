from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping, MutableSequence
from functools import cached_property
from pathlib import Path
from typing import Any, Self, Type

from colorama import Fore
from dbt.adapters.utils import classproperty
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts.contracts._core import ContractContext, ContractPart
from dbt_contracts.contracts.conditions import ContractCondition, properties as c_properties, source as c_source
from dbt_contracts.contracts.generators import PropertiesGenerator, \
    ParentPropertiesGenerator, ChildPropertiesGenerator, \
    model as g_model, source as g_source, column as g_column
from dbt_contracts.contracts.terms import ContractTerm, ChildContractTerm, \
    properties as t_properties, node as t_node, \
    model as t_model, source as t_source, column as t_column, macro as t_macro
from dbt_contracts.types import ItemT, ParentT, NodeT


class Contract[I: Any, T: ContractTerm, G: PropertiesGenerator | None](metaclass=ABCMeta):
    """
    Composes the various :py:class:`.ContractPart` objects that make a contract for specific types
    of dbt objects within a manifest.

    A Contract gets the dbt objects from the dbt artifacts and passes them to the :py:class:`.ContractPart` objects
    for processing.
    """
    __config_key__: str
    __supported_terms__: tuple[type[T]]
    __supported_conditions__: tuple[type[ContractCondition]]
    __supported_generator__: type[G] | None = None

    @property
    def config_key(self) -> str:
        """The key used to identify this contract in a configuration."""
        return self.__config_key__

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

    @property
    def needs_manifest(self) -> bool:
        """Do any of the terms in this contract require a manifest to execute"""
        return any(term.needs_manifest for term in self.terms)

    @property
    def needs_catalog(self) -> bool:
        """Do any of the terms in this contract require a catalog to execute"""
        return any(term.needs_catalog for term in self.terms)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any], manifest: Manifest = None, catalog: CatalogArtifact = None) -> Self:
        """
        Create a contract from a given configuration.

        :param config: The configuration to create the contract from.
        :param manifest: The dbt manifest to extract items from.
        :param catalog: The dbt catalog to extract information on database objects from.
        :return: The contract.
        """
        return cls(
            manifest=manifest,
            catalog=catalog,
            conditions=cls._create_conditions(config),
            terms=cls._create_terms(config),
            generator=cls._create_generator(config),
        )

    @classmethod
    def _create_conditions(cls, config: str | Mapping[str, Any]) -> list[ContractCondition]:
        # noinspection PyProtectedMember
        conditions_map = {condition._name(): condition for condition in cls.__supported_conditions__}
        conditions_config = config.get("filter", config.get("conditions", []))
        conditions = tuple(
            cls._create_contract_part_from_dict(conf, part_map=conditions_map) for conf in conditions_config
        )
        return [cond for cond in conditions if cond is not None]

    @classmethod
    def _create_terms(cls, config: str | Mapping[str, Any]) -> list[T]:
        # noinspection PyProtectedMember
        terms_map = {term._name(): term for term in cls.__supported_terms__}
        terms_config = config.get("validations", config.get("terms", []))
        terms = tuple(cls._create_contract_part_from_dict(conf, part_map=terms_map) for conf in terms_config)
        return [term for term in terms if term is not None]

    @classmethod
    def _create_generator(cls, config: str | Mapping[str, Any]) -> G | None:
        if cls.__supported_generator__ is None:
            return

        generator_config = config.get("properties", config.get("generator", {}))
        return cls.__supported_generator__(**generator_config)

    @classmethod
    def _create_contract_part_from_dict[T: ContractPart](
            cls, config: str | Mapping[str, Any], part_map: Mapping[str, Type[T]]
    ) -> T | None:
        if isinstance(config, str):
            part_cls = part_map.get(config, part_map.get(config.rstrip("s")))
            kwargs = {}
        elif isinstance(config, Mapping):
            name, kwargs = next(iter(config.items()))
            part_cls = part_map.get(name, part_map.get(name.rstrip("s")))
        else:
            return

        if part_cls is None:
            return
        if not isinstance(kwargs, Mapping):
            # noinspection PyTypeChecker
            kwargs = {next(iter(part_cls.model_fields)): kwargs}

        return part_cls(**kwargs)

    def __init__(
            self,
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            conditions: MutableSequence[ContractCondition] = (),
            terms: MutableSequence[T] = (),
            generator: G | None = None,
    ):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        if len(terms) > 0 and not self.validate_terms(terms):
            raise Exception("Unsupported terms for this contract.")
        if len(conditions) > 0 and not self.validate_conditions(conditions):
            raise Exception("Unsupported conditions for this contract.")

        #: The dbt manifest to extract items from
        self.manifest = manifest
        #: The dbt catalog to extract information on database objects from
        self.catalog = catalog

        #: The conditions to apply to items when filtering items to process
        self.conditions = conditions
        #: The terms to apply to items when validating items
        self.terms = terms
        #: The generator to use when creating properties files from database objects
        self.generator = generator

    @classmethod
    def validate_terms(cls, terms: ContractTerm | Collection[ContractTerm]) -> bool:
        """.Validate that all the given ``terms`` are supported by this contract."""
        if isinstance(terms, ContractTerm):
            terms = [terms]

        if not cls.__supported_terms__ and len(terms) > 0:
            return False
        elif cls.__supported_terms__ and len(terms) == 0:
            return False

        return all(term.__class__ in cls.__supported_terms__ for term in terms)

    @classmethod
    def validate_conditions(cls, conditions: ContractCondition | Collection[ContractCondition]) -> bool:
        """.Validate that all the given ``conditions`` are supported by this contract."""
        if isinstance(conditions, ContractCondition):
            conditions = [conditions]

        if not cls.__supported_conditions__ and len(conditions) > 0:
            return False
        elif cls.__supported_conditions__ and len(conditions) == 0:
            return False

        return all(condition.__class__ in cls.__supported_conditions__ for condition in conditions)

    @abstractmethod
    def validate(self, terms: Collection[str] = ()) -> list[I]:
        """
        Validate the terms of this contract against the filtered items.

        :param terms: Only run the terms with these names.
        :return: The valid items.
        """
        raise NotImplementedError

    @abstractmethod
    def generate(self) -> dict[Path, int]:
        """
        Generate the properties files for the items in this contract.

        :return: Map of the paths of properties to save to the number of items updated for that path.
        """
        raise NotImplementedError


class ParentContract[I: ItemT, P: ParentT, G: ParentPropertiesGenerator | None](
    Contract[P, ContractTerm, G], metaclass=ABCMeta
):
    __child_contract__: type[ChildContract[I, P]] | None = None

    # noinspection PyMethodParameters
    @classproperty
    def child_config_key(cls) -> str:
        """The key used to identify the child contract for this contract type."""
        return f"{cls.__config_key__}.{cls.__child_contract__.__config_key__}"

    @property
    def filtered_items(self) -> Iterable[P]:
        for item in self.items:
            if not self.conditions or all(condition.run(item) for condition in self.conditions):
                yield item

    def create_child_contract[GC: ChildPropertiesGenerator | None](
            self,
            conditions: MutableSequence[ContractCondition] = (),
            terms: MutableSequence[ChildContractTerm] = (),
            generator: GC = None,
    ) -> ChildContract[I, P, GC] | None:
        """Create a child contract from this parent contract."""
        if self.__child_contract__ is None:
            return
        return self.__child_contract__(parent=self, conditions=conditions, terms=terms, generator=generator)

    def create_child_contract_from_dict(self, config: Mapping[str, Any]) -> list[ChildContract[I, P]]:
        """Create a child contract from this parent contract."""
        if self.__child_contract__ is None:
            return []
        if (config := config.get(self.__child_contract__.__config_key__)) is None:
            return []

        if isinstance(config, Mapping):
            config = [config]

        contracts = [
            self.__child_contract__.from_dict(config=conf, manifest=self.manifest, catalog=self.catalog)
            for conf in config
        ]
        for contract in contracts:
            contract.parent = self

        return contracts

    # noinspection PyUnresolvedReferences
    def validate(self, terms: Collection[str] = ()) -> list[P]:
        if not self.terms:
            return list(self.filtered_items)

        run_terms = [term for term in self.terms if term.name in terms] if terms else self.terms

        key = f"{self.config_key.rstrip("s")}s"
        message = f"Validating {len(tuple(self.filtered_items))} {key} against {len(run_terms)} terms"
        self.logger.info(f"{Fore.LIGHTMAGENTA_EX}{message}{Fore.RESET}")

        return [
            item for item in self.filtered_items
            if all(term.run(item, context=self.context) for term in run_terms)
        ]

    def generate(self) -> dict[Path, int]:
        if self.generator is None:
            return {}

        message = f"Generating properties for {len(tuple(self.filtered_items))} {self.config_key.rstrip("s")}s"
        self.logger.info(f"{Fore.LIGHTMAGENTA_EX}{message}{Fore.RESET}")

        paths = defaultdict[Path, int](int)
        for item in self.filtered_items:
            modified = self.generator.merge(item, context=self.context)
            if not modified:
                continue

            self.generator.update(item, context=self.context)

            try:
                path = self.context.properties.get_path(item, to_absolute=True)
            except FileNotFoundError:
                path = self.generator.generate_properties_path(item)

            paths[path] += 1

        return dict(paths)


class ChildContract[I: ItemT, P: ParentT, G: ChildPropertiesGenerator | None](
    Contract[tuple[I, P], ChildContractTerm, G], metaclass=ABCMeta
):
    """
    Composes the terms and conditions that make a contract for specific types of dbt child objects within a manifest.
    """
    @property
    def config_key(self) -> str:
        return self.parent.child_config_key

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
            conditions: MutableSequence[ContractCondition] = (),
            terms: MutableSequence[ChildContractTerm] = (),
            generator: G | None = None,
            parent: ParentContract[I, P, ParentPropertiesGenerator | None] = None,
    ):
        super().__init__(
            manifest=manifest or (parent.manifest if parent is not None else None),
            catalog=catalog or (parent.catalog if parent is not None else None),
            conditions=conditions,
            terms=terms,
            generator=generator,
        )

        #: The contract object representing the parent contract for this child contract.
        self.parent = parent

    # noinspection PyUnresolvedReferences
    def validate(self, terms: Collection[str] = ()) -> list[tuple[I, P]]:
        if not self.terms:
            return list(self.filtered_items)

        run_terms = [term for term in self.terms if term.name in terms] if terms else self.terms

        key = f"{self.parent.config_key.rstrip('s')} {self.__config_key__.rstrip("s")}s"
        message = f"Validating {len(tuple(self.filtered_items))} {key} against {len(run_terms)} terms"
        self.logger.info(f"{Fore.LIGHTMAGENTA_EX}{message}{Fore.RESET}")

        return [
            (item, parent) for item, parent in self.filtered_items
            if all(term.run(item, parent=parent, context=self.context) for term in run_terms)
        ]

    def generate(self) -> dict[Path, int]:
        if self.generator is None or self.parent.generator is None:
            return {}

        key = f"{self.parent.config_key.rstrip('s')} {self.__config_key__.rstrip("s")}s"
        message = f"Generating properties for {len(tuple(self.filtered_items))} {key}"
        self.logger.info(f"{Fore.LIGHTMAGENTA_EX}{message}{Fore.RESET}")

        paths = defaultdict[Path, int](int)
        for item, parent in self.filtered_items:
            modified = self.generator.merge(item, context=self.context, parent=parent)
            if not modified:
                continue

            self.parent.generator.update(parent, context=self.context)

            try:
                path = self.context.properties.get_path(parent, to_absolute=True)
            except FileNotFoundError:
                path = self.parent.generator.generate_properties_path(parent)
            paths[path] += 1

        return dict(paths)


class ColumnContract[T: NodeT](ChildContract[ColumnInfo, T, g_column.ColumnPropertiesGenerator]):

    __config_key__ = "columns"

    __supported_terms__ = (
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
    )
    __supported_conditions__ = (
        c_properties.NameCondition,
        c_properties.TagCondition,
        c_properties.MetaCondition,
    )
    __supported_generator__ = g_column.ColumnPropertiesGenerator

    @property
    def items(self) -> Iterable[tuple[ColumnInfo, T]]:
        return (
            (col, parent) for parent in self.parent.filtered_items for col in parent.columns.values()
        )


class MacroArgumentContract(ChildContract[MacroArgument, Macro, None]):

    __config_key__ = "arguments"

    __supported_terms__ = (
        t_properties.HasDescription,
        t_macro.HasType,
    )
    __supported_conditions__ = (
        c_properties.NameCondition,
    )

    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        return (
            (arg, mac) for mac in self.parent.filtered_items
            if mac.package_name == self.manifest.metadata.project_name
            for arg in mac.arguments
        )


class ModelContract(ParentContract[ColumnInfo, ModelNode, g_model.ModelPropertiesGenerator]):

    __config_key__ = "models"
    __child_contract__ = ColumnContract

    __supported_terms__ = (
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
        t_node.HasNoFinalSemicolon,
        t_node.HasNoHardcodedRefs,
        t_model.HasConstraints,
    )
    __supported_conditions__ = (
        c_properties.NameCondition,
        c_properties.PathCondition,
        c_properties.TagCondition,
        c_properties.MetaCondition,
        c_properties.IsMaterializedCondition,
    )
    __supported_generator__ = g_model.ModelPropertiesGenerator

    @property
    def items(self) -> Iterable[ModelNode]:
        return (
            node for node in self.manifest.nodes.values()
            if isinstance(node, ModelNode) and node.package_name == self.manifest.metadata.project_name
        )


class SourceContract(ParentContract[ColumnInfo, SourceDefinition, g_source.SourcePropertiesGenerator]):

    __config_key__ = "sources"
    __child_contract__ = ColumnContract

    __supported_terms__ = (
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
    )
    __supported_conditions__ = (
        c_properties.NameCondition,
        c_properties.PathCondition,
        c_properties.TagCondition,
        c_properties.MetaCondition,
        c_source.IsEnabledCondition,
    )
    __supported_generator__ = g_source.SourcePropertiesGenerator

    @property
    def items(self) -> Iterable[SourceDefinition]:
        return (
            source for source in self.manifest.sources.values()
            if source.package_name == self.manifest.metadata.project_name
        )


class MacroContract(ParentContract[MacroArgument, Macro, None]):

    __config_key__ = "macros"
    __child_contract__ = MacroArgumentContract

    __supported_terms__ = (
        t_properties.HasProperties,
        t_properties.HasDescription,
    )
    __supported_conditions__ = (
        c_properties.NameCondition,
        c_properties.PathCondition,
    )

    @property
    def items(self) -> Iterable[Macro]:
        return (
            macro for macro in self.manifest.macros.values()
            if macro.package_name == self.manifest.metadata.project_name
        )


CONTRACT_CLASSES: tuple[type[ParentContract], ...] = (ModelContract, SourceContract, MacroContract)
CONTRACT_MAP = {contract.__config_key__: contract for contract in CONTRACT_CLASSES}
