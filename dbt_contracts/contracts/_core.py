"""
Core abstractions and utilities for all contract implementations.
"""
import inspect
import itertools
import logging
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Collection, Mapping, MutableMapping, Iterable, Generator, MutableSequence
from functools import update_wrapper
from itertools import filterfalse
from pathlib import Path
from typing import Generic, Any, Self, TypeVar

from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource
from dbt.artifacts.schemas.catalog import CatalogArtifact, CatalogTable
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts._comparisons import match_patterns
from dbt_contracts.result import RESULT_PROCESSOR_MAP, Result
from dbt_contracts.types import T, ChildT, ParentT, CombinedT

ProcessorMethodT = Callable[..., bool]


class ProcessorMethod(ProcessorMethodT):
    """
    A decorator for all processor methods.
    Assigns various properties to the method to identify which type of contract method it is.

    :param func: The method to decorate.
    :param is_filter: Tag this method as being a filter method.
    :param is_enforcement: Tag this method as being an enforcement method.
    :param needs_manifest: Tag this method as requiring a manifest to function.
    :param needs_catalog: Tag this method as requiring a catalog to function.
    """
    def __init__(
            self,
            func: ProcessorMethodT,
            is_filter: bool = False,
            is_enforcement: bool = False,
            needs_manifest: bool = True,
            needs_catalog: bool = False
    ):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.name: str = func.__name__
        self.func = func
        self.args = inspect.signature(self.func).parameters
        self.instance: Any = None

        self.is_filter = is_filter
        self.is_enforcement = is_enforcement

        self.needs_manifest = needs_manifest
        self.needs_catalog = needs_catalog

        update_wrapper(self, func)

    def __get__(self, obj, _) -> ProcessorMethodT:
        """Support instance methods."""
        self.instance = obj
        return self

    def __call__(self, *args, **kwargs):
        if self.instance is not None:
            name = f"instance method: {self.instance.__class__.__name__}.{self.name}"
        else:
            name = f"method: {self.name}"

        log_arg_map = self._format_arg_map(*args, **kwargs)
        log_args = (f"{key}={val!r}" for key, val in log_arg_map.items())
        self.logger.debug(f"Running {name} | {', '.join(log_args)}")

        return self.func(self.instance, *args, **kwargs) if self.instance is not None else self.func(*args, **kwargs)

    def _format_arg_map(self, *args, **kwargs) -> dict[str, Any]:
        names = list(self.args.keys())
        if self.instance is not None:
            names.pop(0)

        arg_map = dict(zip(names, args)) | kwargs
        for key, val in arg_map.items():
            if isinstance(val, BaseResource):
                arg_map[key] = f"{val.__class__.__name__}({val.unique_id})"

        return arg_map


def filter_method(
        arg: ProcessorMethodT = None, needs_manifest: bool = True, needs_catalog: bool = False
) -> ProcessorMethod | Callable[[ProcessorMethodT], ProcessorMethod]:
    """
    A decorator for filter methods.
    Assigns the `is_filter` property to the method to identify it as a filter method.

    :param arg: Usually the `func`. Need to allow decorator to be used with or without calling it directly.
    :param needs_manifest: Tag this method as requiring a manifest to function.
    :param needs_catalog: Tag this method as requiring a catalog to function.
    :return: The wrapped method with the property assigned.
    """
    def _decorator(func: ProcessorMethodT) -> ProcessorMethod:
        return ProcessorMethod(func, is_filter=True, needs_manifest=needs_manifest, needs_catalog=needs_catalog)
    return _decorator(arg) if callable(arg) else _decorator


def enforce_method(
        arg: ProcessorMethodT = None, needs_manifest: bool = True, needs_catalog: bool = False
) -> ProcessorMethod | Callable[[ProcessorMethodT], ProcessorMethod]:
    """
    A decorator for enforcement methods.
    Assigns the `is_enforcement` property to the method to identify it as an enforcement method.

    :param arg: Usually the `func`. Need to allow decorator to be used with or without calling it directly.
    :param needs_manifest: Tag this method as requiring a manifest to function.
    :param needs_catalog: Tag this method as requiring a catalog to function.
    :return: The wrapped method with the property assigned.
    """
    def _decorator(func: ProcessorMethodT) -> ProcessorMethod:
        return ProcessorMethod(func, is_enforcement=True, needs_manifest=needs_manifest, needs_catalog=needs_catalog)
    return _decorator(arg) if callable(arg) else _decorator


ProcessorMethodCollection = MutableSequence[tuple[ProcessorMethod, Any]]


class Contract(Generic[T, ParentT], metaclass=ABCMeta):
    """Base class for contracts relating to specific dbt resource types."""

    # noinspection SpellCheckingInspection
    #: The set of available filter method names on this contract.
    __filtermethods__: list[str] = []
    # noinspection SpellCheckingInspection
    #: The set of available enforcement method names on this contract.
    __enforcementmethods__: list[str] = []

    # noinspection PyPropertyDefinition,PyNestedDecorators
    @property
    @classmethod
    @abstractmethod
    def config_key(cls) -> str:
        """The key in a given config relating to the config which configures this contract."""
        raise NotImplementedError

    @property
    def manifest(self) -> Manifest:
        """The dbt manifest."""
        if not self.manifest_is_set:
            raise Exception("Manifest required but manifest is not set.")
        return self._manifest

    @manifest.setter
    def manifest(self, value: Manifest):
        self._manifest = value

    @property
    def manifest_is_set(self) -> bool:
        """Is the manifest set."""
        return self._manifest is not None

    @property
    def needs_manifest(self) -> bool:
        """Is the catalog set."""
        return any(f.needs_manifest for f, args in self._all_methods if isinstance(f, ProcessorMethod))

    @property
    def catalog(self) -> CatalogArtifact:
        """The dbt catalog."""
        if not self.catalog_is_set:
            raise Exception("Catalog required but catalog is not set.")
        return self._catalog

    @catalog.setter
    def catalog(self, value: CatalogArtifact):
        self._catalog = value

    @property
    def catalog_is_set(self) -> bool:
        """Is the catalog set."""
        return self._catalog is not None

    @property
    def needs_catalog(self) -> bool:
        """Is the catalog set."""
        return any(f.needs_catalog for f, args in self._all_methods if isinstance(f, ProcessorMethod))

    @property
    def _all_methods(self) -> ProcessorMethodCollection:
        return list(itertools.chain(self._filters, self._enforcements))

    @property
    def filters(self) -> ProcessorMethodCollection:
        """The filter methods and their associated arguments configured for this contract."""
        return self._filters

    @property
    def enforcements(self) -> ProcessorMethodCollection:
        """The enforcement methods and their associated arguments configured for this contract."""
        return self._enforcements

    @property
    @abstractmethod
    def items(self) -> Iterable[CombinedT]:
        """Gets the items that should be processed by this contract from the manifest."""
        raise NotImplementedError

    @classmethod
    def from_dict(cls, config: Mapping[str, Any], manifest: Manifest = None, catalog: CatalogArtifact = None) -> Self:
        """
        Configure a new contract from configuration map.

        :param config: The config map.
        :param manifest: The dbt manifest.
        :param catalog: The dbt catalog.
        :return: The configured contract.
        """
        filters = cls._get_methods_from_config(
            config.get("filter", ()), expected=cls.__filtermethods__, kind="filters"
        )
        enforcements = cls._get_methods_from_config(
            config.get("enforce", ()), expected=cls.__enforcementmethods__, kind="enforcements"
        )

        cls.__new__(cls)
        return cls(
            manifest=manifest,
            catalog=catalog,
            filters=filters,
            enforcements=enforcements,
        )

    @classmethod
    def _get_methods_from_config(
            cls, config: Iterable[str | Mapping[str, Any]], expected: Collection[str], kind: str
    ) -> list[tuple[ProcessorMethod, Any]]:
        kind = kind.lower().rstrip("s") + "s"

        names = set(next(iter(conf)) if isinstance(conf, Mapping) else str(conf) for conf in config)
        unrecognised_names = names - set(expected)
        if unrecognised_names:
            log = f"Unrecognised {kind} given: {', '.join(unrecognised_names)}. Choose from {', '.join(expected)}"
            raise Exception(log)

        methods: list[tuple[ProcessorMethod, Any]] = []
        for conf in config:
            if isinstance(conf, Mapping):
                method_name = next(iter(conf))
                args = conf[method_name]
            else:
                method_name = str(conf)
                args = None

            method: ProcessorMethod = getattr(cls, method_name)
            methods.append(tuple((method, args)))

        return methods

    def __new__(cls, *_, **__):
        for name in dir(cls):
            method = getattr(cls, name, None)
            if method is None or not isinstance(method, ProcessorMethod):
                continue

            if method.is_filter and method.name not in cls.__filtermethods__:
                cls.__filtermethods__.append(method.name)
            if method.is_enforcement and method.name not in cls.__enforcementmethods__:
                cls.__enforcementmethods__.append(method.name)

        return super().__new__(cls)

    def __init__(
            self,
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            filters: ProcessorMethodCollection = None,
            enforcements: ProcessorMethodCollection = None,
    ):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._manifest: Manifest = manifest
        self._catalog: CatalogArtifact = catalog

        self._filters = filters or []
        self._enforcements = enforcements or []

        self.logger.debug(f"Filters configured: {', '.join(f.name for f, _ in self._filters)}")
        self.logger.debug(f"Enforcements configured: {', '.join(f.name for f, _ in self._enforcements)}")

        self.results: list[Result] = []
        self._patches: MutableMapping[Path, Mapping[str, Any]] = {}

    ###########################################################################
    ## Method execution
    ###########################################################################
    def __call__(self, enforcements: Collection[str] = ()) -> list[CombinedT]:
        return self.run(enforcements=enforcements)

    def run(self, enforcements: Collection[str] = ()) -> list[CombinedT]:
        """
        Run all configured contract methods for this contract.

        :param enforcements: Apply only these enforcements. If none given, apply all configured enforcements.
        :return: The items which failed to pass their enforcements.
        """
        return list(self._enforce_contract_on_items(enforcements=enforcements))

    def _call_methods(self, item: CombinedT, methods: ProcessorMethodCollection) -> bool:
        result = True

        for method, args in methods:
            method.instance = self
            match args:
                case None:
                    result &= method(*item) if isinstance(item, tuple) else method(item)
                case str():
                    result &= method(*item, args) if isinstance(item, tuple) else method(item, args)
                case Mapping():
                    result &= method(*item, **args) if isinstance(item, tuple) else method(item, **args)
                case Iterable():
                    result &= method(*item, *args) if isinstance(item, tuple) else method(item, *args)
                case _:
                    result &= method(*item, args) if isinstance(item, tuple) else method(item, args)

        return result

    def _apply_filters(self, item: CombinedT) -> bool:
        return self._call_methods(item, self._filters)

    def _filter_items(self, items: Iterable[CombinedT]) -> Iterable[CombinedT]:
        return filter(self._apply_filters, items)

    def _apply_enforcements(self, item: CombinedT, enforcements: Collection[str] = ()) -> bool:
        if enforcements:
            enforcements = [val for val in self._enforcements if val[0].name in enforcements]
        else:
            enforcements = self._enforcements

        return self._call_methods(item, enforcements)

    def _enforce_contract_on_items(self, enforcements: Collection[str] = ()) -> Generator[CombinedT, None, None]:
        self.results.clear()
        self._patches.clear()

        seen = set()

        for item in filterfalse(lambda i: self._apply_enforcements(i, enforcements), self.items):
            key = f"{item[1].unique_id}.{item[0].name}" if isinstance(item, tuple) else item.unique_id
            if key not in seen:
                seen.add(key)
                yield item

        self.logger.info(f"Enforcements applied on {self.config_key}. Found {len(self.results)} errors.")

    ###########################################################################
    ## Logging
    ###########################################################################
    def _add_result(self, item: T, name: str, message: str, parent: ParentT = None, **extra) -> None:
        result_processor = RESULT_PROCESSOR_MAP.get(type(item))
        if result_processor is None:
            raise Exception(f"Unexpected item to create result for: {type(item)}")

        result = result_processor.from_resource(
            item=item,
            parent=parent,
            result_name=name,
            result_level="warning",
            message=message,
            patches=self._patches,
            **extra
        )
        self.results.append(result)


class ChildContract(Contract[ChildT, ParentT], Generic[ChildT, ParentT], metaclass=ABCMeta):
    """Base class for contracts which have associated parent contracts relating to specific dbt resource types."""

    @property
    def parents(self) -> Iterable[ParentT]:
        """Gets the parents of the items that should be processed by this contract from the manifest."""
        parents = self._parents
        if isinstance(parents, Contract):  # deferred execution of getting parents
            parents = parents.items
        elif isinstance(parents, Iterable) and not isinstance(parents, Collection):
            parents = list(parents)
            self._parents = parents

        return parents

    @parents.setter
    def parents(self, value: Iterable[ParentT] | Contract[ParentT, None]):
        self._parents = value

    @classmethod
    def from_dict(
            cls,
            config: Mapping[str, Any],
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            parents: Iterable[ParentT] | Contract[ParentT, None] = (),
    ) -> Self:
        """
        Configure a new contract from configuration map.

        :param config: The config map.
        :param manifest: The dbt manifest.
        :param catalog: The dbt catalog.
        :param parents: If this contract is a child contract, give the parents of the items to process.
            If a :py:class:`Contract` is given, the manifest and catalog will be extracted from it
            and the given `manifest` and `catalog` will be ignored if it contains valid values for these objects.
        :return: The configured contract.
        """
        if isinstance(parents, Contract):
            if parents.manifest_is_set:
                manifest = parents.manifest
            if parents.catalog_is_set:
                catalog = parents.catalog

        obj = super().from_dict(config=config, manifest=manifest, catalog=catalog)
        obj.parents = parents
        return obj

    def __init__(
            self,
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            filters: ProcessorMethodCollection = None,
            enforcements: ProcessorMethodCollection = None,
            # defer execution of getting parents to allow for dynamic dbt artifact assignment
            parents: Iterable[ParentT] | Contract[ParentT, None] = (),
    ):
        super().__init__(manifest=manifest, catalog=catalog, filters=filters, enforcements=enforcements)
        self._parents = parents

    ###########################################################################
    ## Processor methods
    ###########################################################################
    @filter_method
    def name(
            self,
            item: T,
            _: ParentT,
            *patterns: str,
            include: Collection[str] | str = (),
            exclude: Collection[str] | str = (),
            match_all: bool = False,
    ) -> bool:
        """
        Check whether a given `item` has a valid name.

        :param item: The item to check.
        :param _: The parent item that the given `item` belongs to if available. Ignored.
        :param patterns: Patterns to match against for paths to include.
        :param include: Patterns to match against for paths to include.
        :param exclude: Patterns to match against for paths to exclude.
        :param match_all: When True, all given patterns must match to be considered a match for either pattern type.
        :return: True if the node has a valid path, False otherwise.
        """
        return match_patterns(
            item.name, *patterns, include=include, exclude=exclude, match_all=match_all
        )


ChildContractT = TypeVar('ChildContractT', bound=ChildContract)


class ParentContract(Contract[ParentT, None], Generic[ParentT, ChildContractT], metaclass=ABCMeta):
    """Base class for contracts which have associated child contracts relating to specific dbt resource types."""

    @property
    def child(self) -> ChildContractT | None:
        """The child contract object"""
        return self._child

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    @abstractmethod
    def child_type(cls) -> type[ChildContractT]:
        """The child contract resource type"""
        raise NotImplementedError

    @property
    def manifest(self) -> Manifest:
        return super().manifest

    @manifest.setter
    def manifest(self, value: Manifest):
        self._manifest = value
        if self.child is not None:
            self.child.manifest = value

    @property
    def manifest_is_set(self) -> bool:
        return super().manifest_is_set and (self.child is None or self.child.manifest_is_set)

    @property
    def needs_manifest(self) -> bool:
        return super().needs_manifest or (self.child is not None and self.child.needs_manifest)

    @property
    def catalog(self) -> CatalogArtifact:
        return super().catalog

    @catalog.setter
    def catalog(self, value: CatalogArtifact):
        self._catalog = value
        if self.child is not None:
            self.child.catalog = value

    @property
    def catalog_is_set(self) -> bool:
        return super().catalog_is_set and (self.child is None or self.child.catalog_is_set)

    @property
    def needs_catalog(self) -> bool:
        return super().needs_catalog or (self.child is not None and self.child.needs_catalog)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any], manifest: Manifest = None, catalog: CatalogArtifact = None) -> Self:
        obj = super().from_dict(config=config, manifest=manifest, catalog=catalog)
        # noinspection PyProtectedMember
        obj._set_child_from_parent_dict(config=config)
        return obj

    def __init__(
            self,
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            filters: ProcessorMethodCollection = None,
            enforcements: ProcessorMethodCollection = None,
    ):
        super().__init__(manifest=manifest, catalog=catalog, filters=filters, enforcements=enforcements)
        self._child: ChildContractT | None = None

    def set_child(self, filters: ProcessorMethodCollection = (), enforcements: ProcessorMethodCollection = ()) -> None:
        """
        Set the child contract object for this parent contract with the given methods configured

        :param filters: The filters to configure.
        :param enforcements: The enforcements to configure.
        """
        self._child = self.child_type(
            manifest=self.manifest, catalog=self.catalog, filters=filters, enforcements=enforcements
        )

    def _set_child_from_parent_dict(self, config: Mapping[str, Any]) -> None:
        if self.child_type.config_key not in config or not (child_config := config[self.child_type.config_key]):
            return
        self._child = self.child_type.from_dict(child_config, parents=self)

    def run(self, enforcements: Collection[str] = (), child: bool = True):
        """
        Run all configured contract methods for this contract.

        :param enforcements: Apply only these enforcements. If none given, apply all configured enforcements.
        :param child: Toggle whether child enforcements should also be run.
        :return: The items which failed their enforcements.
        """
        results = list(self._enforce_contract_on_items(enforcements=enforcements))
        if child and self.child is not None:
            results.extend(self.child.run(enforcements=enforcements))

        return results

    ###########################################################################
    ## Processor methods
    ###########################################################################
    @filter_method
    def name(
            self,
            item: T,
            *patterns: str,
            include: Collection[str] | str = (),
            exclude: Collection[str] | str = (),
            match_all: bool = False,
    ) -> bool:
        """
        Check whether a given `item` has a valid name.

        :param item: The item to check.
        :param patterns: Patterns to match against for paths to include.
        :param include: Patterns to match against for paths to include.
        :param exclude: Patterns to match against for paths to exclude.
        :param match_all: When True, all given patterns must match to be considered a match for either pattern type.
        :return: True if the node has a valid path, False otherwise.
        """
        return match_patterns(
            item.name, *patterns, include=include, exclude=exclude, match_all=match_all
        )

    @filter_method
    def paths(
            self,
            item: T,
            *patterns: str,
            include: Collection[str] | str = (),
            exclude: Collection[str] | str = (),
            match_all: bool = False,
    ) -> bool:
        """
        Check whether a given `item` has a valid path.
        Paths must match patterns which are relative to directory of the dbt project.

        :param item: The item to check.
        :param patterns: Patterns to match against for paths to include.
        :param include: Patterns to match against for paths to include.
        :param exclude: Patterns to match against for paths to exclude.
        :param match_all: When True, all given patterns must match to be considered a match for either pattern type.
        :return: True if the node has a valid path, False otherwise.
        """
        paths = [item.original_file_path, item.path]
        if isinstance(item, ParsedResource) and item.patch_path:
            paths.append(item.patch_path.split("://")[1])

        return any(
            match_patterns(path, *patterns, include=include, exclude=exclude, match_all=match_all)
            for path in paths
        )


class CatalogContract(Contract[ChildT, ParentT], Generic[ChildT, ParentT], metaclass=ABCMeta):
    def get_matching_catalog_table(self, resource: ChildT, test_name: str | None = None) -> CatalogTable | None:
        """
        Check whether the given `resource` exists in the database.

        :param resource: The resource to match.
        :param test_name: The name of the test which called this method.
        :return: The matching catalog table.
        """
        if isinstance(resource, SourceDefinition):
            table = self.catalog.sources.get(resource.unique_id)
        else:
            table = self.catalog.nodes.get(resource.unique_id)

        if table is None and test_name:
            message = f"Could not run test: The {resource.resource_type.lower()} cannot be found in the database"
            self._add_result(item=resource, parent=resource, name=test_name, message=message)

        return table
