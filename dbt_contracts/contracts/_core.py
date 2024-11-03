"""
Core abstractions and utilities for all contract implementations.
"""
import inspect
import logging
import re
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Collection, Mapping, MutableMapping, Iterable, Generator
from itertools import filterfalse
from pathlib import Path
from typing import Generic, Any, Self

from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource
from dbt.artifacts.schemas.catalog import CatalogArtifact, CatalogTable
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.result import RESULT_LOG_MAP, ResultLog
from dbt_contracts.contracts._types import T, ParentT, CombinedT

ProcessorMethodT = Callable[..., bool]


class ProcessorMethod(ProcessorMethodT):
    """
    A decorator for all processor methods.
    Assigns various properties to the method to identify which type of contract method it is.

    :param func: The method to decorate.
    :param is_filter: Tag this method as being a filter method.
    :param is_validation: Tag this method as being a validation method.
    :param needs_manifest: Tag this method as requiring a manifest to function.
    :param needs_catalog: Tag this method as requiring a catalog to function.
    """
    def __init__(
            self,
            func: ProcessorMethodT,
            is_filter: bool = False,
            is_validation: bool = False,
            needs_manifest: bool = True,
            needs_catalog: bool = False
    ):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.name: str = func.__name__
        self.func = func
        self.args = inspect.signature(self.func).parameters
        self.instance: Any = None

        self.is_filter = is_filter
        self.is_validation = is_validation

        self.needs_manifest = needs_manifest
        self.needs_catalog = needs_catalog

    def __get__(self, obj, _):
        """Support instance methods."""
        if self.instance is None:
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


def validation_method(
        arg: ProcessorMethodT = None, needs_manifest: bool = True, needs_catalog: bool = False
) -> ProcessorMethod | Callable[[ProcessorMethodT], ProcessorMethod]:
    """
    A decorator for validation methods.
    Assigns the `is_validation` property to the method to identify it as a validation method.

    :param arg: Usually the `func`. Need to allow decorator to be used with or without calling it directly.
    :param needs_manifest: Tag this method as requiring a manifest to function.
    :param needs_catalog: Tag this method as requiring a catalog to function.
    :return: The wrapped method with the property assigned.
    """
    def _decorator(func: ProcessorMethodT) -> ProcessorMethod:
        return ProcessorMethod(func, is_validation=True, needs_manifest=needs_manifest, needs_catalog=needs_catalog)
    return _decorator(arg) if callable(arg) else _decorator


class Contract(Generic[T, ParentT], metaclass=ABCMeta):
    """Base class for contracts relating to specific dbt resource types."""

    # noinspection SpellCheckingInspection
    #: The set of available filter method names on this contract.
    __filtermethods__: frozenset[str] = frozenset()
    # noinspection SpellCheckingInspection
    #: The set of available validation method names on this contract.
    __validationmethods__: frozenset[str] = frozenset()

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
        return any(f.needs_manifest for f, args in self._filters + self._validations if isinstance(f, ProcessorMethod))

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
        return any(f.needs_catalog for f, args in self._filters + self._validations if isinstance(f, ProcessorMethod))

    @property
    @abstractmethod
    def items(self) -> Iterable[CombinedT]:
        """Gets the items that should be processed by this contract from the manifest."""
        raise NotImplementedError

    @property
    def parents(self) -> Iterable[ParentT]:
        """Gets the parents of the items that should be processed by this contract from the manifest."""
        parents = self._parents
        if callable(parents):  # deferred execution of getting parents
            parents = parents()
        elif isinstance(parents, Iterable) and not isinstance(parents, Collection):
            parents = list(parents)
            self._parents = parents

        return parents

    @classmethod
    def from_dict(
            cls,
            config: Mapping[str, Any],
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            parents: Iterable[ParentT] | Callable[[], Iterable[ParentT]] = (),
    ) -> Self:
        """
        Configure a new contract from configuration map.

        :param config: The config map.
        :param manifest: The dbt manifest.
        :param catalog: The dbt catalog.
        :param parents: If this contract is a child contract, give the parents of the items to process.
        :return: The configured contract.
        """
        filters = config.get("filters", ())
        validations = config.get("validations", ())

        return cls(
            manifest=manifest,
            catalog=catalog,
            filters=filters,
            validations=validations,
            parents=parents,
        )

    def __new__(cls, *_, **__):
        filter_methods = list(cls.__filtermethods__)
        validation_methods = list(cls.__validationmethods__)
        for name in dir(cls):
            method = getattr(cls, name, None)
            if method is None or not isinstance(method, ProcessorMethod):
                continue

            if method.is_filter:
                filter_methods.append(method.name)
            if method.is_validation:
                validation_methods.append(method.name)

        # noinspection SpellCheckingInspection
        cls.__filtermethods__ = frozenset(filter_methods)
        # noinspection SpellCheckingInspection
        cls.__validationmethods__ = frozenset(validation_methods)
        return super().__new__(cls)

    def __init__(
            self,
            manifest: Manifest = None,
            catalog: CatalogArtifact = None,
            filters: Iterable[str | Mapping[str, Any]] = (),
            validations: Iterable[str | Mapping[str, Any]] = (),
            # defer execution of getting parents to allow for dynamic dbt artifact assignment
            parents: Iterable[ParentT] | Callable[[], Iterable[ParentT]] = (),
    ):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._manifest: Manifest = manifest
        self._catalog: CatalogArtifact = catalog
        self._parents = parents

        self._filters = self._get_methods_from_config(
            filters, expected=self.__filtermethods__, kind="filters"
        )
        self._validations = self._get_methods_from_config(
            validations, expected=self.__validationmethods__, kind="validations"
        )

        self.logger.debug(f"Filters configured: {', '.join(f.name for f, _ in self._filters)}")
        self.logger.debug(f"Validations configured: {', '.join(f.name for f, _ in self._validations)}")

        self.results: list[ResultLog] = []
        self._patches: MutableMapping[Path, Mapping[str, Any]] = {}

    def _get_methods_from_config(
            self, config: Iterable[str | Mapping[str, Any]], expected: Collection[str], kind: str
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

            methods.append(tuple((getattr(self, method_name), args)))

        return methods

    ###########################################################################
    ## Method execution
    ###########################################################################
    def __call__(self) -> list[CombinedT]:
        return self.run()

    @staticmethod
    def _call_methods(item: CombinedT, methods: Iterable[tuple[ProcessorMethodT, Any]]) -> bool:
        result = True
        for method, args in methods:
            match args:
                case str():
                    result &= method(*item, args) if isinstance(item, tuple) else method(item, args)
                case Mapping():
                    result &= method(*item, **args) if isinstance(item, tuple) else method(item, **args)
                case Collection():
                    result &= method(*item, *args) if isinstance(item, tuple) else method(item, *args)
                case _:
                    result &= method(*item) if isinstance(item, tuple) else method(item)

        return result

    def _apply_filters(self, item: CombinedT) -> bool:
        return self._call_methods(item, self._filters)

    def _filter_items(self, items: Iterable[CombinedT]) -> Iterable[CombinedT]:
        return filter(self._apply_filters, items)

    def _apply_validations(self, item: CombinedT) -> bool:
        return self._call_methods(item, self._validations)

    def _validate_items(self) -> Generator[CombinedT, None, None]:
        self.results.clear()
        self._patches.clear()

        seen = set()

        for item in filterfalse(self._apply_validations, self.items):
            key = f"{item[1].unique_id}.{item[0].name}" if isinstance(item, tuple) else item.unique_id
            if key not in seen:
                seen.add(key)
                yield item

        self.logger.debug(f"Validations applied. Found {len(self.results)} errors.")

    def run(self):
        """Run all configured contract methods for this contract."""
        return list(self._validate_items())

    ###########################################################################
    ## Logging
    ###########################################################################
    def _log_result(self, item: T, name: str, message: str, parent: ParentT = None, **extra) -> None:
        result_logger = RESULT_LOG_MAP.get(type(item))
        if result_logger is None:
            raise Exception(f"Unexpected item to log: {type(item)}")

        log = result_logger.from_resource(
            item=item, parent=parent, log_name=name, message=message, patches=self._patches, **extra
        )
        self.results.append(log)

    ###########################################################################
    ## Method helpers
    ###########################################################################
    @validation_method(needs_catalog=True)
    def get_matching_catalog_table(self, resource: ParsedResource, test_name: str) -> CatalogTable | None:
        """
        Check whether the given `resource` exists in the database.

        :param resource: The resource to match.
        :param test_name: The name of the validation which called this method.
        :return: The matching catalog table.
        """
        if isinstance(resource, SourceDefinition):
            table = self.catalog.sources.get(resource.unique_id)
        else:
            table = self.catalog.nodes.get(resource.unique_id)

        if table is None:
            name = inspect.currentframe().f_code.co_name
            message = (
                f"Could not run test {test_name!r}: "
                f"The {resource.resource_type.lower()} cannot be found in the database"
            )

            self._log_result(item=resource, parent=resource, name=name, message=message)

        return table

    def _is_in_range(
            self, item: T, kind: str, count: int, min_count: int = 1, max_count: int = None, parent: ParentT = None
    ) -> bool:
        if min_count < 1:
            raise Exception(f"Minimum count must be greater than 0, not {min_count}")
        if max_count is not None and max_count < 1:
            raise Exception(f"Maximum count must be greater than 0, not {max_count}")

        too_small = count < min_count
        too_large = max_count is not None and count > max_count
        if too_small or too_large:
            kind = kind.replace("_", " ").rstrip("s") + "s"
            if too_small:
                message = f"Too few {kind} found: {count}. Expected: {min_count}."
            else:
                message = f"Too many {kind} found: {count}. Expected: {max_count}."

            self._log_result(item, parent=parent, name=f"has_{kind.replace(" ", "_")}", message=message)

        return not too_small and not too_large

    @staticmethod
    def _matches_patterns(
            value: str, *patterns: str, include: Collection[str] | str = (), exclude: Collection[str] | str = ()
    ) -> bool:
        if isinstance(exclude, str):
            exclude = [exclude]
        if exclude and any(re.match(pattern, value) for pattern in exclude):
            return False

        if isinstance(include, str):
            include = [include]
        include += patterns
        return not include or any(re.match(pattern, value) for pattern in include)

    ###########################################################################
    ## Core methods
    ###########################################################################
    @filter_method
    def name(
            self, item: T, *patterns: str, include: Collection[str] | str = (), exclude: Collection[str] | str = ()
    ) -> bool:
        """
        Check whether a given `item` has a valid name.

        :param item: The item to check.
        :param patterns: Patterns to match against for paths to include.
        :param include: Patterns to match against for paths to include.
        :param exclude: Patterns to match against for paths to exclude.
        :return: True if the node has a valid path, False otherwise.
        """
        return self._matches_patterns(item.name, *patterns, include=include, exclude=exclude)

    @filter_method
    def paths(
            self, item: T, *patterns: str, include: Collection[str] | str = (), exclude: Collection[str] | str = ()
    ) -> bool:
        """
        Check whether a given `item` has a valid path.
        Paths must match patterns which are relative to directory of the dbt project.

        :param item: The item to check.
        :param patterns: Patterns to match against for paths to include.
        :param include: Patterns to match against for paths to include.
        :param exclude: Patterns to match against for paths to exclude.
        :return: True if the node has a valid path, False otherwise.
        """
        return self._matches_patterns(item.original_file_path, *patterns, include=include, exclude=exclude)
