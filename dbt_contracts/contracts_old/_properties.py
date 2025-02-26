"""
Contract configuration for common properties.
"""
import inspect
from abc import ABCMeta
from collections.abc import Collection
from typing import Any, TypeVar, Generic

from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.contracts.graph.nodes import Macro, SourceDefinition

from dbt_contracts.contracts_old._core import Contract, enforce_method, filter_method
from dbt_contracts.types import T, ParentT


class DescriptionPropertyContract(Contract[T, ParentT], Generic[T, ParentT], metaclass=ABCMeta):
    """Configures a contract for resources which have description properties."""
    @enforce_method
    def has_description(self, resource: T, parent: ParentT = None) -> bool:
        """
        Check whether the given `resource` has a description set.

        :param resource: The resource to check.
        :param parent: The parent resource that the given `resource` belongs to if available.
        :return: True if the resource's properties are valid, False otherwise.
        """
        missing_description = len(resource.description) == 0
        if missing_description:
            name = inspect.currentframe().f_code.co_name
            self._add_result(resource, parent=parent, name=name, message="Missing description")

        return not missing_description


PatchT = TypeVar('PatchT', ParsedResource, Macro)


class PatchContract(
    DescriptionPropertyContract[PatchT, ParentT], Generic[PatchT, ParentT], metaclass=ABCMeta
):
    """
    Configures a contract for resources that have which do not require patch/properties files by default
    i.e. patch/properties files are optional.
    """
    @enforce_method
    def has_properties(self, resource: PatchT, parent: ParentT = None) -> bool:
        """
        Check whether the given `resource` has properties set in an appropriate properties file.

        :param resource: The resource to check.
        :param parent: The parent resource that the given `resource` belongs to if available.
        :return: True if the resource's properties are valid, False otherwise.
        """
        # sources always have properties files defined
        if isinstance(resource, SourceDefinition):
            return True

        missing_properties = resource.patch_path is None
        if missing_properties:
            name = inspect.currentframe().f_code.co_name
            self._add_result(resource, parent=parent, name=name, message="No properties file found")

        return not missing_properties


TagT = TypeVar('TagT', ParsedResource, ColumnInfo)


class TagContract(Contract[TagT, ParentT], Generic[TagT, ParentT], metaclass=ABCMeta):
    """Configures a contract for resources which have `tag` properties."""
    @filter_method
    def tags(self, resource: TagT, _: ParentT = None, *tags: str) -> bool:
        """
        Check whether a given `resource` has any matching tags to the given values.

        :param resource: The resource to check.
        :param _: The parent resource that the given `resource` belongs to if available. Ignored.
        :param tags: The tags to match on.
        :return: True if the node has matching meta, False otherwise.
        """
        for tag in tags:
            if tag in resource.tags:
                return True

        return False

    @enforce_method
    def tags_have_required_values(self, resource: TagT, parent: ParentT = None, *tags: str) -> bool:
        """
        Check whether the given `resource` has properties set in an appropriate properties file.

        :param resource: The resource to check.
        :param parent: The parent resource that the given `resource` belongs to if available.
        :param tags: The tags that must be defined.
        :return: True if the resource's properties are valid, False otherwise.
        """
        missing_tags = set(tags) - set(resource.tags)
        if missing_tags:
            name = inspect.currentframe().f_code.co_name
            message = f"Missing required tags: {', '.join(missing_tags)}"
            self._add_result(resource, parent=parent, name=name, message=message)

        return not missing_tags

    @enforce_method
    def tags_have_allowed_values(self, resource: TagT, parent: ParentT = None, *tags: str) -> bool:
        """
        Check whether the given `resource` has properties set in an appropriate properties file.

        :param resource: The resource to check.
        :param parent: The parent resource that the given `resource` belongs to if available.
        :param tags: The tags that may be defined.
        :return: True if the resource's properties are valid, False otherwise.
        """
        invalid_tags = set(resource.tags) - set(tags)
        if invalid_tags:
            name = inspect.currentframe().f_code.co_name
            message = f"Contains invalid tags: {', '.join(invalid_tags)}"
            self._add_result(resource, parent=parent, name=name, message=message)

        return len(invalid_tags) == 0


MetaT = TypeVar('MetaT', ParsedResource, ColumnInfo)


class MetaContract(Contract[MetaT, ParentT], Generic[MetaT, ParentT], metaclass=ABCMeta):
    """Configures a contract for resources which have `meta` properties."""
    @filter_method
    def meta(self, resource: MetaT, _: ParentT = None, **accepted_values: Collection[Any] | Any) -> bool:
        """
        Check whether a given `resource` has any matching meta to the accepted_values.

        :param resource: The resource to check.
        :param _: The parent resource that the given `resource` belongs to if available. Ignored.
        :param accepted_values: A map of keys to accepted values of those keys.
        :return: True if the node has matching meta, False otherwise.
        """
        for key, values in accepted_values.items():
            if not isinstance(values, Collection) or isinstance(values, str):
                values = [values]
            if key in resource.meta and resource.meta[key] in values:
                return True

        return False

    @enforce_method
    def meta_has_required_keys(self, resource: MetaT, parent: ParentT = None, *keys: str) -> bool:
        """
        Check whether the resource's `meta` config contains all required keys.

        :param resource: The resource to check.
        :param keys: The keys that must be defined.
        :param parent: The parent resource that the given `resource` belongs to if available.
        :return: True if the resource's properties are valid, False otherwise.
        """
        missing_keys = set(keys) - set(resource.meta.keys())
        if missing_keys:
            name = inspect.currentframe().f_code.co_name
            message = f"Missing required keys: {', '.join(missing_keys)}"
            self._add_result(resource, parent=parent, name=name, message=message)

        return not missing_keys

    @enforce_method
    def meta_has_allowed_keys(self, resource: MetaT, parent: ParentT = None, *keys: str) -> bool:
        """
        Check whether the resource's `meta` config contains only allowed keys.

        :param resource: The resource to check.
        :param keys: The keys that may be defined.
        :param parent: The parent resource that the given `resource` belongs to if available.
        :return: True if the resource's properties are valid, False otherwise.
        """
        invalid_keys = set(resource.meta.keys()) - set(keys)
        if invalid_keys:
            name = inspect.currentframe().f_code.co_name
            message = f"Contains invalid keys: {', '.join(invalid_keys)}"
            self._add_result(resource, parent=parent, name=name, message=message)

        return len(invalid_keys) == 0

    @enforce_method
    def meta_has_accepted_values(
            self, resource: MetaT, parent: ParentT = None, **accepted_values: Collection[Any] | Any
    ) -> bool:
        """
        Check whether the resource's `meta` config is configured as expected.

        :param resource: The resource to check.
        :param parent: The parent resource that the given `resource` belongs to if available.
        :param accepted_values: A map of keys to accepted values of those keys.
        :return: True if the resource's properties are valid, False otherwise.
        """
        invalid_meta: dict[str, str] = {}
        expected_meta: dict[str, Collection[str]] = {}

        for key, values in accepted_values.items():
            if not isinstance(values, Collection) or isinstance(values, str):
                values = [values]
            if key in resource.meta and resource.meta[key] not in values:
                invalid_meta[key] = resource.meta[key]
                expected_meta[key] = values

        if invalid_meta:
            name = inspect.currentframe().f_code.co_name
            message = f"Contains invalid meta values: {invalid_meta} | Accepted values: {expected_meta}"
            self._add_result(resource, parent=parent, name=name, message=message)

        return not invalid_meta
