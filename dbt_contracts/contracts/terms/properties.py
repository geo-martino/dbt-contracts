from collections.abc import Sequence, Collection, Mapping
from copy import copy
from typing import Annotated

from dbt.contracts.graph.nodes import SourceDefinition
from pydantic import BeforeValidator, Field, field_validator

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.terms._core import ParentContractTerm, ChildContractTerm, validate_context
from dbt_contracts.contracts.utils import to_tuple
from dbt_contracts.types import ParentT, PropertiesT, DescriptionT, TagT, MetaT


class HasProperties[I: PropertiesT](ParentContractTerm[I]):
    """Check whether the {kind} have properties files defined."""
    @validate_context
    def run(self, item: I, context: ContractContext) -> bool:
        if isinstance(item, SourceDefinition):  # sources always have properties files defined
            return True

        has_properties = bool(item.patch_path)
        if not has_properties:
            message = "No properties file found"
            context.add_result(name=self.name, message=message, item=item)

        return has_properties


class HasDescription[I: DescriptionT, P: ParentT](ChildContractTerm[I, P]):
    """Check whether the {kind} have descriptions defined in their properties."""
    @validate_context
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        has_description = bool(item.description)
        if not has_description:
            message = "Missing description"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return has_description


class HasRequiredTags[I: TagT, P: ParentT](ChildContractTerm[I, P]):
    """Check whether the {kind} have the expected set of required tags set."""
    tags: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The required tags",
        default=tuple(),
        examples=["tag1", ["tag1", "tag2"]],
    )

    @validate_context
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        missing_tags = set(self.tags) - set(item.tags)
        if missing_tags:
            message = f"Missing required tags: {', '.join(missing_tags)}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return not missing_tags


class HasAllowedTags[I: TagT, P: ParentT](ChildContractTerm[I, P]):
    """Check whether the {kind} have only tags set from a configured permitted list."""
    tags: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The allowed tags",
        default=tuple(),
        examples=["tag1", ["tag1", "tag2"]],
    )

    @validate_context
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        invalid_tags = set(item.tags) - set(self.tags)
        if invalid_tags:
            message = f"Contains invalid tags: {', '.join(invalid_tags)}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return len(invalid_tags) == 0


class HasRequiredMetaKeys[I: MetaT, P: ParentT](ChildContractTerm[I, P]):
    """Check whether the {kind} have the expected set of required meta keys set."""
    keys: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The required meta keys",
        default=tuple(),
        examples=["key1", ["key1", "key2"]],
    )

    @validate_context
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        missing_keys = set(self.keys) - set(item.meta.keys())
        if missing_keys:
            message = f"Missing required keys: {', '.join(missing_keys)}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return not missing_keys


class HasAllowedMetaKeys[I: MetaT, P: ParentT](ChildContractTerm[I, P]):
    """Check whether the {kind} have only meta keys set from a configured permitted list."""
    keys: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The allowed meta keys",
        default=tuple(),
        examples=["key1", ["key1", "key2"]],
    )

    @validate_context
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        invalid_keys = set(item.meta.keys()) - set(self.keys)
        if invalid_keys:
            message = f"Contains invalid keys: {', '.join(invalid_keys)}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return len(invalid_keys) == 0


class HasAllowedMetaValues[I: MetaT, P: ParentT](ChildContractTerm[I, P]):
    """Check whether the {kind} have only meta values set from a configured permitted mapping of keys to values."""
    meta: Mapping[str, Sequence[str]] = Field(
        description="The mapping of meta keys to their allowed values",
        default_factory=dict,
        examples=[{"key1": "val1", "key2": ["val2", "val3"]}],
    )

    # noinspection PyNestedDecorators
    @field_validator("meta", mode="before")
    @classmethod
    def make_meta_values_tuple(cls, meta: Mapping[str, str | Sequence[str]]) -> dict[str, tuple[str]]:
        """Convert all meta values to tuples"""
        meta = dict(copy(meta))

        for key, val in meta.items():
            if not isinstance(val, Collection) or isinstance(val, str):
                meta[key] = (val,)
            else:
                meta[key] = tuple(val)
        # noinspection PyTypeChecker
        return meta

    @validate_context
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        invalid_meta: dict[str, str] = {}
        expected_meta: dict[str, Collection[str]] = {}

        for key, values in self.meta.items():
            if not isinstance(values, Collection) or isinstance(values, str):
                values = [values]
            if key in item.meta and item.meta[key] not in values:
                invalid_meta[key] = item.meta[key]
                expected_meta[key] = values

        if invalid_meta:
            message = f"Contains invalid meta values: {invalid_meta} | Accepted values: {expected_meta}"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return not invalid_meta
