from collections.abc import Sequence, Collection
from typing import Annotated

from dbt.contracts.graph.nodes import SourceDefinition
from pydantic import BeforeValidator, Field, field_validator

from dbt_contracts.contracts._core import ContractTerm, ContractContext
from dbt_contracts.contracts._matchers import to_tuple
from dbt_contracts.types import ParentT, PropertiesT, DescriptionT, TagT, MetaT


class HasProperties[I: PropertiesT](ContractTerm[I, None]):
    def run(self, item: I, context: ContractContext, parent: None = None) -> bool:
        if isinstance(item, SourceDefinition):  # sources always have properties files defined
            return True

        missing_properties = item.patch_path is None
        # if missing_properties:
        #     name = inspect.currentframe().f_code.co_name
        #     self._add_result(item, parent=parent, name=name, message="No properties file found")

        return not missing_properties


class HasDescription[I: DescriptionT, P: ParentT](ContractTerm[I, P]):
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        missing_description = not item.description
        # if missing_description:
        #     name = inspect.currentframe().f_code.co_name
        #     self._add_result(item, parent=parent, name=name, message="Missing description")

        return not missing_description


class HasRequiredTags[I: TagT, P: ParentT](ContractTerm[I, P]):
    tags: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The required tags",
        default=tuple(),
    )

    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        missing_tags = set(self.tags) - set(item.tags)
        # if missing_tags:
        #     name = inspect.currentframe().f_code.co_name
        #     message = f"Missing required tags: {', '.join(missing_tags)}"
        #     self._add_result(item, parent=parent, name=name, message=message)

        return not missing_tags


class HasAllowedTags[I: TagT, P: ParentT](ContractTerm[I, P]):
    tags: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The allowed tags",
        default=tuple(),
    )

    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        invalid_tags = set(item.tags) - set(self.tags)
        # if invalid_tags:
        #     name = inspect.currentframe().f_code.co_name
        #     message = f"Contains invalid tags: {', '.join(invalid_tags)}"
        #     self._add_result(item, parent=parent, name=name, message=message)

        return len(invalid_tags) == 0


class HasRequiredMetaKeys[I: MetaT, P: ParentT](ContractTerm[I, P]):
    keys: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The required meta keys",
        default=tuple(),
    )

    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        missing_keys = set(self.keys) - set(item.meta.keys())
        # if missing_keys:
        #     name = inspect.currentframe().f_code.co_name
        #     message = f"Missing required keys: {', '.join(missing_keys)}"
        #     self._add_result(item, parent=parent, name=name, message=message)

        return not missing_keys


class HasAllowedMetaKeys[I: MetaT, P: ParentT](ContractTerm[I, P]):
    keys: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The allowed meta keys",
        default=tuple(),
    )

    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        invalid_keys = set(item.meta.keys()) - set(self.keys)
        # if invalid_keys:
        #     name = inspect.currentframe().f_code.co_name
        #     message = f"Contains invalid keys: {', '.join(invalid_keys)}"
        #     self._add_result(item, parent=parent, name=name, message=message)

        return len(invalid_keys) == 0


class HasAllowedMetaValues[I: MetaT, P: ParentT](ContractTerm[I, P]):
    meta: dict[str, Sequence[str]] = Field(
        description="The mapping of meta keys to their allowed values",
        default_factory=dict,
    )

    # noinspection PyNestedDecorators
    @field_validator("meta", mode="before")
    @classmethod
    def make_meta_values_tuple(cls, meta: dict[str, str | Sequence[str]]) -> dict[str, tuple[str]]:
        """Convert all meta values to tuples"""
        meta = meta.copy()

        for key, val in meta.items():
            if not isinstance(val, Collection) or isinstance(val, str):
                meta[key] = (val,)
            else:
                meta[key] = tuple(val)
        # noinspection PyTypeChecker
        return meta

    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        invalid_meta: dict[str, str] = {}
        expected_meta: dict[str, Collection[str]] = {}

        for key, values in self.meta.items():
            if not isinstance(values, Collection) or isinstance(values, str):
                values = [values]
            if key in item.meta and item.meta[key] not in values:
                invalid_meta[key] = item.meta[key]
                expected_meta[key] = values

        # if invalid_meta:
        #     name = inspect.currentframe().f_code.co_name
        #     message = f"Contains invalid meta values: {invalid_meta} | Accepted values: {expected_meta}"
        #     self._add_result(item, parent=parent, name=name, message=message)

        return not invalid_meta
