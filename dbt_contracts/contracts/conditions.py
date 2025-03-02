from collections.abc import Sequence, Collection, Mapping
from copy import copy
from typing import Annotated

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource, ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import SourceDefinition
from pydantic import BeforeValidator, Field, field_validator

from dbt_contracts.contracts._core import ContractCondition
from dbt_contracts.contracts.matchers import PatternMatcher
from dbt_contracts.contracts.utils import to_tuple
from dbt_contracts.types import ItemT, TagT, MetaT


class NameCondition(ContractCondition[ItemT], PatternMatcher):
    def validate(self, item: (BaseResource, ColumnInfo, MacroArgument)) -> bool:
        return self._match(item.name)


class PathCondition(ContractCondition[BaseResource], PatternMatcher):
    def validate(self, item: BaseResource) -> bool:
        paths = [item.original_file_path, item.path]
        if isinstance(item, ParsedResource) and item.patch_path:
            paths.append(item.patch_path.split("://")[1])
        return any(map(self._match, paths))


class TagCondition(ContractCondition[TagT]):
    tags: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The tags to match on",
        default=tuple(),
    )

    def validate(self, item: ParsedResource | ColumnInfo) -> bool:
        return not self.tags or any(tag in self.tags for tag in item.tags)


class MetaCondition(ContractCondition[MetaT]):
    meta: Mapping[str, Sequence[str]] = Field(
        description="The mapping of meta keys to their allowed values",
        default_factory=dict,
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

    def validate(self, item: ParsedResource | ColumnInfo) -> bool:
        def _match(key: str) -> bool:
            values = self.meta[key]
            if not isinstance(values, Collection) or isinstance(values, str):
                values = [values]
            return key in item.meta and item.meta[key] in values

        return not self.meta or all(map(_match, self.meta))


class IsMaterializedCondition(ContractCondition[ParsedResource]):
    def validate(self, item: ParsedResource) -> bool:
        return item.config.materialized != "ephemeral"


class IsEnabledCondition(ContractCondition[SourceDefinition]):
    def validate(self, item: SourceDefinition) -> bool:
        return item.config.enabled
