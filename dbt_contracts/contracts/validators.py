from collections.abc import Sequence, Collection
from typing import Annotated

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource, ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import SourceDefinition
from pydantic import BeforeValidator, Field, field_validator

from dbt_contracts.contracts._core import ResourceValidator
from dbt_contracts.contracts._matchers import PatternMatcher, to_tuple


class NameValidator(ResourceValidator[BaseResource | ColumnInfo | MacroArgument], PatternMatcher):
    def validate(self, item: (BaseResource, ColumnInfo, MacroArgument)) -> bool:
        return self._match(item.name)


class PathValidator(ResourceValidator[BaseResource], PatternMatcher):
    def validate(self, item: BaseResource) -> bool:
        paths = [item.original_file_path, item.path]
        if isinstance(item, ParsedResource) and item.patch_path:
            paths.append(item.patch_path.split("://")[1])
        print(paths, list(map(self._match, paths)))
        return any(map(self._match, paths))


class TagValidator(ResourceValidator[ParsedResource | ColumnInfo]):
    tags: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The tags to match on",
        default=tuple(),
    )

    def validate(self, item: ParsedResource | ColumnInfo) -> bool:
        return not self.tags or any(tag in self.tags for tag in item.tags)


class MetaValidator(ResourceValidator[ParsedResource | ColumnInfo]):
    meta: dict[str, Sequence[str]] = Field(
        description="A map of the accepted values for each meta key",
        default=dict(),
    )

    # noinspection PyNestedDecorators
    @field_validator("meta", mode="before")
    @classmethod
    def make_meta_values_tuple(cls, meta: dict[str, str | Sequence[str]]) -> dict[str, tuple[str]]:
        meta = meta.copy()

        for key, val in meta.items():
            if not isinstance(val, Collection) or isinstance(val, str):
                meta[key] = (val,)
            else:
                meta[key] = tuple(val)
        return meta

    def validate(self, item: ParsedResource | ColumnInfo) -> bool:
        def _match(key: str) -> bool:
            values = self.meta[key]
            if not isinstance(values, Collection) or isinstance(values, str):
                values = [values]
            return key in item.meta and item.meta[key] in values

        return not self.meta or any(map(_match, self.meta))


class IsMaterializedValidator(ResourceValidator[ParsedResource]):
    def validate(self, item: ParsedResource) -> bool:
        return item.config.materialized != "ephemeral"


class IsEnabledValidator(ResourceValidator[SourceDefinition]):
    def validate(self, item: SourceDefinition) -> bool:
        return item.config.enabled
