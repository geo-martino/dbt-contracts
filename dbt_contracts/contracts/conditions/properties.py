from collections.abc import Sequence, Collection, Mapping
from copy import copy
from pathlib import Path
from typing import Annotated

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource, ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from pydantic import BeforeValidator, Field, field_validator

from dbt_contracts.contracts.conditions._core import ContractCondition
from dbt_contracts.contracts.matchers import PatternMatcher
from dbt_contracts.contracts.utils import to_tuple
from dbt_contracts.types import ItemT, TagT, MetaT


class NameCondition(ContractCondition[ItemT], PatternMatcher):
    """Filter {kind} based on their names."""
    def run(self, item: (BaseResource, ColumnInfo, MacroArgument)) -> bool:
        return self._match(item.name)


class PathCondition(ContractCondition[BaseResource], PatternMatcher):
    """
    Filter {kind} based on their paths.
    Paths must match patterns which are relative to the root directory of the dbt project.

    __EXAMPLE__

    You may define the paths as a list of lists where each part is a subdirectory within the path.
    These parts will then be unified by joining them with the os-specific path separator.
    This allows for you define os-independent configuration as needed.

    .. code-block:: yaml

        include:
        - ["path", "to", "folder1"]
        - ["path", "to", "folder2"]
        - ["path", "to", "folder3"]
        exclude:
        - ["path", "to", "another", "folder1"]
        - ["path", "to", "another", "folder2"]
        - ["path", "to", "another", "folder3"]
    """
    # noinspection PyNestedDecorators
    @field_validator("include", "exclude", mode="after", check_fields=True)
    @classmethod
    def escape_backslashes_in_windows_paths[T: Sequence[str]](cls, values: T) -> T:
        """
        Replace all single backslashes with double backslashes in Windows paths.

        This is needed to ensure regex patterns are correctly interpreted and avoid any 'bad escape' errors.
        """
        return values.__class__(path.replace("\\", "\\\\") for path in values)

    # noinspection PyNestedDecorators
    @field_validator("include", "exclude", mode="before")
    @classmethod
    def unify_chunked_path_values(cls, values: str | Sequence[str] | Sequence[Sequence[str]]) -> tuple[str, ...]:
        """
        Unify all path values into a tuple of strings.
        Also merges path parts into a single path with the os-specific separator.
        """
        if isinstance(values, str):
            return (values,)
        if not isinstance(values, Sequence):
            raise Exception(f"Unrecognised path types given: {values}")

        paths = []
        for value in values:
            if isinstance(value, str):
                paths.append(value)
                continue
            if not isinstance(value, Sequence):
                raise Exception(f"Unrecognised path types given: {values}")
            paths.append(str(Path(*value)))

        return tuple(paths)

    def run(self, item: BaseResource) -> bool:
        paths = [item.original_file_path, item.path]
        if isinstance(item, ParsedResource) and item.patch_path:
            paths.append(item.patch_path.split("://")[1])

        paths = [path.replace("\\", "\\\\") for path in paths]
        return self._match_values(paths)


class TagCondition(ContractCondition[TagT]):
    """Filter {kind} based on their tags."""
    tags: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The tags to match on",
        default=tuple(),
        examples=["tag1", ["tag1", "tag2"]],
    )

    def run(self, item: ParsedResource | ColumnInfo) -> bool:
        return not self.tags or any(tag in self.tags for tag in item.tags)


class MetaCondition(ContractCondition[MetaT]):
    """Filter {kind} based on their meta values."""
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

    def run(self, item: ParsedResource | ColumnInfo) -> bool:
        def _match(key: str) -> bool:
            values = self.meta[key]
            if not isinstance(values, Collection) or isinstance(values, str):
                values = [values]
            return key in item.meta and item.meta[key] in values

        return not self.meta or all(map(_match, self.meta))


class IsMaterializedCondition(ContractCondition[ParsedResource]):
    """Filter {kind} taking only those which are not ephemeral."""
    def run(self, item: ParsedResource) -> bool:
        return item.config.materialized != "ephemeral"
