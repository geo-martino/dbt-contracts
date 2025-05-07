import re
from collections.abc import Sequence, Collection
from typing import Annotated, Self

from pydantic import Field, BeforeValidator, model_validator

from dbt_contracts._core import BaseModelConfig
from dbt_contracts.contracts.utils import to_tuple


class RangeMatcher(BaseModelConfig):
    min_count: int = Field(
        description="The minimum count allowed.",
        ge=1,
        default=1,
        examples=[1, 2, 3],
    )
    max_count: int | None = Field(
        description="The maximum count allowed.",
        gt=0,
        default=None,
        examples=[4, 5, 6],
    )

    @model_validator(mode="after")
    def validate_max_count(self) -> Self:
        """Ensure that the maximum count is >= the minimum count."""
        if self.max_count is not None and self.max_count < self.min_count:
            raise Exception(f"Maximum count must be >= minimum count. Got {self.max_count} > {self.min_count}")
        return self

    def _match(self, count: int, kind: str) -> str | None:
        too_small = count < self.min_count
        too_large = self.max_count is not None and count > self.max_count
        if not too_small and not too_large:
            return

        quantifier = 'few' if too_small else 'many'
        expected = self.min_count if too_small else self.max_count
        return f"Too {quantifier} {kind} found: {count}. Expected: {expected}."


class StringMatcher(BaseModelConfig):
    ignore_whitespace: bool = Field(
        description="Ignore any whitespaces when comparing data type keys.",
        default=False,
        examples=[True, False],
    )
    case_insensitive: bool = Field(
        description="Ignore cases and compare data type keys only case-insensitively.",
        default=False,
        examples=[True, False],
    )
    compare_start_only: bool = Field(
        description=(
            "Match data type keys when the two values start with the same value. "
            "Ignore the rest of the data type definition in this case."
        ),
        default=False,
        examples=[True, False],
    )

    def _match(self, actual: str | None, expected: str | None) -> bool:
        if not actual or not expected:
            return not actual and not expected

        if self.ignore_whitespace:
            actual = actual.replace(" ", "")
            expected = expected.replace(" ", "")
        if self.case_insensitive:
            actual = actual.casefold()
            expected = expected.casefold()

        if self.compare_start_only:
            match = expected.startswith(actual) or actual.startswith(expected)
        else:
            match = actual == expected

        return match


class PatternMatcher(BaseModelConfig):
    include: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="Patterns to match against for values to include",
        default=tuple(),
        examples=[r".*i\s+am\s+a\s+regex\s+pattern.*", [r"^\w+\d+\s{1,3}$", "include[_-]this"]],
    )
    exclude: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="Patterns to match against for values to exclude",
        default=tuple(),
        examples=[r".*i\s+am\s+a\s+regex\s+pattern.*", [r"^\w+\d+\s{1,3}$", "exclude[_-]this"]],
    )
    match_all: bool = Field(
        description="When True, all given patterns must match to be considered a match for either pattern type",
        default=False,
        examples=[True, False],
    )

    def _match(self, value: str | None) -> bool:
        if not value:
            return False
        if not self.include and not self.exclude:
            return True

        if self._match_exclude(value):
            return False
        return self._match_include(value)

    def _match_values(self, values: Collection[str | None]) -> bool:
        if not self.include and not self.exclude:
            return True

        if any(map(self._match_exclude, values)):
            return False
        return any(map(self._match_include, values))

    def _match_exclude(self, value: str | None) -> bool:
        if not value:
            return True
        if not self.exclude:
            return False

        if self.match_all and all(pattern == value or re.match(pattern, value) for pattern in self.exclude):
            return True
        if any(pattern == value or re.match(pattern, value) for pattern in self.exclude):
            return True
        return False

    def _match_include(self, value: str) -> bool:
        if not value:
            return True
        if not self.include:
            return True  # include all when no include patterns are given

        if self.match_all:
            return all(pattern == value or re.match(pattern, value) for pattern in self.include)
        return any(pattern == value or re.match(pattern, value) for pattern in self.include)
