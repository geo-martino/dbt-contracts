import re
from collections.abc import Collection, Sequence
from typing import Annotated, Any, Self

from pydantic import BaseModel, Field, BeforeValidator, model_validator


def to_tuple(value: Any) -> tuple:
    if value is None:
        return tuple()
    elif isinstance(value, tuple):
        return value
    elif isinstance(value, str):
        value = (value,)
    return tuple(value)


class RangeMatcher(BaseModel):
    min_count: int = Field(
        description="The minimum count allowed.",
        ge=1,
        default=1
    )
    max_count: int | None = Field(
        description="The maximum count allowed.",
        gt=0,
        default=None,
    )

    @model_validator(mode="after")
    def validate_max_count(self) -> Self:
        """Ensure that the maximum count is >= the minimum count."""
        if self.max_count is not None and self.max_count < self.min_count:
            raise Exception(f"Maximum count must be >= minimum count. Got {self.max_count} > {self.min_count}")
        return self

    def _match(self, count: int) -> tuple[bool, bool]:
        too_small = count < self.min_count
        too_large = self.max_count is not None and count > self.max_count
        return too_small, too_large


class StringMatcher(BaseModel):
    ignore_whitespace: bool = Field(
        description="Ignore any whitespaces when comparing data type keys.",
        default=False,
    )
    case_insensitive: bool = Field(
        description="Ignore cases and compare data type keys only case-insensitively.",
        default=False,
    )
    compare_start_only: bool = Field(
        description=(
            "Match data type keys when the two values start with the same value. "
            "Ignore the rest of the data type definition in this case."
        ),
        default=False,
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


class PatternMatcher(BaseModel):
    include: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="Patterns to match against for values to include",
        default=tuple(),
    )
    exclude: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="Patterns to match against for values to exclude",
        default=tuple(),
    )
    match_all: bool = Field(
        description="When True, all given patterns must match to be considered a match for either pattern type",
        default=False,
    )

    def _match(self, value: str | None) -> bool | None:
        if not value:
            return False
        if not self.include and not self.exclude:
            return True

        if self.exclude:
            if self.match_all and all(pattern == value or re.match(pattern, value) for pattern in self.exclude):
                return False
            elif any(pattern == value or re.match(pattern, value) for pattern in self.exclude):
                return False

        if self.match_all:
            return all(pattern == value or re.match(pattern, value) for pattern in self.include)
        return any(pattern == value or re.match(pattern, value) for pattern in self.include)
