import re
from collections.abc import Collection


def is_not_in_range(count: int, min_count: int = 1, max_count: int = None) -> tuple[bool, bool]:
    if min_count < 1:
        raise Exception(f"Minimum count must be > 0. Got {min_count}")
    if max_count is not None and max_count < 1:
        raise Exception(f"Maximum count must be > 0. Got {max_count}")
    if max_count is not None and max_count < min_count:
        raise Exception(f"Maximum count must be >= minimum count. Got {max_count} > {min_count}")

    too_small = count < min_count
    too_large = max_count is not None and count > max_count
    return too_small, too_large


def match_strings(
        actual: str | None,
        expected: str | None,
        ignore_whitespace: bool = False,
        case_insensitive: bool = False,
        compare_start_only: bool = False,
) -> bool:
    if not actual or not expected:
        return not actual and not expected

    if ignore_whitespace:
        actual = actual.replace(" ", "")
        expected = expected.replace(" ", "")
    if case_insensitive:
        actual = actual.casefold()
        expected = expected.casefold()

    if compare_start_only:
        match = expected.startswith(actual) or actual.startswith(expected)
    else:
        match = actual == expected

    return match


def match_patterns(
        value: str | None,
        *patterns: str,
        include: Collection[str] | str = (),
        exclude: Collection[str] | str = (),
        match_all: bool = False,
) -> bool:
    if not value:
        return False

    if isinstance(exclude, str):
        exclude = [exclude]

    if exclude:
        if match_all and all(pattern == value or re.match(pattern, value) for pattern in exclude):
            return False
        elif any(pattern == value or re.match(pattern, value) for pattern in exclude):
            return False

    if isinstance(include, str):
        include = [include]
    include += patterns

    if not include:
        return True
    elif match_all:
        return all(pattern == value or re.match(pattern, value) for pattern in include)
    return any(pattern == value or re.match(pattern, value) for pattern in include)
