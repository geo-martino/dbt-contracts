import pytest

# noinspection PyProtectedMember
from dbt_contracts.contracts._comparisons import is_not_in_range, match_strings, match_patterns


def test_is_not_in_range_checks_range_values_are_valid():
    count_log = "count must be > 0"
    with pytest.raises(Exception, match=f"Minimum {count_log}"):
        is_not_in_range(12, min_count=0, max_count=10)
    with pytest.raises(Exception, match=f"Minimum {count_log}"):
        is_not_in_range(20, min_count=-1, max_count=1)

    with pytest.raises(Exception, match=f"Maximum {count_log}"):
        is_not_in_range(12, min_count=10, max_count=0)
    with pytest.raises(Exception, match=f"Maximum {count_log}"):
        is_not_in_range(20, min_count=12, max_count=-1)

    max_min_count_log = "Maximum count must be >= minimum count"
    with pytest.raises(Exception, match=max_min_count_log):
        is_not_in_range(12, min_count=10, max_count=5)
    with pytest.raises(Exception, match=max_min_count_log):
        is_not_in_range(20, min_count=12, max_count=2)


def test_is_not_in_range():
    assert is_not_in_range(5, min_count=1, max_count=10) == (False, False)
    assert is_not_in_range(5, min_count=5, max_count=5) == (False, False)
    assert is_not_in_range(5, min_count=4, max_count=6) == (False, False)
    assert is_not_in_range(5, min_count=6, max_count=6) == (True, False)
    assert is_not_in_range(5, min_count=2, max_count=4) == (False, True)


# noinspection SpellCheckingInspection
def test_match_strings():
    # conditions when one or more values are None
    assert match_strings(None, None)
    assert not match_strings(None, "not none")
    assert not match_strings("not none", None)

    assert not match_strings("we are equal", "we are not equal")
    assert match_strings("we are equal", "we are equal")

    assert not match_strings("we are equal", "We Are Equal", case_insensitive=False)
    assert match_strings("we are equal", "We Are Equal", case_insensitive=True)

    assert not match_strings("we are equal", "weareequal", ignore_whitespace=False)
    assert match_strings("we are equal", "weareequal", ignore_whitespace=True)

    assert not match_strings("we are equal", "we are", compare_start_only=False)
    assert match_strings("we are equal", "we are", compare_start_only=True)

    assert match_strings(
        "we are equal", "WeAreEqual", case_insensitive=True, ignore_whitespace=True
    )
    assert match_strings(
        "we are equal", "WeAre", case_insensitive=True, ignore_whitespace=True, compare_start_only=True
    )


def test_match_patterns():
    # conditions when value is None or no patterns given
    assert not match_patterns(None)
    assert not match_patterns(None, ".*")
    assert match_patterns("i am a value")

    assert match_patterns("i am a value", "i am a value")
    assert match_patterns("i am a value", include="i am a value")
    assert match_patterns("i am a value", include=["i am a value"])
    assert not match_patterns("i am a value", exclude="i am a value")
    assert not match_patterns("i am a value", exclude=["i am a value"])

    assert match_patterns("i am a value", r".*")
    assert match_patterns("i am a value", include=r".*")
    assert match_patterns("i am a value", include=[r".*"])
    assert not match_patterns("i am a value", exclude=r".*")
    assert not match_patterns("i am a value", exclude=[r".*"])

    # more complex combinations
    assert match_patterns("i am a value", r"i am a \w+", r"i am not a \w+")
    assert not match_patterns(
        "i am a value", r"i am a \w+", r"i am not a \w+", match_all=True
    )
    assert not match_patterns("i am a value", r"i am also a \w+", r"i am not a \w+")

    assert match_patterns(
        "i am a value", r"i am also a \w+", r"i am not a \w+", include=r"i am a \w+"
    )
    assert match_patterns(
        "i am a value", r"i am also a \w+", r"i am not a \w+", include=[r"not me \w+", r"i am a \w+"]
    )
    assert not match_patterns(
        "i am a value", r"i am also a \w+", r"i am not a \w+", include=r"i am a \w+", match_all=True
    )

    assert not match_patterns(
        "i am a value", r"i am a \w+", include=r"i am not a \w+", exclude=".*value$"
    )
    assert match_patterns(
        "i am a value", r"i am a \w+", exclude=["^this.*" ".*value$"], match_all=True
    )
    assert match_patterns(
        "i am a value", r"i am a \w+", r"[^\d]+", exclude=["^this.*" ".*value$"], match_all=True
    )
