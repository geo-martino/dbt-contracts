import pytest

from dbt_contracts.contracts.matchers import RangeMatcher, StringMatcher, PatternMatcher


class TestRangeMatcher:
    def test_checks_range_values_are_valid(self):
        max_min_count_log = "Maximum count must be >= minimum count"
        with pytest.raises(Exception, match=max_min_count_log):
            RangeMatcher(min_count=10, max_count=5)
        with pytest.raises(Exception, match=max_min_count_log):
            RangeMatcher(min_count=12, max_count=2)

    def test_match(self):
        assert RangeMatcher(min_count=1, max_count=10)._match(5, kind="test") is None
        assert RangeMatcher(min_count=5, max_count=5)._match(5, kind="test") is None
        assert RangeMatcher(min_count=4, max_count=6)._match(5, kind="test") is None
        assert RangeMatcher(min_count=6, max_count=6)._match(5, kind="test") is not None
        assert RangeMatcher(min_count=2, max_count=4)._match(5, kind="test") is not None


# noinspection SpellCheckingInspection
class TestStringMatcher:
    def test_match_on_none(self):
        assert StringMatcher()._match(None, None)
        assert not StringMatcher()._match(None, "not none")
        assert not StringMatcher()._match("not none", None)

    def test_match_with_no_processing(self):
        assert not StringMatcher()._match("we are equal", "we are not equal")
        assert StringMatcher()._match("we are equal", "we are equal")

    def test_match_case_insensitive(self):
        assert not StringMatcher(case_insensitive=False)._match("we are equal", "We Are Equal")
        assert StringMatcher(case_insensitive=True)._match("we are equal", "We Are Equal")

    def test_match_ignore_whitespace(self):
        assert not StringMatcher(ignore_whitespace=False)._match("we are equal", "weareequal")
        assert StringMatcher(ignore_whitespace=True)._match("we are equal", "weareequal")

    def test_match_complex(self):
        assert not StringMatcher(compare_start_only=False)._match("we are equal", "we are")
        assert StringMatcher(compare_start_only=True)._match("we are equal", "we are")

        assert StringMatcher(case_insensitive=True, ignore_whitespace=True)._match(
            "we are equal", "WeAreEqual"
        )
        assert StringMatcher(case_insensitive=True, ignore_whitespace=True, compare_start_only=True)._match(
            "we are equal", "WeAre",
        )


class TestPatternMatcher:
    def test_match_on_none_and_no_patterns(self):
        assert not PatternMatcher()._match(None)
        assert not PatternMatcher(include="*")._match(None)
        assert PatternMatcher()._match("i am a value")

    def test_match_includes_all_on_no_include_patterns(self):
        assert PatternMatcher(include=())._match("i am a value")
        assert PatternMatcher(exclude="exclude me")._match("i am a value")

    def test_match_on_simple_patterns(self):
        assert PatternMatcher(include="i am a value")._match("i am a value")
        assert not PatternMatcher(exclude="i am a value")._match("i am a value")

        assert PatternMatcher(include=r".*")._match("i am a value")
        assert not PatternMatcher(exclude=r".*")._match("i am a value")

    def test_match_on_complex_patterns(self):
        assert PatternMatcher(include=[r"i am a \w+", r"i am not a \w+"], match_all=False)._match("i am a value")
        assert not PatternMatcher(include=[r"i am a \w+", r"i am not a \w+"], match_all=True)._match("i am a value")

        assert not PatternMatcher(
            include=[r"i am a \w+", r"i am not a \w+"], exclude=".*value$"
        )._match("i am a value")
        assert PatternMatcher(
            include=r"i am a \w+", exclude=["^this.*" ".*value$"], match_all=True
        )._match("i am a value")
        assert PatternMatcher(
            include=[r"i am a \w+", r"[^\d]+"], exclude=["^this.*" ".*value$"], match_all=True
        )._match("i am a value")
