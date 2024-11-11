from collections.abc import Collection, Mapping
from typing import Any

import pytest

from dbt_contracts.contracts._core import filter_method, enforce_method, ProcessorMethod


class TestProcessorMethod:

    @filter_method
    def decorated_filter_method(self, *args, **kwargs) -> tuple[Collection[Any], Mapping[str, Any]]:
        return args, kwargs

    @filter_method(needs_manifest=False, needs_catalog=True)
    def decorated_filter_method_with_args(self, *args, **kwargs) -> tuple[Collection[Any], Mapping[str, Any]]:
        return args, kwargs

    @enforce_method
    def decorated_enforcement_method(self, *args, **kwargs) -> tuple[Collection[Any], Mapping[str, Any]]:
        return args, kwargs

    @enforce_method(needs_manifest=False, needs_catalog=True)
    def decorated_enforcement_method_with_args(self, *args, **kwargs) -> tuple[Collection[Any], Mapping[str, Any]]:
        return args, kwargs

    @pytest.fixture(scope="class")
    def decorated_methods(self) -> list[ProcessorMethod]:
        # noinspection PyUnboundLocalVariable
        return [
            obj for attr in dir(self)
            if attr.startswith("decorated_")
            and callable(obj := getattr(self, attr))
            and isinstance(obj, ProcessorMethod)
        ]

    @pytest.fixture(scope="class")
    def filter_methods(self, decorated_methods: list[ProcessorMethod]) -> list[ProcessorMethod]:
        # noinspection PyUnboundLocalVariable
        return [method for method in decorated_methods if method.is_filter]

    @pytest.fixture(scope="class")
    def enforce_methods(self, decorated_methods: list[ProcessorMethod]) -> list[ProcessorMethod]:
        # noinspection PyUnboundLocalVariable
        return [method for method in decorated_methods if method.is_enforcement]

    def test_methods_are_wrapped(self, decorated_methods: list[ProcessorMethod]):
        assert len(decorated_methods) == 4

        for attr in dir(self):
            if not attr.startswith("decorated_") or not callable(obj := getattr(self, attr)):
                continue
            if "__pytest_wrapped__" in dir(obj):
                continue

            assert isinstance(obj, ProcessorMethod)

    def test_processor_methods(self, filter_methods: list[ProcessorMethod]):
        args = ("arg1", "arg2")
        kwargs = {"arg3": "val1", "arg4": 2}

        for method in filter_methods:
            assert method(*args, **kwargs) == (args, kwargs)
            assert isinstance(method.instance, self.__class__)

    def test_filter_methods(self, filter_methods: list[ProcessorMethod]):
        for method in filter_methods:
            assert method.is_filter
            assert not method.is_enforcement

        assert sum(method.needs_manifest for method in filter_methods) == 1
        assert sum(method.needs_catalog for method in filter_methods) == 1

    def test_enforce_methods(self, enforce_methods: list[ProcessorMethod]):
        for method in enforce_methods:
            assert method.is_enforcement
            assert not method.is_filter

        assert sum(method.needs_manifest for method in enforce_methods) == 1
        assert sum(method.needs_catalog for method in enforce_methods) == 1
