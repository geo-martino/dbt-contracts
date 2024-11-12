from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any

import pytest
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest

# noinspection PyProtectedMember
from dbt_contracts.contracts._core import filter_method, enforce_method, Contract
from dbt_contracts.types import CombinedT, ParentT


class ContractTester(metaclass=ABCMeta):

    @abstractmethod
    def config(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def manifest(self, available_items: Iterable[CombinedT]) -> Manifest:
        raise NotImplementedError

    @abstractmethod
    def catalog(self, available_items: Iterable[CombinedT]) -> CatalogArtifact:
        raise NotImplementedError

    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> Contract:
        raise NotImplementedError

    @abstractmethod
    def available_items(self) -> Iterable[CombinedT]:
        raise NotImplementedError

    @abstractmethod
    def filtered_items(self, contract: Contract, available_items: Iterable[CombinedT]) -> Iterable[CombinedT]:
        raise NotImplementedError

    @abstractmethod
    def valid_items(self, contract: Contract, filtered_items: Iterable[CombinedT]) -> Iterable[CombinedT]:
        raise NotImplementedError

    @pytest.fixture
    def invalid_items(
            self, filtered_items: Iterable[CombinedT], valid_items: Iterable[CombinedT]
    ) -> Iterable[CombinedT]:
        return [item for item in filtered_items if item not in valid_items]

    @staticmethod
    def mock_method(*args, **kwargs) -> bool:
        return True

    def test_manifest_properties(self, contract: Contract, manifest: Manifest):
        contract._manifest = None
        assert not contract.manifest_is_set
        with pytest.raises(Exception, match="is not set"):
            assert contract.manifest

        contract._manifest = manifest
        assert contract.manifest_is_set
        assert contract.manifest == manifest

        contract._filters.clear()
        contract._enforcements.clear()
        assert not contract.needs_manifest

        contract._filters.append(tuple((filter_method(self.mock_method, needs_manifest=True), {})))
        assert contract.needs_manifest

        contract._filters.clear()
        contract._enforcements.append(tuple((enforce_method(self.mock_method, needs_manifest=True), {})))
        assert contract.needs_manifest

    def test_catalog_properties(self, contract: Contract, catalog: CatalogArtifact):
        contract._catalog = None
        assert not contract.catalog_is_set
        with pytest.raises(Exception, match="is not set"):
            assert contract.catalog

        contract._catalog = catalog
        assert contract.catalog_is_set
        assert contract.catalog == catalog

        contract._filters.clear()
        contract._enforcements.clear()
        assert not contract.needs_catalog

        contract._filters.append(tuple((filter_method(self.mock_method, needs_catalog=True), {})))
        assert contract.needs_catalog

        contract._filters.clear()
        contract._enforcements.append(tuple((enforce_method(self.mock_method, needs_catalog=True), {})))
        assert contract.needs_catalog

    @staticmethod
    def test_call_methods_with_no_methods(contract: Contract, filtered_items: Iterable[CombinedT]):
        for item in filtered_items:
            assert contract._call_methods(item, ())  # defaults to True

    @staticmethod
    def test_call_methods_with_no_args(contract: Contract, filtered_items: Iterable[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 1 + arg_offset
            assert not kwargs

            assert args[0] == contract
            item = args[1:arg_offset + 1]
            if len(item) == 1:
                assert item[0] in filtered_items
            else:
                assert item in filtered_items

            return True

        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, None))])

    @staticmethod
    def test_call_methods_with_single_arg(contract: Contract, filtered_items: Iterable[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 2 + arg_offset
            assert not kwargs

            assert args[0] == contract
            item = args[1:arg_offset + 1]
            if len(item) == 1:
                assert item[0] in filtered_items
            else:
                assert item in filtered_items

            assert isinstance(args[arg_offset + 1], expected.__class__)
            assert args[arg_offset + 1] == expected

            return True

        expected = "I am an argument value"
        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, expected))])

        expected = 123
        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, expected))])

    @staticmethod
    def test_call_methods_with_mapping_args(contract: Contract, filtered_items: Iterable[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 1 + arg_offset
            assert kwargs

            assert args[0] == contract
            item = args[1:arg_offset + 1]
            if len(item) == 1:
                assert item[0] in filtered_items
            else:
                assert item in filtered_items

            assert kwargs == expected

            return True

        expected = dict(param1="value1", param2="value2")
        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, expected))])

    @staticmethod
    def test_call_methods_with_iterable_args(contract: Contract, filtered_items: Iterable[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 1 + arg_offset + len(expected)
            assert not kwargs

            assert args[0] == contract
            item = args[1:arg_offset + 1]
            if len(item) == 1:
                assert item[0] in filtered_items
            else:
                assert item in filtered_items

            assert list(args[arg_offset + 1:]) == list(expected)

            return True

        expected = ["arg1", "arg2"]
        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, expected))])

    @staticmethod
    def test_add_result(contract: Contract, filtered_items: Iterable[CombinedT]):
        item = next(iter(filtered_items))
        parent = None
        if isinstance(item, tuple):
            parent = item[1]
            item = item[0]

        expected_name = "test_name"
        expected_message = "this test has failed"
        contract._add_result(item=item, parent=parent, name=expected_name, message=expected_message)

        assert any(result.name == item.name for result in contract.results)
        assert any(result.result_name == expected_name for result in contract.results)
        assert any(result.message == expected_message for result in contract.results)

    @staticmethod
    def test_filter_items(
            contract: Contract,
            available_items: Iterable[CombinedT],
            filtered_items: Iterable[CombinedT],
    ):
        for item in available_items:
            if contract._apply_filters(item):
                assert item in filtered_items
            else:
                assert item not in filtered_items

        assert list(contract._filter_items(available_items)) == list(filtered_items)
        assert list(contract.items) == list(filtered_items)

    @staticmethod
    def test_enforce_contract(
            contract: Contract,
            filtered_items: Iterable[CombinedT],
            valid_items: Iterable[CombinedT],
            invalid_items: Iterable[CombinedT],
    ):
        for item in filtered_items:
            if contract._apply_enforcements(item):
                assert item in valid_items
            else:
                assert item not in valid_items

        assert list(contract._enforce_contract_on_items()) == invalid_items
        assert contract.run() == invalid_items
        assert contract() == invalid_items

    def test_from_dict(self, contract: Contract, config: dict[str, Any], manifest: Manifest, catalog: CatalogArtifact):
        def _get_key_args(conf: str | Mapping[str, Any]) -> tuple[str, Any]:
            if isinstance(conf, Mapping):
                return next(iter(conf.items()))
            return conf, None

        contract = contract.from_dict(config, manifest=manifest, catalog=catalog)

        assert all(func.is_filter for func in contract.filters)
        assert all(func.is_enforcement for func in contract.filters)

        test_map = {
            "filter": contract.filters,
            "enforce": contract.enforcements,
        }
        for key, methods in test_map.items():
            for conf in config[key]:
                name, args = _get_key_args(conf)
                assert any(func.name == name for func, args in methods)
                assert next(iter(args for _, args in methods)) == args


class ChildContractTester(ContractTester, metaclass=ABCMeta):

    @abstractmethod
    def parents(self) -> Iterable[ParentT]:
        raise NotImplementedError

    # noinspection PyMethodOverriding
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parents: Iterable[ParentT]) -> Iterable[CombinedT]:
        raise NotImplementedError

    # noinspection PyMethodOverriding
    @abstractmethod
    def available_items(self, parents: Iterable[ParentT]) -> Iterable[CombinedT]:
        raise NotImplementedError

    def test_filter_on_name(self, contract: Contract):
        pass


class ParentContractTester(ContractTester, metaclass=ABCMeta):

    def test_filter_on_name(self, contract: Contract):
        pass


class CatalogContractTester(ContractTester, metaclass=ABCMeta):

    def test_get_matching_catalog_table(
            self, contract: Contract, catalog: CatalogArtifact, valid_items: Iterable[CombinedT]
    ):
        pass