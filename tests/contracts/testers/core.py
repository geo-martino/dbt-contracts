from abc import ABCMeta, abstractmethod
from collections.abc import Mapping
from random import choice
from typing import Any

import pytest
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt_contracts.contracts import ChildContract, ParentContract

# noinspection PyProtectedMember
from dbt_contracts.contracts._core import filter_method, enforce_method, Contract
from dbt_contracts.types import CombinedT, ChildT, ParentT


class ContractTester(metaclass=ABCMeta):

    @abstractmethod
    def config(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def manifest(self, available_items: list[CombinedT]) -> Manifest:
        raise NotImplementedError

    @abstractmethod
    def catalog(self, available_items: list[CombinedT]) -> CatalogArtifact:
        raise NotImplementedError

    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> Contract:
        raise NotImplementedError

    @abstractmethod
    def available_items(self) -> list[CombinedT]:
        raise NotImplementedError

    @abstractmethod
    def filtered_items(self, contract: Contract, available_items: list[CombinedT]) -> list[CombinedT]:
        raise NotImplementedError

    @abstractmethod
    def valid_items(self, contract: Contract, filtered_items: list[CombinedT]) -> list[CombinedT]:
        raise NotImplementedError

    @pytest.fixture
    def valid_item(self, valid_items: list[CombinedT]) -> CombinedT:
        return choice(list(valid_items))

    @pytest.fixture
    def invalid_items(
            self, filtered_items: list[CombinedT], valid_items: list[CombinedT]
    ) -> list[CombinedT]:
        return [item for item in filtered_items if item not in valid_items]

    @pytest.fixture
    def invalid_item(self, invalid_items: list[CombinedT]) -> CombinedT:
        return choice(list(invalid_items))

    @staticmethod
    def mock_method(*args, **kwargs) -> bool:
        return True

    @staticmethod
    def assert_result(contract: Contract, item: ChildT, parent: ParentT, name: str, message: str):
        assert any(result.name == item.name for result in contract.results)
        assert any(result.result_name == name for result in contract.results)
        assert any(result.message == message for result in contract.results)

    def test_method_name_store(self, contract: Contract):
        assert contract.__filtermethods__
        assert contract.__class__.__filtermethods__
        assert contract.__enforcementmethods__
        assert contract.__class__.__enforcementmethods__

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
    def test_call_methods_with_no_methods(contract: Contract, filtered_items: list[CombinedT]):
        for item in filtered_items:
            assert contract._call_methods(item, [])  # defaults to True

    @staticmethod
    def test_call_methods_with_no_args(contract: Contract, filtered_items: list[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 1 + arg_offset
            assert not kwargs

            assert args[0] == contract
            it = args[1:arg_offset + 1]
            if len(it) == 1:
                assert it[0] in filtered_items
            else:
                assert it in filtered_items

            return True

        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, None))])

    @staticmethod
    def test_call_methods_with_single_arg(contract: Contract, filtered_items: list[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 2 + arg_offset
            assert not kwargs

            assert args[0] == contract
            it = args[1:arg_offset + 1]
            if len(it) == 1:
                assert it[0] in filtered_items
            else:
                assert it in filtered_items

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
    def test_call_methods_with_mapping_args(contract: Contract, filtered_items: list[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 1 + arg_offset
            assert kwargs

            assert args[0] == contract
            it = args[1:arg_offset + 1]
            if len(it) == 1:
                assert it[0] in filtered_items
            else:
                assert it in filtered_items

            assert kwargs == expected

            return True

        expected = dict(param1="value1", param2="value2")
        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, expected))])

    @staticmethod
    def test_call_methods_with_iterable_args(contract: Contract, filtered_items: list[CombinedT]):
        @enforce_method
        def _test_call(*args, **kwargs) -> bool:
            assert len(args) == 1 + arg_offset + len(expected)
            assert not kwargs

            assert args[0] == contract
            it = args[1:arg_offset + 1]
            if len(it) == 1:
                assert it[0] in filtered_items
            else:
                assert it in filtered_items

            assert list(args[arg_offset + 1:]) == list(expected)

            return True

        expected = ["arg1", "arg2"]
        for item in filtered_items:
            arg_offset = len(item) if isinstance(item, tuple) else 1
            assert contract._call_methods(item, [tuple((_test_call, expected))])

    def test_add_result(self, contract: Contract, valid_item: CombinedT):
        if isinstance(valid_item, tuple):
            parent = valid_item[1]
            item = valid_item[0]
        else:
            parent = None
            item = valid_item

        expected_name = "test_name"
        expected_message = "this test has failed"
        contract._add_result(item=item, parent=parent, name=expected_name, message=expected_message)

        self.assert_result(contract, item=item, parent=parent, name=expected_name, message=expected_message)

    @staticmethod
    def test_filter_items(
            contract: Contract, available_items: list[CombinedT], filtered_items: list[CombinedT],
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
            filtered_items: list[CombinedT],
            valid_items: list[CombinedT],
            invalid_items: list[CombinedT],
    ):
        for item in filtered_items:
            if contract._apply_enforcements(item):
                assert item in valid_items
            else:
                assert item not in valid_items

        assert list(contract._enforce_contract_on_items()) == invalid_items
        assert contract.run() == invalid_items
        assert contract() == invalid_items

    @staticmethod
    @pytest.mark.skip(reason="Not yet implemented")
    def test_enforce_contract_limited(
            contract: Contract,
            filtered_items: list[CombinedT],
            valid_items: list[CombinedT],
            invalid_items: list[CombinedT],
    ):
        pass  # TODO: run with enforcements named

    @staticmethod
    def test_from_dict(contract: Contract, config: dict[str, Any], manifest: Manifest, catalog: CatalogArtifact):
        def _get_key_args(cnf: str | Mapping[str, Any]) -> tuple[str, Any]:
            if isinstance(cnf, Mapping):
                return next(iter(cnf.items()))
            return cnf, None

        contract = contract.from_dict(config, manifest=manifest, catalog=catalog)

        assert all(func.is_filter for func, _ in contract.filters)
        assert all(func.is_enforcement for func, _ in contract.enforcements)

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
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> ParentContract:
        raise NotImplementedError

    # noinspection PyMethodOverriding
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: ParentContract) -> ChildContract:
        raise NotImplementedError

    @staticmethod
    def test_get_parent_items(contract: ChildContract, parent: ParentContract):
        contract = contract.__class__(parents=parent)
        assert contract._parents == parent
        assert list(contract.parents) == list(parent.items)

        parent_items = parent.items
        contract = contract.__class__(parents=parent_items)
        assert contract._parents == parent_items
        assert list(contract.parents) == list(parent.items)
        assert contract._parents == list(parent.items)

    @staticmethod
    def test_from_dict_sets_artifacts_from_parent(
            contract: ChildContract, config: dict[str, Any], parent: ParentContract
    ):
        contract: ChildContract = contract.from_dict(config, parents=parent)
        assert contract._manifest == parent.manifest
        assert contract._catalog == parent.catalog
        assert contract._parents == parent

        parent.manifest = None
        parent.catalog = None
        contract: ChildContract = contract.from_dict(config, parents=parent)
        assert contract._manifest is None
        assert contract._catalog is None
        assert contract._parents == parent

    @staticmethod
    @pytest.mark.skip(reason="Not yet implemented")
    def test_filter_on_name(contract: ChildContract):
        pass


class ParentContractTester(ContractTester, metaclass=ABCMeta):

    @abstractmethod
    def config_with_child(self, config: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def child(self, manifest: Manifest, catalog: CatalogArtifact) -> ChildContract:
        raise NotImplementedError

    @pytest.fixture
    def contract_with_child(self, contract: ParentContract, child: ChildContract) -> ParentContract:
        contract = contract.__class__(
            manifest=contract.manifest,
            catalog=contract.catalog,
            filters=contract.filters,
            enforcements=contract.enforcements,
        )
        contract._child = child
        return contract

    def test_child_manifest_properties(self, contract_with_child: ParentContract, manifest: Manifest):
        contract_with_child.child.manifest = None
        assert not contract_with_child.manifest_is_set

        contract_with_child.manifest = manifest
        assert contract_with_child.child.manifest == manifest
        assert contract_with_child.manifest_is_set

        contract_with_child._filters.clear()
        contract_with_child.child._filters.clear()
        contract_with_child._enforcements.clear()
        contract_with_child.child._enforcements.clear()
        assert not contract_with_child.needs_manifest

        contract_with_child.child._filters.append(tuple((filter_method(self.mock_method, needs_manifest=True), {})))
        assert contract_with_child.needs_manifest

        contract_with_child.child._filters.clear()
        contract_with_child.child._enforcements.append(
            tuple((enforce_method(self.mock_method, needs_manifest=True), {}))
        )
        assert contract_with_child.needs_manifest

    def test_child_catalog_properties(self, contract_with_child: ParentContract, catalog: CatalogArtifact):
        contract_with_child.child.catalog = None
        assert not contract_with_child.catalog_is_set

        contract_with_child.catalog = catalog
        assert contract_with_child.child.catalog == catalog
        assert contract_with_child.catalog_is_set

        contract_with_child._filters.clear()
        contract_with_child.child._filters.clear()
        contract_with_child._enforcements.clear()
        contract_with_child.child._enforcements.clear()
        assert not contract_with_child.needs_catalog

        contract_with_child.child._filters.append(tuple((filter_method(self.mock_method, needs_catalog=True), {})))
        assert contract_with_child.needs_catalog

        contract_with_child.child._filters.clear()
        contract_with_child.child._enforcements.append(
            tuple((enforce_method(self.mock_method, needs_catalog=True), {}))
        )
        assert contract_with_child.needs_catalog

    @staticmethod
    def test_set_child(
            contract: ParentContract, child: ChildContract
    ):
        assert contract.child is None
        contract.set_child(child.filters, child.enforcements)

        assert contract.child is not None
        assert contract.child.filters == child.filters
        assert contract.child.enforcements == child.enforcements

    @staticmethod
    def test_set_child_from_dict(
            contract: ParentContract,
            config: dict[str, Any],
            config_with_child: dict[str, Any],
    ):
        contract: ParentContract = contract.from_dict(config)
        assert contract.child is None

        contract = contract.from_dict(config_with_child)
        assert contract.child is not None

    @staticmethod
    @pytest.mark.skip(reason="Not yet implemented")
    def test_runs_child(contract: ParentContract, contract_with_child: ParentContract):
        pass  # TODO: also does not run child when flagged

    @staticmethod
    @pytest.mark.skip(reason="Not yet implemented")
    def test_filter_on_name(contract: ParentContract):
        pass  # TODO


class CatalogContractTester(ContractTester, metaclass=ABCMeta):

    @staticmethod
    @pytest.mark.skip(reason="Not yet implemented")
    def test_get_matching_catalog_table(
            contract: Contract, catalog: CatalogArtifact, valid_items: list[CombinedT]
    ):
        pass  # TODO
