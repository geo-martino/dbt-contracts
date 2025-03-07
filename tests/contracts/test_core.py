from unittest import mock

import pytest
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.result import Result
from dbt_contracts.types import ItemT, ParentT


class TestContractContext:
    def test_context_init(self):
        context = ContractContext()
        assert len(context.results) == 0
        assert len(context.patches) == 0

        with pytest.raises(TypeError):  # cannot manually set results
            # noinspection PyArgumentList
            ContractContext(results=None)

    @staticmethod
    def assert_result(results: list[Result], name: str, message: str, item: ItemT, parent: ParentT = None):
        """Assert that a result with the given name and message exists in the results list"""
        assert any(result.name == item.name for result in results)
        assert any(result.result_name == name for result in results)
        assert any(result.message == message for result in results)

        if parent is None:
            return

        assert any(result.parent_id == parent.unique_id for result in results)
        assert any(result.parent_name == parent.name for result in results)

    def test_get_patch_path(self, context: ContractContext, model: ModelNode, simple_resource: BaseResource):
        with mock.patch.object(Result, "get_patch_path") as mock_get_patch_path:
            context.get_patch_path(model)
            mock_get_patch_path.assert_called_once_with(model)

        with pytest.raises(Exception):  # unrecognised object type
            context.get_patch_path(simple_resource)

    def test_get_patch_file(self, context: ContractContext, model: ModelNode, simple_resource: BaseResource):
        with mock.patch.object(Result, "get_patch_file") as mock_get_patch_file:
            context.get_patch_file(model)
            mock_get_patch_file.assert_called_once_with(model, patches=context.patches)

        with pytest.raises(Exception):  # unrecognised object type
            context.get_patch_file(simple_resource)

    def test_add_result_on_item(self, context: ContractContext, model: ModelNode):
        expected_name = "test_name"
        expected_message = "this test has failed"
        context.add_result(item=model, name=expected_name, message=expected_message)

        self.assert_result(context.results, item=model, name=expected_name, message=expected_message)

    def test_add_result_on_item_with_parent(self, context: ContractContext, model: ModelNode, column: ColumnInfo):
        expected_name = "test_name"
        expected_message = "this test has failed"
        context.add_result(item=column, parent=model, name=expected_name, message=expected_message)

        self.assert_result(context.results, item=column, parent=model, name=expected_name, message=expected_message)
