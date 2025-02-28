from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.result import Result

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.types import ItemT, ParentT


def assert_result(results: list[Result], name: str, message: str, item: ItemT, parent: ParentT = None):
    assert any(result.name == item.name for result in results)
    assert any(result.result_name == name for result in results)
    assert any(result.message == message for result in results)

    if parent is None:
        return

    assert any(result.parent_id == parent.unique_id for result in results)
    assert any(result.parent_name == parent.name for result in results)


def test_add_result_on_item(context: ContractContext, model: ModelNode):
    expected_name = "test_name"
    expected_message = "this test has failed"
    context.add_result(item=model, name=expected_name, message=expected_message)

    assert_result(context.results, item=model, name=expected_name, message=expected_message)


def test_add_result_on_item_with_parent(context: ContractContext, model: ModelNode, column: ColumnInfo):
    model.columns |= {column.name: column}

    expected_name = "test_name"
    expected_message = "this test has failed"
    context.add_result(item=column, parent=model, name=expected_name, message=expected_message)

    assert_result(context.results, item=column, parent=model, name=expected_name, message=expected_message)