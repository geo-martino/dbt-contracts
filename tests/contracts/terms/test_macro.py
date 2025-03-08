from unittest import mock

from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import Macro

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.terms.macro import HasType


def test_argument_has_type(argument: MacroArgument, macro: Macro, context: ContractContext):
    argument.type = "int"
    assert HasType().run(argument, parent=macro, context=context)

    argument.type = None
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasType().run(argument, parent=macro, context=context)
        mock_add_result.assert_called_once()
