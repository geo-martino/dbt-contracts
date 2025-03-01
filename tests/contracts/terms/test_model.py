from unittest import mock

from dbt.contracts.graph.nodes import ModelNode
from dbt_common.contracts.constraints import ModelLevelConstraint, ConstraintType

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.terms.model import HasConstraints


def test_has_constraints(model: ModelNode, context: ContractContext):
    model.constraints.extend((
        ModelLevelConstraint(type=ConstraintType.not_null, columns=["col1", "col2"]),
        ModelLevelConstraint(type=ConstraintType.unique, columns=["col1", "col2"]),
    ))
    assert HasConstraints().run(model, context=context)

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasConstraints(min_count=5).run(model, context=context)
        mock_add_result.assert_called_once()
