from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.matchers import RangeMatcher
from dbt_contracts.contracts.terms.node import NodeContractTerm


class HasConstraints(NodeContractTerm[ModelNode], RangeMatcher):
    def run(self, item: ModelNode, context: ContractContext, parent: None = None) -> bool:
        count = len(item.constraints)
        log_message = self._match(count=count, kind="constraints")

        if log_message:
            context.add_result(name=self.name, message=log_message, item=item, parent=parent)
        return not log_message
