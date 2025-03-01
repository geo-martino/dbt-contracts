from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts import ContractContext, RangeMatcher
from dbt_contracts.contracts.terms._node import NodeContractTerm


class HasConstraints[T: ModelNode](NodeContractTerm[T], RangeMatcher):
    def run(self, item: T, context: ContractContext, parent: None = None) -> bool:
        count = len(item.constraints)
        log_message = self._match(count=count, kind="constraints")

        if log_message:
            context.add_result(name=self._term_name, message=log_message, item=item, parent=parent)
        return not log_message
