from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import Macro

from dbt_contracts.contracts import ContractContext, ContractTerm


class HasType(ContractTerm[MacroArgument, Macro]):
    def run(self, item: MacroArgument, context: ContractContext, parent: Macro = None) -> bool:
        missing_type = not item.type
        if missing_type:
            message = "Argument does not have a type configured"
            context.add_result(name=self._term_name, message=message, item=item, parent=parent)

        return not missing_type
