from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import Macro

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.terms._core import ContractTerm, validate_context


class HasType(ContractTerm[MacroArgument, Macro]):
    """Check whether macro arguments have a data type configured in their properties."""
    @validate_context
    def run(self, item: MacroArgument, context: ContractContext, parent: Macro = None) -> bool:
        missing_type = not item.type
        if missing_type:
            message = "Argument does not have a type configured"
            context.add_result(name=self.name, message=message, item=item, parent=parent)

        return not missing_type
