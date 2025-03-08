from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts.conditions import ContractCondition


class IsEnabledCondition(ContractCondition[SourceDefinition]):
    """Filter sources taking only those which are enabled."""
    def run(self, item: SourceDefinition) -> bool:
        return item.config.enabled
