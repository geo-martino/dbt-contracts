from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts.conditions import ContractCondition


class IsEnabledCondition(ContractCondition[SourceDefinition]):
    def run(self, item: SourceDefinition) -> bool:
        return item.config.enabled
