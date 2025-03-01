from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts import ContractContext, RangeMatcher
from dbt_contracts.contracts.terms._node import NodeContractTerm


class HasLoader(NodeContractTerm[SourceDefinition]):
    def run(self, item: SourceDefinition, context: ContractContext, parent: None = None) -> bool:
        missing_loader = not item.loader
        if missing_loader:
            message = "Loader is not correctly configured"
            context.add_result(name=self._term_name, message=message, item=item, parent=parent)

        return not missing_loader


class HasFreshness(NodeContractTerm[SourceDefinition]):
    def run(self, item: SourceDefinition, context: ContractContext, parent: None = None) -> bool:
        missing_freshness = not bool(item.loaded_at_field) or not item.has_freshness
        if missing_freshness:
            message = "Freshness is not correctly configured"
            context.add_result(name=self._term_name, message=message, item=item, parent=parent)

        return not missing_freshness


class HasDownstreamDependencies(NodeContractTerm[SourceDefinition], RangeMatcher):
    def run(self, item: SourceDefinition, context: ContractContext, parent: None = None) -> bool:
        count = sum(item.unique_id in node.depends_on_nodes for node in context.manifest.nodes.values())
        print(count)
        log_message = self._match(count=count, kind="downstream dependencies")

        if log_message:
            context.add_result(name=self._term_name, message=log_message, item=item, parent=parent)
        return not log_message
