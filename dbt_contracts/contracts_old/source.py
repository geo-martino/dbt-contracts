"""
Contract configuration for sources.
"""

from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts_old._node import NodeContract


class SourceContract(NodeContract[SourceDefinition]):
    """Configures a contract for sources."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def config_key(cls) -> str:
        return "sources"

    @property
    def items(self):
        return self._filter_items(self.manifest.sources.values())
