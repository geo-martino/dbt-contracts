"""
Contract configuration for models.
"""
from collections.abc import Iterable

from dbt.contracts.graph.nodes import ModelNode
from dbt_contracts.contracts_old._node import CompiledNodeContract


class ModelContract(CompiledNodeContract[ModelNode]):
    """Configures a contract for models."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def config_key(cls) -> str:
        return "models"

    @property
    def items(self) -> Iterable[ModelNode]:
        nodes = self.manifest.nodes.values()
        return self._filter_items(filter(lambda node: isinstance(node, ModelNode), nodes))
