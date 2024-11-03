"""
Contract configuration for models.
"""
from collections.abc import Iterable

from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts._core import filter_method, validation_method
from dbt_contracts.contracts._node import CompiledNodeContract


class ModelContract(CompiledNodeContract[ModelNode]):
    """Configures a contract for models."""

    @property
    def items(self) -> Iterable[ModelNode]:
        nodes = self.manifest.nodes.values()
        return self._filter_items(filter(lambda node: isinstance(node, ModelNode), nodes))

    @filter_method
    def is_materialized(self, node: ModelNode) -> bool:
        """
        Check whether the given `node` is configured to be materialized.

        :param node: The node to check.
        :return: True if the node is configured to be materialized, False otherwise.
        """
        return node.config.materialized != "ephemeral"

    @validation_method
    def has_constraints(self, node: ModelNode, min_count: int = 1, max_count: int = None) -> bool:
        """
        Check whether the given `node` has an appropriate number of constraints.

        :param node: The node to check.
        :param min_count: The minimum number of constraints allowed.
        :param max_count: The maximum number of constraints allowed.
        :return: True if the node's properties are valid, False otherwise.
        """
        count = len(node.constraints)
        return self._is_in_range(item=node, kind="constraints", count=count, min_count=min_count, max_count=max_count)
