"""
Contract configuration for models.
"""
import inspect
from collections.abc import Iterable

from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts_old._comparisons import is_not_in_range
from dbt_contracts.contracts_old._core import filter_method, enforce_method
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

    @filter_method
    def is_materialized(self, node: ModelNode) -> bool:
        """
        Check whether the given `node` is configured to be materialized.

        :param node: The node to check.
        :return: True if the node is configured to be materialized, False otherwise.
        """
        return node.config.materialized != "ephemeral"

    @enforce_method
    def has_constraints(self, node: ModelNode, min_count: int = 1, max_count: int = None) -> bool:
        """
        Check whether the given `node` has an appropriate number of constraints.

        :param node: The node to check.
        :param min_count: The minimum number of constraints allowed.
        :param max_count: The maximum number of constraints allowed.
        :return: True if the node's properties are valid, False otherwise.
        """
        count = len(node.constraints)
        too_small, too_large = is_not_in_range(count=count, min_count=min_count, max_count=max_count)

        if too_small or too_large:
            test_name = inspect.currentframe().f_code.co_name
            quantifier = 'few' if too_small else 'many'
            expected = min_count if too_small else max_count
            message = f"Too {quantifier} constraints found: {count}. Expected: {expected}."

            self._add_result(node, name=test_name, message=message)

        return not too_small and not too_large
