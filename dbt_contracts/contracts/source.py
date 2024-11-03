"""
Contract configuration for sources.
"""
import inspect

from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts._core import filter_method, validation_method
from dbt_contracts.contracts._node import NodeContract


class SourceContract(NodeContract[SourceDefinition]):
    """Configures a contract for sources."""

    @property
    def items(self):
        return self._filter_items(self.manifest.sources.values())

    @filter_method
    def is_enabled(self, source: SourceDefinition) -> bool:
        """
        Check whether the given `source` is enabled.

        :param source: The source to check.
        :return: True if the node is enabled, False otherwise.
        """
        return source.config.enabled

    @validation_method
    def has_loader(self, source: SourceDefinition) -> bool:
        """
        Check whether the given `source` has a loader configured.

        :param source: The source to check.
        :return: True if the source's properties are valid, False otherwise.
        """
        missing_loader = len(source.loader) == 0
        if missing_loader:
            name = inspect.currentframe().f_code.co_name
            self._log_result(source, name=name, message="Loader is not correctly configured")

        return not missing_loader

    @validation_method
    def has_freshness(self, source: SourceDefinition) -> bool:
        """
        Check whether the given `source` has freshness configured.

        :param source: The source to check.
        :return: True if the source's properties are valid, False otherwise.
        """
        missing_freshness = (
                source.loaded_at_field is not None and len(source.loaded_at_field) > 0 and source.has_freshness
        )
        if missing_freshness:
            name = inspect.currentframe().f_code.co_name
            self._log_result(source, name=name, message="Freshness is not correctly configured")

        return not missing_freshness

    @validation_method
    def has_downstream_dependencies(self, source: SourceDefinition, min_count: int = 1, max_count: int = None) -> bool:
        """
        Check whether the given `source` has freshness configured.

        :param source: The source to check.
        :param min_count: The minimum number of downstream dependencies allowed.
        :param max_count: The maximum number of downstream dependencies allowed. When None, no upper limit.
        :return: True if the source's properties are valid, False otherwise.
        """
        count = sum(source.unique_id in node.depends_on_nodes for node in self.manifest.nodes.values())
        return self._is_in_range(
            item=source, kind="downstream dependencies", count=count, min_count=min_count, max_count=max_count
        )
