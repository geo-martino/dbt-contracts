from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.nodes import SourceDefinition
from dbt_common.contracts.metadata import CatalogTable

from dbt_contracts.types import NodeT


def get_matching_catalog_table(item: NodeT, catalog: CatalogArtifact) -> CatalogTable | None:
    """
    Check whether the given `item` exists in the database.

    :param item: The resource to match.
    :param catalog: The catalog of tables.
    :return: The matching catalog table.
    """
    if isinstance(item, SourceDefinition):
        return catalog.sources.get(item.unique_id)
    return catalog.nodes.get(item.unique_id)
