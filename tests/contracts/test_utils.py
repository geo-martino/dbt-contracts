from dbt.artifacts.resources import BaseResource
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.nodes import CompiledNode

from dbt_contracts.contracts.utils import get_matching_catalog_table


def test_get_matching_catalog_table(node: CompiledNode, simple_resource: BaseResource, catalog: CatalogArtifact):
    table = get_matching_catalog_table(item=node, catalog=catalog)
    assert table is not None
    assert table.metadata.name == node.name

    assert get_matching_catalog_table(item=simple_resource, catalog=catalog) is None
