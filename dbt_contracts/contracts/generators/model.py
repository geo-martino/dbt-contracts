from typing import Any

from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts.generators.node import NodePropertiesGenerator
from dbt_contracts.contracts.utils import merge_maps


class ModelPropertiesGenerator(NodePropertiesGenerator):

    def _update_existing_properties(self, item: ModelNode, properties: dict[str, Any]) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        if key not in properties:
            properties[key] = []

        table_in_props = next((prop for prop in properties[key] if prop["name"] == item.name), None)
        table = self._generate_table_properties(item)
        if table_in_props is not None:
            merge_maps(table_in_props, table, overwrite=True, extend=False)
            table = table_in_props
        else:
            properties[key].append(table)

        self._merge_columns(item, table)

        return properties

    def _generate_properties(self, item: ModelNode) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        columns = list(map(self._generate_column_properties, item.columns.values()))
        table = self._generate_table_properties(item) | {"columns": columns}

        return self._properties_defaults | {key: [table]}

    @classmethod
    def _generate_table_properties(cls, item: ModelNode) -> dict[str, Any]:
        table = {
            "name": item.name,
            "description": item.description,
        }
        return {key: val for key, val in table.items() if val}
