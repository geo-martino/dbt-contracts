from typing import Any

from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts.generators.node import NodePropertiesGenerator
from dbt_contracts.contracts.utils import merge_maps


class SourcePropertiesGenerator(NodePropertiesGenerator):

    def _update_existing_properties(self, item: SourceDefinition, properties: dict[str, Any]) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        if key not in properties:
            properties[key] = []

        source = next((source for source in properties[key] if source["name"] == item.source_name), None)
        if source is None:
            source = self._generate_source_properties(item)
            properties[key].append(source)
        if "tables" not in source:
            source["tables"] = []

        table_in_props = next((prop for prop in source["tables"] if prop["name"] == item.name), None)
        table = self._generate_table_properties(item)
        if table_in_props is not None:
            merge_maps(table_in_props, table, overwrite=True, extend=False)
            table = table_in_props
        else:
            source["tables"].append(table)

        self._merge_columns(item, table)

        return properties

    def _generate_properties(self, item: SourceDefinition) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        columns = list(map(self._generate_column_properties, item.columns.values()))
        table = self._generate_table_properties(item) | {"columns": columns}
        source = self._generate_source_properties(item) | {"tables": [table]}

        return self._properties_defaults | {key: [source]}

    @staticmethod
    def _generate_source_properties(item: SourceDefinition) -> dict[str, Any]:
        source = {
            "name": item.source_name,
            "description": item.source_description,
            "database": item.unrendered_database or item.database,
            "schema": item.unrendered_schema or item.schema,
        }
        return {key: val for key, val in source.items() if val}

    @staticmethod
    def _generate_table_properties(item: SourceDefinition) -> dict[str, Any]:
        table = {
            "name": item.name,
            "description": item.description,
        }
        return {key: val for key, val in table.items() if val}
