from typing import Any

from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts._core import ContractContext
from dbt_contracts.contracts.generators.node import NodePropertiesGenerator


class SourcePropertiesGenerator(NodePropertiesGenerator[SourceDefinition]):

    def _update_existing_properties(self, item: SourceDefinition, context: ContractContext) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        properties = context.properties[item]
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
            table_in_props.update(table)
        else:
            source["tables"].append(table)

        return properties

    def _generate_new_properties(self, item: SourceDefinition) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        source = self._generate_full_properties(item)
        return self._properties_defaults | {key: [source]}

    @classmethod
    def _generate_full_properties(cls, item: SourceDefinition) -> dict[str, Any]:
        return cls._generate_source_properties(item) | {"tables": [cls._generate_table_properties(item)]}

    @staticmethod
    def _generate_source_properties(item: SourceDefinition) -> dict[str, Any]:
        source = {
            "name": item.source_name,
            "description": item.source_description,
            "database": item.unrendered_database or item.database,
            "schema": item.unrendered_schema or item.schema,
            "loader": item.loader,
            "meta": item.source_meta,
            "config": item.config.to_dict(),
        }
        return {key: val for key, val in source.items() if val}

    @staticmethod
    def _generate_table_properties(item: SourceDefinition) -> dict[str, Any]:
        table = {
            "name": item.name,
            "description": item.description,
            "identifier": item.identifier,
            "loaded_at_field": item.loaded_at_field,
            "meta": item.meta,
            "tags": item.tags,
            "freshness": item.freshness.to_dict() if item.freshness else None,
            "quoting": item.quoting.to_dict(),
            "external": item.external.to_dict() if item.external else None,
            "columns": [column.to_dict() for column in item.columns.values()],
        }
        return {key: val for key, val in table.items() if val}
