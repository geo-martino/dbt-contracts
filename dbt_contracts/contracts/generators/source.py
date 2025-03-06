from typing import Any

from dbt.contracts.graph.nodes import SourceDefinition

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.node import NodeGenerator


class SourceGenerator(NodeGenerator[SourceDefinition]):

    def _update_existing_patch(self, item: SourceDefinition, context: ContractContext) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        patch = context.get_patch_file(item)
        if key not in patch:
            patch[key] = []

        source = next((source for source in patch[key] if source["name"] == item.source_name), None)
        if source is None:
            source = self._generate_source_patch(item)
            patch[key].append(source)
        if "tables" not in source:
            source["tables"] = []

        properties = next((prop for prop in source["tables"] if prop["name"] == item.name), None)
        table = self._generate_table_patch(item)
        if properties is not None:
            properties.update(table)
        else:
            source["tables"].append(table)

        return patch

    def _generate_new_patch(self, item: SourceDefinition) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        source = self._generate_full_patch(item)
        return self._patch_defaults | {key: [source]}

    @classmethod
    def _generate_full_patch(cls, item: SourceDefinition) -> dict[str, Any]:
        return cls._generate_source_patch(item) | {"tables": [cls._generate_table_patch(item)]}

    @staticmethod
    def _generate_source_patch(item: SourceDefinition) -> dict[str, Any]:
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
    def _generate_table_patch(item: SourceDefinition) -> dict[str, Any]:
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
