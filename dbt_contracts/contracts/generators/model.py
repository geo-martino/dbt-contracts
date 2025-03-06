from typing import Any

from dbt.contracts.graph.nodes import ModelNode

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators.node import NodePropertiesGenerator


class ModelPropertiesGenerator(NodePropertiesGenerator[ModelNode]):

    def _update_existing_patch(self, item: ModelNode, context: ContractContext) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        patch = context.get_patch_file(item)
        if key not in patch:
            patch[key] = []

        properties = next((prop for prop in patch[key] if prop["name"] == item.name), None)
        table = self._generate_table_patch(item)
        if properties is not None:
            properties.update(table)
        else:
            patch[key].append(table)

        return patch

    def _generate_new_patch(self, item: ModelNode) -> dict[str, Any]:
        key = item.resource_type.pluralize()
        table = self._generate_table_patch(item)
        return self._patch_defaults | {key: [table]}

    @staticmethod
    def _generate_table_patch(item: ModelNode) -> dict[str, Any]:
        table = {
            "name": item.name,
            "description": item.description,
            "docs": item.docs.to_dict(),
            "latest_version": item.latest_version,
            "deprecation_date": item.deprecation_date.isoformat() if item.deprecation_date else None,
            "access": item.access.name,
            "config": item.config.to_dict(),
            "constraints": [constraint.to_dict() for constraint in item.constraints],
            "columns": [column.to_dict() for column in item.columns.values()],
            "time_spine": item.time_spine.to_dict() if item.time_spine else None,
        }
        return {key: val for key, val in table.items() if val}
