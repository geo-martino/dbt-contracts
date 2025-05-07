from abc import ABCMeta, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self, ClassVar

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts._core import BaseModelConfig
from dbt_contracts.properties import PropertiesIO
from dbt_contracts.types import ItemT, ParentT


class Result[I: ItemT, P: ParentT](BaseModelConfig, metaclass=ABCMeta):
    """Store information of the result from a contract execution."""
    name: str
    path: Path | None
    result_type: str
    result_level: str
    result_name: str
    message: str
    # properties attributes
    properties_path: Path | None = None
    properties_start_line: int | None = None
    properties_start_col: int | None = None
    properties_end_line: int | None = None
    properties_end_col: int | None = None
    # parent specific attributes
    parent_id: str | None = None
    parent_name: str | None = None
    parent_type: str | None = None
    index: int | None = None

    resource_type: ClassVar[type[ItemT]]

    @property
    def has_parent(self) -> bool:
        """Was this result built using a parent item."""
        return self.parent_id is not None or self.parent_name is not None or self.parent_type is not None

    @classmethod
    def from_resource(
            cls, item: I, parent: P = None, properties: PropertiesIO = None, **kwargs
    ) -> Self:
        """
        Create a new :py:class:`Result` from a given resource.

        :param item: The resource to log a result for.
        :param parent: The parent item that the given child `item` belongs to if available.
        :param properties: The properties IO handler to use when loading properties files.
        :return: The :py:class:`Result` instance.
        """
        props = {}
        props_path = None
        props_item = parent if parent is not None else item
        if properties is not None and (item_properties := properties.get(props_item)) is not None:
            props = cls._extract_properties_for_item(item_properties, item=item, parent=parent) or {}
            props_path = properties.get_path(props_item, to_absolute=False)

        if parent is not None:
            kwargs |= dict(
                parent_id=parent.unique_id,
                parent_name=parent.name,
                parent_type=parent.resource_type.name.title(),
            )

        path = None
        if isinstance(path_item := parent if parent is not None else item, BaseResource):
            path = Path(path_item.original_file_path)

        # noinspection PyUnresolvedReferences
        field_names: set[str] = set(cls.model_fields.keys())

        return cls(
            name=item.name,
            path=path,
            result_type=cls._get_result_type(item=item, parent=parent),
            properties_path=props_path,
            properties_start_line=props.get("__start_line__"),
            properties_start_col=props.get("__start_col__"),
            properties_end_line=props.get("__end_line__"),
            properties_end_col=props.get("__end_col__"),
            **{key: val for key, val in kwargs.items() if key in field_names},
        )

    @staticmethod
    def _get_result_type(item: I, parent: P = None) -> str:
        result_type = item.resource_type.name.title()
        if parent:
            result_type = f"{parent.resource_type.name.title()} {result_type}"
        return result_type

    @classmethod
    @abstractmethod
    def _extract_properties_for_item(
            cls, properties: Mapping[str, Any], item: ItemT, parent: ParentT = None
    ) -> Mapping[str, Any] | None:
        raise NotImplementedError

    def as_github_annotation(self) -> Mapping[str, str]:
        """
        Format this result to a GitHub annotation. Raises an exception if the result does not
        have all the required parameters set to build a valid GitHub annotation.
        """
        if not self.can_format_to_github_annotation:
            raise Exception("Cannot format this result to a GitHub annotation.")
        return self._as_github_annotation()

    @property
    def can_format_to_github_annotation(self) -> bool:
        """Can this result be formatted as a valid GitHub annotation."""
        required_keys = {"path", "start_line", "end_line", "annotation_level", "message"}
        annotation = self._as_github_annotation()
        return all(annotation.get(key) is not None for key in required_keys)

    def _as_github_annotation(self) -> Mapping[str, str | int | list[str] | dict[str, str]]:
        """
        See annotations spec in the `output` param 'Update a check run':
        https://docs.github.com/en/rest/checks/runs?apiVersion=2022-11-28#update-a-check-run
        """
        return {
            "path": str(self.properties_path or self.path),
            "start_line": self.properties_start_line,
            "start_column": self.properties_start_col,
            "end_line": self.properties_end_line,
            "end_column": self.properties_end_col,
            "annotation_level": self.result_level,
            "title": self.result_name.replace("_", " ").title(),
            "message": self.message,
            "raw_details": {
                "result_type": self.result_type,
                "name": self.name,
            },
        }


class ModelResult(Result[ModelNode, None]):
    resource_type = ModelNode

    @classmethod
    def _extract_properties_for_item(
            cls, properties: Mapping[str, Any], item: ModelNode, parent: None = None
    ) -> Mapping[str, Any] | None:
        models = (model for model in properties.get("models", ()) if model.get("name", "") == item.name)
        return next(models, None)


class SourceResult(Result[SourceDefinition, None]):
    resource_type = SourceDefinition

    @classmethod
    def _extract_properties_for_item(
            cls, properties: Mapping[str, Any], item: SourceDefinition, parent: None = None
    ) -> Mapping[str, Any] | None:
        sources = (
            table
            for source in properties.get("sources", ()) if source.get("name", "") == item.source_name
            for table in source.get("tables", ()) if table.get("name", "") == item.name
        )
        return next(sources, None)


class ColumnResult[P: ParentT](Result[ColumnInfo, P]):
    resource_type = ColumnInfo

    @classmethod
    def _extract_properties_for_item(
            cls, properties: Mapping[str, Any], item: ColumnInfo, parent: P = None
    ) -> Mapping[str, Any] | None:
        result_processor = RESULT_PROCESSOR_MAP.get(type(parent))
        if result_processor is None:
            return

        # noinspection PyProtectedMember
        parent_properties = result_processor._extract_properties_for_item(properties=properties, item=parent)
        if parent_properties is None:
            return

        columns = (column for column in parent_properties.get("columns", ()) if column.get("name", "") == item.name)
        return next(columns, None)

    @classmethod
    def _get_result_type(cls, item: ColumnInfo, parent: P = None) -> str:
        result_type = "Column"
        if parent is not None:
            result_type = f"{parent.resource_type.name.title()} {result_type}"
        return result_type

    @classmethod
    def from_resource(
            cls, item: ColumnInfo, parent: P = None, properties: PropertiesIO = None, **kwargs
    ) -> Self:
        try:
            index = list(parent.columns.keys()).index(item.name) if parent is not None else None
        except ValueError:
            index = None

        return super().from_resource(item=item, parent=parent, index=index, **kwargs)


class MacroResult(Result[Macro, None]):
    resource_type = Macro

    @classmethod
    def _extract_properties_for_item(
            cls, properties: Mapping[str, Any], item: Macro, parent: None = None
    ) -> Mapping[str, Any] | None:
        macros = (macro for macro in properties.get("macros", ()) if macro.get("name", "") == item.name)
        return next(macros, None)


class MacroArgumentResult(Result[MacroArgument, Macro]):
    resource_type = MacroArgument

    @classmethod
    def _extract_properties_for_item(
            cls, properties: Mapping[str, Any], item: MacroArgument, parent: Macro = None
    ) -> Mapping[str, Any] | None:
        # noinspection PyProtectedMember
        macro = MacroResult._extract_properties_for_item(properties=properties, item=parent)
        if macro is None:
            return

        arguments = (argument for argument in macro.get("arguments", ()) if argument.get("name", "") == item.name)
        return next(arguments, None)

    @classmethod
    def _get_result_type(cls, item: MacroArgument, parent: Macro = None) -> str:
        return "Macro Argument"

    @classmethod
    def from_resource(
            cls, item: MacroArgument, parent: Macro = None, properties: PropertiesIO = None, **kwargs
    ) -> Self:
        index = parent.arguments.index(item) if parent is not None else None
        return super().from_resource(item=item, parent=parent, index=index, **kwargs)


RESULT_PROCESSORS: tuple[type[Result], ...] = (
    ModelResult, SourceResult, MacroResult, ColumnResult, MacroArgumentResult
)
RESULT_PROCESSOR_MAP: Mapping[type[ItemT], type[Result]] = {cls.resource_type: cls for cls in RESULT_PROCESSORS}
