from abc import ABCMeta, abstractmethod
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any, Self, ClassVar
from urllib.parse import urlparse, unquote

import yaml
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource, ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro
from pydantic import BaseModel
from yaml import MappingNode

from dbt_contracts.contracts.utils import get_absolute_project_path
from dbt_contracts.types import ItemT, ParentT


class SafeLineLoader(yaml.SafeLoader):
    """YAML safe loader which applies line and column number information to every mapping read."""

    def construct_mapping(self, node: MappingNode, deep: bool = False):
        """Construct mapping object and apply line and column numbers"""
        mapping = super().construct_mapping(node, deep=deep)
        # Add 1 so line/col numbering starts at 1
        mapping["__start_line__"] = node.start_mark.line + 1
        mapping["__start_col__"] = node.start_mark.column + 1
        mapping["__end_line__"] = node.end_mark.line + 1
        mapping["__end_col__"] = node.end_mark.column + 1
        return mapping


class Result[I: ItemT, P: ParentT](BaseModel, metaclass=ABCMeta):
    """Store information of the result from a contract execution."""
    name: str
    path: Path | None
    result_type: str
    result_level: str
    result_name: str
    message: str
    # patch attributes
    patch_path: Path | None = None
    patch_start_line: int | None = None
    patch_start_col: int | None = None
    patch_end_line: int | None = None
    patch_end_col: int | None = None
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
            cls, item: I, parent: P = None, patches: MutableMapping[Path, dict[str, Any]] = None, **kwargs
    ) -> Self:
        """
        Create a new :py:class:`Result` from a given resource.

        :param item: The resource to log a result for.
        :param parent: The parent item that the given child `item` belongs to if available.
        :param patches: A map of loaded patches with associated line/col identifiers.
            When defined, will attempt to find the patch for the given item in this map before trying to load from disk.
            If a patch is not a found and is subsequently loaded by this method,
            the loaded patch will be added to this map.
        :return: The :py:class:`Result` instance.
        """
        # noinspection PyUnresolvedReferences
        field_names: set[str] = set(cls.model_fields.keys())
        patch = cls.get_patch_file(item=parent or item, patches=patches)
        patch_object = cls._extract_patch_object_for_item(patch=patch, item=item, parent=parent) or {}

        if parent is not None:
            kwargs |= dict(
                parent_id=parent.unique_id,
                parent_name=parent.name,
                parent_type=parent.resource_type.name.title(),
            )

        path = None
        if isinstance(path_item := parent if parent is not None else item, BaseResource):
            path = Path(path_item.original_file_path)

        return cls(
            name=item.name,
            path=path,
            result_type=cls._get_result_type(item=item, parent=parent),
            patch_path=cls.get_patch_path(item=parent if parent is not None else item, to_absolute=False),
            patch_start_line=patch_object.get("__start_line__"),
            patch_start_col=patch_object.get("__start_col__"),
            patch_end_line=patch_object.get("__end_line__"),
            patch_end_col=patch_object.get("__end_col__"),
            **{key: val for key, val in kwargs.items() if key in field_names},
        )

    @staticmethod
    def _get_result_type(item: I, parent: P = None) -> str:
        result_type = item.resource_type.name.title()
        if parent:
            result_type = f"{parent.resource_type.name.title()} {result_type}"
        return result_type

    @staticmethod
    def get_patch_path(item: I | P, to_absolute: bool = False) -> Path | None:
        """
        Get the patch path for a given item from its properties.

        :param item: The item to get a patch path for.
        :param to_absolute: Format the path to be absolute.
        :return: The patch path if found.
        """
        patch_path = None
        if isinstance(item, ParsedResource) and item.patch_path:
            patch_path = Path(item.patch_path.split("://")[1])
        elif isinstance(item, BaseResource) and (path := Path(item.original_file_path)).suffix in {".yml", ".yaml"}:
            patch_path = path

        if patch_path is None or not to_absolute or patch_path.is_absolute():
            return patch_path
        return get_absolute_project_path(patch_path)

    @classmethod
    def get_patch_file(
            cls, item: I, patches: MutableMapping[Path, dict[str, Any]] = None
    ) -> dict[str, Any]:
        """
        Get the patch file by either extracting from the given patches or loading from disk.

        :param item: The item to get the patch object for.
        :param patches: The loaded patches to search through.
        :return: The loaded patch file if found.
        """
        patch_path = cls.get_patch_path(item=item, to_absolute=True)
        if patch_path is None or not patch_path.is_file():
            return {}

        if patches is None:
            patch = cls._read_patch_file(patch_path)
        elif patch_path not in patches:
            patch = cls._read_patch_file(patch_path)
            patches[patch_path] = patch
        else:
            patch = patches[patch_path]

        return patch

    @classmethod
    def _read_patch_file(cls, path: Path) -> dict[str, Any]:
        with path.open("r") as file:
            patch = yaml.load(file, Loader=SafeLineLoader)
        return patch or {}

    @classmethod
    @abstractmethod
    def _extract_patch_object_for_item(
            cls, patch: Mapping[str, Any], item: ItemT, parent: ParentT = None
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
            "path": str(self.patch_path or self.path),
            "start_line": self.patch_start_line,
            "start_column": self.patch_start_col,
            "end_line": self.patch_end_line,
            "end_column": self.patch_end_col,
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
    def _extract_patch_object_for_item(
            cls, patch: Mapping[str, Any], item: ModelNode, parent: None = None
    ) -> Mapping[str, Any] | None:
        models = (model for model in patch.get("models", ()) if model.get("name", "") == item.name)
        return next(models, None)


class SourceResult(Result[SourceDefinition, None]):
    resource_type = SourceDefinition

    @classmethod
    def _extract_patch_object_for_item(
            cls, patch: Mapping[str, Any], item: SourceDefinition, parent: None = None
    ) -> Mapping[str, Any] | None:
        sources = (
            table
            for source in patch.get("sources", ()) if source.get("name", "") == item.source_name
            for table in source.get("tables", ()) if table.get("name", "") == item.name
        )
        return next(sources, None)


class ColumnResult[P: ParentT](Result[ColumnInfo, P]):
    resource_type = ColumnInfo

    @classmethod
    def _extract_patch_object_for_item(
            cls, patch: Mapping[str, Any], item: ColumnInfo, parent: P = None
    ) -> Mapping[str, Any] | None:
        result_processor = RESULT_PROCESSOR_MAP.get(type(parent))
        if result_processor is None:
            return

        # noinspection PyProtectedMember
        parent_patch = result_processor._extract_patch_object_for_item(patch=patch, item=parent)
        if parent_patch is None:
            return

        columns = (column for column in parent_patch.get("columns", ()) if column.get("name", "") == item.name)
        return next(columns, None)

    @classmethod
    def _get_result_type(cls, item: ColumnInfo, parent: P = None) -> str:
        result_type = "Column"
        if parent is not None:
            result_type = f"{parent.resource_type.name.title()} {result_type}"
        return result_type

    @classmethod
    def from_resource(
            cls, item: ColumnInfo, parent: P = None, patches: MutableMapping[Path, Mapping[str, Any]] = None, **kwargs
    ) -> Self:
        try:
            index = list(parent.columns.keys()).index(item.name) if parent is not None else None
        except ValueError:
            index = None

        return super().from_resource(item=item, parent=parent, index=index, **kwargs)


class MacroResult(Result[Macro, None]):
    resource_type = Macro

    @classmethod
    def _extract_patch_object_for_item(
            cls, patch: Mapping[str, Any], item: Macro, parent: None = None
    ) -> Mapping[str, Any] | None:
        macros = (macro for macro in patch.get("macros", ()) if macro.get("name", "") == item.name)
        return next(macros, None)


class MacroArgumentResult(Result[MacroArgument, Macro]):
    resource_type = MacroArgument

    @classmethod
    def _extract_patch_object_for_item(
            cls, patch: Mapping[str, Any], item: MacroArgument, parent: Macro = None
    ) -> Mapping[str, Any] | None:
        # noinspection PyProtectedMember
        macro = MacroResult._extract_patch_object_for_item(patch=patch, item=parent)
        if macro is None:
            return

        arguments = (argument for argument in macro.get("arguments", ()) if argument.get("name", "") == item.name)
        return next(arguments, None)

    @classmethod
    def _get_result_type(cls, item: MacroArgument, parent: Macro = None) -> str:
        return "Macro Argument"

    @classmethod
    def from_resource(
            cls,
            item: MacroArgument,
            parent: Macro = None,
            patches: MutableMapping[Path, Mapping[str, Any]] = None,
            **kwargs
    ) -> Self:
        index = parent.arguments.index(item) if parent is not None else None
        return super().from_resource(item=item, parent=parent, index=index, **kwargs)


RESULT_PROCESSORS: tuple[type[Result], ...] = (
    ModelResult, SourceResult, MacroResult, ColumnResult, MacroArgumentResult
)
RESULT_PROCESSOR_MAP: Mapping[type[ItemT], type[Result]] = {cls.resource_type: cls for cls in RESULT_PROCESSORS}
