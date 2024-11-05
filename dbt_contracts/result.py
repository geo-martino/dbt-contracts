import dataclasses
import json
import os
from abc import ABCMeta, abstractmethod
from collections.abc import Mapping, MutableMapping, Collection, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Self, Generic, Any

import yaml
from dbt.artifacts.resources.v1.components import ParsedResource, ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import Macro, ModelNode, SourceDefinition
from dbt.flags import get_flags
from dbt_common.dataclass_schema import dbtClassMixin

from dbt_contracts.types import T, ParentT


class SafeLineLoader(yaml.SafeLoader):
    """YAML safe loader which applies line and column number information to every mapping read."""

    def construct_mapping(self, node, deep=False):
        """Construct mapping object and apply line and column numbers"""
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=deep)
        # Add 1 so line/col numbering starts at 1
        mapping['__start_line__'] = node.start_mark.line + 1
        mapping['__start_col__'] = node.start_mark.column + 1
        mapping['__end_line__'] = node.end_mark.line + 1
        mapping['__end_col__'] = node.end_mark.column + 1
        return mapping


@dataclass(kw_only=True)
class Result(Generic[T], metaclass=ABCMeta):
    """Store a result from contract execution."""
    name: str
    path: Path
    result_type: str
    result_level: str
    result_name: str
    message: str
    patch_path: Path | None
    patch_start_line: int | None
    patch_start_col: int | None
    patch_end_line: int | None
    patch_end_col: int | None
    extra: Mapping

    # noinspection PyPropertyDefinition,PyNestedDecorators
    @property
    @classmethod
    @abstractmethod
    def resource_type(cls) -> type[T]:
        """The resource type that this :py:class:`Result` can process."""
        raise NotImplementedError

    @classmethod
    def from_resource(
            cls, item: T, patches: MutableMapping[Path, Mapping[str, Any]] = None, **kwargs
    ) -> Self:
        """
        Create a new :py:class:`Result` from a given resource.

        :param item: The resource to log a result for.
        :param patches: A map of loaded patches with associated line/col identifiers.
            When defined, will attempt to find the patch for the given item in this map before trying to load.
            If a patch is loaded, will update this map with the loaded patch.
        :return: The :py:class:`Result` instance.
        """
        field_names = [field.name for field in dataclasses.fields(cls)]
        patch_object = cls._get_patch_object_from_item(item=item, patches=patches, **kwargs)

        return cls(
            name=item.name,
            path=cls._get_path_from_item(item=item, **kwargs),
            result_type=cls._get_result_type(item=item, **kwargs),
            patch_path=cls._get_patch_path_from_item(item=item, **kwargs),
            patch_start_line=patch_object["__start_line__"] if patch_object else None,
            patch_start_col=patch_object["__start_col__"] if patch_object else None,
            patch_end_line=patch_object["__end_line__"] if patch_object else None,
            patch_end_col=patch_object["__end_col__"] if patch_object else None,
            **{key: val for key, val in kwargs.items() if key in field_names},
            extra={
                key: val for key, val in kwargs.items()
                if key not in field_names and val is not None and not isinstance(val, dbtClassMixin)
            },
        )

    @staticmethod
    def _get_result_type(item: T, **__) -> str:
        return item.resource_type.name.title()

    @staticmethod
    def _get_path_from_item(item: T, **__) -> Path | None:
        return Path(item.path)

    @staticmethod
    def _get_patch_path_from_item(item: T, to_absolute: bool = False, **__) -> Path | None:
        patch_path = None
        if isinstance(item, ParsedResource) and item.patch_path:
            patch_path = Path(item.patch_path.split("://")[1])
        elif (path := Path(item.path)).suffix in [".yml", ".yaml"]:
            patch_path = path
        print("initial", patch_path, to_absolute)
        if patch_path is None or not to_absolute or patch_path.is_absolute():
            return patch_path

        flags = get_flags()
        project_dir = getattr(flags, "PROJECT_DIR", None)
        print("project dir", project_dir)

        if (path_in_project := Path(project_dir, patch_path)).exists():
            print("in project", path_in_project)
            patch_path = path_in_project
        elif (path_in_cwd := Path(os.getcwd(), patch_path)).exists():
            print("in cwd", path_in_cwd)
            patch_path = path_in_cwd

        print("final", patch_path)
        return patch_path

    @classmethod
    def _read_patch_file(cls, path: Path) -> dict[str, Any]:
        with path.open("r") as file:
            patch = yaml.load(file, Loader=SafeLineLoader)

        return patch

    @classmethod
    def _get_patch_object_from_item(
            cls, item: T, patches: MutableMapping[Path, Mapping[str, Any]] = None, **kwargs
    ) -> Mapping[str, Any] | None:
        patch_path = cls._get_patch_path_from_item(item=item, to_absolute=True, **kwargs)
        if patch_path is None or not patch_path.is_file():
            return None

        if patches is None:
            patch = cls._read_patch_file(patch_path)
        elif patch_path not in patches:
            patch = cls._read_patch_file(patch_path)
            patches[patch_path] = patch
        else:
            patch = patches[patch_path]

        return cls._extract_nested_patch_object(patch=patch, item=item, **kwargs)

    @classmethod
    @abstractmethod
    def _extract_nested_patch_object(cls, patch: Mapping[str, Any], item: T, **__) -> Mapping[str, Any] | None:
        raise NotImplementedError

    def as_dict(self) -> dict[str, Any]:
        """Format this result as a dictionary."""
        return dataclasses.asdict(self)

    def as_json(self) -> dict[str, Any]:
        """Format this result as a dictionary suitable for dumping to JSON."""
        return {k: self._as_json(v) for k, v in self.as_dict().items()}

    def _as_json(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, MutableMapping):
            return {k: self._as_json(v) for k, v in value.items()}
        if isinstance(value, Iterable):
            return [self._as_json(v) for v in value]

        return str(value)

    @property
    def _github_annotation(self) -> Mapping[str, str]:
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
            "raw_details": self.result_type,
        }

    @property
    def can_format_to_github_annotation(self) -> bool:
        """Can this result be formatted as a valid GitHub annotation."""
        required_keys = ["path", "start_line", "end_line", "annotation_level", "message"]
        return all(key in self._github_annotation for key in required_keys)

    def as_github_annotation(self) -> Mapping[str, str]:
        """
        Format this result to a GitHub annotation. Raises an exception if the result does not
        have all the required parameters set to build a valid GitHub annotation.
        """
        if not self.can_format_to_github_annotation:
            raise Exception("Cannot format this result to a GitHub annotation.")
        return self._github_annotation


class ResultModel(Result[ModelNode]):
    # noinspection PyPropertyDefinition,PyNestedDecorators
    @classmethod
    @property
    def resource_type(cls) -> type[T]:
        return ModelNode

    @classmethod
    def _extract_nested_patch_object(cls, patch: Mapping[str, Any], item: ModelNode, **__):
        models = (model for model in patch.get("models", ()) if model.get("name", "") == item.name)
        return next(models, None)


class ResultSource(Result[SourceDefinition]):
    # noinspection PyPropertyDefinition,PyNestedDecorators
    @classmethod
    @property
    def resource_type(cls) -> type[T]:
        return SourceDefinition

    @classmethod
    def _extract_nested_patch_object(cls, patch: Mapping[str, Any], item: SourceDefinition, **__):
        sources = (
            table
            for source in patch.get("sources", ()) if source.get("name", "") == item.source_name
            for table in source.get("tables", ()) if table.get("name", "") == item.name
        )
        return next(sources, None)


class ResultMacro(Result[Macro]):
    # noinspection PyPropertyDefinition,PyNestedDecorators
    @classmethod
    @property
    def resource_type(cls) -> type[T]:
        return Macro

    @classmethod
    def _extract_nested_patch_object(cls, patch: Mapping[str, Any], item: Macro, **__):
        macros = (macro for macro in patch.get("macros", ()) if macro.get("name", "") == item.name)
        return next(macros, None)


@dataclass(kw_only=True)
class ResultChild(Result[T], Generic[T, ParentT], metaclass=ABCMeta):
    parent_id: str
    parent_name: str
    index: int

    # noinspection PyMethodOverriding
    @classmethod
    def from_resource(cls, item: T, parent: ParentT, **kwargs) -> Self:
        return super().from_resource(
            item=item, parent=parent, parent_id=parent.unique_id, parent_name=parent.name, **kwargs
        )

    @staticmethod
    def _get_result_type(item: T, parent: ParentT = None, **__) -> str:
        return f"{parent.resource_type.name.title()} {item.resource_type.name.title()}"

    # noinspection PyMethodOverriding
    @classmethod
    def _get_path_from_item(cls, item: T, parent: ParentT, **__) -> Path | None:
        return super()._get_path_from_item(parent)

    # noinspection PyMethodOverriding
    @classmethod
    def _get_patch_path_from_item(cls, item: T, parent: ParentT, **__) -> Path | None:
        return super()._get_patch_path_from_item(parent)

    # noinspection PyMethodOverriding
    @classmethod
    @abstractmethod
    def _extract_nested_patch_object(cls, patch: Mapping[str, Any], item: T, parent: ParentT, **__):
        raise NotImplementedError


class ResultColumn(ResultChild[ColumnInfo, ParentT]):
    # noinspection PyPropertyDefinition,PyNestedDecorators
    @classmethod
    @property
    def resource_type(cls) -> type[T]:
        return ColumnInfo

    @classmethod
    def from_resource(cls, item: ColumnInfo, parent: ParentT, **kwargs) -> Self:
        index = list(parent.columns.keys()).index(item.name)
        return super().from_resource(item=item, parent=parent, index=index, **kwargs)

    @staticmethod
    def _get_result_type(item: T, parent: ParentT = None, **__) -> str:
        return f"{parent.resource_type.name.title()} Column"

    @classmethod
    def _extract_nested_patch_object(cls, patch: Mapping[str, Any], item: ColumnInfo, parent: ParentT, **__):
        # noinspection PyProtectedMember
        result_processor = RESULT_PROCESSOR_MAP.get(type(parent))
        if result_processor is None:
            return

        # noinspection PyProtectedMember
        parent_patch = result_processor._extract_nested_patch_object(patch=patch, item=parent)
        if parent_patch is None:
            return

        columns = (column for column in parent_patch.get("columns", ()) if column.get("name", "") == item.name)
        return next(columns, None)


class ResultMacroArgument(ResultChild[MacroArgument, Macro]):
    # noinspection PyPropertyDefinition,PyNestedDecorators
    @classmethod
    @property
    def resource_type(cls) -> type[T]:
        return MacroArgument

    @classmethod
    def from_resource(cls, item: MacroArgument, parent: Macro, **kwargs) -> Self:
        index = parent.arguments.index(item)
        return super().from_resource(item=item, parent=parent, index=index, **kwargs)

    @staticmethod
    def _get_result_type(*_, **__) -> str:
        return "Macro Argument"

    @classmethod
    def _extract_nested_patch_object(cls, patch: Mapping[str, Any], item: MacroArgument, parent: Macro, **__):
        # noinspection PyProtectedMember
        macro = ResultMacro._extract_nested_patch_object(patch=patch, item=parent)
        if macro is None:
            return

        arguments = (argument for argument in macro.get("arguments", ()) if argument.get("name", "") == item.name)
        return next(arguments, None)


RESULT_PROCESSORS: list[type[Result]] = [ResultModel, ResultSource, ResultMacro, ResultColumn, ResultMacro]
# noinspection PyTypeChecker
RESULT_PROCESSOR_MAP: Mapping[type[T], type[Result]] = {cls.resource_type: cls for cls in RESULT_PROCESSORS}
