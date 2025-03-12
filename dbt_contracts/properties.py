from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Collection

import yaml
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource

from dbt_contracts.contracts.utils import get_absolute_project_path
from dbt_contracts.types import PropertiesT


class SafeLineLoader(yaml.SafeLoader):
    """YAML safe loader which applies line and column number information to every mapping read."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False):
        """Construct mapping object and apply line and column numbers"""
        mapping = super().construct_mapping(node, deep=deep)
        # Add 1 so line/col numbering starts at 1
        mapping["__start_line__"] = node.start_mark.line + 1
        mapping["__start_col__"] = node.start_mark.column + 1
        mapping["__end_line__"] = node.end_mark.line + 1
        mapping["__end_col__"] = node.end_mark.column + 1
        return mapping


class IndentedDumper(yaml.Dumper):
    """YAML dumper which sets extra indentation for flow collections"""

    # noinspection PyMissingOrEmptyDocstring,SpellCheckingInspection
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentedDumper, self).increase_indent(flow, False)


class PropertiesIO(MutableMapping[Path, dict[str, Any]]):
    """Manages loading and saving of properties for dbt objects from/to their associated properties files"""

    def __init__(self, data: dict[Path, dict[str, Any]] = None):
        self._properties: dict[Path, dict[str, Any]] = data or {}

    def __getitem__(self, item: str | Path | PropertiesT):
        if not isinstance(item, (str, Path)):
            try:
                path = self.get_path(item=item, to_absolute=True)
                if path is None:
                    raise FileNotFoundError
            except FileNotFoundError:
                raise KeyError(f"No properties path found for the given item: {item.name!r}.")
        else:
            path = Path(item)

        if path in self._properties:
            return self._properties[path]

        if not path.suffix:
            path = path.with_suffix(".yml")

        if not path.is_file():
            raise KeyError(f"File {path} not found.")
        if path.suffix.casefold() not in {".yml", ".yaml"}:
            raise KeyError("Extension of the given properties path is not a yaml file")

        properties = self._read_file(path)
        self._properties[path] = properties
        return properties

    def __setitem__(self, key: Path, value: dict[str, Any]):
        self._properties[key] = value

    def __delitem__(self, key: Path):
        del self._properties[key]

    def __len__(self):
        return len(self._properties)

    def __iter__(self):
        return iter(self._properties)

    @staticmethod
    def _read_file(path: Path) -> dict[str, Any]:
        with path.open("r") as file:
            properties = yaml.load(file, Loader=SafeLineLoader)
        return properties

    @staticmethod
    def get_path(item: PropertiesT, to_absolute: bool = False) -> Path | None:
        """
        Get the properties path for a given item from its attributes.

        :param item: The item to get a properties path for.
        :param to_absolute: Format the path to be absolute. Only returns the path if it exists in the project.
        :return: The properties path if found.
        """
        properties_path = None
        if isinstance(item, ParsedResource) and item.patch_path:
            properties_path = Path(item.patch_path.split("://")[1])
        elif isinstance(item, BaseResource):
            if (path := Path(item.original_file_path)).suffix.casefold() in {".yml", ".yaml"}:
                properties_path = path

        if properties_path is None or not to_absolute or properties_path.is_absolute():
            return properties_path
        return get_absolute_project_path(properties_path)

    def save(self, paths: Collection[Path] = ()) -> list[Path]:
        """
        Save the stored properties.

        :param paths: Optionally, only save the properties from this list.
        """
        if not paths:
            paths = tuple(self._properties)

        paths_updated = []
        for path in paths:
            if path not in self._properties:
                continue

            properties = self._clean_properties(self._properties[path])
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w") as file:
                yaml.dump(properties, file, Dumper=IndentedDumper, sort_keys=False, allow_unicode=True)

            paths_updated.append(path)

        return paths_updated

    @classmethod
    def _clean_properties(cls, properties: dict[str, Any]) -> dict[str, Any]:
        properties = properties.copy()

        for key, value in properties.copy().items():
            if key.startswith("__"):
                del properties[key]

            if isinstance(value, dict):
                properties[key] = cls._clean_properties(value)
            elif isinstance(value, Collection) and not isinstance(value, str):
                properties[key] = [cls._clean_properties(val) if isinstance(val, dict) else val for val in value]

        return properties
