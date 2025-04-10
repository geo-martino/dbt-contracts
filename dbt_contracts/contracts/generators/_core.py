from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Annotated, Any

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource
from dbt.flags import get_flags
from dbt_common.dataclass_schema import dbtClassMixin
from pydantic import Field, BeforeValidator

from dbt_contracts.contracts._core import ContractContext, ContractPart
from dbt_contracts.contracts.utils import to_tuple
from dbt_contracts.types import ItemT, PropertiesT

CORE_FIELDS = Literal["description"]


class PropertyGenerator[S: ItemT, T: dbtClassMixin](ContractPart, metaclass=ABCMeta):
    """
    A part of a contract meant to generate a property for a resource based on its related database object.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """
    overwrite: bool = Field(
        description=(
            "Whether to overwrite existing properties with properties from the database. "
            "When false, keeps the values already present in the properties if present."
        ),
        default=True,
        examples=[True, False],
    )

    @abstractmethod
    def run(self, source: S, target: T) -> bool:
        """
        Run this generator on the given source, merging properties from the target item.

        :param source: The item to modify.
        :param target: The item containing the target properties to set onto the source.
        :return: Boolean for if the source was modified.
        """
        raise NotImplementedError


class PropertiesGenerator[I: ItemT, G: PropertyGenerator](ContractPart, metaclass=ABCMeta):
    """
    A part of a contract meant to generate properties for a resource based on its related database object.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """
    exclude: Annotated[Sequence[str], BeforeValidator(to_tuple)] = Field(
        description="The fields to exclude from the generated properties.",
        default=(),
    )

    @classmethod
    def _name(cls) -> str:
        """The name of this generator in snake_case."""
        class_name = cls.__name__.replace("Properties", "").replace("Generator", "")
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", class_name).lower()

    @property
    def generators(self) -> list[PropertyGenerator]:
        """
        Get all the single property generators set on this generator,
        excluding those that are configured to be excluded.
        """
        return [
            attr for name, attr in vars(self).items()
            if name not in self.exclude and isinstance(attr, PropertyGenerator)
        ]

    @abstractmethod
    def merge(self, item: I, context: ContractContext) -> bool:
        """
        Merge the properties of the given item with the properties of its remote database object.

        :param item: The item to modify properties for.
        :param context: The contract context to use.
        :return: Whether the properties of the item were modified.
        """
        raise NotImplementedError


class ChildPropertiesGenerator[I: ItemT, P: PropertiesT, G: PropertyGenerator](
    PropertiesGenerator[I, G], metaclass=ABCMeta
):
    @abstractmethod
    def merge(self, item: I, context: ContractContext, parent: P = None) -> bool:
        """
        Merge the properties of the given item with the properties of its remote database object.

        :param item: The item to modify properties for.
        :param context: The contract context to use.
        :param parent: The parent item that the given child `item` belongs to if available.
        :return: Whether the properties of the item were modified.
        """
        raise NotImplementedError


class ParentPropertiesGenerator[I: PropertiesT, G: PropertyGenerator](PropertiesGenerator[I, G], metaclass=ABCMeta):
    _properties_defaults: dict[str, Any] = {"version": 2}

    filename: str = Field(
        description="The name to give to new properties files generated by this generator.",
        default="_config.yml",
        examples=["properties.yml", "config.yml"],
    )
    depth: int | None = Field(
        description=(
            "The depth at which to place newly generated files within the resource folder. "
            "e.g. 0 would place the file in the root of the resource folder, "
            "1 would place it in a subfolder of the root of the resource folder, etc. "
            "By default, the file is placed in the same folder in which the resource is stored."
        ),
        default=None,
        ge=0,
        examples=[0, 1, 2, 3],
    )

    def update(self, item: I, context: ContractContext) -> dict[str, Any]:
        """
        Update/generate the properties of the given item as a properties file.

        If the item already has a properties path configured, properties will be updated in-place.
        Otherwise, a new properties file will be generated based on the configuration of this generator.
        This new properties file will be added to the context's properties store.
        The path for this new properties file will be added to the item's attributes.

        :param item: The item to update properties for.
        :param context: The contract context to use.
        :return: The updated properties.
        """
        try:
            path = context.properties.get_path(item, to_absolute=True)
        except FileNotFoundError:
            path = None

        if path is not None and path.is_file():
            return self._update_existing_properties(item, properties=context.properties[path])

        flags = get_flags()
        project_dir = Path(getattr(flags, "PROJECT_DIR", None) or "")

        path = self.generate_properties_path(item)
        if isinstance(item, ParsedResource):
            item.patch_path = f"{context.manifest.metadata.project_name}://{path.relative_to(project_dir)}"
        elif isinstance(item, BaseResource):
            item.original_file_path = str(path.relative_to(project_dir))

        if (existing_properties_file := context.properties.get(path)) is not None:
            # file exists but item's properties are not in it
            # update properties file with the item's generated properties
            return self._update_existing_properties(item, properties=existing_properties_file)

        properties = self._generate_properties(item)
        context.properties[path] = properties
        return properties

    @abstractmethod
    def _update_existing_properties(self, item: I, properties: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _generate_properties(item: Any) -> dict[str, Any]:
        """Generate a properties mapping for the given item."""
        raise NotImplementedError

    def generate_properties_path(self, item: I) -> Path:
        """Generate a new properties path to use for the given item."""
        flags = get_flags()
        project_dir = Path(getattr(flags, "PROJECT_DIR", None) or "")

        item_path = item.original_file_path or item.path
        if self.depth is None:
            path = project_dir.joinpath(item_path).with_name(self.filename)
        else:
            parts = Path(item_path).parts
            path = project_dir.joinpath(*parts[:self.depth + 1]).joinpath(self.filename)

        if path.suffix.casefold() not in {".yml", ".yaml"}:
            path = path.with_suffix(".yml")

        return path
