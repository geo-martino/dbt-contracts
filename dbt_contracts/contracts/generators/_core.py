from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from collections.abc import Sequence, Mapping
from pathlib import Path
from random import choice
from typing import Literal, Annotated, get_args, Any

import yaml
from dbt.flags import get_flags
from pydantic import Field, BeforeValidator

from dbt_contracts.contracts._core import ContractContext, ContractPart
from dbt_contracts.contracts.utils import to_tuple
from dbt_contracts.types import ItemT, PropertiesT

CORE_FIELDS = Literal["description"]


class PropertiesGenerator[I: ItemT](ContractPart, metaclass=ABCMeta):
    """
    A part of a contract meant to generate properties for a resource based on its related database object.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """
    exclude: Annotated[Sequence[CORE_FIELDS], BeforeValidator(to_tuple)] = Field(
        description="The fields to exclude from the generated properties.",
        default=(),
        examples=[choice(get_args(CORE_FIELDS)), list(get_args(CORE_FIELDS))]
    )
    overwrite: bool = Field(
        description=(
            "Whether to overwrite existing properties with properties from the database. "
            "When false, keeps the values already present in the properties if present."
        ),
        default=False,
        examples=[True, False],
    )
    description_terminator: str | None = Field(
        description=(
            "When processing descriptions, only take the description up to this terminating string. "
            "e.g. if you only want to take the first line of a multi-line description, set this to '\\n'"
        ),
        default=None,
        examples=["\\n", "__END__", "."],
    )

    @classmethod
    def _name(cls) -> str:
        """The name of this term in snake_case."""
        class_name = cls.__name__.replace("Generator", "")
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", class_name).lower()

    def _set_description(self, item: ItemT, description: str | None) -> bool:
        if "description" in self.exclude:
            return False
        if not description:
            return False
        if item.description and not self.overwrite:
            return False

        if self.description_terminator:
            description = description.split(self.description_terminator)[0]
        if item.description == description:
            return False

        item.description = description
        return True

    @abstractmethod
    def merge(self, item: I, context: ContractContext) -> bool:
        """
        Merge the properties of the given item with the properties of its remote database object.

        :param item: The item to modify properties for.
        :param context: The contract context to use.
        :return: Whether the properties of the item were modified.
        """
        raise NotImplementedError


class ParentPropertiesGenerator[I: PropertiesT](PropertiesGenerator[I], metaclass=ABCMeta):
    _patch_defaults: dict[str, Any] = {"version": 2}

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

    def save(self, item: I, context: ContractContext) -> None:
        """
        Save the properties of the given item to a properties file.

        If the item already has a patch path configured, properties will be updated in-place.
        Otherwise, a new properties file will be generated based on the configuration of this generator.

        :param item: The item to save properties for.
        :param context: The contract context to use.
        """
        if context.get_patch_path(item):
            patch = self._update_existing_patch(item, context=context)
        else:
            patch = self._generate_new_patch(item)
        return self._save_patch(item, context=context, patch=patch)

    @abstractmethod
    def _update_existing_patch(self, item: I, context: ContractContext) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def _generate_new_patch(self, item: I) -> dict[str, Any]:
        raise NotImplementedError

    def _save_patch(self, item: I, context: ContractContext, patch: Mapping[str, Any]) -> None:
        path = self._get_patch_path(item, context=context)
        with path.open("w") as file:
            yaml.dump(patch, file)

    def _get_patch_path(self, item: I, context: ContractContext) -> Path:
        """Get the patch path to use for the given item."""
        if (path := context.get_patch_path(item)) is not None:
            return path

        flags = get_flags()
        project_dir = Path(getattr(flags, "PROJECT_DIR", None) or "")

        item_path = item.original_file_path or item.path
        if self.depth is None:
            path = project_dir.joinpath(item_path).with_name(self.filename)
        else:
            parts = Path(item_path).parts
            path = project_dir.joinpath(*parts[:self.depth + 1]).joinpath(self.filename)

        if path.suffix not in {".yml", ".yaml"}:
            path = path.with_suffix(".yml")

        return path


class ChildPropertiesGenerator[I: ItemT, P: PropertiesT](PropertiesGenerator[I], metaclass=ABCMeta):
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
