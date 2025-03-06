from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from pydantic import BaseModel

from dbt_contracts.contracts.result import Result, RESULT_PROCESSOR_MAP
from dbt_contracts.types import ItemT, ParentT


class ContractPart(BaseModel, metaclass=ABCMeta):
    """A part of a contract."""

    @property
    def name(self) -> str:
        """The name of this contract part in snake_case."""
        return self._name()

    @classmethod
    @abstractmethod
    def _name(cls) -> str:
        """The name of this contract part in snake_case."""
        raise NotImplementedError


@dataclass
class ContractContext:
    """
    Context for a contract to run within.
    Stores artifacts for the loaded DBT project and handles logging of results.
    """
    manifest: Manifest | None = None
    catalog: CatalogArtifact | None = None
    patches: dict[Path, dict[str, Any]] = field(default_factory=dict)

    @property
    def results(self) -> list[Result]:
        """The list of stored results from term validations."""
        return self._results

    def __post_init__(self) -> None:
        self._results = []

    @staticmethod
    def get_patch_path(item: ItemT) -> Path | None:
        """
        Get the patch path for a given item from its properties.

        :param item: The item to get a patch path for.
        :return: The patch path if found.
        """
        processor = RESULT_PROCESSOR_MAP.get(type(item))
        if processor is None:
            raise Exception(f"Unexpected item to get patch for: {type(item)}")

        return processor.get_patch_path(item)

    def get_patch_file(self, item: ItemT) -> dict[str, Any]:
        """
        Get the patch file by either extracting from the stored patches or loading from disk.

        :param item: The item to get the patch object for.
        :return: The loaded patch file if found.
        """
        processor = RESULT_PROCESSOR_MAP.get(type(item))
        if processor is None:
            raise Exception(f"Unexpected item to get patch for: {type(item)}")

        return processor.get_patch_file(item, self.patches)

    def add_result(self, name: str, message: str, item: ItemT, parent: ParentT = None, **kwargs) -> None:
        """
        Create and add a new :py:class:`.Result` to the current list

        :param name: The name to give to the generated result.
        :param message: The result message.
        :param item: The item that produced the result.
        :param parent: The parent of the item that produced the result if available.
        :param kwargs: Other result kwargs to pass to the result
        """
        processor = RESULT_PROCESSOR_MAP.get(type(item))
        if processor is None:
            raise Exception(f"Unexpected item to create result for: {type(item)}")

        result = processor.from_resource(
            item=item,
            parent=parent,
            result_name=name,
            result_level="warning",
            message=message,
            patches=self.patches,
            **kwargs
        )
        self.results.append(result)
