from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from pydantic import BaseModel

from dbt_contracts.contracts.result import Result, RESULT_PROCESSOR_MAP
from dbt_contracts.types import ItemT, ParentT


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
        return self._results

    def __post_init__(self) -> None:
        self._results = []

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


class ContractTerm[I: ItemT, P: ParentT](BaseModel, metaclass=ABCMeta):
    """
    A part of a contract meant to apply checks on a specific item according to a set of rules.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """
    @property
    def _term_name(self) -> str:
        class_name = self.__class__.__name__
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", class_name).lower()

    @abstractmethod
    def run(self, item: I, context: ContractContext, parent: P = None) -> bool:
        """
        Run this term on the given item and its parent.

        :param item: The item to check.
        :param context: The contract context to use.
        :param parent: The parent item that the given child `item` belongs to if available.
        :return: Boolean for if the item passes the term.
        """
        raise NotImplementedError


class ContractCondition[T: ItemT](BaseModel, metaclass=ABCMeta):
    """
    Conditional logic to apply to items within the manifest to determine
    whether they should be processed by subsequent terms.
    """
    @abstractmethod
    def validate(self, item: T) -> bool:
        """Check whether the given item should be processed."""
        raise NotImplementedError
