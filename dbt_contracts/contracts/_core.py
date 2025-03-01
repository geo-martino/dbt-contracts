from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from collections.abc import Mapping, Iterable, Sequence, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from pydantic import BaseModel, Field

from dbt_contracts.contracts.result import Result
from dbt_contracts.types import ItemT, ParentT


@dataclass
class ContractContext:
    """
    Context for a contract to run within.
    Stores artifacts for the loaded DBT project and handles logging of results.
    """
    manifest: Manifest | None = None
    catalog: CatalogArtifact | None = None
    result_processor: type[Result] | None = None

    @property
    def results(self) -> list[Result]:
        return self._results

    def __post_init__(self) -> None:
        self._results = []
        self._patches: dict[Path, Mapping[str, Any]] = {}

    def add_result(self, name: str, message: str, item: ItemT, parent: ParentT = None, **kwargs) -> None:
        """
        Create and add a new :py:class:`.Result` to the current list

        :param name: The name to give to the generated result.
        :param message: The result message.
        :param item: The item that produced the result.
        :param parent: The parent of the item that produced the result if available.
        :param kwargs: Other result kwargs to pass to the result
        """
        result = self.result_processor.from_resource(
            item=item,
            parent=parent,
            result_name=name,
            result_level="warning",
            message=message,
            patches=self._patches,
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


class Contract[I: ItemT](BaseModel, metaclass=ABCMeta):
    """
    Composes the terms and conditions that make a contract for specific types of dbt objects within a manifest.
    """
    conditions: Sequence[ContractCondition] = Field(
        description="The conditions to apply to items when filtering items to process",
        default=tuple(),
    )
    terms: Sequence[ContractTerm] = Field(
        description="The terms to apply to items when validating items",
        default=tuple(),
    )
    manifest: Manifest = Field(
        description="The dbt manifest to extract items from",
        default=None
    )
    catalog: CatalogArtifact | None = Field(
        description="The dbt catalog to extract information on database objects from",
        default=None
    )

    @property
    @abstractmethod
    def result_processor(self) -> type[Result]:
        """Get the result processor to use when storing results."""
        raise NotImplementedError

    @property
    @abstractmethod
    def items(self) -> Iterable[I]:
        """Get all the items that this contract can process from the manifest."""
        raise NotImplementedError

    @property
    def filtered_items(self) -> Generator[I]:
        """
        Get all the items that this contract can process from the manifest
        filtered according to the given conditions.
        """
        for item in self.items:
            if all(condition.validate(item) for condition in self.conditions):
                yield item

    @property
    def context(self) -> ContractContext:
        """Generate a context object from the current loaded dbt artifacts"""
        return ContractContext(manifest=self.manifest, catalog=self.catalog, result_processor=self.result_processor)


class ParentContract[I: ParentT](Contract[I], metaclass=ABCMeta):
    @abstractmethod
    def create_child_contract(
            self, conditions: Sequence[ContractCondition], terms: Sequence[ContractTerm]
    ) -> ChildContract[I] | None:
        """Create a child contract from this parent contract if available"""
        return ChildContract[I](parent_contract=self, conditions=conditions, terms=terms)


class ChildContract[I: ItemT, P: ParentT](BaseModel, metaclass=ABCMeta):
    parent_contract: Contract[ParentT] = Field(
        description="The contract object representing the parent contract for this child contract."
    )
    conditions: Sequence[ContractCondition] = Field(
        description="The conditions to apply to items when filtering items to process",
        default=tuple(),
    )
    terms: Sequence[ContractTerm] = Field(
        description="The terms to apply to items when validating items",
        default=tuple(),
    )

    @property
    @abstractmethod
    def result_processor(self) -> type[Result]:
        """Get the result processor to use when storing results."""
        raise NotImplementedError

    @property
    @abstractmethod
    def items(self) -> Iterable[I]:
        """Get all the items that this contract can process from the manifest."""
        raise NotImplementedError

    @property
    def filtered_items(self) -> Generator[I]:
        """
        Get all the items that this contract can process from the manifest
        filtered according to the given conditions.
        """
        for item in self.items:
            if all(condition.validate(item) for condition in self.conditions):
                yield item

