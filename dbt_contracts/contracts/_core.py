from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest

from dbt_contracts._core import BaseModelConfig
from dbt_contracts.contracts.result import Result, RESULT_PROCESSOR_MAP
from dbt_contracts.properties import PropertiesIO
from dbt_contracts.types import ItemT, ParentT


class ContractPart(BaseModelConfig, metaclass=ABCMeta):
    """
    A part of a contract.

    A ContractPart may only process the items it is given.
    It does not know how to get those items from any dbt artifacts.
    These items must be provided by a :py:class:`.Contract` object.
    """

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
    properties: ClassVar[PropertiesIO] = PropertiesIO()

    @property
    def results(self) -> list[Result]:
        """The list of stored results from term validations."""
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
            properties=self.properties,
            result_name=name,
            result_level="warning",
            message=message,
            **kwargs
        )
        self.results.append(result)
