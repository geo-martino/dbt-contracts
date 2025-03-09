from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from typing import ClassVar, Union

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest

from dbt_contracts.contracts._core import ContractContext, ContractPart
from dbt_contracts.types import ItemT, ChildT, ParentT


class ContractTerm[I: ItemT](ContractPart, metaclass=ABCMeta):
    """
    A part of a contract meant to apply checks on a specific item according to a set of rules.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """
    #: Mark if this term requires a loaded manifest to operate.
    #: The manifest must then be provided by the ContractContext for the term to execute successfully.
    needs_manifest: ClassVar[bool] = False

    #: Mark if this term requires a loaded catalog to operate.
    #: The catalog must then be provided by the ContractContext for the term to execute successfully.
    needs_catalog: ClassVar[bool] = False

    @classmethod
    def _name(cls) -> str:
        """The name of this term in snake_case."""
        class_name = cls.__name__.replace("Term", "")
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", class_name).lower()

    @abstractmethod
    def run(self, item: I, context: ContractContext) -> bool:
        """
        Run this term on the given item.

        :param item: The item to check.
        :param context: The contract context to use.
        :return: Boolean for if the item passes the term.
        """
        raise NotImplementedError


class ParentContractTerm[P: ParentT](ContractTerm[P], metaclass=ABCMeta):
    pass


class ChildContractTerm[I: ChildT, P: ParentT](ContractTerm[I], metaclass=ABCMeta):
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


validator_type = Union[
    Callable[[ContractTerm, ItemT, ContractContext], bool],
    Callable[[ContractTerm, ItemT, ContractContext, ParentT | None], bool]
]


def validate_context(func: validator_type) -> validator_type:
    """Decorator to validate the context before running a term."""
    def wrapper(term: ContractTerm, item: ItemT, context: ContractContext, parent: ParentT = None) -> bool:
        """Validate the context before running the term."""
        if term.needs_manifest and (context.manifest is None or not isinstance(context.manifest, Manifest)):
            raise Exception(
                "This term requires a manifest to execute. Provide a manifest through the ContractContext."
            )
        if term.needs_catalog and (context.catalog is None or not isinstance(context.catalog, CatalogArtifact)):
            raise Exception(
                "This term requires a catalog to execute. Provide a catalog through the ContractContext."
            )

        if isinstance(term, ChildContractTerm):
            return func(term, item, context, parent)
        return func(term, item, context)

    return wrapper
