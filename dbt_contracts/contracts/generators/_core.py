from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod

from dbt_contracts.contracts._core import ContractContext, ContractPart
from dbt_contracts.types import ItemT, ParentT


class ContractGenerator[I: ItemT, P: ParentT](ContractPart, metaclass=ABCMeta):
    """
    A part of a contract meant to generate properties for a resource based on its related database object.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """
    @classmethod
    def _name(cls) -> str:
        """The name of this term in snake_case."""
        class_name = cls.__name__.replace("Generator", "")
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", class_name).lower()

    @abstractmethod
    def run(self, item: I, context: ContractContext, parent: P = None) -> None:
        """
        Run this generator on the given item and its parent.

        :param item: The item to generate properties for.
        :param context: The contract context to use.
        :param parent: The parent item that the given child `item` belongs to if available.
        """
        raise NotImplementedError
