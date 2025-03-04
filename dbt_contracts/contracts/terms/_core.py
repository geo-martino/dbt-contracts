from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod

from pydantic import BaseModel

from dbt_contracts.contracts import ContractContext
from dbt_contracts.types import ItemT, ParentT


class ContractTerm[I: ItemT, P: ParentT](BaseModel, metaclass=ABCMeta):
    """
    A part of a contract meant to apply checks on a specific item according to a set of rules.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """
    @property
    def name(self) -> str:
        """The name of this condition in snake_case."""
        return self._name()

    @classmethod
    def _name(cls) -> str:
        """The name of this term in snake_case."""
        class_name = cls.__name__.replace("Term", "")
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
