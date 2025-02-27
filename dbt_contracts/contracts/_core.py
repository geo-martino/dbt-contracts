from abc import ABCMeta, abstractmethod

from pydantic import BaseModel

from dbt_contracts.types import ItemT, ParentT


class ContractTerm[I: ItemT, P: ParentT](BaseModel, metaclass=ABCMeta):
    """
    A part of a contract meant to apply checks on a specific item according to a set of rules.

    May also process an item while also taking into account its parent item
    e.g. a Column (child item) within a Model (parent item)
    """

    @abstractmethod
    def run(self, item: I, parent: P = None) -> bool:
        """
        Run this term on the given item and its parent.

        :param item: The item to check.
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
