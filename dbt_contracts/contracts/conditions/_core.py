from __future__ import annotations

import re
from abc import ABCMeta, abstractmethod

from dbt_contracts.contracts._contracts import ContractPart
from dbt_contracts.types import ItemT


class ContractCondition[T: ItemT](ContractPart, metaclass=ABCMeta):
    """
    Conditional logic to apply to items within the manifest to determine
    whether they should be processed by subsequent terms.
    """
    @classmethod
    def _name(cls) -> str:
        """The name of this condition in snake_case."""
        class_name = cls.__name__.replace("Condition", "")
        return re.sub(r"([a-z])([A-Z])", r"\1_\2", class_name).lower()

    @abstractmethod
    def run(self, item: T) -> bool:
        """Run this condition to check whether the given item should be processed."""
        raise NotImplementedError
