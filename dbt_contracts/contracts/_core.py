from abc import ABCMeta, abstractmethod

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from pydantic import BaseModel


class ResourceValidator[T: (BaseResource, ColumnInfo, MacroArgument)](BaseModel, metaclass=ABCMeta):

    @abstractmethod
    def validate(self, item: T) -> bool:
        """Check whether the given item should be processed."""
        raise NotImplementedError


class Contract[T: BaseResource](BaseModel, metaclass=ABCMeta):

    @abstractmethod
    def run(self, item: T) -> None:
        """Run this contract on the given item."""
        raise NotImplementedError


class ChildContract[C: (ColumnInfo, MacroArgument), P: BaseResource](BaseModel, metaclass=ABCMeta):

    @abstractmethod
    def run(self, child: C, parent: P) -> None:
        """Run this contract on the given child and its parent resource."""
        raise NotImplementedError
