from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Collection, Iterable
from typing import Any

from dbt_contracts._core import BaseModelConfig
from dbt_contracts.contracts.result import Result

type KeysT[T] = str | Callable[[T], Any]


def get_value_from_object[T](obj: T, key: KeysT[T]) -> Any:
    """
    Get a values from the given `obj` for the given `key`.

    :param obj: The object to get a value from.
    :param key: The key from which to get the value.
        May either be a string of the attribute name, or a lambda function for more complex logic.
    :return: The value from the object.
    """
    return key(obj) if callable(key) else getattr(obj, key)


def get_values_from_object[T](obj: T, keys: Collection[KeysT[T]]) -> Iterable[Any]:
    """
    Get many values from the given `obj` for the given `key`.

    :param obj: The object to get values from.
    :param keys: The keys from which to get the values.
        May either be a collection strings of the attribute name,
        or a collection of lambda functions for more complex logic.
    :return: The value from the object.
    """
    return (get_value_from_object(obj, key) for key in keys)


class ResultsFormatter[T: Result](BaseModelConfig, metaclass=ABCMeta):
    """
    Base class for implementations which format a set of :py:class:`.Result` objects to a string for displaying results.
    Usually used to format results for logging purposes.
    This allows for the separation of how results should be formatted for logging purposes from their implementations.
    """
    @abstractmethod
    def add_results(self, results: Collection[T]) -> None:
        """
        Format the given results and update the stored output to be built.

        :param results: The results to format.
        """
        raise NotImplementedError

    @abstractmethod
    def build(self) -> str:
        """Build the output and return it. This clears the stored output."""
        raise NotImplementedError
