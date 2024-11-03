from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Collection, Iterable
from typing import Any, TypeVar, Generic

ObjT = TypeVar('ObjT')
KeysT = str | Callable[[ObjT], Any]


def get_value_from_object(obj: ObjT, key: KeysT[ObjT]) -> Any:
    """
    Get a values from the given `obj` for the given `key`.

    :param obj: The object to get a value from.
    :param key: The key from which to get the value.
        May either be a string of the attribute name, or a lambda function for more complex logic.
    :return: The value from the object.
    """
    return key(obj) if callable(key) else getattr(obj, key)


def get_values_from_object(obj: ObjT, keys: Collection[KeysT[ObjT]]) -> Iterable[Any]:
    """
    Get many values from the given `obj` for the given `key`.

    :param obj: The object to get values from.
    :param keys: The keys from which to get the values.
        May either be a collection strings of the attribute name,
        or a collection of lambda functions for more complex logic.
    :return: The value from the object.
    """
    return (get_value_from_object(obj, key) for key in keys)


class ObjectFormatter(Generic[ObjT], metaclass=ABCMeta):
    """
    Base class for implementations which format a set of objects to strings.
    Usually used to format objects for logging purposes.
    This allows for the separation of how objects should be formatted for logging purposes from their implementations.
    """
    @abstractmethod
    def format(self, objects: Collection[ObjT], **__) -> Collection[str]:
        """
        Format the given objects to a collection of strings which can be used to log as needed.

        :param objects: The objects to format.
        :return: The formatted strings.
        """
        raise NotImplementedError

    @abstractmethod
    def combine(self, values: Collection[str]) -> str:
        """
        Combine the output of the :py:meth:`format` method to a single string.

        :param values: The values to combine.
        :return: The combined values.
        """
        raise NotImplementedError
