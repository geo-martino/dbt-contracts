"""
Contract configuration for macros.
"""
import inspect
from collections.abc import Iterable
from itertools import chain

from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import Macro

from dbt_contracts.contracts._core import validation_method
from dbt_contracts.contracts.properties import PatchContract, DescriptionPropertyContract


class MacroContract(PatchContract[Macro, None]):
    """Configures a contract for macros."""

    @property
    def items(self) -> Iterable[Macro]:
        macros = self.manifest.macros.values()
        package_macros = filter(lambda macro: macro.package_name == self.manifest.metadata.project_name, macros)
        return self._filter_items(package_macros)


class MacroArgumentContract(DescriptionPropertyContract[MacroArgument, Macro]):
    """Configures a contract for macro arguments."""

    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        arguments = map(lambda macro: [(argument, macro) for argument in macro.arguments], self.parents)
        return self._filter_items(chain.from_iterable(arguments))

    @validation_method
    def has_type(self, argument: MacroArgument, parent: Macro) -> bool:
        """
        Check whether the given `argument` has its type set in an appropriate properties file.

        :param argument: The argument to check.
        :param parent: The parent macro that the argument belongs to.
        :return: True if the resource's properties are valid, False otherwise.
        """
        missing_type = not argument.type
        if missing_type:
            name = inspect.currentframe().f_code.co_name
            self._log_result(argument, parent=parent, name=name, message="Argument does not have a type configured")

        return not missing_type
