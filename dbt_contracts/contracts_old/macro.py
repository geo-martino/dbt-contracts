"""
Contract configuration for macros.
"""
import inspect
import itertools
from collections.abc import Iterable

from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import Macro

from dbt_contracts.contracts_old._core import enforce_method, ParentContract, ChildContract
from dbt_contracts.contracts_old._properties import PatchContract, DescriptionPropertyContract


class MacroArgumentContract(DescriptionPropertyContract[MacroArgument, Macro], ChildContract[MacroArgument, Macro]):
    """Configures a contract for macro arguments."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def config_key(cls) -> str:
        return "arguments"

    @property
    def items(self) -> Iterable[tuple[MacroArgument, Macro]]:
        arguments = map(lambda macro: [(argument, macro) for argument in macro.arguments], self.parents)
        return self._filter_items(itertools.chain.from_iterable(arguments))

    @enforce_method
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
            self._add_result(argument, parent=parent, name=name, message="Argument does not have a type configured")

        return not missing_type


class MacroContract(PatchContract[Macro, None], ParentContract[Macro, MacroArgumentContract]):
    """Configures a contract for macros."""

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def config_key(cls) -> str:
        return "macros"

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def child_type(cls) -> type[MacroArgumentContract]:
        return MacroArgumentContract

    @property
    def items(self) -> Iterable[Macro]:
        macros = self.manifest.macros.values()
        package_macros = filter(lambda macro: macro.package_name == self.manifest.metadata.project_name, macros)
        return self._filter_items(package_macros)
