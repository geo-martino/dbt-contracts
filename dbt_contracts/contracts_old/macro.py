"""
Contract configuration for macros.
"""
import itertools
from collections.abc import Iterable

from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import Macro

from dbt_contracts.contracts_old._core import ParentContract, ChildContract


class MacroArgumentContract(ChildContract[MacroArgument, Macro]):
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


class MacroContract(ParentContract[Macro, MacroArgumentContract]):
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
