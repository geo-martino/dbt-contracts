"""
Implementations for dbt contracts according to the dbt object type.
"""
from ._core import Contract, ParentContract, ChildContract, ProcessorMethod
from ._properties import PatchT, MetaT, TagT
from ._node import NodeT, NodeContract, CompiledNodeT, CompiledNodeContract
from .macro import MacroContract
from .model import ModelContract
from .source import SourceContract

CONTRACTS: list[type[ParentContract]] = [ModelContract, SourceContract, MacroContract]
CONTRACTS_CONFIG_MAP = {str(cls.config_key): cls for cls in CONTRACTS}
