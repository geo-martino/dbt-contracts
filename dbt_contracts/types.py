"""
Generic types to use for all contracts.
"""
from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import SourceDefinition, Macro, BaseNode

ChildT = ColumnInfo | MacroArgument
ParentT = BaseResource | None
ItemT = ChildT | ParentT

PropertiesT = ParsedResource | SourceDefinition | Macro
DescriptionT = ParsedResource | ColumnInfo | SourceDefinition | Macro | MacroArgument
TagT = ParsedResource | ColumnInfo
MetaT = ParsedResource | ColumnInfo
NodeT = BaseNode | SourceDefinition
