"""
Generic types to use for all contracts.
"""
from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import SourceDefinition, Macro, BaseNode

type ChildT = ColumnInfo | MacroArgument
type ParentT = BaseResource | None
type ItemT = ChildT | ParentT

type PropertiesT = ParsedResource | SourceDefinition | Macro
type DescriptionT = ParsedResource | ColumnInfo | SourceDefinition | Macro | MacroArgument
type TagT = ParsedResource | ColumnInfo
type MetaT = ParsedResource | ColumnInfo
type NodeT = ParsedResource | SourceDefinition
