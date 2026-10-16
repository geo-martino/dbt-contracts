"""
Generic types to use for all contracts.
"""
from dbt.contracts.graph.nodes import SourceDefinition, Macro

from dbt_contracts._compat import BaseResource, ColumnInfo, ParsedResource, MacroArgument

type ChildT = ColumnInfo | MacroArgument
type ParentT = BaseResource | None
type ItemT = ColumnInfo | MacroArgument | BaseResource | None

type PropertiesT = ParsedResource | SourceDefinition | Macro
type DescriptionT = ParsedResource | ColumnInfo | SourceDefinition | Macro | MacroArgument
type TagT = ParsedResource | ColumnInfo
type MetaT = ParsedResource | ColumnInfo
type NodeT = ParsedResource | SourceDefinition
