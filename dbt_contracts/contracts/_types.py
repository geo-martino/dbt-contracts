from typing import TypeVar

from dbt.artifacts.resources.base import BaseResource
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument

T = TypeVar('T', BaseResource, ColumnInfo, MacroArgument)
ParentT = TypeVar('ParentT', BaseResource, None)
CombinedT = T | tuple[T, ParentT]
