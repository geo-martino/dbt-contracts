from abc import ABCMeta

from dbt_common.dataclass_schema import dbtClassMixin
from pydantic import Field

from dbt_contracts.contracts.generators._core import PropertyGenerator
from dbt_contracts.types import DescriptionT


class SetDescription[S: DescriptionT, T: dbtClassMixin](PropertyGenerator[S, T], metaclass=ABCMeta):
    terminator: str | None = Field(
        description=(
            "Only take the description up to this terminating string. "
            "e.g. if you only want to take the first line of a multi-line description, set this to '\\n'"
        ),
        default=None,
        examples=["\\n", "__END__", "."],
    )

    @classmethod
    def _name(cls) -> str:
        return "description"

    def _set_description(self, item: S, description: str | None) -> bool:
        if not description:
            return False
        if item.description and not self.overwrite:
            return False

        if self.terminator:
            description = description.split(self.terminator)[0]
        if item.description == description:
            return False

        item.description = description
        return True
