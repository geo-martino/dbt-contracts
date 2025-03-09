from abc import ABCMeta, abstractmethod

from dbt_common.dataclass_schema import dbtClassMixin

from dbt_contracts.contracts.generators.properties import SetDescription
from dbt_contracts.types import DescriptionT


class SetDescriptionTester[S: DescriptionT, T: dbtClassMixin](metaclass=ABCMeta):
    """Base class for testing implementations of the SetDescription property generator"""

    @abstractmethod
    def generator(self) -> SetDescription[S, T]:
        """Fixture for the property generator to test."""
        raise NotImplementedError

    @abstractmethod
    def item(self, **kwargs) -> S:
        """Fixture for the item to test."""
        raise NotImplementedError

    @staticmethod
    def test_skips_on_empty_description(generator: SetDescription[S, T], item: S) -> None:
        original_description = item.description

        generator.overwrite = True
        generator.terminator = None

        assert not generator._set_description(item, description=None)
        assert not generator._set_description(item, description="")
        assert item.description == original_description

    @staticmethod
    def test_skips_on_not_overwrite(generator: SetDescription[S, T], item: S) -> None:
        original_description = "old description"
        item.description = original_description
        description = "new description"

        generator.overwrite = False
        generator.terminator = None

        assert not generator._set_description(item, description=description)
        assert item.description == original_description

    @staticmethod
    def test_skips_on_matching_description(generator: SetDescription[S, T], item: S) -> None:
        original_description = "description line 1\ndescription line 2"
        item.description = original_description

        generator.overwrite = True
        generator.terminator = None

        assert not generator._set_description(item, description=original_description)
        assert item.description == original_description

        generator.terminator = "\n"
        original_description_line_1 = original_description.split(generator.terminator)[0]
        item.description = original_description_line_1
        assert not generator._set_description(item, description=original_description)
        assert item.description == original_description_line_1

    @staticmethod
    def test_valid_set(generator: SetDescription[S, T], item: S) -> None:
        original_description = "old description"
        item.description = original_description
        description = "new description"

        generator.overwrite = True
        generator.terminator = None

        assert generator._set_description(item, description=description)
        assert item.description == description

        item.description = original_description
        generator.terminator = "\n"
        assert generator._set_description(item, description=description + "\n")
        assert item.description == description

        item.description = original_description
        assert generator._set_description(item, description=description + "\nanother line")
        assert item.description == description
