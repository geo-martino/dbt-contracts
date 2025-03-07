from abc import ABCMeta, abstractmethod
from pathlib import Path
from unittest import mock

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource
from dbt.flags import GLOBAL_FLAGS

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators import PropertiesGenerator, ParentPropertiesGenerator, ChildPropertiesGenerator
from dbt_contracts.types import ItemT, PropertiesT


class ContractPropertiesGeneratorTester[I: ItemT](metaclass=ABCMeta):
    """Base class for testing contract generators."""
    @abstractmethod
    def generator(self) -> PropertiesGenerator[I]:
        """Fixture for the contract generator to test."""
        raise NotImplementedError

    @abstractmethod
    def item(self, **kwargs) -> I:
        """Fixture for the item to test."""
        raise NotImplementedError

    @staticmethod
    def test_set_description_skips_on_exclude(generator: PropertiesGenerator[I], item: ItemT) -> None:
        original_description = item.description
        description = "description"
        assert item.description != description

        generator.exclude = ["description"]
        generator.overwrite = True
        generator.description_terminator = None

        assert not generator._set_description(item, description=description)
        assert item.description == original_description

    @staticmethod
    def test_set_description_skips_on_empty_description(generator: PropertiesGenerator[I], item: ItemT) -> None:
        original_description = item.description

        assert not generator.exclude
        generator.overwrite = True
        generator.description_terminator = None

        assert not generator._set_description(item, description=None)
        assert not generator._set_description(item, description="")
        assert item.description == original_description

    @staticmethod
    def test_set_description_skips_on_not_overwrite(generator: PropertiesGenerator[I], item: ItemT) -> None:
        original_description = "old description"
        item.description = original_description
        description = "new description"

        assert not generator.exclude
        generator.overwrite = False
        generator.description_terminator = None

        assert not generator._set_description(item, description=description)
        assert item.description == original_description

    @staticmethod
    def test_set_description_skips_on_matching_description(generator: PropertiesGenerator[I], item: ItemT) -> None:
        original_description = "description line 1\ndescription line 2"
        item.description = original_description

        assert not generator.exclude
        generator.overwrite = True
        generator.description_terminator = None

        assert not generator._set_description(item, description=original_description)
        assert item.description == original_description

        generator.description_terminator = "\n"
        original_description_line_1 = original_description.split(generator.description_terminator)[0]
        item.description = original_description_line_1
        assert not generator._set_description(item, description=original_description)
        assert item.description == original_description_line_1

    @staticmethod
    def test_set_description(generator: PropertiesGenerator[I], item: ItemT) -> None:
        original_description = "old description"
        item.description = original_description
        description = "new description"

        assert not generator.exclude
        generator.overwrite = True
        generator.description_terminator = None

        assert generator._set_description(item, description=description)
        assert item.description == description

        item.description = original_description
        generator.description_terminator = "\n"
        assert generator._set_description(item, description=description + "\n")
        assert item.description == description

        item.description = original_description
        assert generator._set_description(item, description=description + "\nanother line")
        assert item.description == description


class ParentPropertiesGeneratorTester[I: PropertiesT](ContractPropertiesGeneratorTester[I], metaclass=ABCMeta):
    @abstractmethod
    def generator(self) -> ParentPropertiesGenerator[I]:
        raise NotImplementedError

    @staticmethod
    def test_update_with_no_existing_patch(
            generator: ParentPropertiesGenerator[I], item: PropertiesT, context: ContractContext, tmp_path: Path
    ):
        GLOBAL_FLAGS.PROJECT_DIR = tmp_path

        item.original_file_path = ""
        item.patch_path = None
        assert not context.get_patch_path(item)

        with (
            mock.patch.object(generator.__class__, "_update_existing_patch") as mock_update,
            mock.patch.object(generator.__class__, "_generate_new_patch") as mock_generate,
        ):
            generator.update(item, context=context)

            mock_update.assert_not_called()
            mock_generate.assert_called_once()

            patch_path = context.get_patch_path(item, to_absolute=True)
            assert patch_path is not None
            assert tmp_path.joinpath(patch_path) in context.patches

            if isinstance(item, ParsedResource):
                assert item.patch_path == f"{context.manifest.metadata.project_name}://{patch_path}"
            elif isinstance(item, BaseResource):
                assert item.original_file_path == str(patch_path)

    @staticmethod
    def test_update_with_existing_patch(
            generator: ParentPropertiesGenerator[I], item: PropertiesT, context: ContractContext
    ):
        assert context.get_patch_path(item)

        with (
            mock.patch.object(generator.__class__, "_update_existing_patch") as mock_update,
            mock.patch.object(generator.__class__, "_generate_new_patch") as mock_generate,
        ):
            generator.update(item, context=context)

            mock_update.assert_called_once()
            mock_generate.assert_not_called()

    @staticmethod
    def test_get_patch_path_on_existing_patch_path(
            generator: ParentPropertiesGenerator[I], item: PropertiesT, context: ContractContext, tmp_path: Path
    ):
        patch_path = tmp_path.joinpath("path", "to", "patch.yml")
        with mock.patch.object(ContractContext, "get_patch_path", return_value=patch_path):
            assert generator._get_patch_path(item, context=context) == patch_path

    @staticmethod
    def test_get_patch_path_generates_patch_path_with_no_set_depth(
            generator: ParentPropertiesGenerator[I], item: PropertiesT, context: ContractContext, tmp_path: Path
    ):
        GLOBAL_FLAGS.PROJECT_DIR = tmp_path
        assert generator.depth is None
        generator.filename = "patch"  # no extension given, extension suffix will be added

        expected = tmp_path.joinpath("path", "to", "a", "different", f"{generator.filename}.yml")
        item.original_file_path = expected.relative_to(tmp_path).with_name(Path(item.path).name)
        with mock.patch.object(ContractContext, "get_patch_path", return_value=None):
            assert generator._get_patch_path(item, context=context) == expected

    @staticmethod
    def test_get_patch_path_generates_patch_path_with_depth(
            generator: ParentPropertiesGenerator[I], item: PropertiesT, context: ContractContext, tmp_path: Path
    ):
        GLOBAL_FLAGS.PROJECT_DIR = tmp_path
        item.path = str(tmp_path.joinpath("path", "to", "a", "model"))
        generator.depth = 1  # takes parents up to index=1
        generator.filename = "patch.yaml"  # valid extension given, suffix will be kept as is

        expected = tmp_path.joinpath("path", "to", generator.filename)
        item.path = expected.relative_to(tmp_path).with_name(Path(item.path).name)
        item.original_file_path = None
        with mock.patch.object(ContractContext, "get_patch_path", return_value=None):
            assert generator._get_patch_path(item, context=context) == expected


class ChildPropertiesGeneratorTester[I: ItemT, P: PropertiesT](ContractPropertiesGeneratorTester[I], metaclass=ABCMeta):
    @abstractmethod
    def generator(self) -> ChildPropertiesGenerator[I, P]:
        raise NotImplementedError

    @abstractmethod
    def parent(self, **kwargs) -> P:
        """Fixture for the parent item to test."""
        raise NotImplementedError
