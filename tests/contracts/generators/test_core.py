from abc import ABCMeta, abstractmethod
from pathlib import Path
from unittest import mock

from dbt.artifacts.resources import BaseResource
from dbt.artifacts.resources.v1.components import ParsedResource
from dbt.flags import GLOBAL_FLAGS

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.generators import PropertyGenerator, PropertiesGenerator, ParentPropertiesGenerator, \
    ChildPropertiesGenerator
from dbt_contracts.properties import PropertiesIO
from dbt_contracts.types import ItemT, PropertiesT


class PropertiesGeneratorTester[I: ItemT, G: PropertyGenerator](metaclass=ABCMeta):
    """Base class for testing contract generators."""
    @abstractmethod
    def generator(self) -> PropertiesGenerator[I, G]:
        """Fixture for the properties generator to test."""
        raise NotImplementedError

    @abstractmethod
    def item(self, **kwargs) -> I:
        """Fixture for the item to test."""
        raise NotImplementedError

    @staticmethod
    def test_get_generators(generator: PropertiesGenerator[I, G]):
        assert not generator.exclude
        assert generator.generators
        assert all(isinstance(gen, PropertyGenerator) for gen in generator.generators)

        excluded = generator.generators[0]
        generator.exclude = [excluded.name]
        assert excluded not in generator.generators


class ParentPropertiesGeneratorTester[I: PropertiesT, G: PropertyGenerator](
    PropertiesGeneratorTester[I, G], metaclass=ABCMeta
):
    @abstractmethod
    def generator(self) -> ParentPropertiesGenerator[I, G]:
        raise NotImplementedError

    @staticmethod
    def test_update_with_no_existing_properties_path(
            generator: ParentPropertiesGenerator[I, G], item: PropertiesT, context: ContractContext, tmp_path: Path
    ):
        GLOBAL_FLAGS.PROJECT_DIR = tmp_path

        item.original_file_path = ""
        item.patch_path = None
        assert not context.properties.get_path(item)

        expected_path = tmp_path.joinpath(generator.generate_properties_path(item))
        expected_path.parent.mkdir(parents=True)
        expected_path.touch()

        with (
            mock.patch.object(generator.__class__, "_update_existing_properties") as mock_update,
            mock.patch.object(generator.__class__, "_generate_properties") as mock_generate,
            mock.patch.object(PropertiesIO, "__getitem__", return_value=None) as mock_get,
            mock.patch.object(PropertiesIO, "__setitem__") as mock_set,
        ):
            generator.update(item, context=context)

            mock_update.assert_not_called()
            mock_generate.assert_called_once()
            mock_get.assert_called_once()
            mock_set.assert_called_once()

            properties_path = context.properties.get_path(item, to_absolute=True)
            assert properties_path is not None
            assert tmp_path.joinpath(properties_path) in context.properties

            relative_path = properties_path.relative_to(tmp_path)
            if isinstance(item, ParsedResource):
                assert item.patch_path == f"{context.manifest.metadata.project_name}://{relative_path}"
            elif isinstance(item, BaseResource):
                assert item.original_file_path == str(relative_path)

    @staticmethod
    def test_update_with_existing_properties_path(
            generator: ParentPropertiesGenerator[I, G], item: PropertiesT, context: ContractContext, tmp_path: Path
    ):
        assert context.properties.get_path(item)

        GLOBAL_FLAGS.PROJECT_DIR = tmp_path
        expected_path = tmp_path.joinpath(context.properties.get_path(item))
        expected_path.parent.mkdir(parents=True)
        expected_path.touch()

        with (
            mock.patch.object(generator.__class__, "_update_existing_properties") as mock_update,
            mock.patch.object(generator.__class__, "_generate_properties") as mock_generate,
            mock.patch.object(PropertiesIO, "__getitem__", return_value=None) as mock_get,
            mock.patch.object(PropertiesIO, "__setitem__") as mock_set,
        ):
            generator.update(item, context=context)

            mock_update.assert_called_once()
            mock_generate.assert_not_called()
            mock_get.assert_called_once()
            mock_set.assert_not_called()

    @staticmethod
    def test_update_with_existing_properties_file(
            generator: ParentPropertiesGenerator[I, G], item: PropertiesT, context: ContractContext
    ):
        properties = {"key": "value"}
        item.original_file_path = ""
        item.patch_path = None
        assert not context.properties.get_path(item)

        with (
            mock.patch.object(generator.__class__, "_update_existing_properties") as mock_update,
            mock.patch.object(generator.__class__, "_generate_properties") as mock_generate,
            mock.patch.object(PropertiesIO, "__getitem__", return_value=properties) as mock_get,
            mock.patch.object(PropertiesIO, "__setitem__") as mock_set,
        ):
            generator.update(item, context=context)

            mock_update.assert_called_once()
            mock_generate.assert_not_called()
            mock_get.assert_called_once()
            mock_set.assert_not_called()

    @staticmethod
    def test_generate_properties_path_with_no_set_depth(
            generator: ParentPropertiesGenerator[I, G], item: PropertiesT, tmp_path: Path
    ):
        GLOBAL_FLAGS.PROJECT_DIR = tmp_path
        assert generator.depth is None
        generator.filename = "props"  # no extension given, extension suffix will be added

        expected = tmp_path.joinpath("path", "to", "a", "different", f"{generator.filename}.yml")
        item.original_file_path = expected.relative_to(tmp_path).with_name(Path(item.path).name)
        with mock.patch.object(PropertiesIO, "get_path", return_value=None):
            assert generator.generate_properties_path(item) == expected

    @staticmethod
    def test_generates_properties_path_with_depth(
            generator: ParentPropertiesGenerator[I, G], item: PropertiesT, tmp_path: Path
    ):
        GLOBAL_FLAGS.PROJECT_DIR = tmp_path
        item.path = str(tmp_path.joinpath("path", "to", "a", "model"))
        generator.depth = 1  # takes parents up to index=1
        generator.filename = "properties.yaml"  # valid extension given, suffix will be kept as is

        expected = tmp_path.joinpath("path", "to", generator.filename)
        item.path = expected.relative_to(tmp_path).with_name(Path(item.path).name)
        item.original_file_path = None
        with mock.patch.object(PropertiesIO, "get_path", return_value=None):
            assert generator.generate_properties_path(item) == expected


class ChildPropertiesGeneratorTester[I: ItemT, P: PropertiesT, G](PropertiesGeneratorTester[I, G], metaclass=ABCMeta):
    @abstractmethod
    def generator(self) -> ChildPropertiesGenerator[I, P, G]:
        raise NotImplementedError

    @abstractmethod
    def parent(self, **kwargs) -> P:
        """Fixture for the parent item to test."""
        raise NotImplementedError
