from pathlib import Path
from random import sample
from unittest import mock

import pytest
import yaml
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition

from dbt_contracts.properties import PropertiesIO


class TestPropertiesIO:

    @pytest.fixture
    def properties(self) -> PropertiesIO:
        """Fixture for the PropertiesIO object to test."""
        return PropertiesIO()

    def test_get_path_with_properties_path(self, model: ModelNode):
        assert model.patch_path
        with mock.patch("dbt_contracts.properties.get_absolute_project_path") as mock_func:
            assert PropertiesIO.get_path(model) == Path(model.patch_path.split("://")[1])
            mock_func.assert_not_called()

    def test_get_path_without_properties_path(self, source: SourceDefinition):
        source.properties_path = None
        with mock.patch("dbt_contracts.properties.get_absolute_project_path") as mock_func:
            assert PropertiesIO.get_path(source) == Path(source.original_file_path)
            mock_func.assert_not_called()

    def test_get_absolute_properties_path(self, source: SourceDefinition):
        with mock.patch("dbt_contracts.properties.get_absolute_project_path") as mock_func:
            assert PropertiesIO.get_path(source, to_absolute=True).is_absolute()
            mock_func.assert_called_once()

    def test_get_properties_file_for_invalid_properties_path(
            self, properties: PropertiesIO, model: ModelNode, tmp_path: Path
    ):
        with pytest.raises(KeyError):  # properties file doesn't exist
            assert not properties[model]

        model.patch_path = None
        with pytest.raises(KeyError):  # properties path is None
            assert not properties[model]

    def test_get_properties_file_for_valid_properties_path(
            self, properties: PropertiesIO, model: ModelNode, tmp_path: Path
    ):
        path = tmp_path.joinpath(model.original_file_path).with_suffix(".yml")
        model.patch_path = f"{model.package_name}://{path}"

        path.parent.mkdir(parents=True, exist_ok=True)
        expected = {"key": "value"}
        with path.open("w") as file:
            yaml.dump(expected, file)

        with mock.patch.object(PropertiesIO, "_read_file", return_value=expected) as read_properties_file:
            assert properties[model] == expected
            read_properties_file.assert_called_once_with(path)
            assert path in properties  # loaded properties are stored

            # properties are pulled from loaded properties and file is not read again
            assert properties[model] == expected
            read_properties_file.assert_called_once_with(path)

        for path in properties:
            assert path.is_absolute()

    def test_read_properties_file(self, source: SourceDefinition, tmp_path: Path):
        path = tmp_path.joinpath(source.original_file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        output = {"key1": "value", "key2": "value"}
        with path.open("w") as file:
            yaml.dump(output, file)

        assert PropertiesIO._read_file(path) == output | {
            "__start_line__": 1,
            "__start_col__": 1,
            "__end_line__": 3,
            "__end_col__": 1,
        }

    def test_save(self, model: ModelNode, tmp_path: Path):
        properties = PropertiesIO({
            tmp_path.joinpath("properties1.yml"): {"key1": "value1"},
            tmp_path.joinpath("properties2.yml"): {"key2": "value2"},
            tmp_path.joinpath("properties3.yml"): {"key3": "value3"},
            tmp_path.joinpath("properties4.yml"): {"key4": "value4"},
        })
        paths = sample(list(properties), k=2)

        properties.save(paths)
        for path, properties in properties.items():
            if path in paths:
                assert path.is_file()
                assert yaml.full_load(path.read_text()) == properties
            else:
                assert not path.is_file()
