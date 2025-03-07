from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import ModelNode, Macro, SourceDefinition

from dbt_contracts.contracts.result import Result, ModelResult, SourceResult, ColumnResult, MacroResult, \
    MacroArgumentResult, RESULT_PROCESSOR_MAP
from dbt_contracts.properties import PropertiesIO
from dbt_contracts.types import ItemT, ParentT


class TestResult:
    @pytest.mark.parametrize("item", ["model", "source", "macro"])
    def test_get_result_type(self, item: str, model: ModelNode, request: FixtureRequest):
        item: ItemT = request.getfixturevalue(item)
        expected = item.resource_type.name.title()
        assert Result._get_result_type(item=item) == expected
        assert Result._get_result_type(item=item, parent=model) == f"Model {expected}"

    # noinspection PyTestUnpassedFixture
    @pytest.fixture
    def result(self, model: ModelNode, source: SourceDefinition) -> Result:
        """Fixture for a Result object"""
        path = PropertiesIO.get_path(source)
        props = {
            "__start_line__": 90,
            "__start_col__": 1,
            "__end_line__": 115,
            "__end_col__": 15,
        }
        properties = PropertiesIO({path: props})

        cls = RESULT_PROCESSOR_MAP[type(model)]
        with (
            mock.patch.object(cls, "_extract_properties_for_item", return_value=props),
            mock.patch.object(PropertiesIO, "__getitem__", return_value=path),
        ):
            result = cls.from_resource(
                item=model,
                parent=source,
                properties=properties,
                result_level="ERROR",
                result_name="Failure",
                message="Something bad happened"
            )

        return result

    def test_from_resource(self, result: Result, model: ModelNode, source: SourceDefinition):
        assert result.name == model.name
        assert result.path == Path(source.original_file_path)
        assert result.result_type == "Source Model"
        assert result.result_level == "ERROR"
        assert result.result_name == "Failure"
        assert result.message == "Something bad happened"
        assert result.properties_path == Path(source.original_file_path)
        assert result.properties_start_line == 90
        assert result.properties_start_col == 1
        assert result.properties_end_line == 115
        assert result.properties_end_col == 15
        assert result.parent_id == source.unique_id
        assert result.parent_name == source.name
        assert result.parent_type == source.resource_type.name.title()

    def test_github_annotation(self, result: Result):
        assert result.can_format_to_github_annotation

        expected_keys = {
            "path",
            "start_line",
            "start_column",
            "end_line",
            "end_column",
            "annotation_level",
            "title",
            "message",
        }
        assert set(result.as_github_annotation()) & expected_keys == expected_keys

        result.properties_start_line = None
        assert not result.can_format_to_github_annotation


class TestModelResult:
    @pytest.fixture
    def properties(self) -> dict[str, Any]:
        """Fixture for a loaded properties object"""
        return {
            "models": [
                {"name": "model1", "config": {"test": 1}},
                {"name": "model2", "config": {"test": 2}},
                {"name": "model3", "config": {"test": 3}},
            ]
        }

    def test_extract_properties_for_item(self, properties: dict[str, Any], model: ModelNode):
        model.name = "does not exist"
        assert ModelResult._extract_properties_for_item(properties, model) is None

        model.name = "model2"
        assert ModelResult._extract_properties_for_item(properties, model) == properties["models"][1]


class TestSourceResult:
    @pytest.fixture
    def properties(self) -> dict[str, Any]:
        """Fixture for a loaded properties object"""
        return {
            "sources": [
                {
                    "name": "source1",
                    "tables": [
                        {"name": "table1", "config": {"test": 1}},
                        {"name": "table2", "config": {"test": 2}},
                        {"name": "table3", "config": {"test": 3}},
                    ],
                },
                {
                    "name": "source2",
                    "tables": [
                        {"name": "other-table1", "config": {"test": 1}},
                        {"name": "other-table2", "config": {"test": 2}},
                        {"name": "other-table3", "config": {"test": 3}},
                    ],
                }
            ]
        }

    def test_extract_properties_for_item(self, properties: dict[str, Any], source: SourceDefinition):
        source.source_name = "does not exist"
        assert SourceResult._extract_properties_for_item(properties, source) is None

        source.source_name = "source1"
        source.name = "table2"
        assert SourceResult._extract_properties_for_item(properties, source) == properties["sources"][0]["tables"][1]


class TestColumnResult:
    @pytest.fixture
    def properties(self) -> dict[str, Any]:
        """Fixture for a loaded properties object"""
        return {
            "models": [
                {"name": "model1", "columns": [{"name": "col1", "tags": ["tag1"]}]},
                {"name": "model2", "columns": [{"name": "col2", "tags": ["tag2"]}]},
                {"name": "model3", "columns": [{"name": "col3", "tags": ["tag3"]}]},
            ],
            "sources": [
                {
                    "name": "source1",
                    "tables": [
                        {"name": "table1", "columns": [{"name": "col1", "tags": ["tag1"]}]},
                        {"name": "table2", "columns": [{"name": "col2", "tags": ["tag2"]}]},
                        {"name": "table3", "columns": [{"name": "col3", "tags": ["tag3"]}]},
                    ],
                },
                {
                    "name": "source2",
                    "tables": [
                        {"name": "other-table1", "columns": [{"name": "col1", "tags": ["tag1"]}]},
                        {"name": "other-table2", "columns": [{"name": "col2", "tags": ["tag2"]}]},
                        {"name": "other-table3", "columns": [{"name": "col3", "tags": ["tag3"]}]},
                    ],
                }
            ]
        }

    def test_extract_properties_for_model(self, properties: dict[str, Any], model: ModelNode, column: ColumnInfo):
        model.name = "model2"
        column.name = "col2"

        expected = properties["models"][1]["columns"][0]
        assert ColumnResult._extract_properties_for_item(properties, item=column, parent=model) == expected

    def test_extract_properties_for_source(
            self, properties: dict[str, Any], source: SourceDefinition, column: ColumnInfo
    ):
        source.source_name = "source1"
        source.name = "table2"
        column.name = "col2"

        expected = properties["sources"][0]["tables"][1]["columns"][0]
        assert ColumnResult._extract_properties_for_item(properties, item=column, parent=source) == expected

    @pytest.mark.parametrize("parent", ["model", "source"])
    def test_get_result_type(self, parent: str, column: ColumnInfo, request: FixtureRequest):
        parent: ParentT = request.getfixturevalue(parent)
        expected = parent.resource_type.name.title()
        assert ColumnResult._get_result_type(item=column, parent=parent) == f"{expected} Column"

    def test_from_resource_gets_index(self, source: SourceDefinition, column: ColumnInfo):
        assert column.name not in ("col1", "col2", "col3")
        source.columns = {
            "col1": ColumnInfo(name="col1", tags=["tag1"]),
            "col2": ColumnInfo(name="col2", tags=["tag2"]),
            "col3": ColumnInfo(name="col3", tags=["tag3"]),
            column.name: column,
        }

        result = ColumnResult.from_resource(
            item=column, parent=source, result_level="ERROR", result_name="Failure", message="Something bad happened"
        )
        assert result.index == 3


class TestMacroResult:
    @pytest.fixture
    def properties(self) -> dict[str, Any]:
        """Fixture for a loaded properties object"""
        return {
            "macros": [
                {"name": "macro1", "arguments": [{"name": "arg1", "description": "I am arg1"}]},
                {"name": "macro2", "arguments": [{"name": "arg2", "description": "I am arg2"}]},
                {"name": "macro3", "arguments": [{"name": "arg3", "description": "I am arg3"}]},
            ]
        }

    def test_extract_properties_for_item(self, properties: dict[str, Any], macro: Macro):
        macro.name = "does not exist"
        assert MacroResult._extract_properties_for_item(properties, item=macro) is None

        macro.name = "macro2"
        assert MacroResult._extract_properties_for_item(properties, item=macro) == properties["macros"][1]


class TestMacroArgumentResult:
    @pytest.fixture
    def properties(self) -> dict[str, Any]:
        """Fixture for a loaded properties object"""
        return {
            "macros": [
                {"name": "macro1", "arguments": [{"name": "arg1", "description": "I am arg1"}]},
                {"name": "macro2", "arguments": [{"name": "arg2", "description": "I am arg2"}]},
                {"name": "macro3", "arguments": [{"name": "arg3", "description": "I am arg3"}]},
            ]
        }

    def test_extract_properties_for_item(self, properties: dict[str, Any], macro: Macro, argument: MacroArgument):
        macro.name = "macro2"
        argument.name = "does not exist"
        assert MacroArgumentResult._extract_properties_for_item(properties, item=argument, parent=macro) is None

        argument.name = "arg2"
        expected = properties["macros"][1]["arguments"][0]
        assert MacroArgumentResult._extract_properties_for_item(properties, item=argument, parent=macro) == expected

    @pytest.mark.parametrize("parent", ["model", "source", "macro"])
    def test_get_result_type(self, parent: str, argument: MacroArgument, request: FixtureRequest):
        parent: ParentT = request.getfixturevalue(parent)
        # always gives the same value
        assert MacroArgumentResult._get_result_type(item=argument, parent=parent) == "Macro Argument"

    def test_from_resource_gets_index(self, macro: Macro, argument: MacroArgument):
        assert argument.name not in ("arg1", "arg2", "arg3")
        macro.arguments = [
            MacroArgument(name="arg1"),
            MacroArgument(name="arg2"),
            argument,
            MacroArgument(name="arg3"),
        ]

        result = MacroArgumentResult.from_resource(
            item=argument, parent=macro, result_level="ERROR", result_name="Failure", message="Something bad happened"
        )
        assert result.index == 2
