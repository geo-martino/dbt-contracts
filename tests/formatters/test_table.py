from pathlib import Path

import pytest
from colorama import Fore

from dbt_contracts.contracts.result import ModelResult, Result
from dbt_contracts.formatters.table import TableCellBuilder, TableRowBuilder, TableFormatter


class TestTableCellBuilder:
    @pytest.fixture
    def result(self, tmp_path: Path) -> Result:
        """A fixture for a result object."""
        return ModelResult(
            name="this is a result",
            path=tmp_path,
            result_type="failure",
            result_level="error",
            result_name="result",
            message="This is an error message.",
        )

    def test_validate_wrap(self):
        TableCellBuilder(key="name", wrap=True, max_width=30)  # valid
        with pytest.raises(Exception):
            TableCellBuilder(key="name", wrap=True)

    def test_prefix_coloured(self):
        assert TableCellBuilder(key="name").prefix_coloured == ""
        assert TableCellBuilder(key="name", prefix="pre-").prefix_coloured == "pre-"

        result = TableCellBuilder(key="name", prefix="pre-", colour=Fore.CYAN).prefix_coloured
        expected = f"{Fore.CYAN.replace("m", ";1m")}pre-{Fore.RESET.replace("m", ";0m")}"
        assert result == expected

    def test_get_value(self, result: Result):
        builder = TableCellBuilder(key="name")
        assert builder._get_value(result) == "this is a result"

        builder = TableCellBuilder(key=lambda x: f"{x.name} ({x.result_type})")
        assert builder._get_value(result) == "this is a result (failure)"

    def test_apply_prefix(self):
        builder = TableCellBuilder(key="name")
        assert builder._apply_prefix("value") == "value"

        builder = TableCellBuilder(key="name", prefix="pre-")
        assert builder._apply_prefix("value") == "pre-value"

        builder = TableCellBuilder(key="name", prefix="pre-", colour=Fore.CYAN)
        assert builder._apply_prefix("value") == f"{builder.prefix_coloured}value" != "pre-value"

    # noinspection SpellCheckingInspection
    def test_truncate_value(self):
        builder = TableCellBuilder(key="name", max_width=5)
        assert builder._truncate_value("value") == "value"

        builder = TableCellBuilder(key="name", max_width=5)
        assert builder._truncate_value("value") == "value"

        builder = TableCellBuilder(key="name", max_width=5)
        assert builder._truncate_value("valuevalue") == "valu…"

    def test_apply_padding_and_alignment(self):
        builder = TableCellBuilder(key="name")
        assert builder._apply_padding_and_alignment("value", width=10) == f"{"value":<10}"

        builder = TableCellBuilder(key="name", alignment=">")
        assert builder._apply_padding_and_alignment("value", width=10) == f"{"value":>10}"

        builder = TableCellBuilder(key="name", alignment="^")
        assert builder._apply_padding_and_alignment("value", width=10) == f"{"value":^10}"

    def test_apply_colour(self):
        builder = TableCellBuilder(key="name")
        assert builder._apply_colour("value") == "value"

        builder = TableCellBuilder(key="name", colour=Fore.CYAN)
        assert builder._apply_colour("value") == f"{Fore.CYAN}value{Fore.RESET}"

    def test_apply_wrap(self):
        builder = TableCellBuilder(key="name")
        value = "i am a very long value"
        assert builder._apply_wrap(value) == [value]

        builder = TableCellBuilder(key="name", max_width=0)
        assert builder._apply_wrap(value) == [value]

        builder = TableCellBuilder(key="name", max_width=10, wrap=True)
        assert builder._apply_wrap(value) == ["i am a", "very long", "value"]

    def test_build(self, result: Result):
        builder = TableCellBuilder(key="name")
        assert builder.build(result) == "this is a result"

        builder = TableCellBuilder(key="name", prefix="pre-", min_width=20, max_width=14, colour=Fore.CYAN)
        assert builder.build(result) == f"{builder.prefix_coloured}{Fore.CYAN}this is a…{Fore.RESET}      "

        builder = TableCellBuilder(
            key="name", prefix="pre-", alignment=">", min_width=15, max_width=10, wrap=True, colour=Fore.RED
        )
        expected_width = builder.min_width + len(Fore.RED) + len(Fore.RESET)
        expected_width_with_prefix = expected_width + len(builder.prefix_coloured) - len(builder.prefix)
        expected = (
            f"{f"{builder.prefix_coloured}{builder.colour}this{Fore.RESET}":>{expected_width_with_prefix}}\n"
            f"{f"{builder.colour}is a{Fore.RESET}":>{expected_width}}\n"
            f"{f"{builder.colour}result{Fore.RESET}":>{expected_width}}"
        )
        assert builder.build(result) == expected

    def test_build_uses_min_width(self, result: Result):
        builder = TableCellBuilder(key="name", min_width=20, max_width=10)
        assert builder.build(result) == f"{"this is a…":<20}"
        assert builder.build(result, min_width=40) == f"{"this is a…":<40}"


class TestTableRowBuilder:
    @pytest.fixture
    def result(self, tmp_path: Path) -> Result:
        """A fixture for a result object."""
        return ModelResult(
            name="this is a result",
            path=tmp_path,
            result_type="failure",
            result_level="error",
            result_name="result",
            message="This is an error message.",
        )

    @pytest.fixture
    def cells(self) -> list[TableCellBuilder]:
        """A fixture for a table cell builders."""
        return [
            TableCellBuilder(key="name", min_width=10, max_width=20),
            TableCellBuilder(key="result_type", min_width=10, max_width=20),
            TableCellBuilder(key="result_level", min_width=10, max_width=20),
        ]

    def test_separator_coloured(self, cells: list[TableCellBuilder]):
        assert TableRowBuilder(cells=cells).separator_coloured == "|"
        assert TableRowBuilder(cells=cells, separator=",").separator_coloured == ","

        builder = TableRowBuilder(cells=cells, separator=",", colour=Fore.RED)
        expected = f"{Fore.RED.replace("m", ";1m")},{Fore.RESET.replace("m", ";0m")}"
        assert builder.separator_coloured == expected

    def test_get_widths_from_lines(self):
        lines = [["this is a cell", "this is another cell", "this is the last cell"]]
        assert TableRowBuilder.get_widths_from_lines(lines) == [len(lines[0][0]), len(lines[0][1]), len(lines[0][2])]

        lines = [
            ["this is", "this is", "this"],
            ["a", "another cell", "is"],
            ["cell", " " * len("another cell"), "the"],
            [" " * len("this is"), " " * len("another cell"), "last cell"],
        ]
        assert TableRowBuilder.get_widths_from_lines(lines) == [7, 12, 9]

    def test_get_max_lines(self):
        lines = ["this is a cell", "this is another cell", "this is the last cell"]
        assert TableRowBuilder._get_max_lines(lines) == 1

        lines[2] = "this is\nthe last\ncell"
        assert TableRowBuilder._get_max_lines(lines) == 3

        lines[1] = "this is\nanother cell"
        assert TableRowBuilder._get_max_lines(lines) == 3

        lines[0] = "this\nis\na\ncell"
        assert TableRowBuilder._get_max_lines(lines) == 4

    def test_pad_cell_lines(self):
        lines = ["this is a cell", "this is another cell", "this is the last cell"]
        result = TableRowBuilder._pad_cell_lines(lines)
        assert result == [lines]

        lines[1] = "this is\nanother cell"
        result = TableRowBuilder._pad_cell_lines(lines)
        assert result == [
            ["this is a cell", "this is", "this is the last cell"],
            [" " * len(lines[0]), "another cell", " " * len(lines[2])],
        ]

        lines[2] = "this\nis\nthe\nlast cell"
        result = TableRowBuilder._pad_cell_lines(lines)
        assert result == [
            ["this is a cell", "this is", "this"],
            [" " * len(lines[0]), "another cell", "is"],
            [" " * len(lines[0]), " " * len("another cell"), "the"],
            [" " * len(lines[0]), " " * len("another cell"), "last cell"],
        ]

        lines[0] = "this is\na\ncell"
        result = TableRowBuilder._pad_cell_lines(lines)
        assert result == [
            ["this is", "this is", "this"],
            ["a", "another cell", "is"],
            ["cell", " " * len("another cell"), "the"],
            [" " * len("this is"), " " * len("another cell"), "last cell"],
        ]

    def test_remove_empty_lines(self):
        lines = [["this is a cell", "this is another cell"]]
        assert TableRowBuilder._remove_empty_lines(lines) == lines

        lines.append(["this is a 2nd row cell", "         "])
        assert TableRowBuilder._remove_empty_lines(lines) == lines

        lines.append(["            ", "         "])
        assert TableRowBuilder._remove_empty_lines(lines) == lines[:2]

    def test_build_lines(self, cells: list[TableCellBuilder], result: Result):
        pass  # TODO

    def test_build_lines_uses_min_widths(self, cells: list[TableCellBuilder], result: Result):
        pass  # TODO

    def test_extend_line_widths(self, cells: list[TableCellBuilder], result: Result):
        pass  # TODO

    def test_join(self, cells: list[TableCellBuilder], result: Result):
        pass  # TODO

    def test_build(self, cells: list[TableCellBuilder], result: Result):
        builder = TableRowBuilder(cells=cells, separator=":")
        assert builder.build(result) == "this is a result : failure    : error     "

        cells.append(TableCellBuilder(key="message", min_width=10, max_width=6, wrap=True))
        builder = TableRowBuilder(cells=cells, colour=Fore.RED)
        sep = builder.separator_coloured
        assert builder.build(result) == "\n".join((
            f"this is a result {sep} failure    {sep} error      {sep} This      ",
            f"                 {sep}            {sep}            {sep} is an     ",
            f"                 {sep}            {sep}            {sep} error     ",
            f"                 {sep}            {sep}            {sep} message.  ",
        ))

    def test_build_uses_min_widths(self, cells: list[TableCellBuilder], result: Result):
        pass  # TODO


class TestTableBuilder:
    @pytest.fixture
    def results(self, tmp_path: Path) -> list[Result]:
        """A fixture for the results."""
        return [
            ModelResult(
                name="this is the 1st result",
                path=tmp_path,
                result_type="failure",
                result_level="error",
                result_name="result",
                message="This is an error message.",
            ),
            ModelResult(
                name="this is the 2nd result",
                path=tmp_path,
                result_type="a very great success",
                result_level="info",
                result_name="result",
                message="This is a success message.",
            )
        ]

    @pytest.fixture
    def formatter(self, ) -> TableFormatter:
        """A fixture for the table formatter."""
        cells = [
            TableCellBuilder(key="name", min_width=10, max_width=20),
            TableCellBuilder(key="result_type", min_width=10, max_width=20),
            TableCellBuilder(key="result_level", min_width=10, max_width=20),
        ]
        row = TableRowBuilder(cells=cells)
        return TableFormatter(builder=row)

    def test_add_header(self, formatter: TableFormatter):
        formatter.add_header("i am a header")
        assert formatter._lines == ["i am a header", ""]

        formatter.add_header("i am a 2nd header")
        assert formatter._lines == ["i am a 2nd header", ""]

        formatter._lines = ["i am a line", "i am another line"]
        formatter.add_header("i am a 3rd header")
        assert formatter._lines == ["i am a 3rd header", "", "i am a line", "i am another line"]

    def test_add_results(self, results: list[Result], formatter: TableFormatter):
        formatter.consistent_widths = False
        formatter.add_results(results)
        assert formatter._results == [
            ["this is the 1st res…", "failure   ", "error     "],
            ["this is the 2nd res…", "a very great success", "info      "],
        ]

    def test_add_results_with_consistent_widths(self, results: list[Result], formatter: TableFormatter):
        formatter.consistent_widths = True
        formatter.add_results(results)
        assert formatter._results == [
            ["this is the 1st res…", "failure             ", "error     "],
            ["this is the 2nd res…", "a very great success", "info      "],
        ]

    def test_build(self, results: list[Result], formatter: TableFormatter):
        formatter.consistent_widths = True
        formatter.add_results(results)
        assert formatter.build() == (
            'this is the 1st res… | failure              | error     \n'
            'this is the 2nd res… | a very great success | info      '
        )


class TestGroupedTableFormatter:
    pass  # TODO
