from pathlib import Path
from unittest import mock

import pytest
from colorama import Fore

from dbt_contracts.contracts.result import ModelResult, Result
from dbt_contracts.formatters.table import TableCellBuilder, TableRowBuilder, TableFormatter, GroupedTableFormatter


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
        assert builder._apply_prefix("") == ""
        assert builder._apply_prefix(f"{Fore.CYAN}{Fore.RESET}") == f"{Fore.CYAN}{Fore.RESET}"
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
        expected_width = builder.min_width + len(Fore.RED + Fore.RESET)

        expected_width_with_prefix = expected_width - len(builder.prefix)
        expected = (
            f"{builder.prefix_coloured}{f"{builder.colour}this{Fore.RESET}":>{expected_width_with_prefix}}\n"
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

    @pytest.fixture
    def builder(self, cells: list[TableCellBuilder]) -> TableRowBuilder:
        """Fixture for a table row builder."""
        return TableRowBuilder(cells=cells)

    def test_validate_cells(self, cells: list[TableCellBuilder]):
        cells_row_2 = cells.copy()
        # noinspection PyTypeChecker
        cells_row_2[0] = None

        # all valid
        TableRowBuilder(cells=cells)
        TableRowBuilder(cells=[cells])
        TableRowBuilder(cells=[cells, cells_row_2])

        with pytest.raises(Exception):
            TableRowBuilder(cells=cells_row_2)
        with pytest.raises(Exception):
            TableRowBuilder(cells=[cells_row_2])
        with pytest.raises(Exception):
            TableRowBuilder(cells=[cells, cells[:-1]])

    def test_separator_coloured(self, cells: list[TableCellBuilder]):
        assert TableRowBuilder(cells=cells).separator_coloured == "|"
        assert TableRowBuilder(cells=cells, separator=",").separator_coloured == ","

        builder = TableRowBuilder(cells=cells, separator=",", colour=Fore.RED)
        expected = f"{Fore.RED.replace("m", ";1m")},{Fore.RESET.replace("m", ";0m")}"
        assert builder.separator_coloured == expected

    def test_get_lines_simple(self, result: Result, cells: list[TableCellBuilder]):
        min_widths = [None] * len(cells)

        builder = TableRowBuilder(cells=cells)
        assert builder._get_lines(result, min_widths) == ["this is a result", "failure   ", "error     "]
        builder = TableRowBuilder(cells=[cells])
        assert builder._get_lines(result, min_widths) == ["this is a result", "failure   ", "error     "]

    def test_get_lines_multi_row(self, result: Result, cells: list[TableCellBuilder]):
        cells = [cells, [None, TableCellBuilder(key="message"), None]]
        min_widths = [None] * len(cells[0])
        expected = ["this is a result\n", "failure   \nThis is an error message.", "error     \n"]

        builder = TableRowBuilder(cells=cells)
        assert builder._get_lines(result, min_widths) == expected

    def test_get_lines_with_min_widths(self, result: Result, cells: list[TableCellBuilder]):
        cells = [cells, [None, TableCellBuilder(key="message"), None]]
        min_widths = [20, 30, 0]
        expected = [
            "this is a result    \n",
            "failure                       \nThis is an error message.     ",
            "error\n"
        ]

        builder = TableRowBuilder(cells=cells)
        assert builder._get_lines(result, min_widths) == expected

    def test_get_lines_with_wrap(self, result: Result, cells: list[TableCellBuilder]):
        cells[0].max_width = 5
        cells[0].wrap = True
        cells = [cells, [TableCellBuilder(key="result_name"), TableCellBuilder(key="message"), None]]
        min_widths = [None] * len(cells[0])
        expected = [
            "this      \nis a      \nresult    \nresult    ",
            "failure   \nThis is an error message.",
            "error     \n"
        ]

        builder = TableRowBuilder(cells=cells)
        assert builder._get_lines(result, min_widths) == expected

    def test_get_max_length(self):
        lines = ["this is a cell", "this is another cell", "this is the last cell"]
        assert TableRowBuilder._get_max_rows(lines) == 1

        lines[2] = "this is\nthe last\ncell"
        assert TableRowBuilder._get_max_rows(lines) == 3

        lines[1] = "this is\nanother cell"
        assert TableRowBuilder._get_max_rows(lines) == 3

        lines[0] = "this\nis\na\ncell"
        assert TableRowBuilder._get_max_rows(lines) == 4

    def test_to_matrix(self):
        lines = ["this is a cell", "this is another cell", "this is the last cell"]
        result = TableRowBuilder._to_matrix(lines)
        assert result == [tuple(lines)]

        lines[1] = "this is\nanother cell"
        result = TableRowBuilder._to_matrix(lines)
        assert result == [
            ("this is a cell", "this is", "this is the last cell"),
            ("", "another cell", ""),
        ]

        lines[2] = "this\nis\nthe\nlast cell"
        result = TableRowBuilder._to_matrix(lines)
        assert result == [
            ("this is a cell", "this is", "this"),
            ("", "another cell", "is"),
            ("", "", "the"),
            ("", "", "last cell"),
        ]

        lines[0] = "this is\na\ncell"
        result = TableRowBuilder._to_matrix(lines)
        assert result == [
            ("this is", "this is", "this"),
            ("a", "another cell", "is"),
            ("cell", "", "the"),
            ("", "", "last cell"),
        ]

    def test_remove_empty_lines(self):
        lines = [["this is a cell", "this is another cell"]]
        assert TableRowBuilder._remove_empty_lines(lines) == lines

        lines.append(["this is a 2nd row cell", "         "])
        assert TableRowBuilder._remove_empty_lines(lines) == lines

        lines.append(["            ", "         "])
        assert TableRowBuilder._remove_empty_lines(lines) == lines[:2]

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

    def test_extend_line_widths_fails(self, builder: TableRowBuilder, result: Result):
        # noinspection PyTypeChecker
        min_widths = [None] * (len(builder.cells[0]) - 1)
        with pytest.raises(Exception):  # too few min widths given
            builder.extend_line_widths([], min_widths=min_widths)

    def test_extend_line_widths(self, builder: TableRowBuilder, result: Result):
        lines = [[
            ["this is a result", "failure", "error", "keep as is"],
            ["result", "This is an error message.", "", ""],
            ["this is a result", "failure", "error", ""],
        ]]
        builder.cells[0][2].alignment = ">"
        builder.cells[0].append(None)
        min_widths = [20, 30, 10, None]
        expected = [[
            ["this is a result    ", "failure                       ", "     error", "keep as is"],
            ["result              ", "This is an error message.     ", "          ", ""],
            ["this is a result    ", "failure                       ", "     error", ""],
        ]]

        assert builder.extend_line_widths(lines, min_widths=min_widths) == expected

    def test_build_lines_fails(self, builder: TableRowBuilder, result: Result):
        builder.build_lines(result)  # valid because no min_widths given

        # noinspection PyTypeChecker
        min_widths = [None] * (len(builder.cells[0]) - 1)
        with pytest.raises(Exception):  # too few min widths given
            builder.build_lines(result, min_widths=min_widths)

    def test_build_lines(self, builder: TableRowBuilder, result: Result):
        assert builder.build_lines(result) == [["this is a result", "failure   ", "error     "]]

        builder.cells = [
            builder.cells[0],
            [TableCellBuilder(key="result_name"), TableCellBuilder(key="message"), None],
            [  # produces an empty line
                TableCellBuilder(key="properties_path"),
                TableCellBuilder(key="properties_start_line"),
                TableCellBuilder(key="properties_start_col")
            ],
            builder.cells[0],
        ]
        expected = [
            ["this is a result", "failure                  ", "error     "],
            ["result          ", "This is an error message.", "          "],
            ["this is a result", "failure                  ", "error     "],
        ]
        assert builder.build_lines(result) == expected

    def test_build_lines_uses_min_widths(self, builder: TableRowBuilder, result: Result):
        builder.cells = [
            builder.cells[0],
            [TableCellBuilder(key="result_name"), TableCellBuilder(key="message"), None],
            [  # produces an empty line
                TableCellBuilder(key="properties_path"),
                TableCellBuilder(key="properties_start_line"),
                TableCellBuilder(key="properties_start_col")
            ],
            builder.cells[0],
        ]
        min_widths = [20, 30, 0]
        expected = [
            ["this is a result    ", "failure                       ", "error"],
            ["result              ", "This is an error message.     ", "     "],
            ["this is a result    ", "failure                       ", "error"],
        ]
        assert builder.build_lines(result, min_widths=min_widths) == expected

    def test_build_lines_pads_cell_lines(self, builder: TableRowBuilder, result: Result):
        builder.cells = [
            builder.cells[0],
            [TableCellBuilder(key="result_name"), TableCellBuilder(key="message", max_width=5, wrap=True), None],
            builder.cells[0],
        ]
        expected = [
            ["this is a result", "failure   ", "error     "],
            ["result          ", "This      ", "          "],
            ["this is a result", "is an     ", "error     "],
            ["                ", "error     ", "          "],
            ["                ", "message.  ", "          "],
            ["                ", "failure   ", "          "]
        ]
        assert builder.build_lines(result) == expected

    def test_join(self, builder: TableRowBuilder, result: Result):
        builder.cells[0].append(TableCellBuilder(key="message", min_width=10, max_width=6, wrap=True))
        builder.colour = Fore.RED
        sep = builder.separator_coloured
        lines = [[
            ["this is a result", "failure   ", "error     ", "This      "],
            ["                ", "          ", "          ", "is an     "],
            ["                ", "          ", "          ", "error     "],
            ["                ", "          ", "          ", "message.  "]
        ], [
            ["this is result 2", "success   ", "pass      ", "This      "],
            ["                ", "          ", "          ", "is a      "],
            ["                ", "          ", "          ", "success   "],
            ["                ", "          ", "          ", "message.  "]
        ]]

        assert builder.join(lines) == "\n".join((
            f"this is a result {sep} failure    {sep} error      {sep} This      ",
            f"                 {sep}            {sep}            {sep} is an     ",
            f"                 {sep}            {sep}            {sep} error     ",
            f"                 {sep}            {sep}            {sep} message.  ",
            f"this is result 2 {sep} success    {sep} pass       {sep} This      ",
            f"                 {sep}            {sep}            {sep} is a      ",
            f"                 {sep}            {sep}            {sep} success   ",
            f"                 {sep}            {sep}            {sep} message.  ",
        ))

    def test_build(self, builder: TableRowBuilder, result: Result):
        builder.separator = ":"
        assert builder.build(result) == "this is a result : failure    : error     "

        builder.cells[0].append(TableCellBuilder(key="message", min_width=10, max_width=6, wrap=True))
        builder.colour = Fore.RED
        sep = builder.separator_coloured
        assert builder.build(result) == "\n".join((
            f"this is a result {sep} failure    {sep} error      {sep} This      ",
            f"                 {sep}            {sep}            {sep} is an     ",
            f"                 {sep}            {sep}            {sep} error     ",
            f"                 {sep}            {sep}            {sep} message.  ",
        ))

    def test_build_uses_min_widths(self, builder: TableRowBuilder, result: Result):
        min_widths = [1, 2, 3]
        with mock.patch.object(TableRowBuilder, "build_lines", return_value=[]) as mock_build_lines:
            builder.build(result, min_widths=min_widths)
            mock_build_lines.assert_called_once_with(result, min_widths=min_widths)


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
            [["this is the 1st res…", "failure   ", "error     "]],
            [["this is the 2nd res…", "a very great success", "info      "]],
        ]

    def test_add_results_with_consistent_widths(self, results: list[Result], formatter: TableFormatter):
        formatter.consistent_widths = True
        formatter.add_results(results)
        assert formatter._results == [
            [["this is the 1st res…", "failure             ", "error     "]],
            [["this is the 2nd res…", "a very great success", "info      "]],
        ]

    def test_build(self, results: list[Result], formatter: TableFormatter):
        formatter.consistent_widths = True
        formatter.add_results(results)
        assert formatter.build() == (
            "this is the 1st res… | failure              | error     \n"
            "this is the 2nd res… | a very great success | info      "
        )


class TestGroupedTableFormatter:
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
                name="this is the 5th result",
                path=tmp_path,
                result_type="warning",
                result_level="warning",
                result_name="result",
                message="This is a warning message.",
            ),
            ModelResult(
                name="this is the 3rd result",
                path=tmp_path,
                result_type="another very great success",
                result_level="info",
                result_name="result",
                message="This is a success message.",
            ),
            ModelResult(
                name="this is the 4th result",
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
            ),
        ]

    @pytest.fixture
    def formatter(self, ) -> GroupedTableFormatter:
        """A fixture for the grouped table formatter."""
        cells = [
            TableCellBuilder(key="name", min_width=10, max_width=20),
            TableCellBuilder(key="result_type", min_width=10, max_width=20),
            TableCellBuilder(key="result_level", min_width=10, max_width=20),
        ]
        row = TableRowBuilder(cells=cells)
        table = TableFormatter(builder=row)
        return GroupedTableFormatter(formatter=table, group_key="result_level")

    def test_get_value(self, results: list[Result]):
        assert GroupedTableFormatter._get_value(results[0], getter="name") == "this is the 1st result"

        assert GroupedTableFormatter._get_value(
            results[0], getter=lambda x: f"{x.name} ({x.result_type})"
        ) == "this is the 1st result (failure)"

    def test_add_results_basic(self, formatter: GroupedTableFormatter, results: list[Result]):
        assert not formatter.header_key
        assert not formatter.sort_key

        with (
                mock.patch.object(TableFormatter, "add_header") as mock_header,
                mock.patch.object(TableFormatter, "add_results") as mock_results,
                mock.patch.object(TableFormatter, "build") as mock_build,
        ):
            formatter.add_results(results)

            assert len(mock_header.mock_calls) == 3
            assert len(mock_results.mock_calls) == 3
            assert len(mock_build.mock_calls) == 3

            group_keys = {result.result_level for result in results}
            for key in group_keys:
                mock_header.assert_any_call(key)
                group = [result for result in results if result.result_level == key]
                mock_results.assert_any_call(group)

    def test_add_results_with_header(self, formatter: GroupedTableFormatter, results: list[Result]):
        assert not formatter.sort_key
        formatter.header_key = "message"

        with (
                mock.patch.object(TableFormatter, "add_header") as mock_header,
                mock.patch.object(TableFormatter, "add_results") as mock_results,
                mock.patch.object(TableFormatter, "build") as mock_build,
        ):
            formatter.add_results(results)

            assert len(mock_header.mock_calls) == 3
            assert len(mock_results.mock_calls) == 3
            assert len(mock_build.mock_calls) == 3

            group_headers = {result.result_level: result.message for result in results}
            for key, message in group_headers.items():
                mock_header.assert_any_call(message)
                group = [result for result in results if result.result_level == key]
                mock_results.assert_any_call(group)

    def test_add_results_with_sort(self, formatter: GroupedTableFormatter, results: list[Result]):
        assert not formatter.header_key
        formatter.sort_key = ["name"]

        with (
                mock.patch.object(TableFormatter, "add_header") as mock_header,
                mock.patch.object(TableFormatter, "add_results") as mock_results,
                mock.patch.object(TableFormatter, "build") as mock_build,
        ):
            formatter.add_results(results)

            assert len(mock_header.mock_calls) == 3
            assert len(mock_results.mock_calls) == 3
            assert len(mock_build.mock_calls) == 3

            group_keys = {result.result_level for result in results}
            for key in group_keys:
                group = [result for result in results if result.result_level == key]
                group = sorted(group, key=lambda r: r.name)
                mock_results.assert_any_call(group)

    def test_build(self, formatter: GroupedTableFormatter, results: list[Result]):
        assert not formatter._tables
        assert not formatter.build()

        formatter.add_results(results)
        assert formatter._tables

        assert formatter.build()
        assert not formatter._tables
