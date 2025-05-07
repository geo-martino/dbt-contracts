import itertools
import re
import textwrap
from collections.abc import Sequence, Callable, Iterable, Collection
from typing import Self, Any, Annotated, Literal

from colorama import Fore
from pydantic import Field, model_validator, BeforeValidator

from dbt_contracts._core import BaseModelConfig
from dbt_contracts.contracts.result import Result
from dbt_contracts.contracts.utils import to_tuple
from dbt_contracts.formatters import ResultsFormatter


def _get_print_length(value: str) -> int:
    pattern = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return len(pattern.sub("", value))


def _align_and_pad_print_length(value: str, alignment: Literal["<", "^", ">"], width: int) -> str:
    width += len(value) - _get_print_length(value)
    return f"{value:{alignment}{width}}"


class TableCellBuilder[T: Result](BaseModelConfig):
    key: str | Callable[[T], str] = Field(
        description="The key to use to get the value from the :py:class:`.Result` "
                    "or a callable object which returns a formatted string for the value",
    )
    prefix: str | None = Field(
        description=(
            "The prefix to prepend to the value. "
            "This prefix will always be left aligned regardless of the alignment setting."
        ),
        default=None
    )
    alignment: Literal["<", "^", ">"] = Field(
        description="The alignment of the value in the cell.",
        default="<"
    )
    colour: str | None = Field(
        description="The colour to apply to the value.",
        default=None
    )
    min_width: int | None = Field(
        description="The minimum width of the cell. Any value shorter than this will pad the cell.",
        default=None,
        ge=1,
    )
    max_width: int | None = Field(
        description="The maximum width of the cell. Any value larger than this will be truncated with '…'.",
        default=None,
        ge=1,
    )
    wrap: bool = Field(
        description="Whether to wrap the value in the cell.",
        default=False
    )

    @model_validator(mode="after")
    def validate_wrap(self) -> Self:
        """Validate the required parameters are set when using wrap=True."""
        if self.wrap and not self.max_width:
            raise Exception("Cannot wrap a cell without a max width.")
        return self

    @property
    def prefix_coloured(self) -> str:
        """Get the prefix with colour applied if set."""
        if not self.prefix:
            return ""
        if not self.colour:
            return self.prefix
        return f"{self.colour.replace("m", ";1m")}{self.prefix}{Fore.RESET.replace("m", ";0m")}"

    def _get_value(self, result: T) -> str:
        value = self.key(result) if callable(self.key) else getattr(result, self.key, "")
        if value is None:
            return ""
        return str(value)

    def _apply_prefix(self, value: str) -> str:
        if not self.prefix or not _get_print_length(value):
            return value
        return f"{self.prefix_coloured}{value}"

    def _truncate_value(self, value: str) -> str:
        width = self.max_width
        if not width or _get_print_length(value) <= width:
            return value

        if self.prefix_coloured:
            width -= len(self.prefix)
        return value[:width - 1] + "…"

    def _apply_padding_and_alignment(self, value: str, width: int | None, has_prefix: bool = True) -> str:
        if not self.alignment or not width:
            return value

        if has_prefix and self.prefix:
            width -= len(self.prefix)

        return _align_and_pad_print_length(value, self.alignment, width)

    def _apply_colour(self, value: str) -> str:
        if not self.colour:
            return value
        return f"{self.colour}{value}{Fore.RESET}"

    def _apply_wrap(self, value: str) -> list[str]:
        if not self.wrap or not self.max_width:
            return [value]

        lines = textwrap.wrap(
            (self.prefix if self.prefix else "") + value,
            width=self.max_width,
            break_long_words=False,
            break_on_hyphens=False
        )
        if self.prefix:
            lines[0] = lines[0][len(self.prefix):]

        return lines

    def build(self, result: T, min_width: int = None) -> str:
        """
        Build a formatted cell for the given `result`.

        :param result: The result to build the cell for.
        :param min_width: Ignore settings for min width and force the cell to be at least this given width instead.
        :return: The formatted cell.
        """
        min_width = min_width if min_width is not None else self.min_width

        value = self._get_value(result)
        if self.wrap and self.max_width:
            lines = self._apply_wrap(value)
            lines = list(map(self._apply_colour, lines))
            lines = [
                self._apply_padding_and_alignment(line, width=min_width, has_prefix=i == 0)
                for i, line in enumerate(lines)
            ]
            lines[0] = self._apply_prefix(lines[0])
            value = "\n".join(lines)
        else:
            value = self._truncate_value(value)
            value = self._apply_colour(value)
            value = self._apply_padding_and_alignment(value, width=min_width)
            value = self._apply_prefix(value)

        return value


class TableRowBuilder[T: Result](BaseModelConfig):
    cells: Sequence[TableCellBuilder[T]] | Sequence[Sequence[TableCellBuilder[T] | None]] = Field(
        description="The cell builders to use to build the row.",
    )
    separator: str = Field(
        description="The separator to use between the cells.",
        default="|"
    )
    colour: str | None = Field(
        description="The colour to apply to the separator.",
        default=None
    )

    @model_validator(mode="after")
    def remap_and_validate_cells(self) -> Self:
        """Remap the cells if they are not in the matrix format and validate the cells."""
        if all(isinstance(cell, TableCellBuilder) for cell in self.cells):
            self.cells = [self.cells]

        if not len(set(map(len, self.cells))) == 1:
            raise Exception("All cell rows must be of equal length")
        if not all(isinstance(cell, TableCellBuilder) for cell in self.cells[0]):
            raise Exception("All cells in the 1st row must be filled")

        return self

    @property
    def separator_coloured(self) -> str:
        """Get the separator with colour applied if set."""
        if not self.colour:
            return self.separator
        return f"{self.colour.replace("m", ";1m")}{self.separator}{Fore.RESET.replace("m", ";0m")}"

    def _get_lines(self, result: Result, min_widths: Sequence[int | None]) -> list[str]:
        values: list[list[str]] = []  # col -> row list

        for cells in self.cells:
            line: list[str] = [
                cell.build(result, min_width=min_width) if cell is not None else ""
                for cell, min_width in zip(cells, min_widths, strict=True)
            ]
            values.append(line)

            lines = [value.splitlines() for value in map("\n".join, zip(*values, strict=True))]
            min_widths = self.get_widths_from_lines(list(zip(*lines)))

        return list(map("\n".join, zip(*values, strict=True)))

    @staticmethod
    def _get_max_rows(values: Iterable[str]) -> int:
        return max(len(value.splitlines()) for value in values)

    @classmethod
    def _to_matrix(cls, values: Sequence[str]) -> list[tuple[str]]:
        """
        Expand the lines in the given values, ensuring that a valid NxM matrix of values is returned.

        :param values: The values of the cells in the row.
        :return: The matrix of cells for each row.
        """
        row_count = cls._get_max_rows(values)

        columns = []
        for value in values:
            rows = value.splitlines()
            rows.extend([""] * (row_count - len(rows)))
            columns.append(rows)

        return list(zip(*columns, strict=True))

    @staticmethod
    def _remove_empty_lines[T: Iterable[str]](lines: Iterable[T]) -> list[T]:
        """
        Remove empty lines from the given `values`.

        :param lines: The values to remove empty lines from.
        """
        return [line for line in lines if any(bool(cell.strip()) for cell in line)]

    @staticmethod
    def get_widths_from_lines(lines: Iterable[Iterable[str]]) -> list[int]:
        """Get the maximum width for each column from the given `lines`."""
        return [max(map(_get_print_length, column)) for column in zip(*lines, strict=True)]

    def extend_line_widths(
            self, rows: Iterable[Collection[Iterable[str]]], min_widths: Sequence[int | None]
    ) -> list[list[list[str]]]:
        """
        Adjust the given `rows` to the given `min_widths` aligning according to the matching cell config.
        Each row must consist of a sequence of lines that match the dimensions of the configured cells of this builder.

        :param rows: The collection of rows to adjust.
        :param min_widths: The widths to extend the lines to.
        :return: The extended lines.
        """
        # noinspection PyTypeChecker
        if len(min_widths) != len(self.cells[0]):
            # noinspection PyTypeChecker
            raise Exception(
                f"Given widths do not equal the number of cells configured for this row builder: "
                f"cells={len(self.cells[0])}, widths={len(min_widths)}"
            )

        result: list[list[list[str]]] = []
        for lines in rows:
            cells_padded = list(self.cells) + list(itertools.repeat(self.cells[-1], len(lines) - len(self.cells)))
            result_lines: list[list[str]] = []

            for line, cells in zip(lines, cells_padded, strict=True):
                result_line: list[str] = []

                for value, cell, width in zip(line, cells, min_widths, strict=True):
                    if not value and width is not None:
                        value = " " * width
                    elif width is not None:
                        alignment = cell.alignment if cell is not None else self.cells[0][0].alignment
                        value = _align_and_pad_print_length(value, alignment, width)

                    result_line.append(value)
                result_lines.append(result_line)
            result.append(result_lines)

        return result

    def build_lines(self, result: T, min_widths: Sequence[int | None] = ()) -> list[list[str]]:
        """
        Build a formatted row or set of rows for the given `result` and returns them as their individual lines.

        :param result: The result to build the row for.
        :param min_widths: When provided, ignore settings for min width for each cell and force the cells to be
            at least these given widths instead.
            Must contain the same number of elements as there are cells configured for this builder.
        :return: The formatted row as a set of lines.
        """
        if not min_widths:
            # noinspection PyTypeChecker
            min_widths = [None] * len(self.cells[0])
        # noinspection PyTypeChecker
        if len(min_widths) != len(self.cells[0]):
            raise Exception(
                f"Given widths do not equal the number of cells configured for this row builder: "
                f"cells={len(self.cells)}, widths={len(min_widths)}"
            )

        lines = self._get_lines(result, min_widths=min_widths)
        lines = self._to_matrix(lines)
        min_widths = self.get_widths_from_lines(lines)
        lines = self.extend_line_widths([lines], min_widths=min_widths)[0]
        lines = self._remove_empty_lines(lines)
        return lines

    def join(self, rows: Iterable[Iterable[Iterable[str]]]) -> str:
        """Join the given rows into a single string."""
        return "\n".join(map(f" {self.separator_coloured} ".join, itertools.chain.from_iterable(rows)))

    def build(self, result: T, min_widths: Sequence[int] = ()) -> str:
        """
        Build a formatted row or set of rows for the given `result`.

        :param result: The result to build the row for.
        :param min_widths: When provided, ignore settings for min width for each cell and force the cells to be
            at least these given widths instead.
            Must contain the same number of elements as there are cells configured for this builder.
        :return: The formatted row.
        """
        lines = self.build_lines(result, min_widths=min_widths)
        return self.join([lines])


class TableFormatter[T: Result](ResultsFormatter[T]):
    builder: TableRowBuilder[T] = Field(
        description="The builder for the rows of the table",
    )
    consistent_widths: bool = Field(
        description="Whether to ensure all rows have the same width for each column.",
        default=False,
    )

    @property
    def widths(self) -> list[int]:
        """Get the max widths of each column in the currently stored result"""
        flattened_results = itertools.chain.from_iterable(self._results)
        return self.builder.get_widths_from_lines(flattened_results)

    def __init__(self, /, **data: Any):
        super().__init__(**data)

        self._lines: list[str] = []
        self._results: list[list[list[str]]] = []

    def add_header(self, header: str) -> None:
        """Add a header to the table."""
        if len(self._lines) >= 2 and self._lines[1] == "":
            self._lines.pop(0)
            self._lines.pop(0)

        self._lines.insert(0, "")
        self._lines.insert(0, header)

    def add_results(self, results: Iterable[T]) -> None:
        for result in results:
            widths = self.widths if self.consistent_widths else None
            row = self.builder.build_lines(result, min_widths=widths)
            self._results.append(row)

        if self.consistent_widths:
            self._results = self.builder.extend_line_widths(self._results, min_widths=self.widths)

    def build(self) -> str:
        output = "\n".join(self._lines) + self.builder.join(self._results)

        self._lines.clear()
        self._results.clear()

        return output


class GroupedTableFormatter[T: Result](ResultsFormatter[T]):
    formatter: TableFormatter[T] = Field(
        description="The formatter to use for each table.",
    )
    group_key: str | Callable[[T], str] = Field(
        description="The key to use to get the group value for each table from a :py:class:`.Result` "
                    "or a callable object which returns a formatted string for the value",
    )
    header_key: str | Callable[[T], str] | None = Field(
        description="The key to use to get the header value for each table from a :py:class:`.Result` "
                    "or a callable object which returns a formatted string for the value",
        default=None,
    )
    sort_key: Annotated[Sequence[str | Callable[[T], Any]], BeforeValidator(to_tuple)] = Field(
        description="The key to use to get the sort value for each :py:class:`.Result` in a table "
                    "or a callable object which returns a formatted string for the value",
        default=None,
    )

    def __init__(self, /, **data: Any):
        super().__init__(**data)

        self._tables: dict[str, str] = {}

    @staticmethod
    def _get_value(result: T, getter: str | Callable[[T], Any]) -> Any:
        if callable(getter):
            return getter(result)
        return getattr(result, getter, "") or ""

    @classmethod
    def _get_values(cls, result: T, getters: Iterable[str | Callable[[T], Any]]) -> tuple[Any, ...]:
        return tuple(cls._get_value(result, getter=getter) for getter in getters)

    def add_results(self, results: Iterable[T]) -> None:
        results = sorted(results, key=lambda r: self._get_value(result=r, getter=self.group_key))
        groups = itertools.groupby(results, key=lambda r: self._get_value(result=r, getter=self.group_key))

        for group_key, group in groups:
            if self.sort_key:
                group = sorted(group, key=lambda r: self._get_values(result=r, getters=self.sort_key))
            else:
                group = list(group)
            header = self._get_value(result=group[0], getter=self.header_key) if self.header_key else group_key

            self.formatter.add_header(header)
            self.formatter.add_results(group)

            self._tables[group_key] = self.formatter.build()

    def build(self) -> str:
        output = "\n\n".join(self._tables.values())
        self._tables.clear()
        return output
