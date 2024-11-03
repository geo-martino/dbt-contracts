import functools
import itertools
import textwrap
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from colorama import Fore

from dbt_contracts.result import Result, ResultParent


KeysT = str | Callable[[Result], Any]


def get_value_from_result(result: Result, key: KeysT) -> Any:
    """
    Get a values from the given `result` for the given `key`.

    :param result: The result to get a value from.
    :param key: The key from which to get the value.
        May either be a string of the attribute name, or a lambda function for more complex logic.
    :return: The value from the result.
    """
    return key(result) if callable(key) else getattr(result, key)


def get_values_from_result(result: Result, keys: Collection[KeysT]) -> Iterable[Any]:
    """
    Get many values from the given `result` for the given `key`.

    :param result: The result to get values from.
    :param keys: The keys from which to get the values.
        May either be a collection strings of the attribute name,
        or a collection of lambda functions for more complex logic.
    :return: The value from the result.
    """
    return (get_value_from_result(result, key) for key in keys)


@dataclass
class ResultLogColumn:
    """Configure a column of values for a log table."""
    keys: Collection[KeysT] | KeysT
    prefixes: Collection[str] | str = ()
    alignment: str = "<"
    colours: Collection[str] | str = ()
    min_width: int = 5
    max_width: int = 30
    wrap: bool = False

    def __post_init__(self):
        if isinstance(self.keys, str) or callable(self.keys):
            self.keys = [self.keys]
        if isinstance(self.prefixes, str):
            self.prefixes = [self.prefixes] * len(self.keys)
        if isinstance(self.colours, str):
            self.colours = [self.colours] * len(self.keys)

    def get_width(self, results: Iterable[Result]) -> int:
        """Calculate the width of this column for a given set of `logs`."""
        values = itertools.chain.from_iterable(map(
            lambda result: (
                prefix + val
                for prefix, val in itertools.zip_longest(self.prefixes, self._get_values(result), fillvalue="")
            ),
            results
        ))
        return max(self.min_width, min(max(map(len, values)), self.max_width))

    def _get_values(self, result: Result) -> Iterable[str]:
        return map(str, map(lambda x: x if x is not None else "", get_values_from_result(result, self.keys)))

    @staticmethod
    def _truncate_value(value: str, width: int) -> str:
        if len(value) > width:
            value = value[:width - 3] + "..."
        return value

    def get_column(self, result: Result, width: int = None) -> list[str]:
        """
        Get the column values for the given `result`.

        :param result: The result to populate the column with.
        :param width: The width of this column. When not given, take the max width of the values for this result.
        :return: The column values.
        """
        values = list(self._get_values(result))
        if width is None:
            width = max(map(len, values))

        if not self.wrap:
            column = map(
                lambda x: self._get_value(*x, width=width),
                itertools.zip_longest(values, self.prefixes, self.colours, fillvalue="")
            )
        else:
            column = itertools.chain.from_iterable(map(
                lambda x: self._wrap_value(*x, width=width),
                itertools.zip_longest(values, self.prefixes, self.colours, fillvalue="")
            ))

        return list(column)

    def _get_value(self, value: str, prefix: str, colour: str, width: int) -> str:
        if not value:
            return " " * width

        width = width - len(prefix)
        fmt = f"{self.alignment}{width}.{width}"

        prefix = f"{colour.replace("m", ";1m")}{prefix}{Fore.RESET}"
        value_formatted = f"{colour}{self._truncate_value(value, width):{fmt}}{Fore.RESET}"
        return prefix + value_formatted

    @staticmethod
    def _wrap_value(value: str, prefix: str, colour: str, width: int) -> list[str]:
        lines = textwrap.wrap(
            value,
            width=width,
            initial_indent=f"{colour.replace("m", ";1m")}{prefix}{Fore.RESET}{colour}",
            break_long_words=False,
            break_on_hyphens=False
        )

        for i, line in enumerate(lines):
            if i == 0:
                lines[0] += Fore.RESET
                continue
            lines[i] = f"{colour}{line}{Fore.RESET}"

        return lines


TERMINAL_RESULT_LOG_COLUMNS = [
    ResultLogColumn(
        keys=lambda result: result.result_name,
        colours=Fore.RED, max_width=50,
    ),
    ResultLogColumn(
        keys=[
            lambda result: result.patch_start_line,
            lambda result: result.patch_start_col,
        ],
        prefixes=["L: ", "P: "], alignment=">", colours=Fore.LIGHTBLUE_EX, min_width=6, max_width=9
    ),
    ResultLogColumn(
        keys=[
            lambda result: result.parent_name if isinstance(result, ResultParent) else result.name,
            lambda result: result.name if isinstance(result, ResultParent) else "",
        ],
        colours=Fore.CYAN, prefixes=["", "> "], max_width=40
    ),
    ResultLogColumn(
        keys=lambda result: result.message,
        colours=Fore.YELLOW, max_width=60, wrap=True
    ),
]


def format_results_header(title: str, results: Iterable[Result]) -> str:
    """
    Format a header log with the given `title` for a log table from the given `results`.

    :param title: The title value of the header.
    :param results: The results to format a header value for.
        Used to append the patch/properties file to the header.
    :return: The formatted header.
    """
    header = (
        f"{Fore.LIGHTWHITE_EX.replace("m", ";1m")}->{Fore.RESET} "
        f"{Fore.LIGHTBLUE_EX}{title}{Fore.RESET}"
    )

    patch_path = next((log.patch_path for log in results), None)
    if patch_path and str(patch_path) != title:
        header += f" @ {Fore.LIGHTCYAN_EX}{patch_path}{Fore.RESET}"

    return header


def format_results_to_table(
        results: Collection[Result],
        columns: Sequence[ResultLogColumn],
        widths: Collection[int] = (),
        column_sep_value: str = "|",
        column_sep_colour: str = Fore.LIGHTWHITE_EX
) -> list[str]:
    """
    Format the given results to a log table.

    :param results: The results to format
    :param columns: The columns to log.
    :param widths: Optional, provide a set of widths for each of the columns.
        Ignored if the number of items in the collection do not equal the number of given `columns`.
    :param column_sep_value: The value of the separator char to use between columns.
    :param column_sep_colour: The colour of the separator char to use between columns.
    :return: The rows of the table.
    """
    logs = []

    def _join_if_populated(left: str, right: str) -> str:
        sep = f"{column_sep_colour}{column_sep_value}{Fore.RESET}" if left.strip() or right.strip() else " "
        return f"{left} {sep} {right}"

    def _join_row(row: list[str]) -> str:
        return functools.reduce(_join_if_populated, row)

    calculate_widths = len(widths) != len(columns)

    for result in sorted(results, key=lambda r: get_value_from_result(r, columns[0].keys[0])):
        if calculate_widths:
            widths = [column.get_width(results) for column in columns]
        cols = [column.get_column(result, width=width) for column, width in zip(columns, widths)]

        row_count = max(map(len, cols))
        cols = [
            values + ([" " * width] * (row_count - len(values)))
            for values, column, width in zip(cols, columns, widths)
            if not calculate_widths or any(val.strip() for val in values)
        ]

        rows = list(map(list, zip(*cols)))
        log = f"\n".join(map(_join_row, rows))
        logs.append(log)

    return logs


def format_results_to_table_in_groups(
        results: Collection[Result],
        columns: Sequence[ResultLogColumn],
        header_key: str | Callable[[Result], Any],
        sort_keys: Collection[KeysT] = (),
        consistent_widths: bool = False,
        column_sep_value: str = "|",
        column_sep_colour: str = Fore.LIGHTWHITE_EX,
) -> Mapping[str, list[str]]:
    """
    Formats multiple tables grouped by the given `header_key`.

    :param results: The results to format
    :param columns: The columns to log.
    :param header_key: The key to group by.
        May either be a string of the attribute name, or a lambda function for more complex logic.
    :param sort_keys: The keys to sort by before grouping.
        May either be a collection strings of the attribute name,
        or a collection of lambda functions for more complex logic.
    :param consistent_widths: Whether to keep the widths of all tables equal.
        When disabled, also drops empty columns in individual tables.
    :param column_sep_value: The value of the separator char to use between columns.
    :param column_sep_colour: The colour of the separator char to use between columns.
    :return: The rows of the table.
    :return: A map of header row values to table rows.
    """
    results = sorted(results, key=lambda result: tuple(get_values_from_result(result, sort_keys)))
    results_grouped = itertools.groupby(results, key=lambda result: get_value_from_result(result, header_key))

    widths = ()
    if consistent_widths:
        widths = [column.get_width(results) for column in columns]

    tables = {}
    for title, group in results_grouped:
        group = list(group)
        header = format_results_header(title, group)
        table = format_results_to_table(
            group,
            columns=columns,
            widths=widths,
            column_sep_value=column_sep_value,
            column_sep_colour=column_sep_colour
        )
        tables[header] = table

    return tables
