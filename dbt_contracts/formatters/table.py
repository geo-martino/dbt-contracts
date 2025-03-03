import functools
import itertools
import textwrap
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass

from colorama import Fore

from dbt_contracts.formatters._core import KeysT, ObjectFormatter, get_value_from_object, get_values_from_object


@dataclass
class TableColumnFormatter[T]:
    """Configure a column of values for a table."""
    keys: Collection[KeysT[T]] | KeysT[T]
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

    def _get_str_values_from_object(self, obj: T) -> Iterable[str]:
        return map(str, map(lambda x: x if x is not None else "", get_values_from_object(obj, self.keys)))

    def get_width(self, objects: Iterable[T]) -> int:
        """Calculate the width of this column for a given set of `logs`."""
        values = itertools.chain.from_iterable(map(
            lambda obj: (
                prefix + val for prefix, val in itertools.zip_longest(
                    self.prefixes, self._get_str_values_from_object(obj), fillvalue=""
                )
            ),
            objects
        ))
        return max(self.min_width, min(max(map(len, values)), self.max_width))

    def get_column(self, obj: T, width: int = None) -> list[str]:
        """
        Get the column values for the given `obj`.

        :param obj: The object to populate the column with.
        :param width: The width of this column. When not given, take the max width of the values for this object.
        :return: The column values.
        """
        values = list(self._get_str_values_from_object(obj))
        if width is None:
            width = max(map(len, values))

        if not self.wrap:
            column = map(
                lambda x: self._get_column_value(*x, width=width),
                itertools.zip_longest(values, self.prefixes, self.colours, fillvalue="")
            )
        else:
            column = itertools.chain.from_iterable(map(
                lambda x: self._wrap_value(*x, width=width),
                itertools.zip_longest(values, self.prefixes, self.colours, fillvalue="")
            ))

        return list(column)

    def _get_column_value(self, value: str, prefix: str, colour: str, width: int) -> str:
        if not value:
            return " " * width

        width = width - len(prefix)
        fmt = f"{self.alignment}{width}.{width}"

        prefix = f"{colour.replace('m', ';1m')}{prefix}{Fore.RESET.replace('m', ';0m')}"
        value_formatted = f"{colour}{self._truncate_value(value, width):{fmt}}{Fore.RESET}"
        return prefix + value_formatted

    @staticmethod
    def _truncate_value(value: str, width: int) -> str:
        if len(value) > width:
            value = value[:width - 3] + "..."
        return value

    @staticmethod
    def _wrap_value(value: str, prefix: str, colour: str, width: int) -> list[str]:
        lines = textwrap.wrap(
            value,
            width=width,
            initial_indent=f"{colour.replace('m', ';1m')}{prefix}{Fore.RESET.replace('m', ';0m')}{colour}",
            break_long_words=False,
            break_on_hyphens=False
        )

        for i, line in enumerate(lines):
            if i == 0:
                lines[0] += Fore.RESET
                continue
            lines[i] = f"{colour}{line}{Fore.RESET}"

        return lines


class TableFormatter[T](ObjectFormatter[T]):

    def __init__(
            self,
            columns: Sequence[TableColumnFormatter[T]],
            column_sep_value: str = "|",
            column_sep_colour: str = Fore.LIGHTWHITE_EX
    ):
        self.columns = columns
        self.column_sep_value = column_sep_value
        self.column_sep_colour = column_sep_colour

    def _join_if_populated(self, left: str, right: str) -> str:
        sep = " "
        if left.strip() or right.strip():
            sep = f"{self.column_sep_colour}{self.column_sep_value}{Fore.RESET}"
        return f"{left} {sep} {right}"

    def _join_row(self, row: list[str]) -> str:
        return functools.reduce(self._join_if_populated, row)

    def format(self, objects: Collection[T], widths: Collection[int] = (), **__) -> list[str]:
        logs = []

        calculate_widths = len(widths) != len(self.columns)

        for obj in objects:
            if calculate_widths:
                widths = [column.get_width(objects) for column in self.columns]
            cols = [column.get_column(obj, width=width) for column, width in zip(self.columns, widths)]

            row_count = max(map(len, cols))
            cols = [
                values + ([" " * width] * (row_count - len(values)))
                for values, column, width in zip(cols, self.columns, widths)
                if not calculate_widths or any(val.strip() for val in values)
            ]

            rows = list(map(list, zip(*cols)))
            log = "\n".join(map(self._join_row, rows))
            logs.append(log)

        return logs

    def combine(self, values: Collection[str]) -> str:
        return "\n".join(values)


class GroupedTableFormatter[T](ObjectFormatter[T]):
    """

    :param group_key: The key to group by.
        May either be a string of the attribute name, or a lambda function for more complex logic.
    :param header_key: An optional key to use for the table headers.
        May either be a string of the attribute name, or a lambda function for more complex logic.
        When None, uses the `group_key` to get the header.
    :param sort_key: The key/s to sort by before grouping.
        May either be a collection strings of the attribute name,
        or a collection of lambda functions for more complex logic.
    :param consistent_widths: Whether to keep the widths of all tables equal.
        When disabled, also drops empty columns in individual tables.
    """
    def __init__(
            self,
            table_formatter: TableFormatter[T],
            group_key: KeysT[T],
            header_key: KeysT[T] = None,
            sort_key: Collection[KeysT[T]] | KeysT[T] = (),
            consistent_widths: bool = False,
    ):
        self.table_formatter = table_formatter

        self.group_key = group_key
        self.header_key = header_key
        if not isinstance(sort_key, Collection) and not isinstance(sort_key, str):
            sort_key = [sort_key]
        self.sort_key = sort_key

        self.consistent_widths = consistent_widths

    def format(self, objects: Collection[T], **__) -> dict[str, list[str]]:
        objects = sorted(objects, key=lambda obj: tuple(get_values_from_object(obj, self.sort_key)))
        groups = itertools.groupby(objects, key=lambda obj: get_value_from_object(obj, self.group_key))

        widths = ()
        if self.consistent_widths:
            widths = [column.get_width(objects) for column in self.table_formatter.columns]

        tables = {}
        for header, group in groups:
            group = list(group)
            if self.header_key:
                header = next(map(lambda obj: get_value_from_object(obj, self.header_key), group))

            tables[header] = self.table_formatter.format(group, widths=widths)

        return tables

    def combine(self, values: Mapping[str, Collection[str]]) -> str:
        lines = itertools.chain.from_iterable([header] + list(rows) + ["\n"] for header, rows in values.items())
        return "\n".join(lines)
