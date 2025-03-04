import logging
from functools import cached_property

from colorama import Fore
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.cli.main import dbtRunner
from dbt.config import RuntimeConfig
from distlib.manifest import Manifest

from dbt_contracts import dbt_cli
from dbt_contracts.cli import DEFAULT_CONFIG_FILE_NAME, DEFAULT_OUTPUT_FILE_NAME
from dbt_contracts.contracts.result import Result
from dbt_contracts.formatters.table import TableCellBuilder, GroupedTableFormatter, TableFormatter, TableRowBuilder


logging.basicConfig(level=logging.INFO, format="%(message)s")


def _get_default_table_header(result: Result) -> str:
    path = result.path
    header_path = (
        f"{Fore.LIGHTWHITE_EX.replace('m', ';1m')}->{Fore.RESET.replace('m', ';0m')} "
        f"{Fore.LIGHTBLUE_EX}{path}{Fore.RESET}"
    )

    patch_path = result.patch_path
    if patch_path and patch_path != path:
        header_path += f" @ {Fore.LIGHTCYAN_EX}{patch_path}{Fore.RESET}"

    return f"{result.result_type}: {header_path}"


DEFAULT_TERMINAL_LOG_BUILDER_CELLS = [
    [
        TableCellBuilder(
            key="result_name", colour=Fore.RED, max_width=50
        ),
        TableCellBuilder(
            key="patch_start_line", prefix="L: ", alignment=">", colour=Fore.LIGHTBLUE_EX, min_width=6, max_width=9
        ),
        TableCellBuilder(
            key=lambda result: result.parent_name if result.has_parent else result.name,
            colour=Fore.CYAN, max_width=40
        ),
        TableCellBuilder(
            key="message", colour=Fore.YELLOW, max_width=60, wrap=True
        ),
    ],
    [
        None,
        TableCellBuilder(
            key="patch_start_col", prefix="P: ", alignment=">", colour=Fore.LIGHTBLUE_EX, min_width=6, max_width=9
        ),
        TableCellBuilder(
            key=lambda result: result.name if result.has_parent else "",
            prefix="> ", colour=Fore.CYAN, max_width=40
        ),
        None,
    ],
]

DEFAULT_TERMINAL_LOG_FORMATTER_TABLE = TableFormatter(
    builder=TableRowBuilder(cells=DEFAULT_TERMINAL_LOG_BUILDER_CELLS, colour=Fore.WHITE),
    consistent_widths=True,
)

DEFAULT_TERMINAL_LOG_FORMATTER = GroupedTableFormatter(
    formatter=DEFAULT_TERMINAL_LOG_FORMATTER_TABLE,
    group_key=lambda result: f"{result.result_type}-{result.path}",
    header_key=_get_default_table_header,
    sort_key=[
        "result_type",
        "path",
        lambda result: result.parent_name if result.has_parent else result.name,
        lambda result: result.index or 0,
        lambda result: result.name if result.has_parent else "",
    ],
)


class ContractsRunner:
    """Handles loading config for contracts and their execution."""
    default_config_file_name: str = DEFAULT_CONFIG_FILE_NAME
    default_output_file_name: str = DEFAULT_OUTPUT_FILE_NAME

    @cached_property
    def dbt(self) -> dbtRunner:
        """The dbt runner"""
        return dbtRunner(manifest=self.manifest)

    @cached_property
    def config(self) -> RuntimeConfig:
        """The dbt runtime config"""
        return dbt_cli.get_config()

    @cached_property
    def manifest(self) -> Manifest:
        """The dbt manifest"""
        return dbt_cli.get_manifest(runner=self.__dict__.get("dbt"), config=self.config)

    @cached_property
    def catalog(self) -> CatalogArtifact:
        """The dbt catalog"""
        return dbt_cli.get_catalog(runner=self.dbt, config=self.config)
