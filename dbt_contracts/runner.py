import json
import logging
import os
from argparse import Namespace
from collections.abc import Collection, Mapping, Callable
from functools import cached_property
from pathlib import Path
from typing import Self, Any

import yaml
from colorama import Fore
from dbt.adapters.utils import classproperty
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.cli.main import dbtRunner
from dbt.config import RuntimeConfig
from distlib.manifest import Manifest

from dbt_contracts import dbt_cli
from dbt_contracts.cli import DEFAULT_CONFIG_FILE_NAME, DEFAULT_OUTPUT_FILE_NAME, CORE_PARSER
from dbt_contracts.contracts import Contract, CONTRACT_MAP, ParentContract
from dbt_contracts.contracts.conditions.properties import PathCondition
from dbt_contracts.contracts.result import Result
from dbt_contracts.contracts.utils import get_absolute_project_path
from dbt_contracts.formatters import ResultsFormatter
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

    # noinspection PyMethodParameters
    @classproperty
    def output_writers_map(cls) -> Mapping[str, Callable[[Collection[Result], Path], Path]]:
        """A mapping of output types to functions that write the results to a file in that format."""
        return {
            "txt": cls._write_results_as_text,
            "text": cls._write_results_as_text,
            "json": cls._write_results_as_json,
            "jsonl": cls._write_results_as_jsonl,
            "github_annotations": cls._write_results_as_github_annotations,
        }

    @property
    def config(self) -> RuntimeConfig:
        """The dbt runtime config"""
        if self._config is None:
            self._config = dbt_cli.get_config(CORE_PARSER)
        return self._config

    @cached_property
    def dbt(self) -> dbtRunner:
        """The dbt runner"""
        return dbtRunner(manifest=self.manifest)

    @cached_property
    def manifest(self) -> Manifest:
        """The dbt manifest"""
        return dbt_cli.get_manifest(runner=self.__dict__.get("dbt"), config=self.config)

    @cached_property
    def catalog(self) -> CatalogArtifact:
        """The dbt catalog"""
        return dbt_cli.get_catalog(runner=self.dbt, config=self.config)

    @property
    def paths(self) -> PathCondition | None:
        """An additional set of paths to filter on when filter contract items."""
        return self._paths

    @paths.setter
    def paths(self, value: Collection[str]):
        """Set the path patterns to filter on. Expects a collection regex patterns."""
        paths = []
        for path in value:
            path = Path(path)
            if not (project_root := Path(self.config.project_root)).is_absolute():
                project_root = Path(os.getcwd(), project_root)

            path = get_absolute_project_path(path)
            if path.is_relative_to(project_root):
                paths.append(str(path.relative_to(project_root)))

        self._paths = PathCondition(include=paths)

    @classmethod
    def from_config(cls, config: RuntimeConfig) -> Self:
        """
        Set up a new runner from the dbt runtime config with custom args parsed from CLI.

        :param config: The dbt runtime config with args associated.
        :return: The configured runner.
        """
        obj = cls.from_yaml(config.args.config)
        obj._config = config
        return obj

    @classmethod
    def from_args(cls, args: Namespace) -> Self:
        """
        Set up a new runner from the args parsed from CLI.

        :param args: The parsed CLI args.
        :return: The configured runner.
        """
        obj = cls.from_yaml(args.config)
        obj._config = args
        return obj

    @classmethod
    def _resolve_config_path(cls, path: str | Path) -> Path:
        path = Path(path).resolve()
        if path.is_dir():
            path = path.joinpath(cls.default_config_file_name)
        if not path.is_file():
            raise FileNotFoundError(f"Could not find config file at path: {path!r}")

        return path

    @classmethod
    def from_yaml(cls, path: str | Path) -> Self:
        """
        Set up a new runner from the config in a yaml file at the given `path`.

        :param path: The path to the yaml file.
            May either be a path to a yaml file or a path to the directory where the file is located.
            If a directory is given, the default file name will be appended.
        :return: The configured runner.
        """
        with cls._resolve_config_path(path).open("r") as file:
            config = yaml.full_load(file)
        return cls.from_dict(config)

    @classmethod
    def from_json(cls, path: str | Path) -> Self:
        """
        Set up a new runner from the config in a json file at the given `path`.

        :param path: The path to the json file.
            May either be a path to a json file or a path to the directory where the file is located.
            If a directory is given, the default file name will be appended.
        :return: The configured runner.
        """
        with cls._resolve_config_path(path).open("r") as file:
            config = json.load(file)
        return cls.from_dict(config)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> Self:
        """
        Set up a new runner from the given `config`.

        :param config: The config to configure the runner with.
        :return: The configured runner.
        """
        contracts = [
            contract
            for key, contract_configs in config.items()
            for conf in contract_configs
            for contract in cls._create_contracts_from_config(key, config=conf)
        ]

        obj = cls(contracts)
        obj.logger.debug(f"Configured {len(contracts)} sets of contracts from config")
        return obj

    @classmethod
    def _create_contracts_from_config(cls, key: str, config: Mapping[str, Any]) -> list[Contract]:
        key = key.replace(" ", "_").casefold().rstrip("s") + "s"
        if key not in CONTRACT_MAP:
            raise Exception(f"Unrecognised enforcement key: {key}")

        contract = CONTRACT_MAP[key].from_dict(config=config)

        contracts = [contract]
        if isinstance(contract, ParentContract):
            contracts.append(contract.create_child_contract_from_dict(config))

        return contracts

    def __init__(
            self,
            contracts: Collection[Contract],
            results_formatter: ResultsFormatter = DEFAULT_TERMINAL_LOG_FORMATTER
    ):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._contracts = contracts
        self._results_formatter = results_formatter

        self._config: RuntimeConfig | None = None
        self._paths: PathCondition | None = None

    def validate(self, contract_key: str = None, terms: Collection[str] = ()) -> list[Result]:
        """
        Validate the project against the contracts and log the results.

        :param contract_key: Only process the contract with this key.
        :param terms: Only run the terms with these names.
        :return: The results of the validation.
        """
        contracts = [self._get_contract_by_key(contract_key)] if contract_key is not None else self._contracts
        self._set_artifacts_on_contracts(contracts)

        results: list[Result] = []
        for contract in contracts:
            contract.validate(terms=terms)

            if not (results := contract.context.results):
                log = f"All {contract.config_key} contracts passed successfully"
                self.logger.info(f"{Fore.LIGHTGREEN_EX}{log}{Fore.RESET}")
                continue

            self.log_results(contract.context.results)
            results.extend(results)

        if not results:
            self.logger.info(f"{Fore.LIGHTGREEN_EX}All contracts passed successfully{Fore.RESET}")
            return results

        self.logger.error(f"{Fore.LIGHTRED_EX}Found {len(results)} contract violations{Fore.RESET}")
        return results

    def _set_artifacts_on_contracts(self, contracts: Collection[Contract]) -> None:
        if any(contract.needs_manifest for contract in contracts):
            for contract in self._contracts:
                contract.manifest = self.manifest
        if any(contract.needs_catalog for contract in contracts):
            for contract in self._contracts:
                contract.catalog = self.catalog

        for contract in contracts:
            if contract.validate_conditions(self.paths):
                contract.conditions.append(self.paths)

    def _get_contract_by_key(self, contract_key: str = None) -> Contract:
        for contract in self._contracts:
            if contract.config_key == contract_key:
                return contract

        raise Exception(f"Could not find a configured contract for the key: {contract_key}")

    def log_results(self, results: Collection[Result]) -> None:
        """
        Log the results of the validation.

        :param results: The results of the validation.
        """
        if not results:
            return
        for line in self._build_results(results).split("\n"):
            self.logger.info(line)

    def _build_results(self, results: Collection[Result]) -> str:
        self._results_formatter.add_results(results)
        return self._results_formatter.build()

    def write_results(self, results: Collection[Result], path: str | Path, output_type: str) -> Path | None:
        """
        Write the given results to an output file with the given `output_type`.

        :param results: The results to write.
        :param path: The path to a directory or file to write to.
        :param output_type: The format to write the file to e.g. 'txt', 'json' etc.
        :return: The path the file was written to.
        """
        if not results:
            return

        if (path := Path(path)).is_dir():
            path = path.joinpath(self.default_output_file_name)
        path.parent.mkdir(parents=True, exist_ok=True)

        output_type = output_type.replace("-", "_")
        output_path = self.output_writers_map.get(output_type)(results, path)

        log = f"Wrote {output_type.replace("_", " ")} output to {str(output_path)!r}"
        self.logger.info(f"{Fore.LIGHTBLUE_EX}{log}{Fore.RESET}")
        return output_path

    def _write_results_as_text(self, results: Collection[Result], output_path: Path) -> Path:
        output = self._build_results(results)
        with (path := output_path.with_suffix(".txt")).open("w") as file:
            file.write(output)

        return path

    @staticmethod
    def _write_results_as_json(results: Collection[Result], output_path: Path) -> Path:
        output = [result.model_dump_json() for result in results]
        with (path := output_path.with_suffix(".json")).open("w") as file:
            json.dump(output, file, indent=2)

        return path

    @staticmethod
    def _write_results_as_jsonl(results: Collection[Result], output_path: Path) -> Path:
        output = [result.model_dump_json() for result in results]
        with (path := output_path.with_suffix(".json")).open("w") as file:
            for result in output:
                json.dump(result, file)
                file.write("\n")

        return path

    @staticmethod
    def _write_results_as_github_annotations(results: Collection[Result], output_path: Path) -> Path:
        output = [result.as_github_annotation() for result in results]
        with (path := output_path.with_suffix(".json")).open("w") as file:
            json.dump(output, file, indent=2)

        return path
