import json
import logging
import os
from argparse import Namespace
from collections.abc import Mapping, Collection, Iterable, Callable
from pathlib import Path
from typing import Any, Self

import yaml
from colorama import Fore
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.cli.main import dbtRunner
from dbt.config import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest

from dbt_contracts.cli import DEFAULT_CONFIG_FILE_NAME, DEFAULT_OUTPUT_FILE_NAME
from dbt_contracts.contracts import Contract, CONTRACTS_CONFIG_MAP, ParentContract
from dbt_contracts.dbt_cli import get_manifest, get_catalog, get_config
from dbt_contracts.formatters import ObjectFormatter
from dbt_contracts.formatters.table import TableFormatter, TableColumnFormatter, GroupedTableFormatter
from dbt_contracts.result import Result, ResultChild

logging.basicConfig(level=logging.INFO, format="%(message)s")


def _get_default_table_header(result: Result) -> str:
    path = result.path
    header_path = (
        f"{Fore.LIGHTWHITE_EX.replace("m", ";1m")}->{Fore.RESET.replace("m", ";0m")} "
        f"{Fore.LIGHTBLUE_EX}{path}{Fore.RESET}"
    )

    patch_path = result.patch_path
    if patch_path and patch_path != path:
        header_path += f" @ {Fore.LIGHTCYAN_EX}{patch_path}{Fore.RESET}"

    return f"{result.result_type}: {header_path}"


DEFAULT_TERMINAL_RESULT_LOG_COLUMNS = [
    TableColumnFormatter(
        keys=lambda result: result.result_name,
        colours=Fore.RED, max_width=50,
    ),
    TableColumnFormatter(
        keys=[
            lambda result: result.patch_start_line,
            lambda result: result.patch_start_col,
        ],
        prefixes=["L: ", "P: "], alignment=">", colours=Fore.LIGHTBLUE_EX, min_width=6, max_width=9
    ),
    TableColumnFormatter(
        keys=[
            lambda result: result.parent_name if isinstance(result, ResultChild) else result.name,
            lambda result: result.name if isinstance(result, ResultChild) else "",
        ],
        colours=Fore.CYAN, prefixes=["", "> "], max_width=40
    ),
    TableColumnFormatter(
        keys=lambda result: result.message,
        colours=Fore.YELLOW, max_width=60, wrap=True
    ),
]

DEFAULT_TERMINAL_TABLE_FORMATTER = TableFormatter(
    columns=DEFAULT_TERMINAL_RESULT_LOG_COLUMNS,
)

DEFAULT_TERMINAL_FORMATTER = GroupedTableFormatter(
    table_formatter=DEFAULT_TERMINAL_TABLE_FORMATTER,
    group_key=lambda result: f"{result.result_type}: {result.path}",
    header_key=_get_default_table_header,
    sort_key=[
        lambda result: result.result_type,
        lambda result: result.path,
        lambda result: result.parent_name if isinstance(result, ResultChild) else result.name,
        lambda result: result.index if isinstance(result, ResultChild) else 0,
        lambda result: result.name if isinstance(result, ResultChild) else "",
    ],
    consistent_widths=True,
)


class ContractRunner:
    """Handles loading config for contracts and their execution."""

    default_config_file_name: str = DEFAULT_CONFIG_FILE_NAME
    default_output_file_name: str = DEFAULT_OUTPUT_FILE_NAME

    @property
    def dbt(self) -> dbtRunner:
        """The dbt runner"""
        if self._dbt is None:
            self._dbt = dbtRunner(manifest=self._manifest)
        return self._dbt

    @property
    def config(self) -> RuntimeConfig:
        """The dbt runtime config"""
        if self._config is None:
            self._config = get_config()
        return self._config

    @property
    def manifest(self) -> Manifest:
        """The dbt manifest"""
        if self._manifest is None:
            self._manifest = get_manifest(runner=self.dbt, config=self.config)
            self._dbt = None
        return self._manifest

    @property
    def catalog(self) -> CatalogArtifact:
        """The dbt catalog"""
        if self._catalog is None:
            self._catalog = get_catalog(runner=self.dbt, config=self.config)
        return self._catalog

    @property
    def paths(self) -> Collection[str]:
        """An additional set of paths to filter on when filter contract items."""
        return self._paths

    @paths.setter
    def paths(self, value: Collection[str]):
        paths = []
        for path in value:
            path = Path(path)
            project_root = Path(self.config.project_root)
            if not project_root.is_absolute():
                project_root = Path(os.getcwd(), project_root)

            if path.is_absolute():
                path = path
            elif (path_in_project := Path(project_root, path)).exists():
                path = path_in_project
            elif (path_in_cwd := Path(os.getcwd(), path)).exists():
                path = path_in_cwd

            if not path.is_relative_to(project_root):
                raise Exception(f"Could not determine relative path to {project_root} for {path}")

            paths.append(str(path.relative_to(project_root)))

        self._paths = paths

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
        obj._config = get_config(args)
        return obj

    @classmethod
    def from_yaml(cls, path: str | Path) -> Self:
        """
        Set up a new runner from the config in a yaml file at the given `path`.

        :param path: The path to the yaml file.
            May either be a path to a yaml file or a path to the directory where the file is located.
            If a directory is given, the default file name will be appended.
        :return: The configured runner.
        """
        path = Path(path)
        if path.is_dir():
            path = path.joinpath(cls.default_config_file_name)
        if not path.is_file():
            raise FileNotFoundError(f"Could not find config file at path: {path!r}")

        with path.open("r") as file:
            config = yaml.full_load(file)

        return cls.from_dict(config)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> Self:
        """
        Set up a new runner from the given `config`.

        :param config: The config to configure the runner with.
        :return: The configured runner.
        """
        contracts = [cls._create_contract_from_config(key, config=conf) for key, conf in config.items()]

        obj = cls(contracts)
        obj.logger.debug(f"Configured {len(contracts)} sets of contracts from config")
        return obj

    @classmethod
    def _create_contract_from_config(cls, key: str, config: Mapping[str, Any]) -> Contract:
        key = key.replace(" ", "_").casefold().rstrip("s") + "s"
        if key not in CONTRACTS_CONFIG_MAP:
            raise Exception(f"Unrecognised enforcement key: {key}")

        return CONTRACTS_CONFIG_MAP[key].from_dict(config=config)

    def __init__(
            self,
            contracts: Collection[Contract],
            results_formatter: ObjectFormatter[Result] = DEFAULT_TERMINAL_FORMATTER
    ):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._contracts: Collection[Contract] = contracts
        self._results_formatter = results_formatter

        self._dbt: dbtRunner | None = None
        self._config: RuntimeConfig | None = None
        self._manifest: Manifest | None = None
        self._catalog: CatalogArtifact | None = None

        self._paths: Collection[str] = []

    def __call__(self, contract_key: str = None, enforcements: Collection[str] = ()) -> list[Result]:
        return self.run(contract_key=contract_key, enforcements=enforcements)

    def run(self, contract_key: str = None, enforcements: Collection[str] = ()) -> list[Result]:
        """
        Run all contracts and get the results.

        :param contract_key: Only the run the contract which matches this config key.
            Specify granular contracts by separating keys by a '.' e.g. 'model', 'model.columns'.
        :param enforcements: Apply only these enforcements. If none given, apply all configured enforcements.
        :return: The results.
        """
        if not contract_key:
            contracts = [contract for contract in self._contracts]
        elif any(contract.config_key == contract_key for contract in self._contracts):
            contracts = [next(iter(c for c in self._contracts if c.config_key == contract_key))]
        else:  # assume contract key relates to a child contract
            parent = self._get_parent_contract(contract_key)
            if parent is None:
                raise Exception(f"Could not find a configured contract for the key: {contract_key}")

            # ensure parent has artifacts assigned to get parent items on child
            self._assign_artifacts_to_contracts([parent])
            contracts = [parent.child]

        self._assign_artifacts_to_contracts(contracts)

        results = []
        for parent in contracts:
            results.extend(self._run_contract(parent, enforcements=enforcements, run_children=not contract_key))

        if not results:
            self.logger.info(f"{Fore.LIGHTGREEN_EX}All contracts passed successfully{Fore.RESET}")
            return results

        output_lines = self._results_formatter.format(results)
        output_str = self._results_formatter.combine(output_lines)
        for line in output_str.split("\n"):
            self.logger.info(line)

        return results

    def _get_parent_contract(self, contract: Contract | str) -> ParentContract | None:
        parents = (
            parent for parent in self._contracts if isinstance(parent, ParentContract) and parent.child is not None
        )

        if isinstance(contract, Contract):
            return next(iter(parent for parent in parents if parent.child.config_key == contract.config_key), None)
        return next(
            iter(parent for parent in parents if f"{parent.config_key}.{parent.child.config_key}" == contract), None
        )

    def _assign_artifacts_to_contracts(self, contracts: Iterable[Contract]) -> None:
        for contract in contracts:
            if contract.needs_manifest:
                contract.manifest = self.manifest
            if contract.needs_catalog:
                contract.catalog = self.catalog
            if isinstance(contract, ParentContract) and self.paths:
                contract.filters.append((contract.paths, self.paths))

            parent = self._get_parent_contract(contract)
            if parent is not None:
                self._assign_artifacts_to_contracts([parent])

    @staticmethod
    def _run_contract(contract: Contract, enforcements: Collection[str] = (), run_children: bool = True) -> list[Result]:
        if isinstance(contract, ParentContract):
            contract.run(enforcements=enforcements, child=run_children)
        else:
            contract.run(enforcements=enforcements)

        return contract.results

    def write_results(self, results: Collection[Result], format: str, output: str | Path) -> Path:
        """
        Write the given results to an output file with the given `format`.

        :param results: The results to write.
        :param format: The format to write the file to e.g. 'txt', 'json' etc.
        :param output: The path to a directory or file to write to.
        :return: The path the file was written to.
        """
        method_name = f"_write_results_as_{format.lower().replace('-', '_').replace(' ', '_')}"
        try:
            method: Callable[[Collection[Result], Path], Path] = getattr(self, method_name)
        except AttributeError:
            raise Exception(f"Unrecognised format: {format}")

        if (output := Path(output)).is_dir():
            output = output.joinpath(self.default_output_file_name)
        output.parent.mkdir(parents=True, exist_ok=True)

        output_path = method(results, output)
        self.logger.info(f"{Fore.LIGHTBLUE_EX}Wrote {format} output to {str(output_path)!r}{Fore.RESET}")
        return output_path

    def _write_results_as_text(self, results: Collection[Result], output_path: Path) -> Path:
        output_lines = self._results_formatter.format(results)
        output = self._results_formatter.combine(output_lines)

        with (path := output_path.with_suffix(".txt")).open("w") as file:
            file.write(output)

        return path

    @staticmethod
    def _write_results_as_json(results: Collection[Result], output_path: Path) -> Path:
        output = [result.as_json() for result in results]
        with (path := output_path.with_suffix(".json")).open("w") as file:
            json.dump(output, file, indent=2)

        return path

    @staticmethod
    def _write_results_as_jsonl(results: Collection[Result], output_path: Path) -> Path:
        with  (path := output_path.with_suffix(".json")).open("w") as file:
            for result in results:
                json.dump(result.as_json(), file)
                file.write("\n")

        return path

    @staticmethod
    def _write_results_as_github_annotations(results: Collection[Result], output_path: Path) -> Path:
        output = [result.as_github_annotation() for result in results]
        with (path := output_path.with_suffix(".json")).open("w") as file:
            json.dump(output, file, indent=2)

        return path
