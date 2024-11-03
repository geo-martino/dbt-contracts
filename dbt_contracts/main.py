import itertools
import logging
import operator
import textwrap
from collections.abc import Mapping, Collection, Iterable
from pathlib import Path
from typing import Any

import yaml
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from dbt_contracts.dbt import clean_paths, get_manifest, get_catalog
from dbt_contracts.contracts import Contract, NodeContract
from dbt_contracts.contracts.column import ColumnContract
from dbt_contracts.contracts.macro import MacroContract, MacroArgumentContract
from dbt_contracts.contracts.model import ModelContract
from dbt_contracts.contracts.source import SourceContract
from dbt_contracts.result import ResultLog

logging.basicConfig(level=logging.INFO, format="%(message)s")


class ContractRunner:
    """Handles loading config for contracts and their execution."""

    default_config_file_name: str = "contracts.yml"
    _contract_key_map: Mapping[str, type[Contract]] = {
        "models": ModelContract,
        "sources": SourceContract,
        "macros": MacroContract,
    }

    @property
    def dbt(self) -> dbtRunner:
        """The dbt runner"""
        if self._dbt is not None:
            self._dbt = dbtRunner(manifest=self._manifest)
        return self._dbt

    @property
    def manifest(self) -> Manifest:
        """The dbt manifest"""
        if self._manifest is None:
            self._manifest = get_manifest(runner=self.dbt)
            self._dbt = None
        return self._manifest

    @property
    def catalog(self) -> CatalogArtifact:
        """The dbt catalog"""
        if self._catalog is None:
            self._catalog = get_catalog(runner=self.dbt)
        return self._catalog

    def __init__(self, config_path: str | Path):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self._dbt: dbtRunner | None = None
        self._manifest: Manifest | None = None
        self._catalog: CatalogArtifact | None = None

        self._contracts: list[Contract] = []
        self._load_config(config_path)

    def __call__(self) -> list[ResultLog]:
        return self.run()

    def run(self) -> list[ResultLog]:
        """
        Run all contracts and get the results.

        :return: The log results.
        """
        clean_paths()

        results = []
        for contract in self._contracts:
            if contract.needs_manifest:
                contract.manifest = self.manifest
            if contract.needs_catalog:
                contract.catalog = self.catalog

            contract.run()
            results.extend(contract.results)

        self._log_results(results)
        return results

    ###########################################################################
    ## Setup
    ###########################################################################
    def _load_config(self, path: str | Path) -> None:
        path = Path(path)
        if path.is_dir():
            path = path.joinpath(self.default_config_file_name)
        if not path.is_file():
            raise FileNotFoundError(f"Could not find config file at path: {path!r}")

        with path.open("r") as file:
            config = yaml.full_load(file)

        for key, val in config.items():
            self._set_contract(key, config=val)

        self.logger.debug(f"Configured {len(self._contracts)} sets of contracts")

    def _set_contract(self, key: str, config: Mapping[str, Any]) -> None:
        key = key.replace(" ", "_").casefold().rstrip("s") + "s"
        if key not in self._contract_key_map:
            raise Exception(f"Unrecognised validator key: {key}")

        contract = self._contract_key_map[key].from_dict(config=config)
        self._contracts.append(contract)

        if isinstance(contract, MacroContract) and (config_child := config.get("arguments")):
            child_contract = MacroArgumentContract.from_dict(
                config=config_child, parents=lambda: contract.items
            )
            self._contracts.append(child_contract)

        elif isinstance(contract, NodeContract) and (config_child := config.get("columns")):
            child_contract = ColumnContract.from_dict(
                config=config_child, parents=lambda: contract.items
            )
            self._contracts.append(child_contract)

    ###########################################################################
    ## Format log output
    ###########################################################################
    @staticmethod
    def _get_col_map(
            values: Iterable[Mapping[str, str]],
            key: str,
            value_prefix: str = "",
            align: str = "<",
            min_width: int = 5,
            max_width: int = 30,
    ) -> tuple[str, str, str, int]:
        values = map(str, (val.get(key, "") for val in values))
        return key, value_prefix, align, max(min_width, min(max(map(len, values)), max_width))

    def _log_results(self, logs: Iterable[ResultLog]) -> None:
        logs = sorted(logs, key=lambda x: (x.get("path"), x.get("parent_name", ""), x.get("index", x.get("name"))))
        logs_grouped = itertools.groupby(logs, key=operator.itemgetter("path"))

        for path, log_group in logs_grouped:
            log_group = sorted(log_group, key=operator.itemgetter("validation_type"))

            log_path = self._format_log_header(path, log_group)
            self.logger.info(log_path)

            cols = [
                self._get_col_map(log_group, "validation_type", max_width=30),
                self._get_col_map(log_group, "patch_start_line", "L: ", ">", min_width=3, max_width=6),
                self._get_col_map(log_group, "patch_start_col", "P: ", ">", min_width=2, max_width=3),
                self._get_col_map(log_group, "parent_name", max_width=30),
                self._get_col_map(log_group, "name", max_width=30),
                self._get_col_map(log_group, "validation_name", max_width=30),
                ("message", "", "<", 60),
            ]

            for log in log_group:
                log_line = self._format_log_result(log, cols)
                self.logger.info(log_line)

            self.logger.info("")

    @staticmethod
    def _format_log_header(path: Path, logs: Iterable[ResultLog]) -> str:
        patch_path = next((log.patch_path for log in logs), None)
        if patch_path and patch_path != path:
            return f"\33[95;1m->\33[0m \33[94m{path}\33[0m @ \33[96m{patch_path}\33[0m"
        return f"\33[97;1m-> \33[94m{path}\33[0m"

    @staticmethod
    def _format_log_result(
            log: ResultLog, cols: Collection[tuple[str, str, str, int]], col_sep: str = "|"
    ) -> str:
        log_items = []
        col_sep = f" \33[97m{col_sep}\33[0m "
        last_col = list(map(operator.itemgetter(0), cols))[-1]

        for key, prefix, align, width in cols:
            value = str(log.get(key, ""))
            prefix = f"\33[96;1m{prefix}\33[0m"

            if key == last_col:
                widths_sum = sum(list(map(operator.itemgetter(3), cols))[:-1])
                widths_sum += sum(list(map(len, map(operator.itemgetter(1), cols)))[:-1])
                new_line_indent_len = widths_sum + (3 * (len(cols) - 2))
                new_line_indent = f"{' ' * new_line_indent_len}{col_sep}"
                log_line = f"\n{new_line_indent}".join(
                    textwrap.wrap(value, width, break_long_words=False, break_on_hyphens=False)
                )
            elif width:
                log_line = f"{prefix}\33[96m{value:{align}{width}.{width}}\33[0m"
            else:
                log_line = f"{prefix}\33[96m{value}\33[0m"

            log_items.append(log_line)

        return col_sep.join(log_items)


if __name__ == "__main__":
    path = ""
    runner = ContractRunner(path)
    results = runner()
