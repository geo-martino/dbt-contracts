import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from dbt_contracts.contracts import Contract, NodeContract
from dbt_contracts.contracts.column import ColumnContract
from dbt_contracts.contracts.macro import MacroContract, MacroArgumentContract
from dbt_contracts.contracts.model import ModelContract
from dbt_contracts.contracts.source import SourceContract
from dbt_contracts.dbt_cli import clean_paths, get_manifest, get_catalog
from dbt_contracts.log import TERMINAL_RESULT_LOG_COLUMNS, format_results_to_table_in_groups
from dbt_contracts.result import Result, ResultParent

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

    def __call__(self) -> list[Result]:
        return self.run()

    def run(self) -> list[Result]:
        """
        Run all contracts and get the results.

        :return: The results.
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

        tables = format_results_to_table_in_groups(
            results,
            sort_keys=[
                lambda result: result.result_type,
                lambda result: result.path,
                lambda result: result.parent_name if isinstance(result, ResultParent) else "",
                lambda result: result.index if isinstance(result, ResultParent) else 0,
                lambda result: result.name,
            ],
            header_key=lambda result: f"{result.result_type}: {result.path}",
            columns=TERMINAL_RESULT_LOG_COLUMNS,
            consistent_widths=True,
        )
        for header, rows in tables.items():
            self.logger.info(header)
            for row in rows:
                self.logger.info(row)
            self.logger.info("")

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


if __name__ == "__main__":
    path = "/Users/gmarino/Desktop/Projects/dlh-datamodel/cibc"
    runner = ContractRunner(path)
    results = runner()
