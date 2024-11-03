import logging
from collections.abc import Mapping, Collection
from pathlib import Path
from typing import Any, Self

import yaml
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest

from dbt_contracts.contracts import Contract, CONTRACTS_CONFIG_MAP
from dbt_contracts.dbt_cli import clean_paths, get_manifest, get_catalog

from dbt_contracts.formatters import ObjectFormatter
from dbt_contracts.result import Result, DEFAULT_TERMINAL_FORMATTER

logging.basicConfig(level=logging.INFO, format="%(message)s")


class ContractRunner:
    """Handles loading config for contracts and their execution."""

    default_config_file_name: str = "contracts.yml"

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
            raise Exception(f"Unrecognised validator key: {key}")

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
        self._manifest: Manifest | None = None
        self._catalog: CatalogArtifact | None = None

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

        output_lines = self._results_formatter.format(results)
        output_str = self._results_formatter.combine(output_lines)
        for line in output_str.split("\n"):
            self.logger.info(line)

        return results


if __name__ == "__main__":
    path = "/Users/gmarino/Desktop/Projects/dlh-datamodel/cibc"
    runner = ContractRunner.from_yaml(path)
    results = runner()
