import re
from collections.abc import Collection
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from random import randrange
from typing import Any

import pytest
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import Macro
from faker import Faker

from dbt_contracts.contracts import MacroContract
from dbt_contracts.contracts.macro import MacroArgumentContract
from tests.contracts.testers.core import ParentContractTester, ChildContractTester

fake = Faker()


class TestMacro(ParentContractTester):

    @classmethod
    def generate_macro(cls, name: str) -> Macro:
        path = Path(
            fake.file_path(depth=randrange(3, 6), extension="sql", absolute=False)
        ).with_name(name)

        return Macro(
            name=name,
            path=str(path),
            original_file_path=str(Path("macros", path)),
            package_name=path.parent.name,
            resource_type=NodeType.Macro,
            unique_id=f"macros.{fake.word()}.{name}",
            macro_sql=""
        )

    @pytest.fixture
    def config(self) -> dict[str, Any]:
        filters = [
            {"name": ".*[02468]$"},
        ]

        enforcements = [
            "has_description", "has_properties"
        ]

        return dict(filter=filters, enforce=enforcements)

    @pytest.fixture
    def config_with_child(self, config: dict[str, Any]) -> dict[str, Any]:
        filters = [
            {"name": ".*[02468]$"},
        ]

        enforcements = [
            "has_description", "has_type"
        ]

        config = deepcopy(config)
        config[str(MacroArgumentContract.config_key)] = dict(filter=filters, enforce=enforcements)
        return config

    @pytest.fixture
    def manifest(self, available_items: list[Macro]) -> Manifest:
        manifest = Manifest()
        manifest.metadata.project_name = fake.word()

        manifest.macros = {macro.name: macro for macro in available_items}
        return manifest

    @pytest.fixture
    def catalog(self, available_items: list[Macro]) -> CatalogArtifact:
        return CatalogArtifact.from_results(
            generated_at=datetime.now(), nodes={}, sources={}, compile_results=None, errors=None
        )

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        filters = [
            tuple((MacroContract.name, r".*[02468]$"))
        ]

        enforcements = [
        ]
        return MacroContract(manifest=manifest, catalog=catalog, filters=filters, enforcements=enforcements)

    @pytest.fixture
    def child(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroArgumentContract:
        return MacroArgumentContract(manifest=manifest, catalog=catalog)

    @pytest.fixture
    def available_items(self) -> list[Macro]:
        return [
            self.generate_macro("macro1"),
            self.generate_macro("macro2"),
            self.generate_macro("macro3"),
            self.generate_macro("macro4"),
            self.generate_macro("macro5"),
            self.generate_macro("macro6"),
            self.generate_macro("macro7"),
            self.generate_macro("macro8"),
        ]

    @pytest.fixture
    def filtered_items(self, contract: MacroContract, available_items: list[Macro]) -> list[Macro]:
        for item in available_items:
            item.package_name = contract.manifest.metadata.project_name

        return [
            macro for macro in available_items
            if macro.package_name == contract.manifest.metadata.project_name
            and not int(re.match(r".*(\d+)", macro.name).group(1)) % 2
        ]

    @pytest.fixture
    def valid_items(self, contract: MacroContract, filtered_items: list[Macro]) -> list[Macro]:
        return filtered_items

    def test_filter_macros_with_invalid_package(
            self, contract: MacroContract, available_items: Collection[Macro], filtered_items: Collection[Macro]
    ):
        for macro in available_items:
            macro.package_name = fake.word()

        assert all(macro.package_name != contract.manifest.metadata.project_name for macro in available_items)
        assert not list(contract.items)

        for macro in available_items[:len(available_items) // 2]:
            macro.package_name = contract.manifest.metadata.project_name

        assert list(contract.items) == [
            macro for macro in available_items
            if macro.package_name == contract.manifest.metadata.project_name
            and macro in filtered_items
        ]


class TestMacroArgument(ChildContractTester):

    @classmethod
    def generate_macro_argument(cls, macro: Macro, name: str) -> MacroArgument:
        argument = MacroArgument(name=name)
        macro.arguments.append(argument)
        return argument

    @pytest.fixture
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        return MacroContract(manifest=manifest, catalog=catalog)

    @pytest.fixture
    def config(self) -> dict[str, Any]:
        filters = [
            {"name": ".*[02468]$"},
        ]

        enforcements = [
            "has_description"
        ]

        return dict(filter=filters, enforce=enforcements)

    @pytest.fixture
    def manifest(self, available_items: list[MacroArgument]) -> Manifest:
        manifest = Manifest()
        manifest.metadata.project_name = fake.word()

        manifest.macros = {macro.name: macro for argument, macro in available_items}
        return manifest

    @pytest.fixture
    def catalog(self, available_items: list[MacroArgument]) -> CatalogArtifact:
        return CatalogArtifact.from_results(
            generated_at=datetime.now(), nodes={}, sources={}, compile_results=None, errors=None
        )

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: MacroContract) -> MacroArgumentContract:
        filters = [
            tuple((MacroArgumentContract.name, r".*[02468]$"))
        ]

        enforcements = [
        ]
        return MacroArgumentContract(
            manifest=manifest, catalog=catalog, filters=filters, enforcements=enforcements, parents=parent
        )

    @pytest.fixture
    def available_items(self) -> list[tuple[MacroArgument, Macro]]:
        macros = [TestMacro.generate_macro(f"macro{i}") for i in range(1, 4)]
        return [
            (self.generate_macro_argument(macro, f"macro_argument{i}"), macro)
            for macro in macros for i in range(1, 4)
        ]

    @pytest.fixture
    def filtered_items(
            self, contract: MacroArgumentContract, available_items: list[tuple[MacroArgument, Macro]]
    ) -> list[tuple[MacroArgument, Macro]]:
        for _, macro in available_items:
            macro.package_name = contract.manifest.metadata.project_name

        return [
            (argument, macro) for argument, macro in available_items
            if not int(re.match(r".*(\d+)", argument.name).group(1)) % 2
        ]

    @pytest.fixture
    def valid_items(
            self, contract: MacroArgumentContract, filtered_items: list[tuple[MacroArgument, Macro]]
    ) -> list[tuple[MacroArgument, Macro]]:
        return filtered_items
