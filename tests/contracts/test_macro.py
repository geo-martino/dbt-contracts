import re
from collections.abc import Collection
from datetime import datetime
from pathlib import Path
from random import randrange
from typing import Iterable, Any

import pytest
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import Macro
from faker import Faker

from dbt_contracts.contracts import MacroContract
from dbt_contracts.contracts.macro import MacroArgumentContract
from tests.contracts.testers import ParentContractTester, ChildContractTester

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

        return dict(filters=filters, enforcements=enforcements)

    @pytest.fixture
    def manifest(self, available_items: Iterable[Macro]) -> Manifest:
        manifest = Manifest()
        manifest.metadata.project_name = fake.word()

        manifest.macros = {macro.name: macro for macro in available_items}
        return manifest

    @pytest.fixture
    def catalog(self, available_items: Iterable[Macro]) -> CatalogArtifact:
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
    def available_items(self) -> Iterable[Macro]:
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
    def filtered_items(self, contract: MacroContract, available_items: Iterable[Macro]) -> Iterable[Macro]:
        for item in available_items:
            item.package_name = contract.manifest.metadata.project_name

        return [
            macro for macro in available_items
            if macro.package_name == contract.manifest.metadata.project_name
            and not int(re.match(r".*(\d+)", macro.name).group(1)) % 2
        ]

    @pytest.fixture
    def valid_items(self, contract: MacroContract, filtered_items: Iterable[Macro]) -> Iterable[Macro]:
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
    def parents(self) -> Iterable[Macro]:
        return [
            TestMacro.generate_macro("macro1"),
            TestMacro.generate_macro("macro2"),
            TestMacro.generate_macro("macro3"),
            TestMacro.generate_macro("macro4"),
            TestMacro.generate_macro("macro5"),
            TestMacro.generate_macro("macro6"),
        ]

    @pytest.fixture
    def config(self) -> dict[str, Any]:
        filters = [
            {"name": ".*[02468]$"},
        ]

        enforcements = [
            "has_description"
        ]

        return dict(filters=filters, enforcements=enforcements)

    @pytest.fixture
    def manifest(self, available_items: Iterable[MacroArgument]) -> Manifest:
        manifest = Manifest()
        manifest.metadata.project_name = fake.word()

        manifest.macros = {macro.name: macro for argument, macro in available_items}
        return manifest

    @pytest.fixture
    def catalog(self, available_items: Iterable[MacroArgument]) -> CatalogArtifact:
        return CatalogArtifact.from_results(
            generated_at=datetime.now(), nodes={}, sources={}, compile_results=None, errors=None
        )

    @pytest.fixture
    def contract(
            self, manifest: Manifest, catalog: CatalogArtifact, parents: Iterable[Macro]
    ) -> MacroArgumentContract:
        filters = [
            tuple((MacroArgumentContract.name, r".*[02468]$"))
        ]

        enforcements = [
        ]
        return MacroArgumentContract(
            manifest=manifest, catalog=catalog, filters=filters, enforcements=enforcements, parents=parents
        )

    @pytest.fixture
    def available_items(self, parents: Iterable[Macro]) -> Iterable[tuple[MacroArgument, Macro]]:
        return [
            (self.generate_macro_argument(macro, f"macro_argument{i}"), macro)
            for macro in parents for i in range(1, 4)
        ]

    @pytest.fixture
    def filtered_items(
            self, contract: MacroArgumentContract, available_items: Iterable[tuple[MacroArgument, Macro]]
    ) -> Iterable[tuple[MacroArgument, Macro]]:
        return [
            (argument, macro) for argument, macro in available_items
            if not int(re.match(r".*(\d+)", argument.name).group(1)) % 2
        ]

    @pytest.fixture
    def valid_items(
            self, contract: MacroArgumentContract, filtered_items: Iterable[tuple[MacroArgument, Macro]]
    ) -> Iterable[tuple[MacroArgument, Macro]]:
        return filtered_items
