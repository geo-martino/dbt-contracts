from abc import ABCMeta, abstractmethod
from collections.abc import Collection
from random import sample, choice
from typing import Any

import pytest
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from dbt_contracts.contracts import Contract, ParentContract, ContractTerm, ContractCondition, ChildContract, \
    ModelContract, SourceContract, ColumnContract, MacroContract, MacroArgumentContract
from dbt_contracts.contracts.conditions import NameCondition, TagCondition
from dbt_contracts.contracts.terms import node, properties, model, source, column, macro
from dbt_contracts.types import ItemT, ParentT


class ContractTester[I: ItemT](metaclass=ABCMeta):
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> Contract[I]:
        raise NotImplementedError

    @abstractmethod
    def items(self, **kwargs) -> list[I]:
        raise NotImplementedError

    @abstractmethod
    def filtered_items(self, items: list[I]) -> list[I]:
        raise NotImplementedError

    @staticmethod
    def test_create_context(contract: Contract[I]):
        assert contract.manifest is not None
        assert contract.catalog is not None

        context = contract.context
        assert context.manifest == contract.manifest
        assert context.catalog == contract.catalog
        assert not context.results

    @abstractmethod
    def _items_sort_key(self, item: I) -> Any:
        raise NotImplementedError

    def test_get_items(self, contract: Contract[I], items: list[I], filtered_items: list[I]):
        assert sorted(contract.items, key=self._items_sort_key) == sorted(items, key=self._items_sort_key)
        assert sorted(contract.filtered_items, key=self._items_sort_key) == sorted(filtered_items, key=self._items_sort_key)


class ParentContractTester[I: ParentT](ContractTester[I]):
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> ParentContract[I]:
        raise NotImplementedError

    @abstractmethod
    def child_conditions(self) -> Collection[ContractCondition]:
        raise NotImplementedError

    @abstractmethod
    def child_terms(self) -> Collection[ContractTerm]:
        raise NotImplementedError

    def _items_sort_key(self, item: I) -> Any:
        return item.unique_id

    def test_create_child_contract(
            self,
            contract: ParentContract[I],
            child_conditions: Collection[ContractCondition],
            child_terms: Collection[ContractTerm]
    ):
        child = contract.create_child_contract(conditions=child_conditions, terms=child_terms)
        assert child.parent == contract
        assert child.conditions == child_conditions
        assert child.terms == child_terms


class ChildContractTester[I: ItemT, P: ParentT](ContractTester[I]):
    @abstractmethod
    def items(self, parent: ParentContract[I], **kwargs) -> list[tuple[I, P]]:
        raise NotImplementedError

    @abstractmethod
    def filtered_items(self, items: list[tuple[I, P]]) -> list[tuple[I, P]]:
        raise NotImplementedError

    # noinspection PyMethodOverriding
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: ParentContract[I]) -> ChildContract[I, P]:
        raise NotImplementedError

    @abstractmethod
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> ParentContract[I]:
        raise NotImplementedError

    def _items_sort_key(self, item: tuple[I, P]) -> Any:
        return item[1].unique_id, item[0].name


class TestModelContract(ParentContractTester[ModelNode]):
    @pytest.fixture
    def items(self, models: list[ModelNode]) -> list[ModelNode]:
        return models

    @pytest.fixture
    def filtered_items(self, items: list[ModelNode]) -> list[ModelNode]:
        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("model1", "model2"))
            item.tags.append("include")

        return items

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> ModelContract:
        conditions = [
            NameCondition(include=["model1", "model2"]),
            TagCondition(tags=["include"])
        ]
        terms = [
            node.HasExpectedColumns(min_count=3),
            model.HasConstraints(min_count=3),
        ]
        return ModelContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture
    def child_conditions(self) -> Collection[ContractCondition]:
        return [
            NameCondition(include=["col1", "col2"]),
            TagCondition(tags=["valid"])
        ]

    @pytest.fixture
    def child_terms(self) -> Collection[ContractTerm]:
        return [
            column.HasDataType(min_count=3),
        ]


class TestSourceContract(ParentContractTester[SourceDefinition]):
    @pytest.fixture
    def items(self, sources: list[SourceDefinition]) -> list[SourceDefinition]:
        return sources

    @pytest.fixture
    def filtered_items(self, items: list[SourceDefinition]) -> list[SourceDefinition]:
        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("source1", "source2"))
            item.tags.append("include")

        return items

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> SourceContract:
        conditions = [
            NameCondition(include=["source1", "source2"]),
            TagCondition(tags=["include"])
        ]
        terms = [
            node.HasExpectedColumns(min_count=3),
            source.HasLoader(),
        ]
        return SourceContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture
    def child_conditions(self) -> Collection[ContractCondition]:
        return [
            NameCondition(include=["col1", "col2"]),
            TagCondition(tags=["valid"])
        ]

    @pytest.fixture
    def child_terms(self) -> Collection[ContractTerm]:
        return [
            column.HasDataType(min_count=3),
        ]


class TestColumnContract(ChildContractTester[ColumnInfo, ModelNode]):
    # noinspection PyTestUnpassedFixture
    @pytest.fixture
    def items(self, parent: ModelContract, **__) -> list[tuple[ColumnInfo, ModelNode]]:
        parent_items = list(parent.filtered_items)
        assert parent_items
        return [(col, item) for item in parent_items for col in item.columns.values()]

    @pytest.fixture
    def filtered_items(self, items: list[tuple[ColumnInfo, ModelNode]]) -> list[tuple[ColumnInfo, ModelNode]]:
        columns = list({col.name: col for col, _ in items}.values())
        columns = sample(columns, k=len(columns) // 3)
        for col in columns:
            col.tags.append("valid")

        return [(col, parent) for col, parent in items if "valid" in col.tags]

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: ModelContract) -> ColumnContract:
        conditions = [
            TagCondition(tags=["valid"])
        ]
        terms = [
            column.HasDataType(min_count=3),
        ]
        return ColumnContract(parent=parent, conditions=conditions, terms=terms)

    @pytest.fixture
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> ModelContract:
        return ModelContract(manifest=manifest, catalog=catalog)


class TestMacroContract(ParentContractTester[Macro]):
    @pytest.fixture
    def items(self, macros: list[Macro], manifest: Manifest) -> list[Macro]:
        for macro in macros:
            macro.package_name = manifest.metadata.project_name
        return macros

    @pytest.fixture
    def filtered_items(self, items: list[Macro]) -> list[Macro]:
        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("macro1", "macro2"))

        return items

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        conditions = [
            NameCondition(include=["macro1", "macro2"]),
        ]
        terms = [
            properties.HasDescription(),
        ]
        return MacroContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture
    def child_conditions(self) -> Collection[ContractCondition]:
        return [
            NameCondition(include=["arg1", "arg2"]),
        ]

    @pytest.fixture
    def child_terms(self) -> Collection[ContractTerm]:
        return [
            macro.HasType(),
        ]


class TestMacroArgumentContract(ChildContractTester[MacroArgument, Macro]):
    # noinspection PyTestUnpassedFixture
    @pytest.fixture
    def items(self, parent: MacroContract, **__) -> list[tuple[MacroArgument, Macro]]:
        parent_items = list(parent.filtered_items)
        assert parent_items
        return [(arg, item) for item in parent_items for arg in item.arguments]

    @pytest.fixture
    def filtered_items(self, items: list[tuple[MacroArgument, Macro]]) -> list[tuple[MacroArgument, Macro]]:
        arguments = list({arg.name: arg for arg, _ in items}.values())
        arguments = sample(arguments, k=len(arguments) // 3)
        for arg in arguments:
            arg.name = choice(("arg1", "arg2"))

        return [(arg, parent) for arg, parent in items if arg.name in ("arg1", "arg2")]

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: MacroContract) -> MacroArgumentContract:
        conditions = [
            NameCondition(include=["arg1", "arg2"]),
        ]
        terms = [
            macro.HasType(),
        ]
        return MacroArgumentContract(parent=parent, conditions=conditions, terms=terms)

    @pytest.fixture
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        return MacroContract(manifest=manifest, catalog=catalog)
