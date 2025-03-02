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
from dbt_contracts.contracts._contracts import CONTRACT_CLASSES, CONTRACT_MAP
from dbt_contracts.contracts.conditions import NameCondition, TagCondition
from dbt_contracts.contracts.terms import properties, source, column, macro
from dbt_contracts.types import ItemT, ParentT


class ContractTester[I: ItemT](metaclass=ABCMeta):
    """Base class for testing contracts."""
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> Contract[I]:
        """Fixture for creating a contract."""
        raise NotImplementedError

    @abstractmethod
    def items(self, **kwargs) -> list[I]:
        """Fixture for items to be tested."""
        raise NotImplementedError

    @abstractmethod
    def filtered_items(self, items: list[I]) -> list[I]:
        """Fixture for filtered set of items to be tested."""
        raise NotImplementedError

    @abstractmethod
    def valid_items(self, filtered_items: list[I]) -> list[I]:
        """Fixture for valid set of items to be tested."""
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
        assert sorted(contract.filtered_items, key=self._items_sort_key) == sorted(
            filtered_items, key=self._items_sort_key
        )

    def test_validate_items(self, contract: Contract[I], filtered_items: list[I], valid_items: list[I]):
        result = contract.validate()
        assert sorted(result, key=self._items_sort_key) == sorted(valid_items, key=self._items_sort_key)

        if len(filtered_items) < len(valid_items):
            assert contract.context.results

    def test_validate_items_on_no_terms(self, contract: Contract[I], filtered_items: list[I]):
        contract.terms = []
        result = contract.validate()
        assert sorted(result, key=self._items_sort_key) == sorted(filtered_items, key=self._items_sort_key)

    def test_from_dict(self, contract: Contract[I]):
        config = {
            "filter": sample(
                [cls._name() for cls in contract.__supported_conditions__],
                k=min(len(contract.__supported_conditions__), 2)
            ),
            "terms": sample(
                [cls._name() for cls in contract.__supported_terms__],
                k=min(len(contract.__supported_terms__), 3)
            ),
        }
        new_contract: Contract[I] = contract.__class__.from_dict(
            config, manifest=contract.manifest, catalog=contract.catalog
        )

        assert new_contract.manifest == contract.manifest
        assert new_contract.catalog == contract.catalog

        assert [condition.name for condition in new_contract.conditions] == config["filter"]
        assert [term.name for term in new_contract.terms] == config["terms"]


class ParentContractTester[I: ItemT, P: ParentT](ContractTester[P]):
    """Base class for testing parent contracts."""
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> ParentContract[I, P]:
        raise NotImplementedError

    @abstractmethod
    def child_conditions(self) -> Collection[ContractCondition]:
        """Fixture for child conditions."""
        raise NotImplementedError

    @abstractmethod
    def child_terms(self) -> Collection[ContractTerm]:
        """Fixture for child terms."""
        raise NotImplementedError

    def _items_sort_key(self, item: P) -> Any:
        return item.unique_id

    @staticmethod
    def test_contract_is_mapped(contract: ParentContract[I, P]):
        assert contract.__class__ in CONTRACT_CLASSES
        assert CONTRACT_MAP.get(contract.__config_key__) == contract.__class__

    @staticmethod
    def test_validate_terms(contract: ParentContract[I, P]):
        assert contract.validate_terms(contract.terms)

        invalid_classes = [
            cls for cls in contract.create_child_contract().__supported_terms__
            if cls not in contract.__supported_terms__
        ]
        if not invalid_classes:
            return

        invalid_cls = choice(invalid_classes)
        assert not contract.validate_terms(list(contract.terms) + [invalid_cls()])

        with pytest.raises(Exception):
            contract.__class__(terms=list(contract.terms) + [invalid_cls()])

    @staticmethod
    def test_validate_conditions(contract: ParentContract[I, P]):
        assert contract.validate_conditions(contract.conditions)

        invalid_classes = [
            cls for cls in contract.create_child_contract().__supported_conditions__
            if cls not in contract.__supported_conditions__
        ]
        if not invalid_classes:
            return

        invalid_cls = choice(invalid_classes)
        assert not contract.validate_conditions(list(contract.conditions) + [invalid_cls()])

        with pytest.raises(Exception):
            contract.__class__(conditions=list(contract.conditions) + [invalid_cls()])

    @staticmethod
    def test_create_child_contract(
            contract: ParentContract[I, P],
            child_conditions: Collection[ContractCondition],
            child_terms: Collection[ContractTerm]
    ):
        child = contract.create_child_contract(conditions=child_conditions, terms=child_terms)
        assert child.parent == contract
        assert child.conditions == child_conditions
        assert child.terms == child_terms

    def test_create_child_contract_from_dict(self, contract: ParentContract[I, P]):
        config = {
            "filter": sample(
                [cls._name() for cls in contract.__supported_conditions__],
                k=min(len(contract.__supported_conditions__), 2)
            ),
            "terms": sample(
                [cls._name() for cls in contract.__supported_terms__],
                k=min(len(contract.__supported_terms__), 3)
            ),
            contract.__child_contract__.__config_key__: {
                "filter": sample(
                    [cls._name() for cls in contract.__child_contract__.__supported_conditions__],
                    k=min(len(contract.__child_contract__.__supported_conditions__), 2)
                ),
                "terms": sample(
                    [cls._name() for cls in contract.__child_contract__.__supported_terms__],
                    k=min(len(contract.__child_contract__.__supported_terms__), 3)
                ),
            }
        }

        child = contract.create_child_contract_from_dict(config)

        assert child.parent == contract
        assert child.manifest == contract.manifest
        assert child.catalog == contract.catalog

        child_config = config[contract.__child_contract__.__config_key__]
        assert [condition.name for condition in child.conditions] == child_config["filter"]
        assert [term.name for term in child.terms] == child_config["terms"]


class ChildContractTester[I: ItemT, P: ParentT](ContractTester[I]):
    @abstractmethod
    def items(self, parent: ParentContract[I, P], **kwargs) -> list[tuple[I, P]]:
        raise NotImplementedError

    @abstractmethod
    def filtered_items(self, items: list[tuple[I, P]]) -> list[tuple[I, P]]:
        raise NotImplementedError

    @abstractmethod
    def valid_items(self, filtered_items: list[tuple[I, P]]) -> list[tuple[I, P]]:
        raise NotImplementedError

    # noinspection PyMethodOverriding
    @abstractmethod
    def contract(
            self, manifest: Manifest, catalog: CatalogArtifact, parent: ParentContract[I, P]
    ) -> ChildContract[I, P]:
        raise NotImplementedError

    @abstractmethod
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> ParentContract[I, P]:
        """Fixture for parent contract."""
        raise NotImplementedError

    def _items_sort_key(self, item: tuple[I, P]) -> Any:
        return item[1].unique_id, item[0].name

    @staticmethod
    def test_validate_terms(contract: ChildContract[I, P], parent: ParentContract[I, P]):
        assert contract.validate_terms(contract.terms)

        invalid_classes = [
            cls for cls in parent.__supported_terms__
            if cls not in contract.__supported_terms__
        ]
        if not invalid_classes:
            return

        invalid_cls = choice(invalid_classes)
        assert not contract.validate_terms(list(contract.terms) + [invalid_cls()])

        with pytest.raises(Exception):
            contract.__class__(terms=list(contract.terms) + [invalid_cls()])

    @staticmethod
    def test_validate_conditions(contract: ChildContract[I, P], parent: ParentContract[I, P]):
        assert contract.validate_conditions(contract.conditions)

        invalid_classes = [
            cls for cls in parent.__supported_conditions__
            if cls not in contract.__supported_conditions__
        ]
        if not invalid_classes:
            return

        invalid_cls = choice(invalid_classes)
        assert not contract.validate_conditions(list(contract.conditions) + [invalid_cls()])

        with pytest.raises(Exception):
            contract.__class__(conditions=list(contract.conditions) + [invalid_cls()])


class TestModelContract(ParentContractTester[ColumnInfo, ModelNode]):
    @pytest.fixture(scope="class")
    def items(self, models: list[ModelNode]) -> list[ModelNode]:
        return models

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[ModelNode]) -> list[ModelNode]:
        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("model1", "model2"))
            item.tags.append("include")

        return items

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[ModelNode]) -> list[ModelNode]:
        items = sample(filtered_items, k=len(filtered_items) // 2)
        for item in items:
            item.tags.append("required_tag")

        return items

    @pytest.fixture(scope="class")
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> ModelContract:
        conditions = [
            NameCondition(include=["model1", "model2"]),
            TagCondition(tags=["include"])
        ]
        terms = [
            properties.HasRequiredTags(tags="required_tag"),
        ]
        return ModelContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def child_conditions(self) -> Collection[ContractCondition]:
        return [
            NameCondition(include=["col1", "col2"]),
            TagCondition(tags=["valid"])
        ]

    @pytest.fixture(scope="class")
    def child_terms(self) -> Collection[ContractTerm]:
        return [
            column.HasDataType(min_count=3),
        ]


class TestSourceContract(ParentContractTester[ColumnInfo, SourceDefinition]):
    @pytest.fixture(scope="class")
    def items(self, sources: list[SourceDefinition]) -> list[SourceDefinition]:
        return sources

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[SourceDefinition]) -> list[SourceDefinition]:
        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("source1", "source2"))
            item.tags.append("include")

        return items

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[SourceDefinition]) -> list[SourceDefinition]:
        items = [item for item in filtered_items if bool(item.loader)]
        items = sample(items, k=len(items) // 2)
        for item in items:
            item.tags.append("required_tag")

        return items

    @pytest.fixture(scope="class")
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> SourceContract:
        conditions = [
            NameCondition(include=["source1", "source2"]),
            TagCondition(tags=["include"])
        ]
        terms = [
            properties.HasRequiredTags(tags="required_tag"),
            source.HasLoader(),
        ]
        return SourceContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def child_conditions(self) -> Collection[ContractCondition]:
        return [
            NameCondition(include=["col1", "col2"]),
            TagCondition(tags=["valid"])
        ]

    @pytest.fixture(scope="class")
    def child_terms(self) -> Collection[ContractTerm]:
        return [
            column.HasDataType(min_count=3),
        ]


class TestColumnContract(ChildContractTester[ColumnInfo, ModelNode]):
    # noinspection PyTestUnpassedFixture
    @pytest.fixture(scope="class")
    def items(self, parent: ModelContract, **__) -> list[tuple[ColumnInfo, ModelNode]]:
        parent_items = list(parent.filtered_items)
        assert parent_items
        return [(col, item) for item in parent_items for col in item.columns.values()]

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[tuple[ColumnInfo, ModelNode]]) -> list[tuple[ColumnInfo, ModelNode]]:
        columns = list({col.name: col for col, _ in items}.values())
        columns = sample(columns, k=len(columns) // 3)
        for col in columns:
            col.tags.append("valid")

        return [(col, parent) for col, parent in items if "valid" in col.tags]

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[tuple[ColumnInfo, ModelNode]]) -> list[tuple[ColumnInfo, ModelNode]]:
        return [
            (col, parent) for col, parent in filtered_items if bool(col.data_type)
        ]

    @pytest.fixture(scope="class")
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: ModelContract) -> ColumnContract:
        conditions = [
            TagCondition(tags=["valid"])
        ]
        terms = [
            column.HasDataType(),
        ]
        return ColumnContract(parent=parent, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> ModelContract:
        return ModelContract(manifest=manifest, catalog=catalog)


class TestMacroContract(ParentContractTester[MacroArgument, Macro]):
    @pytest.fixture(scope="class")
    def items(self, macros: list[Macro], manifest: Manifest) -> list[Macro]:
        for item in macros:
            item.package_name = manifest.metadata.project_name
        return macros

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[Macro]) -> list[Macro]:
        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("macro1", "macro2"))

        return items

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[Macro]) -> list[Macro]:
        return [
            item for item in filtered_items if bool(item.description)
        ]

    @pytest.fixture(scope="class")
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        conditions = [
            NameCondition(include=["macro1", "macro2"]),
        ]
        terms = [
            properties.HasDescription(),
        ]
        return MacroContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def child_conditions(self) -> Collection[ContractCondition]:
        return [
            NameCondition(include=["arg1", "arg2"]),
        ]

    @pytest.fixture(scope="class")
    def child_terms(self) -> Collection[ContractTerm]:
        return [
            macro.HasType(),
        ]


class TestMacroArgumentContract(ChildContractTester[MacroArgument, Macro]):
    # noinspection PyTestUnpassedFixture
    @pytest.fixture(scope="class")
    def items(self, parent: MacroContract, **__) -> list[tuple[MacroArgument, Macro]]:
        parent_items = list(parent.filtered_items)
        assert parent_items
        return [(arg, item) for item in parent_items for arg in item.arguments]

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[tuple[MacroArgument, Macro]]) -> list[tuple[MacroArgument, Macro]]:
        arguments = list({arg.name: arg for arg, _ in items}.values())
        arguments = sample(arguments, k=len(arguments) // 3)
        for arg in arguments:
            arg.name = choice(("arg1", "arg2"))

        return [(arg, parent) for arg, parent in items if arg.name in ("arg1", "arg2")]

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[tuple[MacroArgument, Macro]]) -> list[tuple[MacroArgument, Macro]]:
        return [
            (arg, parent) for arg, parent in filtered_items if bool(arg.type)
        ]

    @pytest.fixture(scope="class")
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: MacroContract) -> MacroArgumentContract:
        conditions = [
            NameCondition(include=["arg1", "arg2"]),
        ]
        terms = [
            macro.HasType(),
        ]
        return MacroArgumentContract(parent=parent, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        return MacroContract(manifest=manifest, catalog=catalog)
