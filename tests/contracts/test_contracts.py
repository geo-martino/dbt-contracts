from abc import ABCMeta, abstractmethod
from collections.abc import MutableSequence
from contextlib import ExitStack
from random import sample, choice
from typing import Any
from unittest import mock

import pytest
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.graph.nodes import SourceDefinition, Macro

from dbt_contracts.contracts import CONTRACT_CLASSES, CONTRACT_MAP
from dbt_contracts.contracts import Contract, ParentContract, ChildContract, \
    ModelContract, SourceContract, ColumnContract, MacroContract, MacroArgumentContract
from dbt_contracts.contracts.conditions import properties as c_properties, source as c_source, ContractCondition
from dbt_contracts.contracts.generators import PropertiesGenerator, ParentPropertiesGenerator, ChildPropertiesGenerator
from dbt_contracts.contracts.generators.column import ColumnPropertiesGenerator
from dbt_contracts.contracts.generators.model import ModelPropertiesGenerator
from dbt_contracts.contracts.generators.source import SourcePropertiesGenerator
from dbt_contracts.contracts.terms import properties as t_properties, source as t_source, column as t_column, \
    macro as t_macro, ContractTerm, ChildContractTerm
from dbt_contracts.types import ItemT, ParentT


class ContractTester[I: ItemT, G: PropertiesGenerator](metaclass=ABCMeta):
    """Base class for testing contracts."""
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> Contract[I, ContractTerm, G]:
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
    def test_needs_manifest(contract: Contract[I, ContractTerm, G]):
        contract.terms = [term() for term in contract.__supported_terms__ if not term.needs_manifest]
        assert not contract.needs_manifest

        contract.terms = [term() for term in contract.__supported_terms__ if term.needs_manifest]
        if not contract.terms:
            return
        assert contract.needs_manifest

    @staticmethod
    def test_needs_catalog(contract: Contract[I, ContractTerm, G]):
        contract.terms = [term() for term in contract.__supported_terms__ if not term.needs_catalog]
        assert not contract.needs_catalog

        contract.terms = [term() for term in contract.__supported_terms__ if term.needs_catalog]
        if not contract.terms:
            return
        assert contract.needs_catalog

    @staticmethod
    def test_create_context(contract: Contract[I, ContractTerm, G]):
        assert contract.manifest is not None
        assert contract.catalog is not None

        context = contract.context
        assert context.manifest == contract.manifest
        assert context.catalog == contract.catalog
        assert not context.results

    @abstractmethod
    def _items_sort_key(self, item: I) -> Any:
        raise NotImplementedError

    def test_get_items(self, contract: Contract[I, ContractTerm, G], items: list[I], filtered_items: list[I]):
        assert sorted(contract.items, key=self._items_sort_key) == sorted(items, key=self._items_sort_key)
        assert sorted(contract.filtered_items, key=self._items_sort_key) == sorted(
            filtered_items, key=self._items_sort_key
        )

    def test_validate_items(
            self, contract: Contract[I, ContractTerm, G], filtered_items: list[I], valid_items: list[I]
    ):
        result = contract.validate()
        assert sorted(result, key=self._items_sort_key) == sorted(valid_items, key=self._items_sort_key)

        if len(filtered_items) < len(valid_items):
            assert contract.context.results

    def test_validate_items_on_no_terms(self, contract: Contract[I, ContractTerm, G], filtered_items: list[I]):
        contract.terms = []
        result = contract.validate()
        assert sorted(result, key=self._items_sort_key) == sorted(filtered_items, key=self._items_sort_key)

    @staticmethod
    def test_validate_on_selected_terms(contract: Contract[I, ContractTerm, G]):
        name = choice(contract.terms).name
        mock_map = {term.name: mock.patch.object(term.__class__, "run") for term in contract.terms}

        contract.conditions.clear()
        assert list(contract.filtered_items)

        with ExitStack() as stack:
            mocks = {mock_name: stack.enter_context(mock_term) for mock_name, mock_term in mock_map.items()}

            contract.validate(terms=[name])
            for mock_name, mock_term in mocks.items():
                mock_term.assert_called() if mock_name == name else mock_term.assert_not_called()

    @staticmethod
    def test_from_dict(contract: Contract[I, ContractTerm, G]):
        conditions = sample(
            [cls._name() for cls in contract.__supported_conditions__],
            k=min(len(contract.__supported_conditions__), 2)
        )
        terms = sample(
            [cls._name() for cls in contract.__supported_terms__],
            k=min(len(contract.__supported_terms__), 3)
        )
        generator = {"exclude": "description"}
        config = {
            choice(["filter", "conditions"]): conditions,
            choice(["validations", "terms"]): terms,
            choice(["properties", "generator"]): generator,
        }

        new_contract: Contract[I, ContractTerm] = contract.__class__.from_dict(
            config, manifest=contract.manifest, catalog=contract.catalog
        )

        assert new_contract.manifest == contract.manifest
        assert new_contract.catalog == contract.catalog

        assert [condition.name for condition in new_contract.conditions] == conditions
        assert [term.name for term in new_contract.terms] == terms
        if new_contract.__supported_generator__ is not None:
            assert new_contract.generator.exclude == (generator["exclude"],)
        else:
            assert new_contract.generator is None

    @staticmethod
    def test_create_contract_part_from_dict_with_invalid(contract: Contract[I, ContractTerm, G]):
        part_map = {cond._name(): cond for cond in contract.__supported_conditions__}
        part_name = c_source.IsEnabledCondition._name()
        part_map[part_name] = c_source.IsEnabledCondition

        # noinspection PyTypeChecker
        assert contract._create_contract_part_from_dict([part_name], part_map) is None  # bad input type
        assert contract._create_contract_part_from_dict("unknown", part_map) is None  # unrecognised part

    @staticmethod
    def test_create_contract_part_from_dict_without_config(contract: Contract[I, ContractTerm, G]):
        part_map = {cond._name(): cond for cond in contract.__supported_conditions__}
        part_name = c_source.IsEnabledCondition._name()
        part_map[part_name] = c_source.IsEnabledCondition

        with mock.patch.object(c_source.IsEnabledCondition, "__new__") as mock_condition:
            contract._create_contract_part_from_dict(part_name, part_map)
            mock_condition.assert_called_once_with(c_source.IsEnabledCondition)

    @staticmethod
    def test_create_contract_part_from_dict_with_config(contract: Contract[I, ContractTerm, G]):
        part_map = {cond._name(): cond for cond in contract.__supported_conditions__}
        part_name = c_source.IsEnabledCondition._name()
        part_map[part_name] = c_source.IsEnabledCondition

        config = {part_name: {"key1": "value", "key2": "value"}}

        with mock.patch.object(c_source.IsEnabledCondition, "__new__") as mock_condition:
            contract._create_contract_part_from_dict(config, part_map)
            mock_condition.assert_called_once_with(c_source.IsEnabledCondition, **config[part_name])


class ParentContractTester[I: ItemT, P: ParentT, G: ParentPropertiesGenerator](ContractTester[P, G]):
    """Base class for testing parent contracts."""
    @abstractmethod
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> ParentContract[I, P, G]:
        raise NotImplementedError

    @abstractmethod
    def child_conditions(self) -> MutableSequence[ContractCondition]:
        """Fixture for child conditions."""
        raise NotImplementedError

    @abstractmethod
    def child_terms(self) -> MutableSequence[ChildContractTerm]:
        """Fixture for child terms."""
        raise NotImplementedError

    @pytest.fixture
    def child_generator(self) -> ChildPropertiesGenerator | None:
        """Fixture for a child generator."""
        return

    def _items_sort_key(self, item: P) -> Any:
        return item.unique_id

    @staticmethod
    def test_contract_is_mapped(contract: ParentContract[I, P, G]):
        assert contract.__class__ in CONTRACT_CLASSES
        assert CONTRACT_MAP.get(contract.__config_key__) == contract.__class__

    @staticmethod
    def test_validate_terms(contract: ParentContract[I, P, G]):
        assert contract.validate_terms(contract.terms)
        if contract.__supported_terms__:
            assert not contract.validate_terms([])

        invalid_classes = [
            cls for cls in contract.__child_contract__.__supported_terms__
            if cls not in contract.__supported_terms__
        ]
        if not invalid_classes:
            return

        invalid_cls = choice(invalid_classes)
        assert not contract.validate_terms(list(contract.terms) + [invalid_cls()])

        with pytest.raises(Exception):
            contract.__class__(terms=list(contract.terms) + [invalid_cls()])

    @staticmethod
    def test_validate_conditions(contract: ParentContract[I, P, G]):
        assert contract.validate_conditions(contract.conditions)
        if contract.__supported_conditions__:
            assert not contract.validate_conditions([])

        invalid_classes = [
            cls for cls in contract.__child_contract__.__supported_conditions__
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
            contract: ParentContract[I, P, G],
            child_conditions: MutableSequence[ContractCondition],
            child_terms: MutableSequence[ChildContractTerm],
            child_generator: ChildPropertiesGenerator,
    ):
        child = contract.create_child_contract(
            conditions=child_conditions, terms=child_terms, generator=child_generator
        )
        assert child.parent == contract
        assert child.conditions == child_conditions
        assert child.terms == child_terms
        assert child.generator == child_generator

    @staticmethod
    def test_create_child_contract_from_dict(contract: ParentContract[I, P, G]):
        config = {
            "filter": sample(
                [cls._name() for cls in contract.__supported_conditions__],
                k=min(len(contract.__supported_conditions__), 2)
            ),
            "terms": sample(
                [cls._name() for cls in contract.__supported_terms__],
                k=min(len(contract.__supported_terms__), 3)
            ),
            contract.__child_contract__.__config_key__: [{
                "filter": sample(
                    [cls._name() for cls in contract.__child_contract__.__supported_conditions__],
                    k=min(len(contract.__child_contract__.__supported_conditions__), 2)
                ),
                "terms": sample(
                    [cls._name() for cls in contract.__child_contract__.__supported_terms__],
                    k=min(len(contract.__child_contract__.__supported_terms__), 3)
                ),
            }]
        }

        child = contract.create_child_contract_from_dict(config)[0]

        assert child.parent == contract
        assert child.manifest == contract.manifest
        assert child.catalog == contract.catalog

        child_config = config[contract.__child_contract__.__config_key__][0]
        assert [condition.name for condition in child.conditions] == child_config["filter"]
        assert [term.name for term in child.terms] == child_config["terms"]

    @staticmethod
    def test_validate_context_for_manifest(contract: ParentContract[I, P, G], items: list[I], manifest: Manifest):
        contract.terms = [term() for term in contract.__supported_terms__ if term.needs_manifest]
        context = contract.context
        context.manifest = None
        for term in contract.terms:
            with pytest.raises(Exception):
                term.run(choice(items), context=context)

        context.manifest = manifest
        for term in contract.terms:  # just check that these don't fail
            term.run(choice(items), context=context)

    @staticmethod
    def test_validate_context_for_catalog(contract: ParentContract[I, P, G], items: list[I], catalog: CatalogArtifact):
        contract.terms = [term() for term in contract.__supported_terms__ if not term.needs_catalog]
        assert not contract.needs_catalog

        contract.terms = [term() for term in contract.__supported_terms__ if term.needs_catalog]
        if not contract.terms:
            return
        assert contract.needs_catalog

        context = contract.context
        context.catalog = None
        for term in contract.terms:
            with pytest.raises(Exception):
                term.run(choice(items), context=context)

        context.catalog = catalog
        for term in contract.terms:  # just check that these don't fail
            term.run(choice(items), context=context)

    @staticmethod
    def test_generate(contract: Contract[I, ContractTerm, G], filtered_items: list[I]):
        contract.generator = None
        assert not contract.generate()

        if contract.__supported_generator__ is None:
            return

        contract.generator = contract.__supported_generator__()

        with (
                mock.patch.object(
                    contract.__class__, "filtered_items", new_callable=mock.PropertyMock, return_value=filtered_items
                ),
                mock.patch.object(contract.generator.__class__, "merge", return_value=True) as mock_merge,
                mock.patch.object(contract.generator.__class__, "update") as mock_update,
        ):
            results = contract.generate()
            assert results
            assert sum(results.values()) == len(filtered_items)

            mock_merge.assert_called()
            assert len(mock_merge.mock_calls) == len(filtered_items)
            mock_update.assert_called()
            assert len(mock_update.mock_calls) == len(filtered_items)

        with (
                mock.patch.object(contract.generator.__class__, "merge", return_value=False) as mock_merge,
                mock.patch.object(contract.generator.__class__, "update") as mock_update,
        ):
            results = contract.generate()
            assert not results

            mock_merge.assert_called()
            assert len(mock_merge.mock_calls) == len(list(contract.filtered_items))
            mock_update.assert_not_called()


class ChildContractTester[I: ItemT, P: ParentT, G: ChildPropertiesGenerator](ContractTester[I, G]):
    @abstractmethod
    def items(self, parent: ParentContract[I, P, G], **kwargs) -> list[tuple[I, P]]:
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
            self, manifest: Manifest, catalog: CatalogArtifact, parent: ParentContract[I, P, ParentPropertiesGenerator]
    ) -> ChildContract[I, P, G]:
        raise NotImplementedError

    @abstractmethod
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> ParentContract[I, P, ParentPropertiesGenerator]:
        """Fixture for parent contract."""
        raise NotImplementedError

    def _items_sort_key(self, item: tuple[I, P]) -> Any:
        return item[1].unique_id, item[0].name

    @staticmethod
    def test_validate_terms(contract: ChildContract[I, P, G], parent: ParentContract[I, P, ParentPropertiesGenerator]):
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
    def test_validate_conditions(
            contract: ChildContract[I, P, G], parent: ParentContract[I, P, ParentPropertiesGenerator]
    ):
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

    @staticmethod
    def test_validate_context_for_manifest(
            contract: ChildContract[I, P, G], items: list[tuple[I, P]], manifest: Manifest
    ):
        contract.terms = [term() for term in contract.__supported_terms__ if term.needs_manifest]
        context = contract.context
        context.manifest = None
        for term in contract.terms:
            with pytest.raises(Exception):
                item, parent = choice(items)
                term.run(item, parent=parent, context=context)

        context.manifest = manifest
        for term in contract.terms:  # just check that these don't fail
            item, parent = choice(items)
            term.run(item, parent=parent, context=context)

    @staticmethod
    def test_validate_context_for_catalog(
            contract: ChildContract[I, P, G], items: list[tuple[I, P]], catalog: CatalogArtifact
    ):
        contract.terms = [term() for term in contract.__supported_terms__ if not term.needs_catalog]
        assert not contract.needs_catalog

        contract.terms = [term() for term in contract.__supported_terms__ if term.needs_catalog]
        if not contract.terms:
            return
        assert contract.needs_catalog

        context = contract.context
        context.catalog = None
        for term in contract.terms:
            with pytest.raises(Exception):
                item, parent = choice(items)
                term.run(item, parent=parent, context=context)

        context.catalog = catalog
        for term in contract.terms:  # just check that these don't fail
            item, parent = choice(items)
            term.run(item, parent=parent, context=context)

    @staticmethod
    def test_generate(contract: ChildContract[I, P, G], filtered_items: list[tuple[I, P]]):
        contract.generator = None
        assert not contract.generate()

        if contract.__supported_generator__ is None:
            return

        contract.generator = contract.__supported_generator__()
        contract.parent.generator = contract.parent.__supported_generator__()

        with (
                mock.patch.object(
                    contract.__class__, "filtered_items", new_callable=mock.PropertyMock, return_value=filtered_items
                ),
                mock.patch.object(contract.generator.__class__, "merge", return_value=True) as mock_merge,
                mock.patch.object(contract.parent.generator.__class__, "update") as mock_update,
        ):
            results = contract.generate()
            assert results
            assert sum(results.values()) == len(filtered_items)

            mock_merge.assert_called()
            assert len(mock_merge.mock_calls) == len(filtered_items)
            mock_update.assert_called()
            assert len(mock_update.mock_calls) == len(filtered_items)

        with (
                mock.patch.object(contract.generator.__class__, "merge", return_value=False) as mock_merge,
                mock.patch.object(contract.parent.generator.__class__, "update") as mock_update,
        ):
            results = contract.generate()
            assert not results

            mock_merge.assert_called()
            assert len(mock_merge.mock_calls) == len(list(contract.filtered_items))
            mock_update.assert_not_called()


class TestModelContract(ParentContractTester[ColumnInfo, ModelNode, ModelPropertiesGenerator]):
    @pytest.fixture(scope="class")
    def items(self, models: list[ModelNode], manifest: Manifest) -> list[ModelNode]:
        return [model for model in models if model.package_name == manifest.metadata.project_name]

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[ModelNode]) -> list[ModelNode]:
        assert len(items) > 3

        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("model1", "model2"))
            item.tags.append("include")

        assert len(items) > 0
        return items

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[ModelNode]) -> list[ModelNode]:
        items = sample(filtered_items, k=len(filtered_items) // 2)
        for item in items:
            item.tags.append("required_tag")

        return items

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> ModelContract:
        conditions = [
            c_properties.NameCondition(include=["model1", "model2"]),
            c_properties.TagCondition(tags=["include"])
        ]
        terms = [
            t_properties.HasRequiredTags(tags="required_tag"),
        ]
        return ModelContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def child_conditions(self) -> MutableSequence[ContractCondition]:
        return [
            c_properties.NameCondition(include=["col1", "col2"]),
            c_properties.TagCondition(tags=["valid"])
        ]

    @pytest.fixture(scope="class")
    def child_terms(self) -> MutableSequence[ContractTerm]:
        return [
            t_column.HasDataType(min_count=3),
        ]

    @pytest.fixture(scope="class")
    def child_generator(self) -> ModelPropertiesGenerator:
        return ModelPropertiesGenerator()


class TestSourceContract(ParentContractTester[ColumnInfo, SourceDefinition, SourcePropertiesGenerator]):
    @pytest.fixture(scope="class")
    def items(self, sources: list[SourceDefinition], manifest: Manifest) -> list[SourceDefinition]:
        return [source for source in sources if source.package_name == manifest.metadata.project_name]

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[SourceDefinition]) -> list[SourceDefinition]:
        assert len(items) > 3

        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("source1", "source2"))
            item.tags.append("include")

        assert len(items) > 0
        return items

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[SourceDefinition]) -> list[SourceDefinition]:
        items = [item for item in filtered_items if bool(item.loader)]
        items = sample(items, k=len(items) // 2)
        for item in items:
            item.tags.append("required_tag")

        return items

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> SourceContract:
        conditions = [
            c_properties.NameCondition(include=["source1", "source2"]),
            c_properties.TagCondition(tags=["include"])
        ]
        terms = [
            t_properties.HasRequiredTags(tags="required_tag"),
            t_source.HasLoader(),
        ]
        return SourceContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def child_conditions(self) -> MutableSequence[ContractCondition]:
        return [
            c_properties.NameCondition(include=["col1", "col2"]),
            c_properties.TagCondition(tags=["valid"])
        ]

    @pytest.fixture(scope="class")
    def child_terms(self) -> MutableSequence[ContractTerm]:
        return [
            t_column.HasDataType(min_count=3),
        ]

    @pytest.fixture(scope="class")
    def child_generator(self) -> SourcePropertiesGenerator:
        return SourcePropertiesGenerator()


class TestColumnContract(ChildContractTester[ColumnInfo, ModelNode, ColumnPropertiesGenerator]):
    # noinspection PyTestUnpassedFixture
    @pytest.fixture(scope="class")
    def items(self, parent: ModelContract, **__) -> list[tuple[ColumnInfo, ModelNode]]:
        parent_items = list(parent.filtered_items)
        assert parent_items
        return [(col, item) for item in parent_items for col in item.columns.values()]

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[tuple[ColumnInfo, ModelNode]]) -> list[tuple[ColumnInfo, ModelNode]]:
        assert len(items) > 3

        columns = list({col.name: col for col, _ in items}.values())
        columns = sample(columns, k=len(columns) // 3)
        for col in columns:
            col.tags.append("valid")

        assert len(columns) > 0
        return [(col, parent) for col, parent in items if "valid" in col.tags]

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[tuple[ColumnInfo, ModelNode]]) -> list[tuple[ColumnInfo, ModelNode]]:
        return [
            (col, parent) for col, parent in filtered_items if bool(col.data_type)
        ]

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: ModelContract) -> ColumnContract:
        conditions = [
            c_properties.TagCondition(tags=["valid"])
        ]
        terms = [
            t_column.HasDataType(),
        ]
        return ColumnContract(parent=parent, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> ModelContract:
        return ModelContract(manifest=manifest, catalog=catalog)


class TestMacroContract(ParentContractTester[MacroArgument, Macro, None]):
    @pytest.fixture(scope="class")
    def items(self, macros: list[Macro], manifest: Manifest) -> list[Macro]:
        return [macro for macro in macros if macro.package_name == manifest.metadata.project_name]

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[Macro]) -> list[Macro]:
        assert len(items) > 3

        items = sample(items, k=len(items) // 3)
        for item in items:
            item.name = choice(("macro1", "macro2"))

        assert len(items) > 0
        return items

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[Macro]) -> list[Macro]:
        return [
            item for item in filtered_items if bool(item.description)
        ]

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        conditions = [
            c_properties.NameCondition(include=["macro1", "macro2"]),
        ]
        terms = [
            t_properties.HasDescription(),
        ]
        return MacroContract(manifest=manifest, catalog=catalog, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def child_conditions(self) -> MutableSequence[ContractCondition]:
        return [
            c_properties.NameCondition(include=["arg1", "arg2"]),
        ]

    @pytest.fixture(scope="class")
    def child_terms(self) -> MutableSequence[ContractTerm]:
        return [
            t_macro.HasType(),
        ]


class TestMacroArgumentContract(ChildContractTester[MacroArgument, Macro, None]):
    # noinspection PyTestUnpassedFixture
    @pytest.fixture(scope="class")
    def items(self, parent: MacroContract, **__) -> list[tuple[MacroArgument, Macro]]:
        parent_items = list(parent.filtered_items)
        assert parent_items
        return [(arg, item) for item in parent_items for arg in item.arguments]

    @pytest.fixture(scope="class")
    def filtered_items(self, items: list[tuple[MacroArgument, Macro]]) -> list[tuple[MacroArgument, Macro]]:
        assert len(items) > 3

        arguments = list({arg.name: arg for arg, _ in items}.values())
        arguments = sample(arguments, k=len(arguments) // 3)
        for arg in arguments:
            arg.name = choice(("arg1", "arg2"))

        assert len(arguments) > 0
        return [(arg, parent) for arg, parent in items if arg.name in ("arg1", "arg2")]

    @pytest.fixture(scope="class")
    def valid_items(self, filtered_items: list[tuple[MacroArgument, Macro]]) -> list[tuple[MacroArgument, Macro]]:
        return [
            (arg, parent) for arg, parent in filtered_items if bool(arg.type)
        ]

    @pytest.fixture
    def contract(self, manifest: Manifest, catalog: CatalogArtifact, parent: MacroContract) -> MacroArgumentContract:
        conditions = [
            c_properties.NameCondition(include=["arg1", "arg2"]),
        ]
        terms = [
            t_macro.HasType(),
        ]
        return MacroArgumentContract(parent=parent, conditions=conditions, terms=terms)

    @pytest.fixture(scope="class")
    def parent(self, manifest: Manifest, catalog: CatalogArtifact) -> MacroContract:
        return MacroContract(manifest=manifest, catalog=catalog)
