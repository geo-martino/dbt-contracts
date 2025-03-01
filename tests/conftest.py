from copy import deepcopy
from pathlib import Path
from random import choice, sample

import pytest
from dbt.artifacts.resources import FileHash, BaseResource
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact, CatalogMetadata
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro, TestNode, GenericTestNode
from dbt_common.contracts.metadata import CatalogTable, TableMetadata, ColumnMetadata

from faker import Faker

from dbt_contracts.contracts import ContractContext


@pytest.fixture(scope="session")
def faker() -> Faker:
    """Sets up and yields a basic Faker object for fake data"""
    return Faker()


@pytest.fixture(scope="session")
def context(manifest: Manifest, catalog: CatalogArtifact) -> ContractContext:
    return ContractContext(manifest=manifest, catalog=catalog)


@pytest.fixture(scope="session")
def manifest(
        models: list[ModelNode],
        sources: list[SourceDefinition],
        macros: list[Macro],
        tests: list[TestNode]
) -> Manifest:
    manifest = Manifest()
    manifest.nodes |= {model.unique_id: model for model in models}
    manifest.nodes |= {test.unique_id: test for test in tests}
    manifest.sources |= {source.unique_id: source for source in sources}
    manifest.macros |= {macro.unique_id: macro for macro in macros}
    return manifest


@pytest.fixture(scope="session")
def catalog(models: list[ModelNode], sources: list[SourceDefinition]) -> CatalogArtifact:
    def _generate_catalog_table(node: ParsedResource | SourceDefinition) -> CatalogTable:
        data_types = ("varchar", "int", "timestamp", "boolean")

        metadata = TableMetadata(type="table", schema=node.schema, name=node.name)
        columns = {
            column.name: ColumnMetadata(type=choice(data_types), index=idx, name=column.name)
            for idx, column in enumerate(node.columns.values())
        }
        return CatalogTable(metadata=metadata, columns=columns, stats={})

    return CatalogArtifact(
        metadata=CatalogMetadata(),
        nodes={model.unique_id: _generate_catalog_table(model) for model in models},
        sources={source.unique_id: _generate_catalog_table(source) for source in sources},
    )


@pytest.fixture
def simple_resource(faker: Faker) -> BaseResource:
    path = faker.file_path(extension=choice(("yml", "yaml", "py")), absolute=False)
    return BaseResource(
        name="_".join(faker.words()),
        path=path,
        original_file_path=str(Path("models", path)),
        package_name=faker.word(),
        unique_id=faker.uuid4(str),
        resource_type=NodeType.Model,
    )


@pytest.fixture(scope="session")
def models(faker: Faker, columns: list[ColumnInfo]) -> list[ModelNode]:
    def _generate() -> ModelNode:
        path = faker.file_path(extension=choice(("sql", "py")), absolute=False)
        return ModelNode(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("models", path)),
            package_name=faker.word(),
            unique_id=".".join(("models", *Path(path).parts)),
            resource_type=NodeType.Model,
            alias=faker.word(),
            fqn=faker.words(3),
            checksum=FileHash(name=faker.word(), checksum="".join(faker.random_letters())),
            database=faker.word(),
            schema=faker.word(),
            patch_path=f"{faker.word()}://{faker.file_path(extension=choice(("yml", "yaml")), absolute=False)}",
            tags=faker.words(),
            meta={key: faker.word() for key in faker.words()},
            columns={column.name: column for column in sample(columns, k=faker.random_int(3, 8))},
        )

    return [_generate() for _ in range(faker.random_int(10, 20))]


@pytest.fixture
def model(models: list[ModelNode], column: ColumnInfo) -> ModelNode:
    model = deepcopy(choice(models))
    model.columns[column.name] = column
    return model


@pytest.fixture(scope="session")
def sources(faker: Faker, columns: list[ColumnInfo]) -> list[SourceDefinition]:
    def _generate() -> SourceDefinition:
        path = faker.file_path(extension=choice(("yml", "yaml")), absolute=False)
        return SourceDefinition(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("models", path)),
            package_name=faker.word(),
            unique_id=".".join(("source", *Path(path).parts)),
            resource_type=NodeType.Source,
            fqn=faker.words(3),
            database=faker.word(),
            schema=faker.word(),
            identifier=faker.word(),
            source_name=faker.word(),
            source_description=faker.sentence(),
            loader=faker.word(),
            columns={column.name: column for column in sample(columns, k=faker.random_int(3, 8))},
        )

    return [_generate() for _ in range(faker.random_int(10, 20))]


@pytest.fixture
def source(sources: list[SourceDefinition], column: ColumnInfo) -> SourceDefinition:
    source = deepcopy(choice(sources))
    source.columns[column.name] = column
    return source


@pytest.fixture(scope="session")
def columns(faker: Faker) -> list[ColumnInfo]:
    def generate():
        data_types = (None, "varchar", "int", "timestamp", "boolean")

        return ColumnInfo(
            name="_".join(faker.words()),
            data_type=choice(data_types),
            tags=faker.words(),
            meta={key: faker.word() for key in faker.words()},
        )

    return [generate() for _ in range(faker.random_int(20, 30))]


@pytest.fixture
def column(columns: list[ColumnInfo]) -> ColumnInfo:
    return deepcopy(choice(columns))


@pytest.fixture(scope="session")
def tests(
        models: list[ModelNode],
        sources: list[SourceDefinition],
        columns: list[ColumnInfo],
        faker: Faker
) -> list[TestNode]:
    def generate(item: BaseResource, column: ColumnInfo = None) -> TestNode:
        path = faker.file_path(extension=choice(("yml", "yaml", "py")), absolute=False)
        test = GenericTestNode(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("tests", path)),
            package_name=faker.word(),
            unique_id=".".join(("test", *Path(path).parts)),
            resource_type=NodeType.Test,
            attached_node=item.unique_id,
            alias=faker.word(),
            fqn=faker.words(3),
            checksum=FileHash(name=faker.word(), checksum="".join(faker.random_letters())),
            database=faker.word(),
            schema=faker.word(),
        )
        if column is not None:
            test.column_name = item.name

        return test

    return [
        generate(item)
        for item in models + sources for _ in range(faker.random_int(1, 5))
    ] + [
        generate(item, column=column)
        for item in models + sources for _ in range(faker.random_int(1, 5))
        for column in item.columns.values()
    ]


@pytest.fixture(scope="session")
def macros(faker: Faker, arguments: list[MacroArgument]) -> list[Macro]:
    def generate() -> Macro:
        path = faker.file_path(extension="sql", absolute=False)
        return Macro(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("macros", path)),
            package_name=faker.word(),
            resource_type=NodeType.Macro,
            unique_id=".".join(("macro", *Path(path).parts)),
            macro_sql="SELECT * FROM table",
            arguments=sample(arguments, k=faker.random_int(3, 8)),
        )

    return [generate() for _ in range(faker.random_int(10, 20))]


@pytest.fixture
def macro(macros: list[Macro]) -> Macro:
    return deepcopy(choice(macros))


@pytest.fixture(scope="session")
def arguments(faker: Faker) -> list[MacroArgument]:
    def generate() -> MacroArgument:
        return MacroArgument(
            name="_".join(faker.words()),
        )

    return [generate() for _ in range(faker.random_int(20, 30))]


@pytest.fixture
def argument(arguments: list[MacroArgument]) -> MacroArgument:
    return deepcopy(choice(arguments))
