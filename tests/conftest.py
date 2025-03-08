from copy import deepcopy
from pathlib import Path
from random import choice, sample

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.artifacts.resources import FileHash, BaseResource
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import ColumnInfo, ParsedResource
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.artifacts.schemas.catalog import CatalogArtifact, CatalogMetadata
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro, TestNode, GenericTestNode, CompiledNode
from dbt_common.contracts.metadata import CatalogTable, TableMetadata, ColumnMetadata
from faker import Faker

from dbt_contracts.contracts import ContractContext


@pytest.fixture(scope="session")
def faker() -> Faker:
    """Sets up and yields a basic Faker object for fake data"""
    return Faker()


@pytest.fixture(scope="session")
def project_name(faker: Faker) -> str:
    """Fixture for the name to assign to the test project."""
    return faker.word()


@pytest.fixture(scope="session")
def context(manifest: Manifest, catalog: CatalogArtifact) -> ContractContext:
    """Fixture for a ContractContext object."""
    return ContractContext(manifest=manifest, catalog=catalog)


@pytest.fixture(scope="session")
def manifest(
        models: list[ModelNode],
        sources: list[SourceDefinition],
        macros: list[Macro],
        tests: list[TestNode],
        project_name: str,
) -> Manifest:
    """Fixture for a Manifest object."""
    manifest = Manifest()
    manifest.nodes |= {model.unique_id: model for model in models}
    manifest.nodes |= {test.unique_id: test for test in tests}
    manifest.sources |= {source.unique_id: source for source in sources}
    manifest.macros |= {macro.unique_id: macro for macro in macros}
    manifest.metadata.project_name = project_name
    return manifest


@pytest.fixture(scope="session")
def catalog(models: list[ModelNode], sources: list[SourceDefinition]) -> CatalogArtifact:
    """Fixture for a CatalogArtifact object."""
    def _generate_catalog_table(node: ParsedResource | SourceDefinition) -> CatalogTable:
        metadata = TableMetadata(type="table", schema=node.schema, name=node.name, comment=node.description)
        columns = {
            column.name: ColumnMetadata(type=column.data_type, index=idx, name=column.name, comment=column.description)
            for idx, column in enumerate(node.columns.values())
        }
        return CatalogTable(metadata=metadata, columns=columns, stats={})

    return CatalogArtifact(
        metadata=CatalogMetadata(),
        nodes={model.unique_id: _generate_catalog_table(model) for model in models},
        sources={source.unique_id: _generate_catalog_table(source) for source in sources},
    )


@pytest.fixture
def simple_resource(faker: Faker, project_name: str) -> BaseResource:
    """Fixture for a simple BaseResource object."""
    path = faker.file_path(extension=choice(("yml", "yaml", "py")), absolute=False)
    return BaseResource(
        name="_".join(faker.words()),
        path=path,
        original_file_path=str(Path("models", path)),
        package_name=project_name,
        unique_id=faker.uuid4(str),
        resource_type=NodeType.Model,
    )


@pytest.fixture(params=["model", "source"])
def node(request: FixtureRequest) -> CompiledNode:
    """Fixture for a CompiledNode object."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def node_column(node: CompiledNode) -> ColumnInfo:
    """Fixture for a ColumnInfo object."""
    return choice(list(node.columns.values()))


@pytest.fixture
def catalog_table(node: CompiledNode, catalog: CatalogArtifact) -> CatalogTable:
    """Fixture for the CatalogTable object matching the current node fixture."""
    if isinstance(node, SourceDefinition):
        return catalog.sources[node.unique_id]
    return catalog.nodes[node.unique_id]


@pytest.fixture
def catalog_column(catalog_table: CatalogTable, node_column: ColumnInfo, catalog: CatalogArtifact) -> ColumnMetadata:
    """Fixture for the ColumnMetadata object matching the current node column fixture."""
    return catalog_table.columns[node_column.name]


@pytest.fixture(scope="session")
def models(faker: Faker, columns: list[ColumnInfo], project_name: str) -> list[ModelNode]:
    """Fixture for the ModelNodes that can be found in the manifest."""
    def _generate() -> ModelNode:
        path = faker.file_path(extension=choice(("sql", "py")), absolute=False)
        return ModelNode(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("models", path)),
            package_name=choice([project_name, faker.word()]),
            unique_id=".".join(("models", *Path(path).with_suffix("").parts)),
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
            description=faker.sentence(),
        )

    return [_generate() for _ in range(faker.random_int(20, 30))]


@pytest.fixture
def model(models: list[ModelNode], column: ColumnInfo) -> ModelNode:
    """Fixture for a single ModelNode that can be found in the manifest."""
    return deepcopy(choice(models))


@pytest.fixture(scope="session")
def sources(faker: Faker, columns: list[ColumnInfo], project_name: str) -> list[SourceDefinition]:
    """Fixture for the SourceDefinitions that can be found in the manifest."""
    def _generate() -> SourceDefinition:
        path = faker.file_path(extension=choice(("yml", "yaml")), absolute=False)
        return SourceDefinition(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("models", path)),
            package_name=choice([project_name, faker.word()]),
            unique_id=".".join(("source", *Path(path).with_suffix("").parts)),
            resource_type=NodeType.Source,
            fqn=faker.words(3),
            database=faker.word(),
            schema=faker.word(),
            identifier=faker.word(),
            source_name=faker.word(),
            source_description=faker.sentence(),
            loader=choice(("", faker.word())),
            columns={column.name: column for column in sample(columns, k=faker.random_int(3, 8))},
            description=faker.sentence(),
        )

    return [_generate() for _ in range(faker.random_int(20, 30))]


@pytest.fixture
def source(sources: list[SourceDefinition]) -> SourceDefinition:
    """Fixture for a single SourceDefinition that can be found in the manifest."""
    return deepcopy(choice(sources))


@pytest.fixture(scope="session")
def columns(faker: Faker) -> list[ColumnInfo]:
    """Fixture for the ColumnInfos that can be found in the manifest."""
    def _generate():
        data_types = (None, "varchar", "int", "timestamp", "boolean")

        return ColumnInfo(
            name="_".join(faker.words()),
            data_type=choice(data_types),
            tags=faker.words(),
            meta={key: faker.word() for key in faker.words()},
            description=faker.sentence(),
        )

    return [_generate() for _ in range(faker.random_int(40, 50))]


@pytest.fixture
def column(columns: list[ColumnInfo]) -> ColumnInfo:
    """Fixture for a single ColumnInfo that can be found in the manifest."""
    return deepcopy(choice([col for col in columns if col.name not in ("col1", "col2", "col3")]))


@pytest.fixture(scope="session")
def tests(models: list[ModelNode], sources: list[SourceDefinition], project_name: str, faker: Faker) -> list[TestNode]:
    """Fixture for the TestNodes that can be found in the manifest."""
    def _generate(item: BaseResource, column: ColumnInfo = None) -> TestNode:
        path = faker.file_path(extension=choice(("yml", "yaml", "py")), absolute=False)
        test = GenericTestNode(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("tests", path)),
            package_name=choice([project_name, faker.word()]),
            unique_id=".".join(("test", *Path(path).with_suffix("").parts)),
            resource_type=NodeType.Test,
            attached_node=item.unique_id,
            alias=faker.word(),
            fqn=faker.words(3),
            checksum=FileHash(name=faker.word(), checksum="".join(faker.random_letters())),
            database=faker.word(),
            schema=faker.word(),
        )
        if column is not None:
            test.column_name = column.name

        return test

    return [
        _generate(item)
        for item in models + sources for _ in range(faker.random_int(1, 5))
    ] + [
        _generate(item, column=column)
        for item in models + sources for _ in range(faker.random_int(1, 5))
        for column in item.columns.values()
    ]


@pytest.fixture(scope="session")
def macros(faker: Faker, arguments: list[MacroArgument], project_name: str) -> list[Macro]:
    """Fixture for the Macros that can be found in the manifest."""
    def _generate() -> Macro:
        path = faker.file_path(extension="sql", absolute=False)
        return Macro(
            name="_".join(faker.words()),
            path=path,
            original_file_path=str(Path("macros", path)),
            package_name=choice([project_name, faker.word()]),
            resource_type=NodeType.Macro,
            unique_id=".".join(("macro", *Path(path).with_suffix("").parts)),
            macro_sql="SELECT * FROM table",
            arguments=sample(arguments, k=faker.random_int(3, 8)),
        )

    return [_generate() for _ in range(faker.random_int(30, 40))]


@pytest.fixture
def macro(macros: list[Macro]) -> Macro:
    """Fixture for a single Macro that can be found in the manifest."""
    return deepcopy(choice(macros))


@pytest.fixture(scope="session")
def arguments(faker: Faker) -> list[MacroArgument]:
    """Fixture for the MacroArguments that can be found in the manifest."""
    def _generate() -> MacroArgument:
        return MacroArgument(
            name="_".join(faker.words()),
        )

    return [_generate() for _ in range(faker.random_int(40, 50))]


@pytest.fixture
def argument(arguments: list[MacroArgument]) -> MacroArgument:
    """Fixture for a single MacroArguments that can be found in the manifest."""
    return deepcopy(choice([arg for arg in arguments if arg.name not in ("arg1", "arg2", "arg3")]))
