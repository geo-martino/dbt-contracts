import pytest
from dbt.artifacts.resources import FileHash
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition, Macro

from faker import Faker


@pytest.fixture
def model(faker: Faker) -> ModelNode:
    return ModelNode(
        name="_".join(faker.words()),
        path=faker.file_path(),
        original_file_path=faker.file_path(),
        package_name=faker.word(),
        unique_id=faker.uuid4(str),
        resource_type=NodeType.Model,
        alias=faker.word(),
        fqn=faker.words(3),
        checksum=FileHash(name=faker.word(), checksum="".join(faker.random_letters())),
        database=faker.word(),
        schema=faker.word(),
        patch_path=f"{faker.word()}://{faker.file_path().lstrip("/")}",
        tags=faker.words(),
        meta={key: faker.word() for key in faker.words()},
    )


@pytest.fixture
def source(faker: Faker) -> SourceDefinition:
    return SourceDefinition(
        name="_".join(faker.words()),
        path=faker.file_path(),
        original_file_path=faker.file_path(),
        package_name=faker.word(),
        unique_id=faker.uuid4(str),
        resource_type=NodeType.Source,
        fqn=faker.words(3),
        database=faker.word(),
        schema=faker.word(),
        identifier=faker.word(),
        source_name=faker.word(),
        source_description=faker.sentence(),
        loader=faker.word(),
    )


@pytest.fixture
def column(faker: Faker) -> ColumnInfo:
    return ColumnInfo(
        name="_".join(faker.words()),
        tags=faker.words(),
        meta={key: faker.word() for key in faker.words()},
    )


@pytest.fixture
def macro(faker: Faker) -> Macro:
    return Macro(
        name="_".join(faker.words()),
        macro_sql="SELECT * FROM table",
        original_file_path=faker.file_path(),
        path=faker.file_path(),
        package_name=faker.word(),
        resource_type=NodeType.Macro,
        unique_id=faker.uuid4(str)
    )


@pytest.fixture
def argument(faker: Faker) -> MacroArgument:
    return MacroArgument(
        name="_".join(faker.words()),
    )