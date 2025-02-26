from random import choice

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.artifacts.resources import FileHash
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.artifacts.resources.v1.macro import MacroArgument
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition

from faker import Faker

from dbt_contracts.contracts.validators import NameValidator, PathValidator, TagValidator, MetaValidator, \
    IsEnabledValidator, IsMaterializedValidator


class TestValidators:
    @pytest.fixture
    def node(self, faker: Faker) -> ModelNode:
        return ModelNode(
            name="_".join(faker.words()),
            path=faker.file_path(),
            original_file_path=faker.file_path(),
            package_name=faker.word(),
            unique_id=faker.uuid4(),
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
    def source(self, faker: Faker) -> SourceDefinition:
        return SourceDefinition(
            name="_".join(faker.words()),
            path=faker.file_path(),
            original_file_path=faker.file_path(),
            package_name=faker.word(),
            unique_id=faker.uuid4(),
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
    def column(self, faker: Faker) -> ColumnInfo:
        return ColumnInfo(
            name="_".join(faker.words()),
            tags=faker.words(),
            meta={key: faker.word() for key in faker.words()},
        )

    @pytest.fixture
    def argument(self, faker: Faker) -> MacroArgument:
        return MacroArgument(
            name="_".join(faker.words()),
        )

    @pytest.mark.parametrize("item", ["node", "column", "argument"])
    def test_name_validation(self, item: str, faker: Faker, request: FixtureRequest):
        item = request.getfixturevalue(item)

        assert NameValidator().validate(item)
        assert NameValidator(include=faker.words() + [item.name]).validate(item)
        assert not NameValidator(exclude=faker.words() + [item.name]).validate(item)

    def test_path_validation(self, node: ModelNode, faker: Faker):
        paths = [faker.file_path() for _ in range(5)]

        assert PathValidator().validate(node)
        assert PathValidator(include=paths + [node.path]).validate(node)
        assert not PathValidator(exclude=paths + [node.patch_path.split("://")[1]]).validate(node)

    @pytest.mark.parametrize("item", ["node", "column"])
    def test_tag_validation(self, item: str, faker: Faker, request: FixtureRequest):
        item = request.getfixturevalue(item)

        assert TagValidator().validate(item)
        assert not TagValidator(tags=faker.words()).validate(item)
        assert TagValidator(tags=faker.words() + [choice(item.tags)]).validate(item)

    @pytest.mark.parametrize("item", ["node", "column"])
    def test_meta_validation(self, item: str, faker: Faker, request: FixtureRequest):
        item = request.getfixturevalue(item)
        meta = {key: faker.words() for key in item.meta}

        assert MetaValidator().validate(item)
        assert not MetaValidator(meta=meta).validate(item)

        key, value = choice(list(item.meta.items()))
        meta[key].append(value)
        assert MetaValidator(meta=meta).validate(item)

    def test_is_materialized_validation(self, node: ModelNode, faker: Faker):
        node.config.materialized = "view"
        assert IsMaterializedValidator().validate(node)

        node.config.materialized = "ephemeral"
        assert not IsMaterializedValidator().validate(node)

    def test_is_enabled_validation(self, source: SourceDefinition, faker: Faker):
        source.config.enabled = True
        assert IsEnabledValidator().validate(source)

        source.config.enabled = False
        assert not IsEnabledValidator().validate(source)
