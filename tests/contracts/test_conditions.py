from random import choice

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.contracts.graph.nodes import ModelNode, SourceDefinition

from faker import Faker

from dbt_contracts.contracts.conditions import NameCondition, PathCondition, TagCondition, MetaCondition, \
    IsEnabledCondition, IsMaterializedCondition


@pytest.mark.parametrize("item", ["model", "column", "argument"])
def test_name_validation(item: str, faker: Faker, request: FixtureRequest):
    item = request.getfixturevalue(item)

    assert NameCondition().run(item)
    assert NameCondition(include=faker.words() + [item.name]).run(item)
    assert not NameCondition(exclude=faker.words() + [item.name]).run(item)


def test_path_validation(model: ModelNode, faker: Faker):
    paths = [faker.file_path() for _ in range(5)]

    assert PathCondition().run(model)
    assert PathCondition(include=paths + [model.path]).run(model)
    assert not PathCondition(exclude=paths + [model.patch_path.split("://")[1]]).run(model)


@pytest.mark.parametrize("item", ["model", "column"])
def test_tag_validation(item: str, faker: Faker, request: FixtureRequest):
    item = request.getfixturevalue(item)

    assert TagCondition().run(item)
    assert not TagCondition(tags=[word for word in faker.words() if word not in item.tags]).run(item)
    assert TagCondition(tags=faker.words() + [choice(item.tags)]).run(item)


@pytest.mark.parametrize("item", ["model", "column"])
def test_meta_validation(item: str, faker: Faker, request: FixtureRequest):
    item = request.getfixturevalue(item)
    meta = {key: faker.words() for key in item.meta}

    assert MetaCondition().run(item)
    assert not MetaCondition(meta=meta).run(item)

    key, value = choice(list(item.meta.items()))
    meta[key].append(value)
    assert not MetaCondition(meta=meta).run(item)

    for key, value in item.meta.items():
        meta[key].append(value)
    assert MetaCondition(meta=meta).run(item)


def test_is_materialized_validation(model: ModelNode, faker: Faker):
    model.config.materialized = "view"
    assert IsMaterializedCondition().run(model)

    model.config.materialized = "ephemeral"
    assert not IsMaterializedCondition().run(model)


def test_is_enabled_validation(source: SourceDefinition, faker: Faker):
    source.config.enabled = True
    assert IsEnabledCondition().run(source)

    source.config.enabled = False
    assert not IsEnabledCondition().run(source)
