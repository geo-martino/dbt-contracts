from random import sample

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.contracts.graph.nodes import SourceDefinition
from faker import Faker

# noinspection PyProtectedMember
from dbt_contracts.contracts.terms._properties import HasProperties, HasDescription, HasRequiredTags, HasAllowedTags, \
    HasRequiredMetaKeys, HasAllowedMetaKeys, HasAllowedMetaValues
from dbt_contracts.types import PropertiesT, DescriptionT, TagT, MetaT


@pytest.mark.parametrize("item", ["model", "source", "macro"])
def test_has_properties(item: str, faker: Faker, request: FixtureRequest):
    item: PropertiesT = request.getfixturevalue(item)

    item.patch_path = faker.file_path()
    assert HasProperties().run(item)

    item.patch_path = None
    if isinstance(item, SourceDefinition):  # always returns true for sources
        assert HasProperties().run(item)
    else:
        assert not HasProperties().run(item)


@pytest.mark.parametrize("item", ["model", "source", "column", "macro", "argument"])
def test_has_description(item: str, faker: Faker, request: FixtureRequest):
    item: DescriptionT = request.getfixturevalue(item)

    item.description = faker.sentence()
    assert HasDescription().run(item)

    item.description = ""
    assert not HasDescription().run(item)


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_required_tags(item: str, faker: Faker, request: FixtureRequest):
    item: TagT = request.getfixturevalue(item)

    item.tags = faker.words(10)
    assert HasRequiredTags(tags=sample(item.tags, k=5)).run(item)
    assert not HasRequiredTags(tags=faker.words(10) + sample(item.tags, k=5)).run(item)

    item.tags.clear()
    assert HasRequiredTags().run(item)
    assert not HasRequiredTags(tags=faker.words(10)).run(item)


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_allowed_tags(item: str, faker: Faker, request: FixtureRequest):
    item: TagT = request.getfixturevalue(item)

    item.tags = faker.words(10)
    assert not HasAllowedTags(tags=sample(item.tags, k=5)).run(item)
    assert not HasAllowedTags(tags=faker.words(10)).run(item)
    assert HasAllowedTags(tags=faker.words(10) + item.tags).run(item)

    item.tags.clear()
    assert HasAllowedTags().run(item)
    assert HasAllowedTags(tags=faker.words(10)).run(item)


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_required_meta_keys(item: str, faker: Faker, request: FixtureRequest):
    item: MetaT = request.getfixturevalue(item)

    item.meta = {key: faker.word() for key in faker.words(10)}
    assert HasRequiredMetaKeys(keys=sample(list(item.meta), k=5)).run(item)
    assert not HasRequiredMetaKeys(keys=faker.words(10) + sample(list(item.meta), k=5)).run(item)

    item.meta.clear()
    assert HasRequiredMetaKeys().run(item)
    assert not HasRequiredMetaKeys(keys=faker.words(10)).run(item)


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_allowed_meta_keys(item: str, faker: Faker, request: FixtureRequest):
    item: MetaT = request.getfixturevalue(item)

    item.meta = {key: faker.word() for key in faker.words(10)}
    assert not HasAllowedMetaKeys(keys=sample(list(item.meta), k=5)).run(item)
    assert not HasAllowedMetaKeys(keys=faker.words(10)).run(item)
    assert HasAllowedMetaKeys(keys=faker.words(10) + list(item.meta)).run(item)

    item.meta.clear()
    assert HasAllowedMetaKeys().run(item)
    assert HasAllowedMetaKeys(keys=faker.words(10)).run(item)


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_allowed_meta_values(item: str, faker: Faker, request: FixtureRequest):
    item: MetaT = request.getfixturevalue(item)

    item.meta = {key: faker.word() for key in faker.words(10)}

    allowed_meta_values = {key: faker.words() for key, val in sample(list(item.meta.items()), k=5)}
    assert not HasAllowedMetaValues(meta=allowed_meta_values).run(item)

    allowed_meta_values = {key: faker.words() + [val] for key, val in sample(list(item.meta.items()), k=5)}
    assert HasAllowedMetaValues(meta=allowed_meta_values).run(item)

    allowed_meta_values = {key: faker.words() for key in faker.words()}
    assert HasAllowedMetaValues(meta=allowed_meta_values).run(item)

    item.meta.clear()
    assert HasAllowedMetaValues().run(item)
    assert HasAllowedMetaValues(meta=allowed_meta_values).run(item)
