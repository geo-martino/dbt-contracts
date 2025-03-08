from random import sample
from unittest import mock

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.contracts.graph.nodes import SourceDefinition
from faker import Faker

from dbt_contracts.contracts import ContractContext
from dbt_contracts.contracts.terms.properties import HasProperties, HasDescription, HasRequiredTags, HasAllowedTags, \
    HasRequiredMetaKeys, HasAllowedMetaKeys, HasAllowedMetaValues
from dbt_contracts.types import PropertiesT, DescriptionT, TagT, MetaT


@pytest.mark.parametrize("item", ["model", "source", "macro"])
def test_has_properties(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: PropertiesT = request.getfixturevalue(item)

    item.patch_path = faker.file_path()
    assert HasProperties().run(item, context=context)

    item.patch_path = None
    if isinstance(item, SourceDefinition):  # always returns true for sources
        assert HasProperties().run(item, context=context)
    else:
        with mock.patch.object(ContractContext, "add_result") as mock_add_result:
            assert not HasProperties().run(item, context=context)
            mock_add_result.assert_called_once()


@pytest.mark.parametrize("item", ["model", "source", "column", "macro", "argument"])
def test_has_description(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: DescriptionT = request.getfixturevalue(item)

    item.description = faker.sentence()
    assert HasDescription().run(item, context=context)

    item.description = ""
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasDescription().run(item, context=context)
        mock_add_result.assert_called_once()


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_required_tags(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: TagT = request.getfixturevalue(item)

    item.tags = faker.words(10)
    assert HasRequiredTags(tags=sample(item.tags, k=5)).run(item, context=context)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasRequiredTags(tags=faker.words(10) + sample(item.tags, k=5)).run(item, context=context)
        mock_add_result.assert_called_once()

    item.tags.clear()
    assert HasRequiredTags().run(item, context=context)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasRequiredTags(tags=faker.words(10)).run(item, context=context)
        mock_add_result.assert_called_once()


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_allowed_tags(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: TagT = request.getfixturevalue(item)

    item.tags = faker.words(10)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasAllowedTags(tags=sample(item.tags, k=5)).run(item, context=context)
        mock_add_result.assert_called_once()

        assert not HasAllowedTags(tags=faker.words(10)).run(item, context=context)
        assert len(mock_add_result.mock_calls) == 2

    assert HasAllowedTags(tags=faker.words(10) + item.tags).run(item, context=context)

    item.tags.clear()
    assert HasAllowedTags().run(item, context=context)
    assert HasAllowedTags(tags=faker.words(10)).run(item, context=context)


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_required_meta_keys(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: MetaT = request.getfixturevalue(item)

    item.meta = {key: faker.word() for key in faker.words(10)}
    assert HasRequiredMetaKeys(keys=sample(list(item.meta), k=5)).run(item, context=context)
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasRequiredMetaKeys(keys=faker.words(10) + sample(list(item.meta), k=5)).run(item, context=context)
        mock_add_result.assert_called_once()

    item.meta.clear()
    assert HasRequiredMetaKeys().run(item, context=context)

    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasRequiredMetaKeys(keys=faker.words(10)).run(item, context=context)
        mock_add_result.assert_called_once()


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_allowed_meta_keys(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: MetaT = request.getfixturevalue(item)

    item.meta = {key: faker.word() for key in faker.words(10)}
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasAllowedMetaKeys(keys=sample(list(item.meta), k=5)).run(item, context=context)
        mock_add_result.assert_called_once()

        assert not HasAllowedMetaKeys(keys=faker.words(10)).run(item, context=context)
        assert len(mock_add_result.mock_calls) == 2

    assert HasAllowedMetaKeys(keys=faker.words(10) + list(item.meta)).run(item, context=context)

    item.meta.clear()
    assert HasAllowedMetaKeys().run(item, context=context)
    assert HasAllowedMetaKeys(keys=faker.words(10)).run(item, context=context)


@pytest.mark.parametrize("item", ["model", "column"])
def test_has_allowed_meta_values(item: str, context: ContractContext, faker: Faker, request: FixtureRequest):
    item: MetaT = request.getfixturevalue(item)

    item.meta = {key: faker.word() for key in faker.words(10)}
    allowed_meta_values = {key: faker.words(5) for key, val in sample(list(item.meta.items()), k=5)}
    with mock.patch.object(ContractContext, "add_result") as mock_add_result:
        assert not HasAllowedMetaValues(meta=allowed_meta_values).run(item, context=context)
        mock_add_result.assert_called_once()

    allowed_meta_values = {key: faker.words(5) + [val] for key, val in sample(list(item.meta.items()), k=5)}
    assert HasAllowedMetaValues(meta=allowed_meta_values).run(item, context=context)

    allowed_meta_values = {key: faker.words(3) for key in faker.words(10) if key not in item.meta}
    assert HasAllowedMetaValues(meta=allowed_meta_values).run(item, context=context)

    item.meta.clear()
    assert HasAllowedMetaValues().run(item, context=context)
    assert HasAllowedMetaValues(meta=allowed_meta_values).run(item, context=context)
