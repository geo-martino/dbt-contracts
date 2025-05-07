from pathlib import Path
from random import choice

import pytest
from _pytest.fixtures import FixtureRequest
from dbt.contracts.graph.nodes import ModelNode
from faker import Faker

from dbt_contracts.contracts.conditions.properties import NameCondition, PathCondition, TagCondition, MetaCondition, \
    IsMaterializedCondition


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
    assert not PathCondition(
        include=paths + [model.path], exclude=paths + [model.patch_path.split("://")[1]]
    ).run(model)


def test_path_condition_escapes_backslashes_in_paths():
    paths = ["path\\to\\folder1", "path\\to\\file1"]
    expected = [path.replace("\\", "\\\\") for path in paths]
    assert PathCondition.escape_backslashes_in_windows_paths(paths) == expected


def test_path_condition_unifies_chunked_path_values():
    # no remapping for these values
    assert PathCondition.unify_chunked_path_values("path") == ("path",)
    assert PathCondition.unify_chunked_path_values(["path1", "path2"]) == ("path1", "path2")
    assert PathCondition.unify_chunked_path_values([["path1"], ["path2"]]) == ("path1", "path2")

    paths = [["path", "to", "folder1"]]
    assert PathCondition.unify_chunked_path_values(paths) == (str(Path(*paths[0])),)

    paths = [["path", "to", "folder1"], ["path", "to", "folder2"], "path/to/folder3"]
    expected = tuple(str(Path(*parts)) if not isinstance(parts, str) else parts for parts in paths)
    assert PathCondition.unify_chunked_path_values(paths) == expected

    condition = PathCondition(include=paths, exclude=paths)
    assert condition.include == expected
    assert condition.exclude == expected


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
