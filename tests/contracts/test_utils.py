import os
from copy import deepcopy
from pathlib import Path
from random import choice
from unittest import mock

import pytest
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.nodes import CompiledNode, ModelNode
from dbt.flags import GLOBAL_FLAGS

from dbt_contracts.contracts.utils import get_matching_catalog_table, get_absolute_project_path, merge_maps


def tests_merge_maps():
    source = {
        1: "value",
        2: {"a": "val a", "b": "val b", "c": {"nested1": "nested val"}},
        3: {"nested1": {"nested2": {"nested3": "old value"}}},
        4: {"a": [1, 2, 3]}
    }
    new = {
        2: {"b": "new value b", "c": {"nested1": "modified nested val"}},
        3: {"nested1": {"nested2": {"nested3": "new value", "new key": "new val"}}},
        4: {"a": [4, 5]}
    }

    test = deepcopy(source)
    merge_maps(source=test, new=new, extend=False, overwrite=False)
    assert test == {
        1: "value",
        2: {"a": "val a", "b": "val b", "c": {"nested1": "nested val"}},
        3: {"nested1": {"nested2": {"nested3": "old value", "new key": "new val"}}},
        4: {"a": [1, 2, 3]}
    }

    test = deepcopy(source)
    merge_maps(source=test, new=new, extend=False, overwrite=True)
    assert test == {
        1: "value",
        2: {"a": "val a", "b": "new value b", "c": {"nested1": "modified nested val"}},
        3: {"nested1": {"nested2": {"nested3": "new value", "new key": "new val"}}},
        4: {"a": [4, 5]}
    }

    test = deepcopy(source)
    merge_maps(source=test, new=new, extend=True, overwrite=False)
    assert test == {
        1: "value",
        2: {"a": "val a", "b": "val b", "c": {"nested1": "nested val"}},
        3: {"nested1": {"nested2": {"nested3": "old value", "new key": "new val"}}},
        4: {"a": [1, 2, 3, 4, 5]}
    }


def test_get_matching_catalog_table(node: CompiledNode, simple_resource: BaseResource, catalog: CatalogArtifact):
    table = get_matching_catalog_table(item=node, catalog=catalog)
    assert table is not None
    # noinspection PyTypeChecker
    assert get_matching_catalog_table(item=simple_resource, catalog=catalog) is None


@pytest.fixture
def relative_path(model: ModelNode, tmp_path: Path) -> Path:
    """Fixture to generate a relative path for testing."""
    paths = [model.original_file_path, model.path]
    if model.patch_path:
        paths.append(model.patch_path.split("://")[1])
    path = choice([path for path in paths if path is not None])

    expected = tmp_path.joinpath(path)
    expected.parent.mkdir(parents=True, exist_ok=True)
    expected.touch()

    return Path(path)


def test_get_absolute_path_in_project_dir(relative_path: Path, tmp_path: Path):
    GLOBAL_FLAGS.PROJECT_DIR = tmp_path
    expected = tmp_path.joinpath(relative_path)
    assert get_absolute_project_path(relative_path) == expected


# TODO: flakey test - fix me
def test_get_absolute_path_in_cwd(relative_path: Path, tmp_path: Path):
    expected = tmp_path.joinpath(relative_path)
    # noinspection SpellCheckingInspection
    with mock.patch.object(os, "getcwd", return_value=str(tmp_path)):
        assert get_absolute_project_path(relative_path) == expected
