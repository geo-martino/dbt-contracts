import os
from pathlib import Path
from random import choice
from unittest import mock

import pytest
from dbt.artifacts.resources import BaseResource
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.contracts.graph.nodes import CompiledNode, ModelNode
from dbt.flags import GLOBAL_FLAGS

from dbt_contracts.contracts.utils import get_matching_catalog_table, get_absolute_project_path


def test_get_matching_catalog_table(node: CompiledNode, simple_resource: BaseResource, catalog: CatalogArtifact):
    table = get_matching_catalog_table(item=node, catalog=catalog)
    assert table is not None
    # noinspection PyTypeChecker
    assert get_matching_catalog_table(item=simple_resource, catalog=catalog) is None


@pytest.fixture
def relative_path(model: ModelNode, tmp_path: Path) -> Path:
    """Fixture to generate a relative path for testing."""
    paths = [model.original_file_path, model.path, model.patch_path]
    path = choice([path for path in paths if path is not None])

    expected = tmp_path.joinpath(path)
    expected.parent.mkdir(parents=True, exist_ok=True)
    expected.touch()

    return Path(path)


def test_get_absolute_patch_path_in_project_dir(relative_path: Path, tmp_path: Path):
    GLOBAL_FLAGS.PROJECT_DIR = tmp_path
    expected = tmp_path.joinpath(relative_path)
    assert get_absolute_project_path(relative_path) == expected


def test_get_absolute_patch_path_in_cwd(relative_path: Path, tmp_path: Path):
    expected = tmp_path.joinpath(relative_path)
    # noinspection SpellCheckingInspection
    with mock.patch.object(os, "getcwd", return_value=str(tmp_path)):
        assert get_absolute_project_path(relative_path) == expected
