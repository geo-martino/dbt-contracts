import json
from argparse import Namespace
from pathlib import Path
from unittest import mock

import pytest
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.constants import MANIFEST_FILE_NAME
from dbt.contracts.graph.manifest import Manifest
from dbt.task.docs.generate import CATALOG_FILENAME

from dbt_contracts.dbt_cli import add_default_args, load_artifact, DEFAULT_GLOBAL_ARGS, get_result, clean_paths, \
    install_dependencies, get_manifest, get_catalog


@pytest.fixture
def config(tmp_path: Path) -> Namespace:
    """Fixture for dbt config."""
    config = Namespace(
        project_root="project_root",
        args=Namespace(profiles_dir="profiles_dir"),
        profile_name="profile_name",
        target_name="target_name",
        project_target_path=str(tmp_path.joinpath("target")),
    )

    with mock.patch("dbt_contracts.dbt_cli.get_config", return_value=config):
        yield config


def test_add_default_args(config: Namespace):
    assert add_default_args("arg1", "arg2") == ["arg1", "arg2"]

    with mock.patch("dbt_contracts.dbt_cli.get_config", return_value=config):
        assert add_default_args("arg1", "arg2", "--profile", "test", config=config) == [
            "arg1",
            "arg2",
            "--profile", "test",
            "--project-dir", "project_root",
            "--profiles-dir", "profiles_dir",
            "--target", "target_name"
        ]


def test_load_artifact(config: Namespace):
    filename = "manifest.json"
    expected = {"key": "value"}

    assert load_artifact(filename, config) is None

    path = Path(config.project_target_path, filename)
    path.parent.mkdir(parents=True)

    assert load_artifact(filename, config) is None

    with path.open("w") as file:
        json.dump(expected, file)

    assert load_artifact(filename, config) == expected


def test_get_result():
    result = dbtRunnerResult(success=True)
    with mock.patch.object(dbtRunner, "invoke", return_value=result) as mock_invoke:
        assert get_result("arg1", "arg2") == result
        mock_invoke.assert_called_once_with(["arg1", "arg2", *DEFAULT_GLOBAL_ARGS])

        result.success = False
        result.exception = Exception
        with pytest.raises(result.exception):
            get_result("arg1", "arg2")


def test_clean_paths(config: Namespace):
    runner = dbtRunner()

    with mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result:
        clean_paths("arg1", "arg2", runner=runner, config=config)
        args = add_default_args("arg1", "arg2", config=config)
        mock_get_result.assert_called_once_with("clean", "--no-clean-project-files-only", *args, runner=runner)


def test_install_dependencies(config: Namespace):
    runner = dbtRunner()

    with mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result:
        install_dependencies("arg1", "arg2", runner=runner, config=config)
        args = add_default_args("arg1", "arg2", config=config)
        mock_get_result.assert_called_once_with("deps", *args, runner=runner)


def test_get_manifest_with_existing_artifact(config: Namespace, manifest: Manifest):
    manifest = manifest.to_dict()
    with (
        mock.patch("dbt_contracts.dbt_cli.load_artifact", return_value=manifest) as mock_load_artifact,
        mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result,
    ):
        assert get_manifest(config=config).to_dict() == manifest
        mock_load_artifact.assert_called_once_with(MANIFEST_FILE_NAME, config=config)
        mock_get_result.assert_not_called()


def test_get_manifest_with_no_existing_artifact(config: Namespace):
    runner = dbtRunner()

    with mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result:
        get_manifest("arg1", "arg2", runner=runner, config=config)
        args = add_default_args("arg1", "arg2", config=config)
        mock_get_result.assert_called_once_with("parse", *args, runner=runner)


def test_get_manifest_with_existing_artifact_with_refresh(config: Namespace, catalog: CatalogArtifact):
    catalog = catalog.to_dict()
    with (
        mock.patch("dbt_contracts.dbt_cli.load_artifact", return_value=catalog) as mock_load_artifact,
        mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result,
    ):
        get_manifest(config=config, refresh=True)
        mock_load_artifact.assert_not_called()
        mock_get_result.assert_called_once()


def test_get_catalog_with_existing_artifact(config: Namespace, catalog: CatalogArtifact):
    catalog = catalog.to_dict()
    with (
        mock.patch("dbt_contracts.dbt_cli.load_artifact", return_value=catalog) as mock_load_artifact,
        mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result,
    ):
        assert get_catalog(config=config).to_dict() == catalog
        mock_load_artifact.assert_called_once_with(CATALOG_FILENAME, config=config)
        mock_get_result.assert_not_called()


def test_get_catalog_with_existing_artifact_with_refresh(config: Namespace, catalog: CatalogArtifact):
    catalog = catalog.to_dict()
    with (
        mock.patch("dbt_contracts.dbt_cli.load_artifact", return_value=catalog) as mock_load_artifact,
        mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result,
    ):
        get_catalog(config=config, refresh=True)
        mock_load_artifact.assert_not_called()
        mock_get_result.assert_called_once()


def test_get_catalog_with_no_existing_artifact(config: Namespace):
    runner = dbtRunner()

    with mock.patch("dbt_contracts.dbt_cli.get_result") as mock_get_result:
        get_catalog("arg1", "arg2", runner=runner, config=config)
        args = add_default_args("arg1", "arg2", config=config)
        mock_get_result.assert_called_once_with("docs", "generate", *args, runner=runner)
