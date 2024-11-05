"""
Invoke various dbt CLI commands needed for hooks to function and return their results.
"""
import json
import os
from argparse import Namespace
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.config import RuntimeConfig
from dbt.constants import MANIFEST_FILE_NAME
from dbt.contracts.graph.manifest import Manifest
from dbt.artifacts.schemas.catalog.v1.catalog import CatalogArtifact
from dbt.flags import set_from_args
from dbt.task.docs.generate import CATALOG_FILENAME
from dbt_common.context import set_invocation_context

from dbt_contracts.cli import CORE_PARSER


def _format_option_key(key: str) -> str:
    key_clean = key.replace("_", "-").strip("-")
    prefix = "-" if len(key_clean) == 1 else "--"
    return prefix + key_clean


def get_config(args: Namespace = None) -> RuntimeConfig:
    """
    Get the dbt config for the current runtime.
    The runtime config can be used to extract common dbt args for the current runtime
    e.g. project_dir, profiles_dir, target_dir etc.

    :param args: The parsed CLI args.
    :return: The runtime config.
    """
    if args is None:
        args = CORE_PARSER.parse_args()

    set_invocation_context(os.environ)
    set_from_args(args, {})
    return RuntimeConfig.from_args(args)


def load_artifact(filename: str, config: RuntimeConfig = None) -> Mapping[str, Any] | None:
    """
    Load an artifact from the currently configured dbt target directory.

    :param filename: The filename of the artifact to load.
    :param config: The runtime config to use when trying to load the artifact from the target path.
    :return: The loaded artifact if found. None otherwise.
    """
    if config is None:
        config = get_config()

    target_dir = Path(config.project_target_path)
    if not target_dir.is_dir():
        return

    target_path = target_dir.joinpath(filename)
    if not target_path.is_file():
        return

    with target_path.open("r") as file:
        artifact = json.load(file)

    return artifact


def get_result(*args, runner: dbtRunner = None, **kwargs) -> dbtRunnerResult:
    """
    Get the result of a dbt invocation with the given `args` and `kwargs` against a given `runner`.

    :param runner: The :py:class:`dbtRunner` to invoke commands against.
        If None, creates a new runner for this invocation.
    :param args: Args to pass to the `runner`.
    :param kwargs: Args to pass to the `runner` in keyword format. Keys will be formatted to CLI appropriate keys.
    :return: The result from the invocation.
    """
    if runner is None:
        runner = dbtRunner()

    kwargs = [item for key, val in kwargs.items() for item in (_format_option_key(key), str(val))]

    result: dbtRunnerResult = runner.invoke(list(args) + kwargs)
    if not result.success:
        raise result.exception

    return result


def clean_paths(*args, runner: dbtRunner = None, **kwargs) -> None:
    """
    Clean the configured paths i.e. run the `dbt clean` command.

    :param runner: The :py:class:`dbtRunner` to invoke commands against.
        If None, creates a new runner for this invocation.
    :param args: Args to pass to the `runner`.
    :param kwargs: Args to pass to the `runner` in keyword format. Keys will be formatted to CLI appropriate keys.
    """
    return get_result("clean", *args, runner=runner, **kwargs).result


def install_dependencies(*args, runner: dbtRunner = None, **kwargs) -> None:
    """
    Install additional dbt dependencies i.e. run the `dbt deps` command.

    :param runner: The :py:class:`dbtRunner` to invoke commands against.
        If None, creates a new runner for this invocation.
    :param args: Args to pass to the `runner`.
    :param kwargs: Args to pass to the `runner` in keyword format. Keys will be formatted to CLI appropriate keys.
    """
    return get_result("deps", *args, runner=runner, **kwargs).result


def get_manifest(*args, runner: dbtRunner = None, config: RuntimeConfig = None, **kwargs) -> Manifest:
    """
    Generate and return the dbt manifest for a project i.e. run the `dbt parse` command.

    :param runner: The :py:class:`dbtRunner` to invoke commands against.
        If None, creates a new runner for this invocation.
    :param config: The runtime config to use when trying to load the artifact from the target path.
    :param args: Args to pass to the `runner`.
    :param kwargs: Args to pass to the `runner` in keyword format. Keys will be formatted to CLI appropriate keys.
    :return: The manifest.
    """
    artifact = load_artifact(MANIFEST_FILE_NAME, config=config)
    if artifact:
        return Manifest.from_dict(artifact)
    return get_result("parse", *args, runner=runner, **kwargs).result


def get_catalog(*args, runner: dbtRunner = None, config: RuntimeConfig = None, **kwargs) -> CatalogArtifact:
    """
    Generate and return the dbt catalog for a project i.e. run the `dbt docs generate` command.

    :param runner: The :py:class:`dbtRunner` to invoke commands against.
        If None, creates a new runner for this invocation.
    :param config: The runtime config to use when trying to load the artifact from the target path.
    :param args: Args to pass to the `runner`.
    :param kwargs: Args to pass to the `runner` in keyword format. Keys will be formatted to CLI appropriate keys.
    :return: The catalog.
    """
    artifact = load_artifact(CATALOG_FILENAME, config=config)
    if artifact:
        return CatalogArtifact.from_dict(artifact)
    return get_result("docs", "generate", *args, runner=runner, **kwargs).result
