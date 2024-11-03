"""
Invoke various dbt CLI commands needed for hooks to function and return their results.
"""
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
from dbt.artifacts.schemas.catalog.v1.catalog import CatalogArtifact


def _format_option_key(key: str) -> str:
    key_clean = key.replace("_", "-").strip("-")
    prefix = "-" if len(key_clean) == 1 else "--"
    return prefix + key_clean


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


def get_manifest(*args, runner: dbtRunner = None, **kwargs) -> Manifest:
    """
    Generate and return the dbt manifest for a project i.e. run the `dbt parse` command.

    :param runner: The :py:class:`dbtRunner` to invoke commands against.
        If None, creates a new runner for this invocation.
    :param args: Args to pass to the `runner`.
    :param kwargs: Args to pass to the `runner` in keyword format. Keys will be formatted to CLI appropriate keys.
    :return: The manifest.
    """
    return get_result("parse", *args, runner=runner, **kwargs).result


def get_catalog(*args, runner: dbtRunner = None, **kwargs) -> CatalogArtifact:
    """
    Generate and return the dbt catalog for a project i.e. run the `dbt docs generate` command.

    :param runner: The :py:class:`dbtRunner` to invoke commands against.
        If None, creates a new runner for this invocation.
    :param args: Args to pass to the `runner`.
    :param kwargs: Args to pass to the `runner` in keyword format. Keys will be formatted to CLI appropriate keys.
    :return: The catalog.
    """
    return get_result("docs", "generate", *args, runner=runner, **kwargs).result
