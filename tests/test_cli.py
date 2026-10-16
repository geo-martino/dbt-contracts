"""Tests for the CLI args, which must parse exactly as the CLI always has."""
from argparse import Namespace
from pathlib import Path

import pytest
from dbt.cli.resolvers import default_profiles_dir, default_project_dir

from dbt_contracts.cli import CoreArgs, ValidatorArgs, GeneratorArgs, MainArgs, CONTRACT_KEYS, OUTPUT_FORMATS

DBT_ENV_VARS = ("DBT_PROFILES_DIR", "DBT_PROJECT_DIR", "DBT_PROFILE", "DBT_TARGET")
ALL_ARGS = (CoreArgs, ValidatorArgs, GeneratorArgs, MainArgs)


@pytest.fixture(autouse=True)
def clear_env(monkeypatch: pytest.MonkeyPatch):
    """Clear any env vars the CLI could read so tests don't depend on the environment."""
    for name in DBT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _parse(model: type[CoreArgs], *args: str) -> dict:
    return model.parse(prog="dbt-test", args=list(args)).model_dump()


@pytest.mark.parametrize("model", ALL_ARGS)
def test_defaults(model: type[CoreArgs]):
    args = _parse(model)
    assert args["profiles_dir"] == default_profiles_dir()
    assert args["project_dir"] == default_project_dir()
    assert {key: args[key] for key in ("profile", "target", "threads", "config", "contract")} == dict.fromkeys(
        ("profile", "target", "threads", "config", "contract")
    )
    assert args["clean"] is False
    assert args["deps"] is False
    assert args["files"] == []


def test_validator_defaults():
    args = _parse(ValidatorArgs)
    assert args["output"] is None
    assert args["format"] is None
    assert args["no_fail"] is False
    assert args["terms"] is None


def test_main_defaults():
    assert _parse(MainArgs)["operation"] == "validate"


@pytest.mark.parametrize("model", ALL_ARGS)
def test_core_args(model: type[CoreArgs]):
    args = _parse(
        model,
        "--profiles-dir", "profiles", "--project-dir", "project", "--profile", "profile", "--target", "dev",
        "--threads", "4", "--clean", "--deps", "--config", "contracts.yml", "--contract", "models.columns",
        "models/model.sql", "models/properties.yml",
    )

    assert args == args | {
        "profiles_dir": "profiles",
        "project_dir": "project",
        "profile": "profile",
        "target": "dev",
        "threads": 4,
        "clean": True,
        "deps": True,
        "config": Path("contracts.yml"),
        "contract": "models.columns",
        "files": ["models/model.sql", "models/properties.yml"],
    }


def test_validator_args():
    args = _parse(ValidatorArgs, "--output", "results", "--format", "json", "--no-fail", "--terms", "a", "b")
    assert args == args | {"output": Path("results"), "format": "json", "no_fail": True, "terms": ["a", "b"]}


@pytest.mark.parametrize("flag", ["--terms", "--term", "--validations", "--validation"])
def test_terms_aliases(flag: str):
    assert _parse(ValidatorArgs, flag, "a", "b")["terms"] == ["a", "b"]


def test_terms_aliases_take_the_last_given():
    assert _parse(ValidatorArgs, "--term", "a", "--validation", "b")["terms"] == ["b"]


def test_terms_and_files():
    args = _parse(ValidatorArgs, "--terms", "a", "b", "--", "file.sql")
    assert args["terms"] == ["a", "b"]
    assert args["files"] == ["file.sql"]

    # files are only taken from one run of positional args, any later ones are ignored as unknown args
    args = _parse(ValidatorArgs, "file.sql", "--terms", "a", "b", "--", "other.sql")
    assert args["terms"] == ["a", "b"]
    assert args["files"] == ["file.sql"]


@pytest.mark.parametrize("args", [["-t", "dev"], ["--target", "dev"], ["--target=dev"]])
def test_target_shortcut(args: list[str]):
    assert _parse(CoreArgs, *args)["target"] == "dev"


@pytest.mark.parametrize("model", ALL_ARGS)
def test_env_vars(model: type[CoreArgs], monkeypatch: pytest.MonkeyPatch):
    for name in DBT_ENV_VARS:
        monkeypatch.setenv(name, name.lower())
    args = _parse(model)
    assert (args["profiles_dir"], args["project_dir"], args["profile"], args["target"]) == tuple(
        name.lower() for name in DBT_ENV_VARS
    )

    # CLI args take precedence over env vars
    assert _parse(model, "--profile", "cli")["profile"] == "cli"


def test_ignores_env_vars_for_other_args(monkeypatch: pytest.MonkeyPatch):
    env = {"CONFIG": "env.yml", "FORMAT": "json", "FILES": '["env.sql"]', "TERMS": '["env"]', "THREADS": "9",
           "NO_FAIL": "true", "CLEAN": "true", "PROFILE": "env"}
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    args = _parse(ValidatorArgs)
    assert args == args | {
        "config": None, "format": None, "files": [], "terms": None, "threads": None,
        "no_fail": False, "clean": False, "profile": None,
    }


@pytest.mark.parametrize("flag", ["--profiles-dir", "--project-dir", "--profile", "--target", "-t", "--threads",
                                  "--config", "--contract", "--output", "--format"])
def test_option_without_value_is_none(flag: str, monkeypatch: pytest.MonkeyPatch):
    for name in DBT_ENV_VARS:
        monkeypatch.setenv(name, name.lower())

    args = _parse(ValidatorArgs, flag)
    key = "target" if flag == "-t" else flag.removeprefix("--").replace("-", "_")
    assert args[key] is None


def test_operation_without_value_is_none():
    assert _parse(MainArgs, "--operation")["operation"] is None


@pytest.mark.parametrize("value", ["null", "None", "1", "true", '{"a": 1}', "[1, 2]", ""])
def test_values_are_kept_as_given(value: str):
    assert _parse(CoreArgs, "--profile", value)["profile"] == value


@pytest.mark.parametrize("values", [
    ["null"], ["true"], ["1"], ['["a","b"]'], ['{"a": 1}'], ["a,b"], ["with space", "ünïcode"], ["plain", "null"],
])
def test_list_values_are_kept_as_given(values: list[str]):
    args = _parse(ValidatorArgs, *values, "--terms", *values)
    assert args["files"] == values
    assert args["terms"] == values


def test_ignores_unknown_args():
    args = _parse(CoreArgs, "--unknown", "--target", "dev")
    assert args["target"] == "dev"
    assert args["files"] == []

    # as for any unknown option, its value is taken as a positional arg
    assert _parse(CoreArgs, "--unknown", "value")["files"] == ["value"]


@pytest.mark.parametrize("model,args,message", [
    (CoreArgs, ["--contract", "bad"], "argument --contract: invalid choice: 'bad'"),
    (CoreArgs, ["--threads", "abc"], "argument --threads: invalid int value: 'abc'"),
    (ValidatorArgs, ["--format", "xml"], "argument --format: invalid choice: 'xml'"),
    (ValidatorArgs, ["--terms"], "argument --terms/--term/--validations/--validation: expected at least one argument"),
    (MainArgs, ["--operation", "bad"], "argument --operation: invalid choice: 'bad'"),
])
def test_invalid_args_exit_with_usage(
        model: type[CoreArgs], args: list[str], message: str, capsys: pytest.CaptureFixture
):
    with pytest.raises(SystemExit) as ex:
        _parse(model, *args)

    assert ex.value.code == 2
    stderr = capsys.readouterr().err
    assert stderr.startswith("usage: dbt-test ")
    assert f"dbt-test: error: {message}" in stderr


@pytest.mark.parametrize("model", ALL_ARGS)
def test_help(model: type[CoreArgs], capsys: pytest.CaptureFixture):
    with pytest.raises(SystemExit) as ex:
        _parse(model, "--help")
    assert ex.value.code == 0

    help_text = " ".join(capsys.readouterr().out.split())
    assert help_text.startswith("usage: dbt-test [-h] [--profiles-dir [PROFILES_DIR]] [--project-dir [PROJECT_DIR]]")
    assert "[--contract [{" + ",".join(CONTRACT_KEYS) + "}]]" in help_text
    assert "[files ...]" in help_text
    assert "(default" not in help_text  # pydantic's help text additions

    for name, field in model.model_fields.items():
        assert " ".join(field.description.split()) in help_text, name


def test_validator_help(capsys: pytest.CaptureFixture):
    with pytest.raises(SystemExit):
        _parse(ValidatorArgs, "--help")

    help_text = " ".join(capsys.readouterr().out.split())
    assert "[--format [{" + ",".join(OUTPUT_FORMATS) + "}]]" in help_text
    assert "[--terms TERMS [TERMS ...]]" in help_text


def test_to_namespace():
    args = ValidatorArgs.parse(args=["--target", "dev", "file.sql"])
    namespace = args.to_namespace()

    assert isinstance(namespace, Namespace)
    assert vars(namespace) == args.model_dump()
