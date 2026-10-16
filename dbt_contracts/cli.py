import json
import logging
import os
from argparse import ArgumentParser, Namespace, Action
from pathlib import Path
from types import UnionType
from typing import Any, Literal, Self, Union, get_args, get_origin

from dbt.cli.resolvers import default_profiles_dir, default_project_dir
from pydantic import Field
from pydantic_settings import BaseSettings, CliApp, CliPositionalArg, CliSettingsSource, CliVariadicArg, \
    PydanticBaseSettingsSource, SettingsConfigDict

from dbt_contracts import PROGRAM_NAME
from dbt_contracts.contracts import CONTRACT_CLASSES
from dbt_contracts.dbt_cli import get_config, clean_paths, install_dependencies, get_manifest, get_catalog
from dbt_contracts.runner import ContractsRunner

CONTRACT_KEYS = tuple(
    key for keys in ((contract.__config_key__, contract.child_config_key) for contract in CONTRACT_CLASSES)
    for key in keys
)
OUTPUT_FORMATS = tuple(ContractsRunner.output_writers_map.keys())


class CoreArgs(BaseSettings):
    """The CLI args common to all commands."""
    model_config = SettingsConfigDict(
        cli_kebab_case=True,
        cli_implicit_flags="toggle",
        cli_shortcuts={"target": "t"},
    )

    ################################################################################
    ## DBT args
    ################################################################################
    profiles_dir: Path = Field(
        description="Which directory to look in for the profiles.yml file. "
                    "If not set, dbt will look in the current working directory first, then HOME/.dbt/",
        default_factory=lambda: Path(os.getenv("DBT_PROFILES_DIR", default_profiles_dir())),
    )
    project_dir: Path = Field(
        description="Which directory to look in for the dbt_project.yml file. "
                    "Default is the current working directory and its parents.",
        default_factory=lambda: Path(os.getenv("DBT_PROJECT_DIR", default_project_dir())),
    )
    profile: str | None = Field(
        description="Which existing profile to load. Overrides setting in dbt_project.yml.",
        default_factory=lambda: os.getenv("DBT_PROFILE"),
    )
    target: str | None = Field(
        description="Which target to load for the given profile",
        default_factory=lambda: os.getenv("DBT_TARGET"),
    )
    threads: int | None = Field(
        description="Specify number of threads to use while executing models. Overrides settings in profiles.yml.",
        default=None,
    )

    ################################################################################
    ## DBT commands
    ################################################################################
    clean: bool = Field(
        description="When this option is passed, run `dbt clean` before operations. "
                    "If not passed, will attempt to load artifacts from the target folder before operations.",
        default=False,
    )
    deps: bool = Field(
        description="When this option is passed, run `dbt deps` before operations.",
        default=False,
    )

    ################################################################################
    ## General runner args
    ################################################################################
    config: Path | None = Field(
        description="Either the path to a contracts configuration file, "
                    f"or the directory to look in for the {ContractsRunner.default_config_file_name!r} file. "
                    "Defaults to the project dir when not specified.",
        default=None,
    )
    contract: Literal[CONTRACT_KEYS] | None = Field(
        description="Run only this contract. If none given, apply all configured contracts. "
                    "Specify granular contracts by separating keys by a '.' e.g. 'model', 'model.columns'",
        default=None,
    )
    files: CliPositionalArg[list[str]] = Field(
        description="Apply contract to only these files. "
                    "Must either be relative to the current folder, relative to the project folder, or absolute.",
        default_factory=list,
    )

    @classmethod
    def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # only read env vars explicitly in the default factories above, never for every field
        return (init_settings,)

    @classmethod
    def _add_argument(cls, parser: ArgumentParser, *args: str, **kwargs: Any) -> Action:
        """
        Add an argument to the CLI parser:

        * argparse validates choices and ints so invalid values give its usual usage errors
        * help text shows argparse's metavars and the field descriptions only
        """
        if (field := cls.model_fields.get(kwargs.get("dest") or args[0])) is None:
            return parser.add_argument(*args, **kwargs)

        kwargs.pop("metavar", None)
        kwargs["help"] = field.description

        types = [field.annotation]
        if get_origin(field.annotation) in (Union, UnionType):
            types = [arg for arg in get_args(field.annotation) if arg is not type(None)]

        if len(types) == 1 and get_origin(types[0]) is Literal:
            kwargs["choices"] = get_args(types[0])
        elif types == [int]:
            kwargs["type"] = int

        return parser.add_argument(*args, **kwargs)

    @staticmethod
    def _parse_args(parser: ArgumentParser, args: list[str] | None = None) -> Namespace:
        """
        Parse the args, passing on the values of args which take many values as a JSON array.
        This keeps each value exactly as given e.g. file paths from pre-commit,
        rather than pydantic-settings splitting values on commas or decoding them as JSON.
        """
        parsed = parser.parse_args(args)
        for key, value in vars(parsed).items():
            if isinstance(value, list):
                setattr(parsed, key, json.dumps(value))
        return parsed

    @classmethod
    def parse(cls, prog: str = PROGRAM_NAME, args: list[str] | None = None) -> Self:
        """
        Parse the CLI args.

        :param prog: The name of the program to show in the help text.
        :param args: The args to parse. Parses the args given to the current process when None.
        :return: The parsed args.
        """
        # a plain parser keeps argparse's default help formatting, without the class docstring as a description
        parser = ArgumentParser(prog=prog)
        source = CliSettingsSource(
            cls, root_parser=parser, parse_args_method=cls._parse_args, add_argument_method=cls._add_argument
        )
        return CliApp.run(cls, cli_args=args, cli_settings_source=source)

    def to_namespace(self) -> Namespace:
        """Convert these args to the namespace that dbt expects."""
        return Namespace(**self.model_dump())


class ValidatorArgs(CoreArgs):
    """The CLI args for the ``dbt-validate`` command."""
    model_config = SettingsConfigDict(
        cli_shortcuts={"target": "t", "terms": ["term", "validations", "validation"]},
    )

    output: Path | None = Field(
        description="Either the path to a file to write to when formatting results output, "
                    f"or the directory to write a file to with filename {ContractsRunner.default_output_file_name!r}. "
                    "Defaults to the project's target folder when not specified.",
        default=None,
    )
    format: Literal[OUTPUT_FORMATS] | None = Field(
        description="Specify the format of results output if desired. "
                    "Output file will not be generated when not specified.",
        default=None,
    )
    no_fail: bool = Field(
        description="When this option is passed, do not fail when contracts do not pass.",
        default=False,
    )
    terms: CliVariadicArg[list[str] | None] = Field(
        description="Apply only these validations/terms. If none given, apply all configured validations/terms.",
        default=None,
    )


class GeneratorArgs(CoreArgs):
    """The CLI args for the ``dbt-generate`` command."""


class MainArgs(ValidatorArgs):
    """The CLI args when running this module directly."""
    operation: Literal["validate", "generate"] = Field(
        description="Run this operation.",
        default="validate",
    )


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()], force=True)


def _setup_runner(args: Namespace) -> ContractsRunner:
    args.profiles_dir = str(Path(args.profiles_dir).resolve())
    args.project_dir = str(Path(args.project_dir).resolve())

    config = get_config(args)

    if args.clean:
        clean_paths(config=config)
    if args.deps:
        install_dependencies(config=config)

    if args.config is None and args.project_dir:
        args.config = Path(args.project_dir)

    args.config = Path(args.config).resolve()

    runner = ContractsRunner.from_config(config)
    if args.files:
        runner.paths = config.args.files

    return runner


def clean() -> None:
    """Main entry point for executing ``dbt clean``"""
    args = CoreArgs.parse(prog="dbt-clean").to_namespace()
    config = get_config(args)
    clean_paths(config=config)


def deps() -> None:
    """Main entry point for executing ``dbt deps``"""
    args = CoreArgs.parse(prog="dbt-deps").to_namespace()
    config = get_config(args)
    install_dependencies(config=config)


def parse() -> None:
    """Main entry point for executing ``dbt parse``"""
    args = CoreArgs.parse(prog="dbt-parse").to_namespace()
    config = get_config(args)
    get_manifest(config=config)


def docs() -> None:
    """Main entry point for executing ``dbt docs generate``"""
    args = CoreArgs.parse(prog="dbt-docs").to_namespace()
    config = get_config(args)
    get_catalog(config=config)


def validate(args: ValidatorArgs | None = None) -> None:
    """
    Main entry point for the ``validator`` CLI command

    :param args: The parsed CLI args. Parses the args given to the current process when None.
    """
    _setup_logging()

    args = (args or ValidatorArgs.parse(prog="dbt-validate")).to_namespace()
    runner = _setup_runner(args)

    if args.output is None:
        args.output = Path(runner.config.project_root, runner.config.target_path)
    args.output = Path(args.output).resolve()

    results = runner.validate(contract_key=args.contract, terms=args.terms)

    if args.format:
        runner.write_results(results, path=args.output, output_type=args.format)

    if not args.no_fail and results:
        raise Exception(f"Found {len(results)} contract violations.")


def generate(args: CoreArgs | None = None) -> None:
    """
    Main entry point for the ``generate`` CLI command

    :param args: The parsed CLI args. Parses the args given to the current process when None.
    """
    _setup_logging()

    args = (args or GeneratorArgs.parse(prog="dbt-generate")).to_namespace()
    runner = _setup_runner(args)

    runner.generate(contract_key=args.contract)


if __name__ == "__main__":
    main_args = MainArgs.parse()
    if main_args.operation == "generate":
        generate(main_args)
    else:
        validate(main_args)
