import json
import logging
import os
from argparse import ArgumentParser, Namespace, Action
from pathlib import Path
from types import UnionType
from typing import Any, ClassVar, Literal, Self, Union, get_args, get_origin

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
    """The CLI args common to all commands. Commands subclass this, running in their ``cli_cmd`` method."""
    model_config = SettingsConfigDict(
        cli_kebab_case=True,
        cli_implicit_flags="toggle",
        cli_shortcuts={"target": "t"},
    )
    #: The name of the command, shown in the help text
    prog: ClassVar[str] = PROGRAM_NAME

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
    def _cli_settings_source(cls, prog: str | None = None) -> CliSettingsSource:
        # a plain parser keeps argparse's default help formatting, without the class docstring as a description
        return CliSettingsSource(
            cls,
            root_parser=ArgumentParser(prog=prog or cls.prog),
            parse_args_method=cls._parse_args,
            add_argument_method=cls._add_argument,
        )

    @classmethod
    def parse(cls, args: list[str] | None = None, prog: str | None = None) -> Self:
        """
        Parse the CLI args without running the command.

        :param args: The args to parse. Parses the args given to the current process when None.
        :param prog: The name of the program to show in the help text. Defaults to the command's name.
        :return: The parsed args.
        """
        source = cls._cli_settings_source(prog)
        return cls(_cli_settings_source=source(args=True if args is None else args))

    @classmethod
    def run(cls, args: list[str] | None = None, prog: str | None = None) -> Self:
        """
        Parse the CLI args and run the command.

        :param args: The args to parse. Parses the args given to the current process when None.
        :param prog: The name of the program to show in the help text. Defaults to the command's name.
        :return: The parsed args.
        """
        return CliApp.run(cls, cli_args=args, cli_settings_source=cls._cli_settings_source(prog))

    def to_namespace(self) -> Namespace:
        """Convert these args to the namespace that dbt expects."""
        return Namespace(**self.model_dump())

    def _create_runner(self) -> ContractsRunner:
        """Create a contracts runner for the dbt project, running ``dbt clean`` and ``dbt deps`` first if set."""
        args = self.to_namespace()
        args.profiles_dir = str(self.profiles_dir.resolve())
        args.project_dir = str(self.project_dir.resolve())
        args.config = Path(self.config or args.project_dir).resolve()

        config = get_config(args)
        if self.clean:
            clean_paths(config=config)
        if self.deps:
            install_dependencies(config=config)

        runner = ContractsRunner.from_config(config)
        if self.files:
            runner.paths = config.args.files

        return runner


################################################################################
## dbt commands
################################################################################
class CleanCommand(CoreArgs):
    """Run ``dbt clean``."""
    prog: ClassVar[str] = "dbt-clean"

    def cli_cmd(self) -> None:
        """Run ``dbt clean``."""
        clean_paths(config=get_config(self.to_namespace()))


class DepsCommand(CoreArgs):
    """Run ``dbt deps``."""
    prog: ClassVar[str] = "dbt-deps"

    def cli_cmd(self) -> None:
        """Run ``dbt deps``."""
        install_dependencies(config=get_config(self.to_namespace()))


class ParseCommand(CoreArgs):
    """Run ``dbt parse`` to generate the manifest."""
    prog: ClassVar[str] = "dbt-parse"

    def cli_cmd(self) -> None:
        """Run ``dbt parse``."""
        get_manifest(config=get_config(self.to_namespace()))


class DocsCommand(CoreArgs):
    """Run ``dbt docs generate`` to generate the catalog."""
    prog: ClassVar[str] = "dbt-docs"

    def cli_cmd(self) -> None:
        """Run ``dbt docs generate``."""
        get_catalog(config=get_config(self.to_namespace()))


################################################################################
## Contracts commands
################################################################################
class ValidateCommand(CoreArgs):
    """Validate the configured contracts."""
    prog: ClassVar[str] = "dbt-validate"
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

    def cli_cmd(self) -> None:
        """Validate the configured contracts, exiting with code 1 when any fail unless ``no_fail`` is set."""
        _setup_logging()
        runner = self._create_runner()

        results = runner.validate(contract_key=self.contract, terms=self.terms)
        if self.format:
            output = self.output or Path(runner.config.project_root, runner.config.target_path)
            runner.write_results(results, path=output.resolve(), output_type=self.format)

        if not self.no_fail and results:
            # the runner has already logged the violations found, so exit without a traceback
            raise SystemExit(1)


class GenerateCommand(CoreArgs):
    """Generate properties files for the configured contracts."""
    prog: ClassVar[str] = "dbt-generate"

    def cli_cmd(self) -> None:
        """Generate properties files for the configured contracts."""
        _setup_logging()
        self._create_runner().generate(contract_key=self.contract)


class MainCommand(ValidateCommand):
    """Run one of the contracts commands when running this module directly."""
    prog: ClassVar[str] = PROGRAM_NAME

    operation: Literal["validate", "generate"] = Field(
        description="Run this operation.",
        default="validate",
    )

    def cli_cmd(self) -> None:
        """Run the given operation with the other args given."""
        command = GenerateCommand if self.operation == "generate" else ValidateCommand
        command(**self.model_dump(include=set(command.model_fields))).cli_cmd()


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()], force=True)


################################################################################
## Entry points
################################################################################
def clean() -> None:
    """Main entry point for executing ``dbt clean``"""
    CleanCommand.run()


def deps() -> None:
    """Main entry point for executing ``dbt deps``"""
    DepsCommand.run()


def parse() -> None:
    """Main entry point for executing ``dbt parse``"""
    ParseCommand.run()


def docs() -> None:
    """Main entry point for executing ``dbt docs generate``"""
    DocsCommand.run()


def validate() -> None:
    """Main entry point for the ``validator`` CLI command"""
    ValidateCommand.run()


def generate() -> None:
    """Main entry point for the ``generate`` CLI command"""
    GenerateCommand.run()


if __name__ == "__main__":
    MainCommand.run()
