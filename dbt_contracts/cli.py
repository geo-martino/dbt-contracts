import argparse
import logging
import os
from copy import deepcopy
from pathlib import Path

from dbt.cli.resolvers import default_profiles_dir, default_project_dir

from dbt_contracts import PROGRAM_NAME
from dbt_contracts.contracts import CONTRACT_CLASSES
from dbt_contracts.dbt_cli import get_config, clean_paths, install_dependencies, get_manifest, get_catalog
from dbt_contracts.runner import ContractsRunner

CORE_PARSER = argparse.ArgumentParser(
    prog=PROGRAM_NAME,
)

################################################################################
## DBT args
################################################################################
profiles_dir = CORE_PARSER.add_argument(
    "--profiles-dir",
    help="Which directory to look in for the profiles.yml file. "
         "If not set, dbt will look in the current working directory first, then HOME/.dbt/",
    nargs="?",
    default=os.getenv("DBT_PROFILES_DIR", default_profiles_dir()),
    type=str,
)

project_dir = CORE_PARSER.add_argument(
    "--project-dir",
    help="Which directory to look in for the dbt_project.yml file. "
         "Default is the current working directory and its parents.",
    nargs="?",
    default=os.getenv("DBT_PROJECT_DIR", default_project_dir()),
    type=str,
)

profile = CORE_PARSER.add_argument(
    "--profile",
    help="Which existing profile to load. Overrides setting in dbt_project.yml.",
    nargs="?",
    default=os.getenv("DBT_PROFILE"),
    type=str,
)

target = CORE_PARSER.add_argument(
    "--target",
    "-t",
    help="Which target to load for the given profile",
    nargs="?",
    default=os.getenv("DBT_TARGET"),
    type=str,
)

threads = CORE_PARSER.add_argument(
    "--threads",
    help="Specify number of threads to use while executing models. Overrides settings in profiles.yml.",
    nargs="?",
    default=None,
    type=int,
)

################################################################################
## DBT commands
################################################################################
CORE_PARSER.add_argument(
    "--clean",
    help="When this option is passed, run `dbt clean` before operations. "
         "If not passed, will attempt to load artifacts from the target folder before operations.",
    action='store_true'
)

CORE_PARSER.add_argument(
    "--deps",
    help="When this option is passed, run `dbt deps` before operations.",
    action='store_true'
)

################################################################################
## General runner args
################################################################################
CORE_PARSER.add_argument(
    "--config",
    help="Either the path to a contracts configuration file, "
         f"or the directory to look in for the {ContractsRunner.default_config_file_name!r} file. "
         "Defaults to the project dir when not specified.",
    nargs="?",
    default=None,
    type=Path,
)

CORE_PARSER.add_argument(
    "--contract",
    help="Run only this contract. If none given, apply all configured contracts. "
         "Specify granular contracts by separating keys by a '.' e.g. 'model', 'model.columns'",
    nargs="?",
    default=None,
    choices=[
        key for keys in (
            (contract.__config_key__, contract.child_config_key) for contract in CONTRACT_CLASSES
        ) for key in keys
    ],
    type=str,
)

CORE_PARSER.add_argument(
    "files",
    help="Apply contract to only these files. "
         "Must either be relative to the current folder, relative to the project folder, or absolute.",
    nargs="*",
    default=None,
    type=str,
)

################################################################################
## Validator runner args
################################################################################
VALIDATOR_PARSER = deepcopy(CORE_PARSER)
VALIDATOR_PARSER.prog = "dbt-validate"

VALIDATOR_PARSER.add_argument(
    "--output",
    help="Either the path to a file to write to when formatting results output, "
         f"or the directory to write a file to with filename {ContractsRunner.default_output_file_name!r}. "
         "Defaults to the project's target folder when not specified.",
    nargs="?",
    default=None,
    type=Path,
)

VALIDATOR_PARSER.add_argument(
    "--format",
    help="Specify the format of results output if desired. Output file will not be generated when not specified.",
    nargs="?",
    default=None,
    choices=ContractsRunner.output_writers_map.keys(),
    type=str,
)

VALIDATOR_PARSER.add_argument(
    "--no-fail",
    help="When this option is passed, do not fail when contracts do not pass.",
    action='store_true'
)

VALIDATOR_PARSER.add_argument(
    "--terms", "--term", "--validations", "--validation",
    help="Apply only these validations/terms. If none given, apply all configured validations/terms.",
    nargs="+",
    default=None,
    type=str,
)

################################################################################
## Generator runner args
################################################################################
GENERATOR_PARSER = deepcopy(CORE_PARSER)
GENERATOR_PARSER.prog = "dbt-generate"


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()], force=True)


def _setup_runner(args: argparse.Namespace) -> ContractsRunner:
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
    """Main entry point for executing `dbt clean`"""
    CORE_PARSER.prog = "dbt-clean"
    args, _ = CORE_PARSER.parse_known_args()
    config = get_config(args)
    clean_paths(config=config)


def deps() -> None:
    """Main entry point for executing `dbt deps`"""
    CORE_PARSER.prog = "dbt-deps"
    args, _ = CORE_PARSER.parse_known_args()
    config = get_config(args)
    install_dependencies(config=config)


def parse() -> None:
    """Main entry point for executing `dbt parse`"""
    CORE_PARSER.prog = "dbt-parse"
    args, _ = CORE_PARSER.parse_known_args()
    config = get_config(args)
    get_manifest(config=config)


def docs() -> None:
    """Main entry point for executing `dbt docs generate`"""
    CORE_PARSER.prog = "dbt-docs"
    args, _ = CORE_PARSER.parse_known_args()
    config = get_config(args)
    get_catalog(config=config)


def validate() -> None:
    """Main entry point for the `validator` CLI command"""
    _setup_logging()

    args, _ = VALIDATOR_PARSER.parse_known_args()
    runner = _setup_runner(args)

    if args.output is None:
        args.output = Path(runner.config.project_root, runner.config.target_path)
    args.output = Path(args.output).resolve()

    results = runner.validate(contract_key=args.contract, terms=args.terms)

    if args.format:
        runner.write_results(results, path=args.output, output_type=args.format)

    if not args.no_fail and results:
        raise Exception(f"Found {len(results)} contract violations.")


def generate() -> None:
    """Main entry point for the `generate` CLI command"""
    _setup_logging()

    args, _ = GENERATOR_PARSER.parse_known_args()
    runner = _setup_runner(args)

    runner.generate(contract_key=args.contract)


if __name__ == "__main__":
    operation = CORE_PARSER.add_argument(
        "--operation",
        help="Run this operation.",
        nargs="?",
        default="validate",
        choices=["validate", "generate"],
        type=str,
    )
    core_args, _ = CORE_PARSER.parse_known_args()

    if core_args.operation == "validate":
        validate()
        exit(0)
    elif core_args.operation == "generate":
        generate()
        exit(0)

    raise RuntimeError(f"Unrecognised operation: {core_args.command!r}")
