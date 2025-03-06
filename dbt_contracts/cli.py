import argparse
import logging
import os
from pathlib import Path

from dbt.cli.resolvers import default_profiles_dir, default_project_dir

from dbt_contracts import PROGRAM_NAME
from dbt_contracts.contracts import CONTRACT_CLASSES
from dbt_contracts.dbt_cli import get_config, clean_paths, install_dependencies
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
clean = CORE_PARSER.add_argument(
    "--clean",
    help="When this option is passed, run `dbt clean` before operations. "
         "If not passed, will attempt to load artifacts from the target folder before operations.",
    action='store_true'
)

install_deps = CORE_PARSER.add_argument(
    "--deps",
    help="When this option is passed, run `dbt deps` before operations.",
    action='store_true'
)

################################################################################
## Runner args
################################################################################
config = CORE_PARSER.add_argument(
    "--config",
    help="Either the path to a contracts configuration file, "
         f"or the directory to look in for the {ContractsRunner.default_config_file_name!r} file. "
         "Defaults to the project dir when not specified.",
    nargs="?",
    default=None,
    type=str,
)

output = CORE_PARSER.add_argument(
    "--output",
    help="Either the path to a file to write to when formatting results output, "
         f"or the directory to write a file to with filename {ContractsRunner.default_output_file_name!r}. "
         "Defaults to the project's target folder when not specified.",
    nargs="?",
    default=None,
    type=str,
)

output_format = CORE_PARSER.add_argument(
    "--format",
    help="Specify the format of results output if desired. Output file will not be generated when not specified.",
    nargs="?",
    default=None,
    choices=ContractsRunner.output_writers_map.keys(),
    type=str,
)

no_fail = CORE_PARSER.add_argument(
    "--no-fail",
    help="When this option is passed, do not fail when contracts do not pass.",
    action='store_true'
)

contract = CORE_PARSER.add_argument(
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

terms = CORE_PARSER.add_argument(
    "--terms",
    help="Apply only these terms. If none given, apply all configured terms.",
    nargs="+",
    default=None,
    type=str,
)

files = CORE_PARSER.add_argument(
    "files",
    help="Apply contract to only these files. "
         "Must either be relative to the current folder, relative to the project folder, or absolute.",
    nargs="*",
    default=None,
    type=str,
)


def main():
    """Main entry point for the CLI"""
    args = CORE_PARSER.parse_args()
    conf = get_config(args)

    if args.clean:
        clean_paths(config=conf)
    if args.deps:
        install_dependencies(config=conf)

    if args.config is None and args.project_dir:
        args.config = args.project_dir
    if args.output is None:
        args.output = Path(conf.project_root, conf.target_path)

    runner = ContractsRunner.from_config(conf)
    if args.files:
        runner.paths = args.files

    results = runner.validate(contract_key=args.contract, terms=args.terms)

    if args.format:
        runner.write_results(results, path=args.output, output_type=args.format)

    if not args.no_fail and results:
        raise Exception(f"Found {len(results)} contract violations.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()], force=True)
    main()
