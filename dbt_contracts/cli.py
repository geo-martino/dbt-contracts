import argparse
import os

from dbt.cli.resolvers import default_profiles_dir, default_project_dir

from dbt_contracts import PROGRAM_NAME
from dbt_contracts.contracts import CONTRACTS, ParentContract

CORE_PARSER = argparse.ArgumentParser(
    prog=PROGRAM_NAME,
)

profiles_dir = CORE_PARSER.add_argument(
    "--profiles-dir",
    help="Which directory to look in for the profiles.yml file. "
         "If not set, dbt will look in the current working directory first, then HOME/.dbt/",
    nargs="?",
    default=os.getenv("DBT_PROFILES_DIR", default_profiles_dir),
    type=str,
)

project_dir = CORE_PARSER.add_argument(
    "--project-dir",
    help="Which directory to look in for the dbt_project.yml file. "
         "Default is the current working directory and its parents.",
    nargs="?",
    default=os.getenv("DBT_PROJECT_DIR", default_project_dir),
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

output_format = CORE_PARSER.add_argument(
    "--format",
    help="Specify the format of results output if desired. Output file will not be generated when not specified.",
    nargs="?",
    default=None,
    choices=["text", "json", "github-annotations"],
    type=str,
)

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

contract = CORE_PARSER.add_argument(
    "--contract",
    help="Limit the execution to a specific contract type. "
         "Specify granular contracts by seperating keys by a '.'. "
         "e.g. 'model', 'model.columns'",
    nargs="?",
    default=None,
    choices=[
        str(contract.config_key) for contract in CONTRACTS
    ] + [
        f"{contract.config_key}.{contract.child_type.config_key}"
        for contract in CONTRACTS if isinstance(contract, ParentContract)
    ],
    type=str,
)

validations = CORE_PARSER.add_argument(
    "--validations",
    help="Limit the execution to specific validations.",
    nargs="*",
    default=None,
    type=str,
)
