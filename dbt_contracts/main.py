from pathlib import Path

from dbt_contracts.cli import CORE_PARSER
from dbt_contracts.dbt_cli import get_config, clean_paths, install_dependencies
from dbt_contracts.runner import ContractsRunner


def main():
    """Main entry point for the CLI"""
    conf = get_config(CORE_PARSER)

    if conf.args.config is None and conf.args.project_dir:
        conf.args.config = conf.args.project_dir
    if conf.args.output is None:
        conf.args.output = Path(conf.project_root, conf.target_path)

    if conf.args.clean:
        clean_paths()
    if conf.args.deps:
        install_dependencies()

    runner = ContractsRunner.from_config(conf)
    if conf.args.files:
        runner.paths = conf.args.files

    results = runner.validate(contract_key=conf.args.contract, terms=conf.args.enforce)

    if conf.args.format:
        runner.write_results(results, path=conf.args.output, output_type=conf.args.format)

    if not conf.args.no_fail and results:
        raise Exception(f"Found {len(results)} contract violations.")


if __name__ == "__main__":
    main()
