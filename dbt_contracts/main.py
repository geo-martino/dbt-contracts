from pathlib import Path

from dbt_contracts.cli import CORE_PARSER
from dbt_contracts.dbt_cli import get_config, clean_paths, install_dependencies
from dbt_contracts.runner import ContractsRunner


def main():
    """Main entry point for the CLI"""
    config = get_config(CORE_PARSER)

    if config.args.config is None and config.args.project_dir:
        config.args.config = config.args.project_dir
    if config.args.output is None:
        config.args.output = Path(config.project_root, config.target_path)

    if config.args.clean:
        clean_paths(config=config)
    if config.args.deps:
        install_dependencies(config=config)

    runner = ContractsRunner.from_config(config)
    runner.config = config
    if config.args.files:
        runner.paths = config.args.files

    results = runner.validate(contract_key=config.args.contract, terms=config.args.enforce)

    if config.args.format:
        runner.write_results(results, path=config.args.output, output_type=config.args.format)

    if not config.args.no_fail and results:
        raise Exception(f"Found {len(results)} contract violations.")


if __name__ == "__main__":
    main()
