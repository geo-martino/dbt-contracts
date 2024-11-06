from argparse import Namespace
from pathlib import Path

from dbt_contracts.dbt_cli import get_config, clean_paths, install_dependencies
from dbt_contracts.runner import ContractRunner


def main():
    config = get_config()

    if config.args.config is None and config.args.project_dir:
        config.args.config = config.args.project_dir
    if config.args.output is None:
        config.args.output = Path(config.project_root, config.target_path)

    if config.args.clean:
        clean_paths()
    if config.args.deps:
        install_dependencies()

    runner = ContractRunner.from_config(config)
    if config.args.files:
        runner.paths = config.args.files

    results = runner.run(contract_key=config.args.contract, enforcements=config.args.enforce)

    if config.args.format:
        runner.write_results(results, format_type=config.args.format, output=config.args.output)

    if not config.args.no_fail and results:
        raise Exception(f"Found {len(results)} contract violations.")


if __name__ == "__main__":
    main()
