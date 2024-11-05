from pathlib import Path

from dbt_contracts.dbt_cli import get_config, clean_paths, install_dependencies
from dbt_contracts.runner import ContractRunner


def main():
    config = get_config()

    if config.args.config is None:
        config.args.config = config.args.project_dir
    if config.args.output is None:
        config.args.output = Path(config.project_root, config.target_path)

    if config.args.clean:
        clean_paths()
    if config.args.deps:
        install_dependencies()

    runner = ContractRunner.from_yaml(config.args.config)
    results = runner.run(contract=config.args.contract, validations=config.args.validations)

    if config.args.format:
        runner.write_results(results, format=config.args.format, output=config.args.output)


if __name__ == "__main__":
    main()
