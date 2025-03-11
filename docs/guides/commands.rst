.. _commands:

Commands
========

This package provides various CLI commands you may use to execute key operations on your dbt project.

All commands provide a set of additional arguments that you may use to configure their operation.
Simple run the command with the ``--help`` flag to view these options.

- `dbt-clean` - Runs `dbt clean`. Delete all folders in the clean-targets list (usually the dbt_packages and
  target directories.)
- `dbt-deps` - Runs `dbt deps`. Installs dbt packages specified.
- `dbt-parse` - Runs `dbt parse`. Parses the project and generate the manifest artifact.
- `dbt-docs` - Runs `dbt docs generate`. Generate the documentation website thereby generating the catalog artifact.
- `dbt-validate` - Run contract validations against a dbt project.
- `dbt-generate` - Generate properties files from database objects for a dbt project.
