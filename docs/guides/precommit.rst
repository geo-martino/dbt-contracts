
.. _precommit:

Pre-commit
==========

This package is best utilised when used as in conjunction with `pre-commit` hooks.
Follow the installation guide below to set this up if needed.

Each contract operation is set up to take a list files that have changed since the last commit
as is required for pre-commit hooks to function as expected.

Set up and add the `dbt-contracts` operations to your `.pre-commit-hooks.yaml <https://pre-commit.com/#2-add-a-pre-commit-configuration>`_
file like the example below.

.. code-block:: yaml

  default_stages: [manual]

  repos:
    - repo: meta
      hooks:
        - id: identity
          name: List files
          stages: [ manual, pre-commit ]
    - repo: https://github.com/geo-martino/dbt-contracts
      rev: v1.0.0
      hooks:
        - id: dbt-clean
          stages: [manual, pre-commit]
          additional_dependencies: [dbt-postgres]
        - id: dbt-deps
          stages: [manual]
          additional_dependencies: [dbt-postgres]
        - id: run-contracts
          alias: run-contracts-no-output
          name: Run models contracts
          stages: [pre-commit]
          args:
            - --contract
            - models
          additional_dependencies: [dbt-postgres]
        - id: run-contracts
          alias: run-contracts-no-output
          name: Run model columns contracts
          stages: [pre-commit]
          args:
            - --contract
            - models.columns
          additional_dependencies: [dbt-postgres]
        - id: run-contracts
          alias: run-contracts-no-output
          name: Run macro contracts
          stages: [pre-commit]
          args:
            - --contract
            - macros
          additional_dependencies: [dbt-postgres]
        - id: run-contracts
          alias: run-contracts-no-output
          name: Run macro arguments contracts
          stages: [pre-commit]
          args:
            - --contract
            - macros.arguments
          additional_dependencies: [dbt-postgres]

        - id: run-contracts
          alias: run-contracts-output-annotations
          name: Run all contracts
          stages: [manual]
          args:
            - --format
            - github-annotations
            - --output
            - contracts_results.json
          additional_dependencies: [dbt-postgres]
