# dbt-contracts

[![PyPI Version](https://img.shields.io/pypi/v/{program_name_lower}?logo=pypi&label=Latest%20Version)](https://pypi.org/project/{program_name_lower})
[![Python Version](https://img.shields.io/pypi/pyversions/{program_name_lower}.svg?logo=python&label=Supported%20Python%20Versions)](https://pypi.org/project/{program_name_lower}/)
[![Documentation](https://img.shields.io/badge/Documentation-red.svg)]({documentation_url})
</br>
[![PyPI Downloads](https://img.shields.io/pypi/dm/{program_name_lower}?label=Downloads)](https://pypi.org/project/{program_name_lower}/)
[![Code Size](https://img.shields.io/github/languages/code-size/{program_owner_user}/{program_name_lower}?label=Code%20Size)](https://github.com/geo-martino/{program_name_lower})
[![Contributors](https://img.shields.io/github/contributors/{program_owner_user}/{program_name_lower}?logo=github&label=Contributors)](https://github.com/{program_owner_user}/{program_name_lower}/graphs/contributors)
[![License](https://img.shields.io/github/license/{program_owner_user}/{program_name_lower}?label=License)](https://github.com/geo-martino/{program_name_lower}/blob/master/LICENSE)
</br>
[![GitHub - Validate](https://github.com/geo-martino/{program_name_lower}/actions/workflows/validate.yml/badge.svg?branch=master)](https://github.com/{program_owner_user}/{program_name_lower}/actions/workflows/validate.yml)
[![GitHub - Deployment](https://github.com/{program_owner_user}/{program_name_lower}/actions/workflows/deploy.yml/badge.svg?event=release)](https://github.com/{program_owner_user}/{program_name_lower}/actions/workflows/deploy.yml)
[![GitHub - Documentation](https://github.com/{program_owner_user}/{program_name_lower}/actions/workflows/docs_publish.yml/badge.svg)](https://github.com/{program_owner_user}/{program_name_lower}/actions/workflows/docs_publish.yml)

### Enforce standards for your dbt projects through automated checks and generators

* Validate that the metadata and properties of objects in your project match required standards
* Automatically generate properties in your project from their related database objects
* Apply complex filtering and validation rules setting for highly granular operations
* Execute these operations as `pre-commit` hooks for automatic project validation

## Contents
* [Installation](#installation)
* [Quick Start](#quick-start)
* [Pre-commit configuration](#pre-commit-configuration)
* [Contracts Reference](#contracts-reference)
{contracts_reference_toc}
* [Release History](#release-history)
* [Contributing and Reporting Issues](#contributing-and-reporting-issues)

## Installation
Install through pip using one of the following commands:

```bash
pip install {program_name_lower}
```
```bash
python -m pip install {program_name_lower}
```

{program_name_lower} is best utilised when used in conjunction with `pre-commit` hooks.
Follow the installation guide for [`pre-commit`](https://pre-commit.com/#installation) to set this up if needed.

## Quick Start

1. Create a contracts file. By default, the package will look for a file named `{default_contracts_filename}`
   in the root of the repository. An example is provided below.
   For a full reference of the available configuration for this file,
   check out the [documentation]({documentation_url}).

2. If configured, run [`dbt-generate`]({documentation_url}/guides/commands.html)
   to generate properties files from database objects.
   It can be useful to run this before validations if your validations require properties 
   set which can be generated from database objects.

3. If configured, run [`dbt-validate`]({documentation_url}/guides/commands.html)
   to validate your contracts against the terms set in the configuration file.

4. Once you are satisfied with your configuration and the validations are passing,
   you may want to set [`pre-commit`]({documentation_url}/guides/precommit.html) hooks to automatically validate your project when running
   git commands against it.

### Example configuration

{contracts_example}

## Pre-commit configuration

This package is best utilised when used as in conjunction with `pre-commit` hooks.
Follow the installation guide below to set this up if needed.

Each contract operation is set up to take a list files that have changed since the last commit
as is required for `pre-commit` hooks to function as expected. 

Set up and add the `dbt-contracts` operations to your [`.pre-commit-hooks.yaml`](https://pre-commit.com/#2-add-a-pre-commit-configuration)
file like the example below.

```yaml
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
     - id: dbt-validate
       alias: dbt-validate-no-output
       name: Run models contracts
       stages: [pre-commit]
       args:
         - --contract
         - models
       additional_dependencies: [dbt-postgres]
     - id: dbt-validate
       alias: dbt-validate-no-output
       name: Run model columns contracts
       stages: [pre-commit]
       args:
         - --contract
         - models.columns
       additional_dependencies: [dbt-postgres]
     - id: dbt-validate
       alias: dbt-validate-no-output
       name: Run sources contracts
       stages: [pre-commit]
       args:
         - --contract
         - sources
       additional_dependencies: [dbt-postgres]
     - id: dbt-validate
       alias: dbt-validate-no-output
       name: Run source columns contracts
       stages: [pre-commit]
       args:
         - --contract
         - sources.columns
       additional_dependencies: [dbt-postgres]

     - id: dbt-validate
       alias: dbt-validate-output-annotations
       name: Run all contracts
       stages: [manual]
       args:
         - --format
         - github-annotations
         - --output
         - contracts_results.json
       additional_dependencies: [dbt-postgres]
     - id: dbt-generate
       name: Generate properties for all contracts
       stages: [manual]
       additional_dependencies: [dbt-postgres]
```

## Contracts Reference

Below you will find a list of all available contracts grouped by the dbt object it operates on.
Refer to this list to help when designing your contracts file.

{contracts_reference}

## Release History

For change and release history, 
check out the [documentation]({documentation_url}/info/release-history.html).


## Contributing and Reporting Issues

If you have any suggestions, wish to contribute, or have any issues to report, please do let me know 
via the issues tab or make a new pull request with your new feature for review. 

For more info on how to contribute to {program_name}, 
check out the [documentation]({documentation_url}/info/contributing.html).


I hope you enjoy using {program_name}!
