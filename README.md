# dbt-contracts

[![PyPI Version](https://img.shields.io/pypi/v/dbt-contracts?logo=pypi&label=Latest%20Version)](https://pypi.org/project/dbt-contracts)
[![Python Version](https://img.shields.io/pypi/pyversions/dbt-contracts.svg?logo=python&label=Supported%20Python%20Versions)](https://pypi.org/project/dbt-contracts/)
[![Documentation](https://img.shields.io/badge/Documentation-red.svg)](https://geo-martino.github.io/dbt-contracts)
</br>
[![PyPI Downloads](https://img.shields.io/pypi/dm/dbt-contracts?label=Downloads)](https://pypi.org/project/dbt-contracts/)
[![Code Size](https://img.shields.io/github/languages/code-size/geo-martino/dbt-contracts?label=Code%20Size)](https://github.com/geo-martino/dbt-contracts)
[![Contributors](https://img.shields.io/github/contributors/geo-martino/dbt-contracts?logo=github&label=Contributors)](https://github.com/geo-martino/dbt-contracts/graphs/contributors)
[![License](https://img.shields.io/github/license/geo-martino/dbt-contracts?label=License)](https://github.com/geo-martino/dbt-contracts/blob/master/LICENSE)
</br>
[![GitHub - Validate](https://github.com/geo-martino/dbt-contracts/actions/workflows/validate.yml/badge.svg?branch=master)](https://github.com/geo-martino/dbt-contracts/actions/workflows/validate.yml)
[![GitHub - Deployment](https://github.com/geo-martino/dbt-contracts/actions/workflows/deploy.yml/badge.svg?event=release)](https://github.com/geo-martino/dbt-contracts/actions/workflows/deploy.yml)
[![GitHub - Documentation](https://github.com/geo-martino/dbt-contracts/actions/workflows/docs_publish.yml/badge.svg)](https://github.com/geo-martino/dbt-contracts/actions/workflows/docs_publish.yml)

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
  * [Models](#models)
  * [Model Columns](#model-columns)
  * [Sources](#sources)
  * [Source Columns](#source-columns)
  * [Macros](#macros)
  * [Macro Arguments](#macro-arguments)
* [Release History](#release-history)
* [Contributing and Reporting Issues](#contributing-and-reporting-issues)

## Installation
Install through pip using one of the following commands:

```bash
pip install dbt-contracts
```
```bash
python -m pip install dbt-contracts
```

dbt-contracts is best utilised when used in conjunction with `pre-commit` hooks.
Follow the installation guide for [`pre-commit`](https://pre-commit.com/#installation) to set this up if needed.

## Quick Start

1. Create a contracts file. By default, the package will look for a file named `contracts`
   in the root of the repository. An example is provided below.
   For a full reference of the available configuration for this file,
   check out the [documentation](https://geo-martino.github.io/dbt-contracts).

2. If configured, run [`dbt-generate`](https://geo-martino.github.io/dbt-contracts/guides/commands.html)
   to generate properties files from database objects.
   It can be useful to run this before validations if your validations require properties 
   set which can be generated from database objects.

3. If configured, run [`dbt-validate`](https://geo-martino.github.io/dbt-contracts/guides/commands.html)
   to validate your contracts against the terms set in the configuration file.

4. Once you are satisfied with your configuration and the validations are passing,
   you may want to set [`pre-commit`](https://geo-martino.github.io/dbt-contracts/guides/precommit.html) hooks to automatically validate your project when running
   git commands against it.

### Example configuration

   ```yaml
   contracts:
     macros:
     - filter:
       - path:
           include: &id002
           - ^\w+\d+\s{1,3}$
           - include[_-]this
           exclude: &id001
           - ^\w+\d+\s{1,3}$
           - exclude[_-]this
           match_all: true
       validations:
       - has_properties
       arguments:
       - filter:
         - name:
             include: .*i\s+am\s+a\s+regex\s+pattern.*
             exclude: *id001
             match_all: true
         validations:
         - has_description
     sources:
     - filter:
       - tag:
           tags:
           - tag1
           - tag2
       - name:
           include: *id002
           exclude: .*i\s+am\s+a\s+regex\s+pattern.*
           match_all: false
       - path:
           include: .*i\s+am\s+a\s+regex\s+pattern.*
           exclude: *id001
           match_all: false
       validations:
       - has_tests:
           min_count: 2
           max_count: 6
       - has_required_tags:
           tags:
           - tag1
           - tag2
       - has_properties
       - has_required_meta_keys:
           keys:
           - key1
           - key2
       - has_allowed_tags:
           tags: &id003
           - tag1
           - tag2
       - exists
       generator:
         exclude:
         - columns
         - description
         filename: config.yml
         depth: 2
         description:
           overwrite: false
           terminator: __END__
         columns:
           overwrite: false
           add: true
           remove: false
           order: false
       columns:
       - filter:
         - meta:
             meta:
               key1: val1
               key2:
               - val2
               - val3
         - tag:
             tags: tag1
         validations:
         - has_matching_data_type:
             ignore_whitespace: true
             case_insensitive: false
             compare_start_only: false
         - has_description
         - has_matching_description:
             ignore_whitespace: false
             case_insensitive: true
             compare_start_only: false
         - has_allowed_meta_keys:
             keys: key1
         - has_matching_index:
             ignore_whitespace: true
             case_insensitive: false
             compare_start_only: true
         - has_data_type
         - has_tests:
             min_count: 2
             max_count: 4
         - has_allowed_tags:
             tags: *id003
         generator:
           exclude: data_type
           description:
             overwrite: true
             terminator: \n
           data_type:
             overwrite: false
   ```

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

### Models

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#name): Filter models based on their names.
- [`path`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#path): Filter models based on their paths.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#tag): Filter models based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#meta): Filter models based on their meta values.
- [`is_materialized`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#is-materialized): Filter models taking only those which are not ephemeral.

#### Terms

- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-properties): Check whether the models have properties files defined.
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-description): Check whether the models have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-required-tags): Check whether the models have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-allowed-tags): Check whether the models have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-required-meta-keys): Check whether the models have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-allowed-meta-keys): Check whether the models have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-allowed-meta-values): Check whether the models have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#exists): Check whether the models exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-tests): Check whether models have an appropriate number of tests configured.
- [`has_all_columns`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-all-columns): Check whether models have all columns set in their properties.
- [`has_expected_columns`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-expected-columns): Check whether models have the expected names of columns set in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-matching-description): Check whether the descriptions configured in models' properties match the descriptions in the database.
- [`has_contract`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-contract): Check whether models have appropriate configuration for a contract in their properties.
- [`has_valid_ref_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-valid-ref-dependencies): Check whether models have an appropriate number of upstream dependencies
- [`has_valid_source_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-valid-source-dependencies): Check whether models have an appropriate number of upstream dependencies for sources
- [`has_valid_macro_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-valid-macro-dependencies): Check whether models have an appropriate number of upstream dependencies for macros
- [`has_no_final_semicolon`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-no-final-semicolon): Check if models have a final semicolon present in their queries.
- [`has_no_hardcoded_refs`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-no-hardcoded-refs): Check if models have any hardcoded references to database objects in their queries.
- [`has_constraints`](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#has-constraints): Check whether models have an appropriate number of constraints configured in their properties.

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/contracts/models.html#generator) to automatically and dynamically generate properties files for these models from database objects.

### Model Columns

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#name): Filter model columns based on their names.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#tag): Filter model columns based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#meta): Filter model columns based on their meta values.

#### Terms

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-description): Check whether the model columns have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-required-tags): Check whether the model columns have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-allowed-tags): Check whether the model columns have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-required-meta-keys): Check whether the model columns have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-allowed-meta-keys): Check whether the model columns have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-allowed-meta-values): Check whether the model columns have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#exists): Check whether the columns exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-tests): Check whether columns have an appropriate number of tests configured.
- [`has_expected_name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-expected-name): Check whether columns have an expected name based on their data type.
- [`has_data_type`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-data-type): Check whether columns have a data type configured in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-matching-description): Check whether the descriptions configured in columns' properties matches the descriptions in the database.
- [`has_matching_data_type`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-matching-data-type): Check whether the data type configured in a column's properties matches the data type in the database.
- [`has_matching_index`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-matching-index): Check whether the index position within the properties of a column's table

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#generator) to automatically and dynamically generate properties files for these columns from database objects.

### Sources

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#name): Filter sources based on their names.
- [`path`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#path): Filter sources based on their paths.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#tag): Filter sources based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#meta): Filter sources based on their meta values.
- [`is_enabled`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#is-enabled): Filter sources taking only those which are enabled.

#### Terms

- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-properties): Check whether the sources have properties files defined.
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-description): Check whether the sources have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-required-tags): Check whether the sources have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-allowed-tags): Check whether the sources have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-required-meta-keys): Check whether the sources have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-allowed-meta-keys): Check whether the sources have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-allowed-meta-values): Check whether the sources have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#exists): Check whether the sources exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-tests): Check whether sources have an appropriate number of tests configured.
- [`has_all_columns`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-all-columns): Check whether sources have all columns set in their properties.
- [`has_expected_columns`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-expected-columns): Check whether sources have the expected names of columns set in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-matching-description): Check whether the descriptions configured in sources' properties match the descriptions in the database.
- [`has_loader`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-loader): Check whether sources have appropriate configuration for a loader in their properties.
- [`has_freshness`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-freshness): Check whether sources have freshness configured in their properties.
- [`has_downstream_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#has-downstream-dependencies): Check whether sources have an appropriate number of downstream dependencies.

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/contracts/sources.html#generator) to automatically and dynamically generate properties files for these sources from database objects.

### Source Columns

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#name): Filter source columns based on their names.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#tag): Filter source columns based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#meta): Filter source columns based on their meta values.

#### Terms

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-description): Check whether the source columns have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-required-tags): Check whether the source columns have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-allowed-tags): Check whether the source columns have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-required-meta-keys): Check whether the source columns have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-allowed-meta-keys): Check whether the source columns have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-allowed-meta-values): Check whether the source columns have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#exists): Check whether the columns exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-tests): Check whether columns have an appropriate number of tests configured.
- [`has_expected_name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-expected-name): Check whether columns have an expected name based on their data type.
- [`has_data_type`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-data-type): Check whether columns have a data type configured in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-matching-description): Check whether the descriptions configured in columns' properties matches the descriptions in the database.
- [`has_matching_data_type`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-matching-data-type): Check whether the data type configured in a column's properties matches the data type in the database.
- [`has_matching_index`](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#has-matching-index): Check whether the index position within the properties of a column's table

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/contracts/columns.html#generator) to automatically and dynamically generate properties files for these columns from database objects.

### Macros

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/macros.html#name): Filter macros based on their names.
- [`path`](https://geo-martino.github.io/dbt-contracts/reference/contracts/macros.html#path): Filter macros based on their paths.

#### Terms

- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/contracts/macros.html#has-properties): Check whether the macros have properties files defined.
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/macros.html#has-description): Check whether the macros have descriptions defined in their properties.


### Macro Arguments

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/contracts/arguments.html#name): Filter macro arguments based on their names.

#### Terms

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/contracts/arguments.html#has-description): Check whether the macro arguments have descriptions defined in their properties.
- [`has_type`](https://geo-martino.github.io/dbt-contracts/reference/contracts/arguments.html#has-type): Check whether macro arguments have a data type configured in their properties.

## Release History

For change and release history, 
check out the [documentation](https://geo-martino.github.io/dbt-contracts/info/release-history.html).


## Contributing and Reporting Issues

If you have any suggestions, wish to contribute, or have any issues to report, please do let me know 
via the issues tab or make a new pull request with your new feature for review. 

For more info on how to contribute to dbt-contracts, 
check out the [documentation](https://geo-martino.github.io/dbt-contracts/info/contributing.html).


I hope you enjoy using dbt-contracts!
