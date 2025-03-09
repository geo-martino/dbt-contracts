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

## Contents
* [Installation](#installation)
* [Contracts Reference](#contracts-reference)
  * [Models](#models)
  * [Model Columns](#model-columns)
  * [Sources](#sources)
  * [Source Columns](#source-columns)
  * [Macros](#macros)
  * [Macro Arguments](#macro-arguments)

## Installation
Install through pip using one of the following commands:

```bash
pip install dbt-contracts
```
```bash
python -m pip install dbt-contracts
```

## Getting Started

#### TODO

## Contracts Reference

Below you will find a list of all available contracts grouped by the dbt object it operates on.
Refer to this list to help when designing your contract file.

### Models

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/models.html#name): Filter models based on their names.
- [`path`](https://geo-martino.github.io/dbt-contracts/reference/models.html#path): Filter models based on their paths.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/models.html#tag): Filter models based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/models.html#meta): Filter models based on their meta values.
- [`is_materialized`](https://geo-martino.github.io/dbt-contracts/reference/models.html#is-materialized): Filter models taking only those which are not ephemeral.

#### Terms

- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-properties): Check whether the models have properties files defined.
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-description): Check whether the models have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-required-tags): Check whether the models have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-allowed-tags): Check whether the models have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-required-meta-keys): Check whether the models have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-allowed-meta-keys): Check whether the models have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-allowed-meta-values): Check whether the models have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/models.html#exists): Check whether the models exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-tests): Check whether models have an appropriate number of tests configured.
- [`has_all_columns`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-all-columns): Check whether models have all columns set in their properties.
- [`has_expected_columns`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-expected-columns): Check whether models have the expected names of columns set in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-matching-description): Check whether the descriptions configured in models' properties match the descriptions in the database.
- [`has_contract`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-contract): Check whether models have appropriate configuration for a contract in their properties.
- [`has_valid_ref_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-valid-ref-dependencies): Check whether models have an appropriate number of upstream dependencies
- [`has_valid_source_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-valid-source-dependencies): Check whether models have an appropriate number of upstream dependencies for sources
- [`has_valid_macro_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-valid-macro-dependencies): Check whether models have an appropriate number of upstream dependencies for macros
- [`has_no_final_semicolon`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-no-final-semicolon): Check if models have a final semicolon present in their queries.
- [`has_no_hardcoded_refs`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-no-hardcoded-refs): Check if models have any hardcoded references to database objects in their queries.
- [`has_constraints`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-constraints): Check whether models have an appropriate number of constraints configured in their properties.

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/models.html#generators) to automatically and dynamically generate properties files for these models from database objects

### Model Columns

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#name): Filter model columns based on their names.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#tag): Filter model columns based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta): Filter model columns based on their meta values.

#### Terms

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-description): Check whether the model columns have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-required-tags): Check whether the model columns have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-allowed-tags): Check whether the model columns have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-required-meta-keys): Check whether the model columns have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-allowed-meta-keys): Check whether the model columns have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-allowed-meta-values): Check whether the model columns have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#exists): Check whether the columns exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-tests): Check whether columns have an appropriate number of tests configured.
- [`has_expected_name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-expected-name): Check whether columns have an expected name based on their data type.
- [`has_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-data-type): Check whether columns have a data type configured in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-description): Check whether the descriptions configured in columns' properties matches the descriptions in the database.
- [`has_matching_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-data-type): Check whether the data type configured in a column's properties matches the data type in the database.
- [`has_matching_index`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-index): Check whether the index position within the properties of a column's table

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/columns.html#generators) to automatically and dynamically generate properties files for these columns from database objects

### Sources

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#name): Filter sources based on their names.
- [`path`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#path): Filter sources based on their paths.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#tag): Filter sources based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#meta): Filter sources based on their meta values.
- [`is_enabled`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#is-enabled): Filter sources taking only those which are enabled.

#### Terms

- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-properties): Check whether the sources have properties files defined.
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-description): Check whether the sources have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-required-tags): Check whether the sources have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-allowed-tags): Check whether the sources have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-required-meta-keys): Check whether the sources have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-allowed-meta-keys): Check whether the sources have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-allowed-meta-values): Check whether the sources have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#exists): Check whether the sources exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-tests): Check whether sources have an appropriate number of tests configured.
- [`has_all_columns`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-all-columns): Check whether sources have all columns set in their properties.
- [`has_expected_columns`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-expected-columns): Check whether sources have the expected names of columns set in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-matching-description): Check whether the descriptions configured in sources' properties match the descriptions in the database.
- [`has_loader`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-loader): Check whether sources have appropriate configuration for a loader in their properties.
- [`has_freshness`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-freshness): Check whether sources have freshness configured in their properties.
- [`has_downstream_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-downstream-dependencies): Check whether sources have an appropriate number of downstream dependencies.

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/sources.html#generators) to automatically and dynamically generate properties files for these sources from database objects

### Source Columns

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#name): Filter source columns based on their names.
- [`tag`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#tag): Filter source columns based on their tags.
- [`meta`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta): Filter source columns based on their meta values.

#### Terms

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-description): Check whether the source columns have descriptions defined in their properties.
- [`has_required_tags`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-required-tags): Check whether the source columns have the expected set of required tags set.
- [`has_allowed_tags`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-allowed-tags): Check whether the source columns have only tags set from a configured permitted list.
- [`has_required_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-required-meta-keys): Check whether the source columns have the expected set of required meta keys set.
- [`has_allowed_meta_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-allowed-meta-keys): Check whether the source columns have only meta keys set from a configured permitted list.
- [`has_allowed_meta_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-allowed-meta-values): Check whether the source columns have only meta values set from a configured permitted mapping of keys to values.
- [`exists`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#exists): Check whether the columns exist in the database.
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-tests): Check whether columns have an appropriate number of tests configured.
- [`has_expected_name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-expected-name): Check whether columns have an expected name based on their data type.
- [`has_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-data-type): Check whether columns have a data type configured in their properties.
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-description): Check whether the descriptions configured in columns' properties matches the descriptions in the database.
- [`has_matching_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-data-type): Check whether the data type configured in a column's properties matches the data type in the database.
- [`has_matching_index`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-index): Check whether the index position within the properties of a column's table

You may also [configure a generator](https://geo-martino.github.io/dbt-contracts/reference/columns.html#generators) to automatically and dynamically generate properties files for these columns from database objects

### Macros

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#name): Filter macros based on their names.
- [`path`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#path): Filter macros based on their paths.

#### Terms

- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#has-properties): Check whether the macros have properties files defined.
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#has-description): Check whether the macros have descriptions defined in their properties.


### Macro Arguments

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/arguments.html#name): Filter macro arguments based on their names.

#### Terms

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/arguments.html#has-description): Check whether the macro arguments have descriptions defined in their properties.
- [`has_type`](https://geo-martino.github.io/dbt-contracts/reference/arguments.html#has-type): Check whether macro arguments have a data type configured in their properties.

## Motivation and Aims

#### TODO

## Release History

For change and release history, 
check out the [documentation](https://geo-martino.github.io/dbt-contracts/info/release-history.html).


## Contributing and Reporting Issues

If you have any suggestions, wish to contribute, or have any issues to report, please do let me know 
via the issues tab or make a new pull request with your new feature for review. 

For more info on how to contribute to dbt-contracts, 
check out the [documentation](https://geo-martino.github.io/dbt-contracts/info/contributing.html).


I hope you enjoy using dbt-contracts!
