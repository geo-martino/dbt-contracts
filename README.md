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

## Contracts Reference

Below you will find a list of all available contracts grouped by the dbt object it operates on.
Refer to this list to help when designing your contract file.

### Models

#### Filters

- [`is_materialized`](https://geo-martino.github.io/dbt-contracts/reference/models.html#is-materialized): Check whether the given `node` is configured to be materialized
- [`name`](https://geo-martino.github.io/dbt-contracts/reference/models.html#name): Check whether a given `item` has a valid name
- [`paths`](https://geo-martino.github.io/dbt-contracts/reference/models.html#paths): Check whether a given `item` has a valid path

#### Validations

- [`has_all_columns`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-all-columns): Check whether the node properties contain all available columns of the node
- [`has_constraints`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-constraints): Check whether the given `node` has an appropriate number of constraints
- [`has_contract`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-contract): Check whether the node properties define a contract
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-description): Check whether the given `resource` has a description set
- [`has_expected_columns`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-expected-columns): Check whether the node properties contain the expected set of `columns`
- [`has_no_final_semicolon`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-no-final-semicolon): Check whether the given `node` has a no closing semicolon at the end of the script
- [`has_no_hardcoded_refs`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-no-hardcoded-refs): Check whether the given `node` has a no hardcoded upstream references i
- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-properties): Check whether the given `resource` has properties set in an appropriate properties file
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-tests): Check whether the given `node` has an appropriate number of tests
- [`has_valid_macro_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-valid-macro-dependencies): Check whether the given `node` has valid upstream macro dependencies i
- [`has_valid_ref_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-valid-ref-dependencies): Check whether the given `node` has valid upstream ref dependencies i
- [`has_valid_source_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/models.html#has-valid-source-dependencies): Check whether the given `node` has valid upstream source dependencies i
- [`meta_has_accepted_values`](https://geo-martino.github.io/dbt-contracts/reference/models.html#meta-has-accepted-values): Check whether the resource's `meta` config is configured as expected
- [`meta_has_allowed_keys`](https://geo-martino.github.io/dbt-contracts/reference/models.html#meta-has-allowed-keys): Check whether the resource's `meta` config contains only allowed keys
- [`meta_has_required_keys`](https://geo-martino.github.io/dbt-contracts/reference/models.html#meta-has-required-keys): Check whether the resource's `meta` config contains all required keys
- [`tags_have_allowed_values`](https://geo-martino.github.io/dbt-contracts/reference/models.html#tags-have-allowed-values): Check whether the given `resource` has properties set in an appropriate properties file
- [`tags_have_required_values`](https://geo-martino.github.io/dbt-contracts/reference/models.html#tags-have-required-values): Check whether the given `resource` has properties set in an appropriate properties file


### Model Columns

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#name): Check whether a given `item` has a valid name

#### Validations

- [`has_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-data-type): Check whether the given `column` of the given `parent` has a data type set
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-description): Check whether the given `resource` has a description set
- [`has_expected_name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-expected-name): Check whether the given `column` of the given `parent` has a name that matches some expectation
- [`has_matching_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-data-type): Check whether the given `column` of the given `parent` has a data type configured which matches the remote resource
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-description): Check whether the given `column` of the given `parent` has a description configured which matches the remote resource
- [`has_matching_index`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-index): Check whether the given `column` of the given `parent` is in the same position in the dbt config as the remote resource
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-tests): Check whether the given `column` of the given `parent` has an appropriate number of tests
- [`meta_has_accepted_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta-has-accepted-values): Check whether the resource's `meta` config is configured as expected
- [`meta_has_allowed_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta-has-allowed-keys): Check whether the resource's `meta` config contains only allowed keys
- [`meta_has_required_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta-has-required-keys): Check whether the resource's `meta` config contains all required keys
- [`tags_have_allowed_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#tags-have-allowed-values): Check whether the given `resource` has properties set in an appropriate properties file
- [`tags_have_required_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#tags-have-required-values): Check whether the given `resource` has properties set in an appropriate properties file


### Sources

#### Filters

- [`is_enabled`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#is-enabled): Check whether the given `source` is enabled
- [`name`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#name): Check whether a given `item` has a valid name
- [`paths`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#paths): Check whether a given `item` has a valid path

#### Validations

- [`has_all_columns`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-all-columns): Check whether the node properties contain all available columns of the node
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-description): Check whether the given `resource` has a description set
- [`has_downstream_dependencies`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-downstream-dependencies): Check whether the given `source` has freshness configured
- [`has_expected_columns`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-expected-columns): Check whether the node properties contain the expected set of `columns`
- [`has_freshness`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-freshness): Check whether the given `source` has freshness configured
- [`has_loader`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-loader): Check whether the given `source` has a loader configured
- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-properties): Check whether the given `resource` has properties set in an appropriate properties file
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#has-tests): Check whether the given `node` has an appropriate number of tests
- [`meta_has_accepted_values`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#meta-has-accepted-values): Check whether the resource's `meta` config is configured as expected
- [`meta_has_allowed_keys`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#meta-has-allowed-keys): Check whether the resource's `meta` config contains only allowed keys
- [`meta_has_required_keys`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#meta-has-required-keys): Check whether the resource's `meta` config contains all required keys
- [`tags_have_allowed_values`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#tags-have-allowed-values): Check whether the given `resource` has properties set in an appropriate properties file
- [`tags_have_required_values`](https://geo-martino.github.io/dbt-contracts/reference/sources.html#tags-have-required-values): Check whether the given `resource` has properties set in an appropriate properties file


### Source Columns

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#name): Check whether a given `item` has a valid name

#### Validations

- [`has_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-data-type): Check whether the given `column` of the given `parent` has a data type set
- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-description): Check whether the given `resource` has a description set
- [`has_expected_name`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-expected-name): Check whether the given `column` of the given `parent` has a name that matches some expectation
- [`has_matching_data_type`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-data-type): Check whether the given `column` of the given `parent` has a data type configured which matches the remote resource
- [`has_matching_description`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-description): Check whether the given `column` of the given `parent` has a description configured which matches the remote resource
- [`has_matching_index`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-matching-index): Check whether the given `column` of the given `parent` is in the same position in the dbt config as the remote resource
- [`has_tests`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#has-tests): Check whether the given `column` of the given `parent` has an appropriate number of tests
- [`meta_has_accepted_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta-has-accepted-values): Check whether the resource's `meta` config is configured as expected
- [`meta_has_allowed_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta-has-allowed-keys): Check whether the resource's `meta` config contains only allowed keys
- [`meta_has_required_keys`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#meta-has-required-keys): Check whether the resource's `meta` config contains all required keys
- [`tags_have_allowed_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#tags-have-allowed-values): Check whether the given `resource` has properties set in an appropriate properties file
- [`tags_have_required_values`](https://geo-martino.github.io/dbt-contracts/reference/columns.html#tags-have-required-values): Check whether the given `resource` has properties set in an appropriate properties file


### Macros

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#name): Check whether a given `item` has a valid name
- [`paths`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#paths): Check whether a given `item` has a valid path

#### Validations

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#has-description): Check whether the given `resource` has a description set
- [`has_properties`](https://geo-martino.github.io/dbt-contracts/reference/macros.html#has-properties): Check whether the given `resource` has properties set in an appropriate properties file


### Macro Arguments

#### Filters

- [`name`](https://geo-martino.github.io/dbt-contracts/reference/arguments.html#name): Check whether a given `item` has a valid name

#### Validations

- [`has_description`](https://geo-martino.github.io/dbt-contracts/reference/arguments.html#has-description): Check whether the given `resource` has a description set
- [`has_type`](https://geo-martino.github.io/dbt-contracts/reference/arguments.html#has-type): Check whether the given `argument` has its type set in an appropriate properties file
