.. _quickstart:
===========
Quick Start
===========

1. Create a contracts file. By default, the package will look for a file named `contracts.yml`
   in the root of the repository. An example is provided below.
   For a full reference of the available configuration for this file,
   please refer to the contracts reference in the sidebar.

2. If configured, run `dbt-generate` to generate properties files from database objects.
   It can be useful to run this before validations if your validations require properties
   set which can be generated from database objects.
   See :ref:`commands` for more info on this operation.

3. If configured, run `dbt-validate` to validate your contracts against the terms set in the configuration file.
   See :ref:`commands` for more info on this operation.

4. Once you are satisfied with your configuration and the validations are passing,
   you may want to set :ref:`precommit` hooks to automatically validate your project when running
   git commands against it. Follow the installation guide for `pre-commit <https://pre-commit.com/#installation>`_ to set this up if needed.

Example configuration
=====================

.. code-block:: yaml

   contracts:
     macros:
     - filter:
       - path:
           include: .*i\s+am\s+a\s+regex\s+pattern.*
           exclude: &id001
           - ^\w+\d+\s{1,3}$
           - exclude[_-]this
           match_all: false
       validations:
       - has_description
       arguments:
       - filter:
         - name:
             include:
             - ^\w+\d+\s{1,3}$
             - include[_-]this
             exclude: *id001
             match_all: false
         validations:
         - has_description
     models:
     - filter:
       - is_materialized
       - meta:
           meta:
             key1: val1
             key2:
             - val2
             - val3
       - name:
           include: .*i\s+am\s+a\s+regex\s+pattern.*
           exclude: .*i\s+am\s+a\s+regex\s+pattern.*
           match_all: false
       validations:
       - has_allowed_meta_values:
           meta: &id002
             key1: val1
             key2:
             - val2
             - val3
       - has_required_meta_keys:
           keys:
           - key1
           - key2
       - has_contract
       - has_required_tags:
           tags: tag1
       - has_valid_ref_dependencies
       - has_valid_source_dependencies
       - has_no_final_semicolon
       - has_allowed_tags:
           tags:
           - tag1
           - tag2
       - has_matching_description:
           ignore_whitespace: true
           case_insensitive: true
           compare_start_only: false
       - has_no_hardcoded_refs
       - has_description
       - has_tests:
           min_count: 2
           max_count: 4
       generator:
         exclude:
         - columns
         - description
         filename: config.yml
         depth: 1
         description:
           overwrite: true
           terminator: \n
         columns:
           overwrite: false
           add: true
           remove: false
           order: false
       columns:
       - filter:
         - tag:
             tags: tag1
         - name:
             include: .*i\s+am\s+a\s+regex\s+pattern.*
             exclude: *id001
             match_all: true
         validations:
         - has_matching_description:
             ignore_whitespace: true
             case_insensitive: false
             compare_start_only: true
         - has_description
         - has_allowed_tags:
             tags: tag1
         - has_data_type
         - exists
         - has_allowed_meta_values:
             meta: *id002
         - has_matching_data_type:
             ignore_whitespace: true
             case_insensitive: false
             compare_start_only: false
         - has_required_meta_keys:
             keys: key1
         - has_matching_index:
             ignore_whitespace: false
             case_insensitive: false
             compare_start_only: false
         - has_allowed_meta_keys:
             keys:
             - key1
             - key2
         generator:
           exclude: data_type
           description:
             overwrite: false
             terminator: __END__
           data_type:
             overwrite: false
