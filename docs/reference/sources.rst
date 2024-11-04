=======
Sources
=======

Main configuration
==================

Filters
-------

Filters for reducing the scope of the contract.
You may limit the number of sources processed by the rules of this contract by defining one or more of the following filters

``is_enabled``
^^^^^^^^^^^^^^

Check whether the given `source` is enabled.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``name``
^^^^^^^^

Check whether a given `item` has a valid name.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **patterns** - Patterns to match against for paths to include
      
      You may define the patterns as a list of values i.e.
      
      .. code-block:: yaml
      
         name:
           - <str>
           - <str>
           - ...


``paths``
^^^^^^^^^

Check whether a given `item` has a valid path.
Paths must match patterns which are relative to directory of the dbt project.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **patterns** - Patterns to match against for paths to include
      
      You may define the patterns as a list of values i.e.
      
      .. code-block:: yaml
      
         paths:
           - <str>
           - <str>
           - ...


Validations
-----------

Validations to apply to the resources of this contract.
These enforce certain standards that must be followed in order for the contract to be fulfilled.

``has_all_columns``
^^^^^^^^^^^^^^^^^^^

Check whether the node properties contain all available columns of the node.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_description``
^^^^^^^^^^^^^^^^^^^

Check whether the given `resource` has a description set.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_downstream_dependencies``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `source` has freshness configured.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   You may define the following keyword arguments: 
     - `min_count` (``int``) - The minimum number of downstream dependencies allowed
     - `max_count` (``int``) - The maximum number of downstream dependencies allowed. When None, no upper limit


``has_expected_columns``
^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the node properties contain the expected set of `columns`.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **columns** - The names of the columns that should exist in the node
      
      You may define the columns as a list of values i.e.
      
      .. code-block:: yaml
      
         has_expected_columns:
           - <str>
           - <str>
           - ...

   **column_data_types** - The column names and associated data types that should exist
      
      You may define the column data types as a map of values i.e.
      
      .. code-block:: yaml
      
         has_expected_columns: 
           <column_name>: <str>
           <column_name>: <str>
           ...


``has_freshness``
^^^^^^^^^^^^^^^^^

Check whether the given `source` has freshness configured.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_loader``
^^^^^^^^^^^^^^

Check whether the given `source` has a loader configured.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_properties``
^^^^^^^^^^^^^^^^^^

Check whether the given `resource` has properties set in an appropriate properties file.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_tests``
^^^^^^^^^^^^^

Check whether the given `node` has an appropriate number of tests.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   You may define the following keyword arguments: 
     - `min_count` (``int``) - The minimum number of tests allowed
     - `max_count` (``int``) - The maximum number of tests allowed


``meta_has_accepted_values``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the resource's `meta` config is configured as expected.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **accepted_values** - A map of keys to accepted values of those keys
      
      You may define the accepted values as a map of values i.e.
      
      .. code-block:: yaml
      
         meta_has_accepted_values: 
           <key>: [<Any>, ...] OR <Any>
           <key>: [<Any>, ...] OR <Any>
           ...


``meta_has_allowed_keys``
^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the resource's `meta` config contains only allowed keys.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **keys** - The keys that may be defined
      
      You may define the keys as a list of values i.e.
      
      .. code-block:: yaml
      
         meta_has_allowed_keys:
           - <str>
           - <str>
           - ...


``meta_has_required_keys``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the resource's `meta` config contains all required keys.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **keys** - The keys that must be defined
      
      You may define the keys as a list of values i.e.
      
      .. code-block:: yaml
      
         meta_has_required_keys:
           - <str>
           - <str>
           - ...


``tags_have_allowed_values``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `resource` has properties set in an appropriate properties file.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **tags** - The tags that may be defined
      
      You may define the tags as a list of values i.e.
      
      .. code-block:: yaml
      
         tags_have_allowed_values:
           - <str>
           - <str>
           - ...


``tags_have_required_values``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `resource` has properties set in an appropriate properties file.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **tags** - The tags that must be defined
      
      You may define the tags as a list of values i.e.
      
      .. code-block:: yaml
      
         tags_have_required_values:
           - <str>
           - <str>
           - ...


Columns configuration
=====================

Filters
-------

Filters for reducing the scope of the contract.
You may limit the number of columns processed by the rules of this contract by defining one or more of the following filters

``name``
^^^^^^^^

Check whether a given `item` has a valid name.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **patterns** - Patterns to match against for paths to include
      
      You may define the patterns as a list of values i.e.
      
      .. code-block:: yaml
      
         name:
           - <str>
           - <str>
           - ...


Validations
-----------

Validations to apply to the resources of this contract.
These enforce certain standards that must be followed in order for the contract to be fulfilled.

``has_data_type``
^^^^^^^^^^^^^^^^^

Check whether the given `column` of the given `parent` has a data type set.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_description``
^^^^^^^^^^^^^^^^^^^

Check whether the given `resource` has a description set.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_expected_name``
^^^^^^^^^^^^^^^^^^^^^

Check whether the given `column` of the given `parent` has a name that matches some expectation.
This expectation can be generic or specific to only columns of a certain data type.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **patterns** - A map of data types to regex patterns for which to validate names of columns which have the matching data type
      To define a generic contract which can apply to all unmatched data types, specify the data type key as an empty key. e.g. {"BOOLEAN": "(is|has|do)_.*", "TIMESTAMP": ".*_at", "": "name_.*", ...}
      
      You may define the patterns as a map of values i.e.
      
      .. code-block:: yaml
      
         has_expected_name: 
           <data_type>: [<str>, ...] OR <str>
           <data_type>: [<str>, ...] OR <str>
           ...

   You may define the following keyword arguments: 
     - `exact` (``bool``) - When True, type must match exactly to the map of keys given in the patterns map. Otherwise, match roughly on keys that start with the same value as the column's data type ignoring any whitespaces


``has_matching_data_type``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `column` of the given `parent`
has a data type configured which matches the remote resource.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   You may define the following keyword arguments: 
     - `exact` (``bool``) - When True, type must match exactly including cases


``has_matching_description``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `column` of the given `parent`
has a description configured which matches the remote resource.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   You may define the following keyword arguments: 
     - `case_sensitive` (``bool``) - When True, cases must match. When False, apply case-insensitive match


``has_matching_index``
^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `column` of the given `parent`
is in the same position in the dbt config as the remote resource.

.. note::
   This method does not need further configuration.    Simply define the method name in your configuration.

``has_tests``
^^^^^^^^^^^^^

Check whether the given `column` of the given `parent` has an appropriate number of tests.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   You may define the following keyword arguments: 
     - `min_count` (``int``) - The minimum number of tests allowed
     - `max_count` (``int``) - The maximum number of tests allowed


``meta_has_accepted_values``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the resource's `meta` config is configured as expected.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **accepted_values** - A map of keys to accepted values of those keys
      
      You may define the accepted values as a map of values i.e.
      
      .. code-block:: yaml
      
         meta_has_accepted_values: 
           <key>: [<Any>, ...] OR <Any>
           <key>: [<Any>, ...] OR <Any>
           ...


``meta_has_allowed_keys``
^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the resource's `meta` config contains only allowed keys.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **keys** - The keys that may be defined
      
      You may define the keys as a list of values i.e.
      
      .. code-block:: yaml
      
         meta_has_allowed_keys:
           - <str>
           - <str>
           - ...


``meta_has_required_keys``
^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the resource's `meta` config contains all required keys.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **keys** - The keys that must be defined
      
      You may define the keys as a list of values i.e.
      
      .. code-block:: yaml
      
         meta_has_required_keys:
           - <str>
           - <str>
           - ...


``tags_have_allowed_values``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `resource` has properties set in an appropriate properties file.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **tags** - The tags that may be defined
      
      You may define the tags as a list of values i.e.
      
      .. code-block:: yaml
      
         tags_have_allowed_values:
           - <str>
           - <str>
           - ...


``tags_have_required_values``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check whether the given `resource` has properties set in an appropriate properties file.

.. dropdown:: Arguments
   :animate: fade-in
   :color: primary
   
   **tags** - The tags that must be defined
      
      You may define the tags as a list of values i.e.
      
      .. code-block:: yaml
      
         tags_have_required_values:
           - <str>
           - <str>
           - ...

