Macros
======

Configuration
-------------

Filters
^^^^^^^

Filters for reducing the scope of the contract.
You may limit the number of macros processed by the rules of this contract by defining one or more of the following filters

``name``
""""""""

Check whether a given `item` has a valid name.

**Arguments**

You may define the patterns as a list of values i.e.

.. code-block:: yaml
   name:
     - <``str``>
     - <``str``>
     - ...


``paths``
"""""""""

Check whether a given `item` has a valid path.
Paths must match patterns which are relative to directory of the dbt project.

**Arguments**

You may define the patterns as a list of values i.e.

.. code-block:: yaml
   paths:
     - <``str``>
     - <``str``>
     - ...


Validations
^^^^^^^^^^^

Validations to apply to the resources of this contract.
These enforce certain standards that must be followed in order for the contract to be fulfilled.

``has_description``
"""""""""""""""""""

Check whether the given `resource` has a description set.

This method does not need further configuration. Simply define the method name in your configuration.

``has_properties``
""""""""""""""""""

Check whether the given `resource` has properties set in an appropriate properties file.

This method does not need further configuration. Simply define the method name in your configuration.

Arguments configuration
-----------------------

Filters
^^^^^^^

Filters for reducing the scope of the contract.
You may limit the number of arguments processed by the rules of this contract by defining one or more of the following filters

``name``
""""""""

Check whether a given `item` has a valid name.

**Arguments**

You may define the patterns as a list of values i.e.

.. code-block:: yaml
   name:
     - <``str``>
     - <``str``>
     - ...


Validations
^^^^^^^^^^^

Validations to apply to the resources of this contract.
These enforce certain standards that must be followed in order for the contract to be fulfilled.

``has_description``
"""""""""""""""""""

Check whether the given `resource` has a description set.

This method does not need further configuration. Simply define the method name in your configuration.

``has_type``
""""""""""""

Check whether the given `argument` has its type set in an appropriate properties file.

This method does not need further configuration. Simply define the method name in your configuration.
