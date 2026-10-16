.. _installation:

Installation
------------

Install through pip using one of the following commands:

.. code-block:: bash

   pip install dbt-contracts
   # or
   python -m pip install dbt-contracts

Supports ``dbt-core`` versions 1.5 and 1.7 or later.
``dbt-core`` 1.6 is not supported as it requires ``pydantic`` v1.
When using ``dbt-core`` 1.5 on Python 3.12 or later, also install ``setuptools``
as it provides the ``distutils`` module ``dbt-core`` 1.5 needs.

dbt-contracts is best utilised when used in conjunction with `pre-commit` hooks.
Follow the installation guide for `pre-commit <https://pre-commit.com/#installation>`_ to set this up if needed.
