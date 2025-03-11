=========================
Welcome to dbt-contracts!
=========================

Enforce standards for your dbt projects through automated checks and generators
-------------------------------------------------------------------------------

* Validate that the metadata and properties of objects in your project match required standards
* Automatically generate properties in your project from their related database objects
* Apply complex filtering and validation rules setting for highly granular operations
* Execute these operations as `pre-commit` hooks for automatic project validation


What's in this documentation
----------------------------

* Guides on getting started with dbt-contracts and other key functionality of the package
* Release history
* How to get started with contributing to dbt-contracts
* Reference documentation

.. include:: guides/install.rst
   :start-after: :

.. toctree::
   :maxdepth: 1
   :caption: 📜 Guides & Getting Started

   guides/install
   guides/quickstart
   guides/commands
   guides/precommit


.. toctree::
   :maxdepth: 1
   :caption: 📖 Contracts Reference

   reference/contracts/models
   reference/contracts/sources
   reference/contracts/columns
   reference/contracts/macros
   reference/contracts/arguments


.. toctree::
   :maxdepth: 1
   :caption: 🛠️ Project Info

   info/release-history
   info/contributing
   info/licence
