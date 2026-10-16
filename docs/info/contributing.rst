============
Contributing
============
To contribute to dbt-contracts, follow these steps:


Setup your development environment
==================================
1. Ensure you have Python 3.12 or greater and `uv <https://docs.astral.sh/uv/getting-started/installation/>`_
   installed on your system.
2. Go to the `GitHub repository <https://github.com/geo-martino/dbt-contracts>`_ and fork the project by clicking
   **Fork** in the top left:

   .. image:: images/contributing/fork.png

3. Go to the page of your GitHub account's fork of dbt-contracts and
   `clone the repository <https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository>`_:

   .. image:: images/contributing/clone.png

4. In a terminal, navigate to the directory of the cloned project and install the package
   along with all of its development dependencies into a virtual environment:

   .. code-block:: bash

      make install

5. Optionally, to ensure inheritance diagrams in the documentation render correctly, install graphviz.
   See `here <https://graphviz.org/download/>`_ for platform-specific info on how to install graphviz.

Development commands
====================
The ``Makefile`` (or ``make.bat`` on Windows) provides the following commands for validating your changes.
Each runs through ``uv``, so there is no need to activate the virtual environment first.

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Command
     - Description
   * - ``make install``
     - Install the package with all dependency groups.
   * - ``make test``
     - Run the tests.
   * - ``make lint``
     - Check the code with ruff.
   * - ``make lint-fix``
     - Check the code with ruff and automatically fix what it can.
   * - ``make docs``
     - Build the HTML documentation to ``docs/_build/html``, failing on any warnings.
   * - ``make check``
     - Run ``lint``, ``test`` and ``docs`` i.e. everything CI validates. Run this before opening a pull request.
   * - ``make rebuild-html``
     - Clean and rebuild the HTML documentation from scratch, then check all of its links.

Run ``make help`` to list these commands along with the other Sphinx documentation targets.

Making changes and testing
==========================
Create a new branch in your local git project and commit changes to this branch as you develop.

All tests are located within ``./tests``.
If you make any changes to the functionality of the package, you **must** either modify or add new tests
for these changes.
To run all tests, or just the tests in a given file:

.. code-block:: bash

   make test
   # OR
   uv run pytest path/to/test/file.py

As part of code validation, your code will be required to pass certain linting checks.
This project uses ruff to help give an indication as to the quality of your code.
For the current checks that your code will be expected to pass,
please check the ``[tool.ruff]`` config in the ``pyproject.toml`` file in the root of the project.

To run these checks locally, and optionally fix what can be fixed automatically, run:

.. code-block:: bash

   make lint
   # OR
   make lint-fix

If you change the documentation or any docstrings, check the documentation still builds without warnings:

.. code-block:: bash

   make docs

Submitting your changes for review
==================================
1. Ensure everything CI validates passes locally first by running ``make check``.
2. Add any changes you made to the :ref:`release-history`.
3. Go to your forked repository and open a pull request for your branch.

   .. image:: images/contributing/pr_new.png

   .. image:: images/contributing/pr_create1.png

4. Add a title, a motivation for your changes, and the changes you have made.
   This can just be a copy-paste from your release history notes.

   .. image:: images/contributing/pr_create2.png

5. Wait for a review and make any necessary changes.
6. Have your changes committed 🎉
