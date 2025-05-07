.. Add log for your proposed changes here.

   The versions shall be listed in descending order with the latest release first.

   Change categories:
      Added          - for new features.
      Changed        - for changes in existing functionality.
      Deprecated     - for soon-to-be removed features.
      Removed        - for now removed features.
      Fixed          - for any bug fixes.
      Security       - in case of vulnerabilities.
      Documentation  - for changes that only affected documentation and no functionality.

   Your additions should keep the same structure as observed throughout the file i.e.

      <release version>
      =================

      <one of the above change categories>
      ------------------------------------
      * <your 1st change>
      * <your 2nd change>
      ...

.. _release-history:

===============
Release History
===============

The format is based on `Keep a Changelog <https://keepachangelog.com/en>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_


1.0.9
=====

Fixed
-----
* Path separators in Windows paths were being intepreted as special characters when path filtering.
  All backslashes in paths are now double escaped to prevent issues with regex matching.


1.0.8
=====

Fixed
-----
* :py:class:`.PathCondition` now excludes paths correctly


1.0.7
=====

Changed
-------
* Properties files now outputted with extra indentation for arrays


1.0.6
=====

Fixed
-----
* When selecting only a child contract with ``dbt-validate``,
  paths were not being set on parent contracts leading to overprocessing. Paths are now set correctly.


1.0.5
=====

Fixed
-----
* Paths were not assigned to runner when passed via CLI. Now passed as expected.


1.0.4
=====

Fixed
-----
* :py:class:`.HasAllColumns` was not logging the correct extra columns. This is now fixed.


1.0.3
=====

Changed
-------
* Drop requirement for pre-release pydantic version


1.0.2
=====

Fixed
-----
* CLI functions now set logging to INFO by default. Results can now be viewed in terminal output.


1.0.1
=====

Fixed
-----
* Fixed the hooks IDs and added generate hook


1.0.0
=====

Stable release! 🎉

* Complete redesign to improve modularity and performance
* Add generators for properties generation from database objects
