# Release Notes

## v0.2.0

* Support for Spark/Databricks.
* Change the translation API.  Instead of generating SQL that updates the
  `cohort` table, translate a cohort definition into a FunSQL query that
  returns the cohort as a query output.
* Require FunSQL >= 0.14.


## v0.1.5

* Fixed non-deterministic primary limit.
* Require FunSQL >= 0.11.


## v0.1.4

* Ignore empty criteria group (fixes #3).


## v0.1.3

* Drop temporary tables before creating them.
* Require FunSQL >= 0.9.
* Fixed `UndefKeywordError: keyword argument args not assigned`.


## v0.1.2

* Compatibitity with FunSQL 0.9 and PrettyPrinting 0.4.


## v0.1.1

* Upgraded to FunSQL 0.8.


## v0.1.0

- proof-of-concept implementation
- coverage of 0.1 of PhenotypeLibrary 
- support for Microsoft SQL Server
- support for Amazon Redshift
- support for PostgreSQL
