# RPresto's implmentation of DBI and `dplyr` backends

There are two main layers of RPresto's code: a
[DBI backend](https://dbi.r-dbi.org/) and a
[`dplyr` remote database backend](https://dbplyr.tidyverse.org/articles/new-backend.html)
mainly using [`dbplyr`](https://dbplyr.tidyverse.org/).

## `DBI` backend

RPresto implements the latest DBI interface between R and Presto.

The required DBI classes are defined in the following files. Their specific DBI
`dbX` generics implementations are listed under each class.

* Initialization function: `Presto.R`
* `PrestoDriver`: `PrestoDriver.R`
  * The `dbUnloadDriver()` method is implemented in `dbUnloadDriver.R`.
  * The `dbDataType()` method is implemented in `dbDataType.R`.
* `PrestoConnection`: `PrestoConnection.R`
  * The `dbConnect()` method is implemented in `dbConnect.R`.
  * The `dbDisconnect()` method is implemented in `dbDisconnect.R`.
  * The `dbExistsTable()` method is implemented in `dbExistsTable.R`.
  * The `dbGetQuery()` method is implemented in `dbGetQuery.R`.
  * The `dbListTables()` method is implemented in `dbListTables.R`.
  * The `dbCreateTable()` method is implemented in `sqlCreateTable.R` and
    `dbCreateTable.R`.
  * The `dbCreateTableAs()` method is implemented in `sqlCreateTableAs.R` and
    `dbCreateTableAs.R`.
  * The `dbWriteTable()` method is implemented in `dbWriteTable.R`.
* `PrestoResult`: `PrestoResult.R`
  * The `dbSendQuery()` method is implemented in `dbSendQuery.R`.
  * The `dbClearResult()` method is implemented in `dbClearResult.R`.
  * The `dbFetch()` method is implemented in `dbFetch.R`.
  * The `dbHasCompleted()` method is implemented in `dbHasCompleted.R`.
  * The `dbIsValid()` method is implemented in `dbIsValud.R`.
  * The `dbGetStatement()` method is implemented in `dbGetStatement.R`.
  * The `dbGetRowCount()` method is implemented in `dbGetRowCount.R`.
* Methods available for multiple classes
  * The `show()` method is defined for all classes in their class definition
    files.
  * The `dbGetInfo()` method is defined for all classes in `dbGetInfo.R`.
  * The `dbListFields()` method is defined for `PrestoConnection` and
    `PrestoResult` classes in `dbListFields.R`.

Besides, we also define two more classes for their side effects. They are
defined using reference class (i.e. R5).

* Query: `PrestoQuery.R`. It's used in the `PrestoResult` class.
* Session: `PrestoSession.R`. It's used in the `PrestoConnection` class.

## `dplyr` remote database backend

RPresto implements a number of `dplyr`'s `db_` generics that execute actions on
the underlying database.
* The `dplyr::db_desc()` method is implemented in `db_desc.PrestoConnection.R`.
* The `dplyr::db_data_type()` method is implemented in
  `db.data.type.PrestoConnection.R`.
* The `dplyr::db_explain()` method is implemented in
  `db.explain.PrestoConnection.R`.
* The `dplyr::db_query_rows()` method is explicitly not implemented in
  `db.query.rows.PrestoConnection.R`.

`dplyr` has a concept called "remote database source" that's basically a wrapper
around a DBI database connection object (i.e. `PrestoConnection`).
* The `src_presto` object is defined in `src.presto.R`.
* The `dplyr::copy_to()` method is explicitly not implemented in
`copy.to.src.presto.R`.
* The `dplyr::tbl()` method is implemented in `tbl.src.presto.R`.
* The `dplyr::collect()` method is implemented in `src.presto.R`.

The `dplyr` database backend also relies on implementation of a few `dbplyr`
methods.
* The `dbplyr::dbplyr_edition()` method is implemented in
  `dbplyr.edition.PrestoConnection.R`.
* The `dbplyr::db_collect()` method is implemented in `db_collect.R`.
* The `dbplyr::sql_escape_date()` method is implemented in
  `sql_escape_date.R`.
* The `dbplyr::sql_escape_datetime()` method is implemented in
  `sql_escape_datetime.R`.
* The `dbplyr::sql_translation()` method is implemented in
  `sql_translation.R`.
