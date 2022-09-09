# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' dbplyr database methods
#' 
#' @rdname dbplyr-db
#' @param con A `PrestoConnection` as returned by `dbConnect()`.
#' @importFrom dplyr db_list_tables
#' @export
#' @md
db_list_tables.PrestoConnection <- function(con) {
  DBI::dbListTables(con)
}

#' @rdname dbplyr-db
#' @param table Table name
#' @importFrom dplyr db_has_table
#' @export
db_has_table.PrestoConnection <- function(con, table) {
  DBI::dbExistsTable(con, table)
}

#' @rdname dbplyr-db
#' @param types Not used. Only NULL is accpeted.
#' @param values A `data.frame`.
#' @param temporary If a temporary table should be used. Not supported. Only
#'   FALSE is accepted.
#' @param overwrite If an existing table should be overwritten.
#' @param with An optional WITH clause for the CREATE TABLE statement.
#' @importFrom dplyr db_write_table
#' @export
db_write_table.PrestoConnection  <- function(
  con, table, types, values, temporary = FALSE, overwrite = FALSE,
  ..., with = NULL
) {
  dbWriteTable(
    conn = con,
    name = table,
    value = values,
    field.types = types,
    temporary = temporary,
    overwrite = overwrite,
    with = with,
    ...
  )
  table
}

#' @rdname dbplyr-db
#' @param unique_indexes,indexes,analyze,in_transaction Ignored. Included
#'   for compatibility with generics.
#' @param ... Extra arguments to be passed to individual methods.
#' @importFrom dbplyr db_copy_to
#' @export
db_copy_to.PrestoConnection  <- function(
  con, table, values, overwrite = FALSE, types = NULL, temporary = TRUE,
  unique_indexes = NULL, indexes = NULL, analyze = TRUE, ...,
  in_transaction = TRUE, with = NULL
) {
  table <- dplyr::db_write_table(
    con, table, types = types, values = values,
    temporary = temporary, overwrite = overwrite, with = with,
    ...
  )
  table
}

#' @rdname dbplyr-db
#' @param sql A SQL statement.
#' @importFrom dbplyr db_compute
#' @export
db_compute.PrestoConnection  <- function(
  con, table, sql, temporary = TRUE, unique_indexes = list(), indexes = list(),
  analyze = TRUE, with = NULL,
  ...
) {
  if (!identical(temporary, FALSE)) {
    stop(
      'Temporary table is not supported. ',
      'Use temporary = FALSE to create a permanent table.',
      call. = FALSE)
  }
  table <- dplyr::db_save_query(
    con, sql, table, temporary = temporary, with = with, ...
  )
  table
}
